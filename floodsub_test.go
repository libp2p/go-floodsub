package floodsub

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	host "github.com/libp2p/go-libp2p-host"
	netutil "github.com/libp2p/go-libp2p-netutil"
	peer "github.com/libp2p/go-libp2p-peer"
	//bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	bhost "github.com/libp2p/go-libp2p-blankhost"
)

func checkMessageRouting(t *testing.T, topic string, pubs []*PubSub, subs []*Subscription) {
	data := make([]byte, 16)
	rand.Read(data)

	for _, p := range pubs {
		err := p.Publish(topic, data)
		if err != nil {
			t.Fatal(err)
		}

		for _, s := range subs {
			assertReceive(t, s, data)
		}
	}
}

func getNetHosts(t *testing.T, ctx context.Context, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		netw := netutil.GenSwarmNetwork(t, ctx)
		h := bhost.NewBlankHost(netw)
		out = append(out, h)
	}

	return out
}

func connect(t *testing.T, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

func sparseConnect(t *testing.T, hosts []host.Host) {
	for i, a := range hosts {
		for j := 0; j < 3; j++ {
			n := rand.Intn(len(hosts))
			if n == i {
				j--
				continue
			}

			b := hosts[n]

			connect(t, a, b)
		}
	}
}

func connectAll(t *testing.T, hosts []host.Host) {
	for i, a := range hosts {
		for j, b := range hosts {
			if i == j {
				continue
			}

			connect(t, a, b)
		}
	}
}

func getPubsubs(ctx context.Context, hs []host.Host) []*PubSub {
	var psubs []*PubSub
	for _, h := range hs {
		psubs = append(psubs, NewFloodSub(ctx, h))
	}
	return psubs
}

func assertReceive(t *testing.T, ch *Subscription, exp []byte) {
	select {
	case msg := <-ch.ch:
		if !bytes.Equal(msg.GetData(), exp) {
			t.Fatalf("got wrong message, expected %s but got %s", string(exp), string(msg.GetData()))
		}
	case <-time.After(time.Second * 5):
		t.Logf("%#v\n", ch)
		t.Fatal("timed out waiting for message of: ", string(exp))
	}
}

func TestBasicFloodsub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getPubsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	//connectAll(t, hosts)
	sparseConnect(t, hosts)

	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish("foobar", msg)

		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}
	}

}

func TestMultihops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 6)

	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[2], hosts[3])
	connect(t, hosts[3], hosts[4])
	connect(t, hosts[4], hosts[5])

	var subs []*Subscription
	for i := 1; i < 6; i++ {
		ch, err := psubs[i].Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, ch)
	}

	time.Sleep(time.Millisecond * 100)

	msg := []byte("i like cats")
	err := psubs[0].Publish("foobar", msg)
	if err != nil {
		t.Fatal(err)
	}

	// last node in the chain should get the message
	select {
	case out := <-subs[4].ch:
		if !bytes.Equal(out.GetData(), msg) {
			t.Fatal("got wrong data")
		}
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for message")
	}
}

func TestReconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)

	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[0], hosts[2])

	A, err := psubs[1].Subscribe("cats")
	if err != nil {
		t.Fatal(err)
	}

	B, err := psubs[2].Subscribe("cats")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	msg := []byte("apples and oranges")
	err = psubs[0].Publish("cats", msg)
	if err != nil {
		t.Fatal(err)
	}

	assertReceive(t, A, msg)
	assertReceive(t, B, msg)

	B.Cancel()

	time.Sleep(time.Millisecond * 50)

	msg2 := []byte("potato")
	err = psubs[0].Publish("cats", msg2)
	if err != nil {
		t.Fatal(err)
	}

	assertReceive(t, A, msg2)
	select {
	case _, ok := <-B.ch:
		if ok {
			t.Fatal("shouldnt have gotten data on this channel")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for B chan to be closed")
	}

	nSubs := len(psubs[2].myTopics["cats"])
	if nSubs > 0 {
		t.Fatal(`B should have 0 subscribers for channel "cats", has`, nSubs)
	}

	ch2, err := psubs[2].Subscribe("cats")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	nextmsg := []byte("ifps is kul")
	err = psubs[0].Publish("cats", nextmsg)
	if err != nil {
		t.Fatal(err)
	}

	assertReceive(t, ch2, nextmsg)
}

// make sure messages arent routed between nodes who arent subscribed
func TestNoConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)

	psubs := getPubsubs(ctx, hosts)

	ch, err := psubs[5].Subscribe("foobar")
	if err != nil {
		t.Fatal(err)
	}

	err = psubs[0].Publish("foobar", []byte("TESTING"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch.ch:
		t.Fatal("shouldnt have gotten a message")
	case <-time.After(time.Millisecond * 200):
	}
}

func TestSelfReceive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host := getNetHosts(t, ctx, 1)[0]

	psub := NewFloodSub(ctx, host)

	msg := []byte("hello world")

	err := psub.Publish("foobar", msg)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)

	ch, err := psub.Subscribe("foobar")
	if err != nil {
		t.Fatal(err)
	}

	msg2 := []byte("goodbye world")
	err = psub.Publish("foobar", msg2)
	if err != nil {
		t.Fatal(err)
	}

	assertReceive(t, ch, msg2)
}

func TestOneToOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])

	sub, err := psubs[1].Subscribe("foobar")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 50)

	checkMessageRouting(t, "foobar", psubs, []*Subscription{sub})
}

func TestValidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	topic := "foobar"

	sub, err := psubs[1].Subscribe(topic, WithValidator(func(ctx context.Context, msg *Message) bool {
		return !bytes.Contains(msg.Data, []byte("illegal"))
	}))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 50)

	msgs := []struct {
		msg       []byte
		validates bool
	}{
		{msg: []byte("this is a legal message"), validates: true},
		{msg: []byte("there also is nothing controversial about this message"), validates: true},
		{msg: []byte("openly illegal content will be censored"), validates: false},
		{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
	}

	for _, tc := range msgs {
		for _, p := range psubs {
			err := p.Publish(topic, tc.msg)
			if err != nil {
				t.Fatal(err)
			}

			select {
			case msg := <-sub.ch:
				if !tc.validates {
					t.Log(msg)
					t.Error("expected message validation to filter out the message")
				}
			case <-time.After(333 * time.Millisecond):
				if tc.validates {
					t.Error("expected message validation to accept the message")
				}
			}
		}
	}
}

func TestValidateTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	topic := "foobar"

	cases := []struct {
		timeout   time.Duration
		msg       []byte
		validates bool
	}{
		{75 * time.Millisecond, []byte("this better time out"), false},
		{150 * time.Millisecond, []byte("this should work"), true},
	}

	for _, tc := range cases {
		sub, err := psubs[1].Subscribe(topic, WithValidator(func(ctx context.Context, msg *Message) bool {
			time.Sleep(100 * time.Millisecond)
			return true
		}), WithValidatorTimeout(tc.timeout))
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Millisecond * 50)

		p := psubs[0]
		err = p.Publish(topic, tc.msg)
		if err != nil {
			t.Fatal(err)
		}

		select {
		case msg := <-sub.ch:
			if !tc.validates {
				t.Log(msg)
				t.Error("expected message validation to filter out the message")
			}
		case <-time.After(333 * time.Millisecond):
			if tc.validates {
				t.Error("expected message validation to accept the message")
			}
		}

		// important: cancel!
		// otherwise the message will still be filtered by the other subscription
		sub.Cancel()
	}

}

func TestValidateCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	topic := "foobar"

	sub, err := psubs[1].Subscribe(topic, WithValidator(func(ctx context.Context, msg *Message) bool {
		<-ctx.Done()
		return true
	}))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 50)

	testmsg := []byte("this is a legal message")
	validates := false // message for which the validator times our are discarded

	p := psubs[0]

	err = p.Publish(topic, testmsg)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-sub.ch:
		if !validates {
			t.Log(msg)
			t.Error("expected message validation to filter out the message")
		}
	case <-time.After(333 * time.Millisecond):
		if validates {
			t.Error("expected message validation to accept the message")
		}
	}
}

func TestValidateOverload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	topic := "foobar"

	block := make(chan struct{})

	sub, err := psubs[1].Subscribe(topic, WithValidator(func(ctx context.Context, msg *Message) bool {
		<-block
		return true
	}))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 50)

	msgs := []struct {
		msg       []byte
		validates bool
	}{
		{msg: []byte("this is a legal message"), validates: true},
		{msg: []byte("but subversive actors will use leetspeek to spread 1ll3g4l content"), validates: true},
		{msg: []byte("there also is nothing controversial about this message"), validates: true},
		{msg: []byte("also fine"), validates: true},
		{msg: []byte("still, all good"), validates: true},
		{msg: []byte("this is getting boring"), validates: true},
		{msg: []byte("foo"), validates: true},
		{msg: []byte("foobar"), validates: true},
		{msg: []byte("foofoo"), validates: true},
		{msg: []byte("barfoo"), validates: true},
		{msg: []byte("barbar"), validates: false},
	}

	if len(msgs) != maxConcurrency+1 {
		t.Fatalf("expected number of messages sent to be maxConcurrency+1. Got %d, expected %d", len(msgs), maxConcurrency+1)
	}

	p := psubs[0]

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for _, tc := range msgs {
			select {
			case msg := <-sub.ch:
				if !tc.validates {
					t.Log(msg)
					t.Error("expected message validation to drop the message because all validator goroutines are taken")
				}
			case <-time.After(333 * time.Millisecond):
				if tc.validates {
					t.Error("expected message validation to accept the message")
				}
			}
		}
		wg.Done()
	}()

	for i, tc := range msgs {
		err := p.Publish(topic, tc.msg)
		if err != nil {
			t.Fatal(err)
		}

		// wait a bit to let pubsub's internal state machine start validating the message
		time.Sleep(10 * time.Millisecond)

		// unblock validator goroutines after we sent one too many
		if i == len(msgs)-1 {
			close(block)
		}
	}

	wg.Wait()
}

func assertPeerLists(t *testing.T, hosts []host.Host, ps *PubSub, has ...int) {
	peers := ps.ListPeers("")
	set := make(map[peer.ID]struct{})
	for _, p := range peers {
		set[p] = struct{}{}
	}

	for _, h := range has {
		if _, ok := set[hosts[h].ID()]; !ok {
			t.Fatal("expected to have connection to peer: ", h)
		}
	}
}

func TestTreeTopology(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[1], hosts[4])
	connect(t, hosts[2], hosts[3])
	connect(t, hosts[0], hosts[5])
	connect(t, hosts[5], hosts[6])
	connect(t, hosts[5], hosts[8])
	connect(t, hosts[6], hosts[7])
	connect(t, hosts[8], hosts[9])

	/*
		[0] -> [1] -> [2] -> [3]
		 |      L->[4]
		 v
		[5] -> [6] -> [7]
		 |
		 v
		[8] -> [9]
	*/

	var chs []*Subscription
	for _, ps := range psubs {
		ch, err := ps.Subscribe("fizzbuzz")
		if err != nil {
			t.Fatal(err)
		}

		chs = append(chs, ch)
	}

	time.Sleep(time.Millisecond * 50)

	assertPeerLists(t, hosts, psubs[0], 1, 5)
	assertPeerLists(t, hosts, psubs[1], 0, 2, 4)
	assertPeerLists(t, hosts, psubs[2], 1, 3)

	checkMessageRouting(t, "fizzbuzz", []*PubSub{psubs[9], psubs[3]}, chs)
}

func assertHasTopics(t *testing.T, ps *PubSub, exptopics ...string) {
	topics := ps.GetTopics()
	sort.Strings(topics)
	sort.Strings(exptopics)

	if len(topics) != len(exptopics) {
		t.Fatalf("expected to have %v, but got %v", exptopics, topics)
	}

	for i, v := range exptopics {
		if topics[i] != v {
			t.Fatalf("expected %s but have %s", v, topics[i])
		}
	}
}

func TestSubReporting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host := getNetHosts(t, ctx, 1)[0]
	psub := NewFloodSub(ctx, host)

	fooSub, err := psub.Subscribe("foo")
	if err != nil {
		t.Fatal(err)
	}

	barSub, err := psub.Subscribe("bar")
	if err != nil {
		t.Fatal(err)
	}

	assertHasTopics(t, psub, "foo", "bar")

	_, err = psub.Subscribe("baz")
	if err != nil {
		t.Fatal(err)
	}

	assertHasTopics(t, psub, "foo", "bar", "baz")

	barSub.Cancel()
	assertHasTopics(t, psub, "foo", "baz")
	fooSub.Cancel()
	assertHasTopics(t, psub, "baz")

	_, err = psub.Subscribe("fish")
	if err != nil {
		t.Fatal(err)
	}

	assertHasTopics(t, psub, "baz", "fish")
}

func TestPeerTopicReporting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 4)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[0], hosts[2])
	connect(t, hosts[0], hosts[3])

	_, err := psubs[1].Subscribe("foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = psubs[1].Subscribe("bar")
	if err != nil {
		t.Fatal(err)
	}
	_, err = psubs[1].Subscribe("baz")
	if err != nil {
		t.Fatal(err)
	}

	_, err = psubs[2].Subscribe("foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = psubs[2].Subscribe("ipfs")
	if err != nil {
		t.Fatal(err)
	}

	_, err = psubs[3].Subscribe("baz")
	if err != nil {
		t.Fatal(err)
	}
	_, err = psubs[3].Subscribe("ipfs")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)

	peers := psubs[0].ListPeers("ipfs")
	assertPeerList(t, peers, hosts[2].ID(), hosts[3].ID())

	peers = psubs[0].ListPeers("foo")
	assertPeerList(t, peers, hosts[1].ID(), hosts[2].ID())

	peers = psubs[0].ListPeers("baz")
	assertPeerList(t, peers, hosts[1].ID(), hosts[3].ID())

	peers = psubs[0].ListPeers("bar")
	assertPeerList(t, peers, hosts[1].ID())
}

func TestSubscribeMultipleTimes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])

	sub1, err := psubs[0].Subscribe("foo")
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := psubs[0].Subscribe("foo")
	if err != nil {
		t.Fatal(err)
	}

	// make sure subscribing is finished by the time we publish
	time.Sleep(1 * time.Millisecond)

	psubs[1].Publish("foo", []byte("bar"))

	msg, err := sub1.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v.", err)
	}

	data := string(msg.GetData())

	if data != "bar" {
		t.Fatalf("data is %s, expected %s.", data, "bar")
	}

	msg, err = sub2.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v.", err)
	}
	data = string(msg.GetData())

	if data != "bar" {
		t.Fatalf("data is %s, expected %s.", data, "bar")
	}
}

func TestPeerDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])

	_, err := psubs[0].Subscribe("foo")
	if err != nil {
		t.Fatal(err)
	}

	_, err = psubs[1].Subscribe("foo")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)

	peers := psubs[0].ListPeers("foo")
	assertPeerList(t, peers, hosts[1].ID())
	for _, c := range hosts[1].Network().ConnsToPeer(hosts[0].ID()) {
		streams, err := c.GetStreams()
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range streams {
			s.Close()
		}
	}

	time.Sleep(time.Millisecond * 10)

	peers = psubs[0].ListPeers("foo")
	assertPeerList(t, peers)
}

func assertPeerList(t *testing.T, peers []peer.ID, expected ...peer.ID) {
	sort.Sort(peer.IDSlice(peers))
	sort.Sort(peer.IDSlice(expected))

	if len(peers) != len(expected) {
		t.Fatalf("mismatch: %s != %s", peers, expected)
	}

	for i, p := range peers {
		if expected[i] != p {
			t.Fatalf("mismatch: %s != %s", peers, expected)
		}
	}
}
