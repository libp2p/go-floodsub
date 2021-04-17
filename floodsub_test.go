package pubsub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	bhost "github.com/libp2p/go-libp2p-blankhost"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"

	"github.com/libp2p/go-msgio/protoio"
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
		netw := swarmt.GenSwarm(t, ctx)
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
	connectSome(t, hosts, 3)
}

func denseConnect(t *testing.T, hosts []host.Host) {
	connectSome(t, hosts, 10)
}

func connectSome(t *testing.T, hosts []host.Host, d int) {
	for i, a := range hosts {
		for j := 0; j < d; j++ {
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

func getPubsub(ctx context.Context, h host.Host, opts ...Option) *PubSub {
	ps, err := NewFloodSub(ctx, h, opts...)
	if err != nil {
		panic(err)
	}
	return ps
}

func getPubsubs(ctx context.Context, hs []host.Host, opts ...Option) []*PubSub {
	var psubs []*PubSub
	for _, h := range hs {
		psubs = append(psubs, getPubsub(ctx, h, opts...))
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

	nSubs := len(psubs[2].mySubs["cats"])
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

	psub, err := NewFloodSub(ctx, host)
	if err != nil {
		t.Fatal(err)
	}

	msg := []byte("hello world")

	err = psub.Publish("foobar", msg)
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

func TestFloodSubPluggableProtocol(t *testing.T) {
	t.Run("multi-procol router acts like a hub", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getNetHosts(t, ctx, 3)

		psubA := mustCreatePubSub(ctx, t, hosts[0], "/esh/floodsub", "/lsr/floodsub")
		psubB := mustCreatePubSub(ctx, t, hosts[1], "/esh/floodsub")
		psubC := mustCreatePubSub(ctx, t, hosts[2], "/lsr/floodsub")

		subA := mustSubscribe(t, psubA, "foobar")
		defer subA.Cancel()

		subB := mustSubscribe(t, psubB, "foobar")
		defer subB.Cancel()

		subC := mustSubscribe(t, psubC, "foobar")
		defer subC.Cancel()

		// B --> A, C --> A
		connect(t, hosts[1], hosts[0])
		connect(t, hosts[2], hosts[0])

		time.Sleep(time.Millisecond * 100)

		psubC.Publish("foobar", []byte("bar"))

		assertReceive(t, subA, []byte("bar"))
		assertReceive(t, subB, []byte("bar"))
		assertReceive(t, subC, []byte("bar"))
	})

	t.Run("won't talk to routers with no protocol overlap", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getNetHosts(t, ctx, 2)

		psubA := mustCreatePubSub(ctx, t, hosts[0], "/esh/floodsub")
		psubB := mustCreatePubSub(ctx, t, hosts[1], "/lsr/floodsub")

		subA := mustSubscribe(t, psubA, "foobar")
		defer subA.Cancel()

		subB := mustSubscribe(t, psubB, "foobar")
		defer subB.Cancel()

		connect(t, hosts[1], hosts[0])

		time.Sleep(time.Millisecond * 100)

		psubA.Publish("foobar", []byte("bar"))

		assertReceive(t, subA, []byte("bar"))

		pass := false
		select {
		case <-subB.ch:
			t.Fatal("different protocols: should not have received message")
		case <-time.After(time.Second * 1):
			pass = true
		}

		if !pass {
			t.Fatal("should have timed out waiting for message")
		}
	})
}

func mustCreatePubSub(ctx context.Context, t *testing.T, h host.Host, ps ...protocol.ID) *PubSub {
	psub, err := NewFloodsubWithProtocols(ctx, h, ps)
	if err != nil {
		t.Fatal(err)
	}

	return psub
}

func mustSubscribe(t *testing.T, ps *PubSub, topic string) *Subscription {
	sub, err := ps.Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	return sub
}

func TestSubReporting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host := getNetHosts(t, ctx, 1)[0]
	psub, err := NewFloodSub(ctx, host)
	if err != nil {
		t.Fatal(err)
	}

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

	time.Sleep(time.Millisecond * 200)

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
	time.Sleep(10 * time.Millisecond)

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

	time.Sleep(time.Millisecond * 300)

	peers := psubs[0].ListPeers("foo")
	assertPeerList(t, peers, hosts[1].ID())
	for _, c := range hosts[1].Network().ConnsToPeer(hosts[0].ID()) {
		c.Close()
	}

	time.Sleep(time.Millisecond * 300)

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

func TestWithNoSigning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts, WithNoAuthor(), WithMessageIdFn(func(pmsg *pb.Message) string {
		// silly content-based test message-ID: just use the data as whole
		return base64.URLEncoding.EncodeToString(pmsg.Data)
	}))

	connect(t, hosts[0], hosts[1])

	topic := "foobar"
	data := []byte("this is a message")

	sub, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)

	err = psubs[0].Publish(topic, data)
	if err != nil {
		t.Fatal(err)
	}

	msg, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Signature != nil {
		t.Fatal("signature in message")
	}
	if msg.From != nil {
		t.Fatal("from in message")
	}
	if msg.Seqno != nil {
		t.Fatal("seqno in message")
	}
	if string(msg.Data) != string(data) {
		t.Fatalf("unexpected data: %s", string(msg.Data))
	}
}

func TestWithSigning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts, WithStrictSignatureVerification(true))

	connect(t, hosts[0], hosts[1])

	topic := "foobar"
	data := []byte("this is a message")

	sub, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)

	err = psubs[0].Publish(topic, data)
	if err != nil {
		t.Fatal(err)
	}

	msg, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Signature == nil {
		t.Fatal("no signature in message")
	}
	if msg.From == nil {
		t.Fatal("from not in message")
	}
	if msg.Seqno == nil {
		t.Fatal("seqno not in message")
	}
	if string(msg.Data) != string(data) {
		t.Fatalf("unexpected data: %s", string(msg.Data))
	}
}

func TestImproperlySignedMessageRejected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	adversary := hosts[0]
	honestPeer := hosts[1]

	// The adversary enables signing, but disables verification to let through
	// an incorrectly signed message.
	adversaryPubSub := getPubsub(
		ctx,
		adversary,
		WithMessageSigning(true),
		WithStrictSignatureVerification(false),
	)
	honestPubSub := getPubsub(
		ctx,
		honestPeer,
		WithStrictSignatureVerification(true),
	)

	connect(t, adversary, honestPeer)

	var (
		topic            = "foobar"
		correctMessage   = []byte("this is a correct message")
		incorrectMessage = []byte("this is the incorrect message")
	)

	adversarySubscription, err := adversaryPubSub.Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}
	honestPeerSubscription, err := honestPubSub.Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 50)

	// First the adversary sends the correct message.
	err = adversaryPubSub.Publish(topic, correctMessage)
	if err != nil {
		t.Fatal(err)
	}

	// Change the sign key for the adversarial peer, and send the second,
	// incorrectly signed, message.
	adversaryPubSub.signID = honestPubSub.signID
	adversaryPubSub.signKey = honestPubSub.host.Peerstore().PrivKey(honestPubSub.signID)
	err = adversaryPubSub.Publish(topic, incorrectMessage)
	if err != nil {
		t.Fatal(err)
	}

	var adversaryMessages []*Message
	adversaryContext, adversaryCancel := context.WithCancel(ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := adversarySubscription.Next(ctx)
				if err != nil {
					return
				}
				adversaryMessages = append(adversaryMessages, msg)
			}
		}
	}(adversaryContext)

	<-time.After(1 * time.Second)
	adversaryCancel()

	// Ensure the adversary successfully publishes the incorrectly signed
	// message. If the adversary "sees" this, we successfully got through
	// their local validation.
	if len(adversaryMessages) != 2 {
		t.Fatalf("got %d messages, expected 2", len(adversaryMessages))
	}

	// the honest peer's validation process will drop the message;
	// next will never furnish the incorrect message.
	var honestPeerMessages []*Message
	honestPeerContext, honestPeerCancel := context.WithCancel(ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := honestPeerSubscription.Next(ctx)
				if err != nil {
					return
				}
				honestPeerMessages = append(honestPeerMessages, msg)
			}
		}
	}(honestPeerContext)

	<-time.After(1 * time.Second)
	honestPeerCancel()

	if len(honestPeerMessages) != 1 {
		t.Fatalf("got %d messages, expected 1", len(honestPeerMessages))
	}
	if string(honestPeerMessages[0].GetData()) != string(correctMessage) {
		t.Fatalf(
			"got %s, expected message %s",
			honestPeerMessages[0].GetData(),
			correctMessage,
		)
	}
}

func TestMessageSender(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "foobar"

	hosts := getNetHosts(t, ctx, 3)
	psubs := getPubsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])

	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 3; i++ {
		for j := 0; j < 100; j++ {
			msg := []byte(fmt.Sprintf("%d sent %d", i, j))

			psubs[i].Publish(topic, msg)

			for k, sub := range msgs {
				got, err := sub.Next(ctx)
				if err != nil {
					t.Fatal(sub.err)
				}
				if !bytes.Equal(msg, got.Data) {
					t.Fatal("got wrong message!")
				}

				var expectedHost int
				if i == k {
					expectedHost = i
				} else if k != 1 {
					expectedHost = 1
				} else {
					expectedHost = i
				}

				if got.ReceivedFrom != hosts[expectedHost].ID() {
					t.Fatal("got wrong message sender")
				}
			}
		}
	}
}

func TestConfigurableMaxMessageSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)

	// use a 4mb limit; default is 1mb; we'll test with a 2mb payload.
	psubs := getPubsubs(ctx, hosts, WithMaxMessageSize(1<<22))

	sparseConnect(t, hosts)
	time.Sleep(time.Millisecond * 100)

	const topic = "foobar"
	var subs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, subch)
	}

	// 2mb payload.
	msg := make([]byte, 1<<21)
	rand.Read(msg)
	err := psubs[0].Publish(topic, msg)
	if err != nil {
		t.Fatal(err)
	}

	// make sure that all peers received the message.
	for _, sub := range subs {
		got, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(sub.err)
		}
		if !bytes.Equal(msg, got.Data) {
			t.Fatal("got wrong message!")
		}
	}

}

func TestAnnounceRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	ps := getPubsub(ctx, hosts[0])
	watcher := &announceWatcher{}
	hosts[1].SetStreamHandler(FloodSubID, watcher.handleStream)

	_, err := ps.Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	// connect the watcher to the pubsub
	connect(t, hosts[0], hosts[1])

	// wait a bit for the first subscription to be emitted and trigger announce retry
	time.Sleep(100 * time.Millisecond)
	go ps.announceRetry(hosts[1].ID(), "test", true)

	// wait a bit for the subscription to propagate and ensure it was received twice
	time.Sleep(time.Second + 100*time.Millisecond)
	count := watcher.countSubs()
	if count != 2 {
		t.Fatalf("expected 2 subscription messages, but got %d", count)
	}
}

type announceWatcher struct {
	mx   sync.Mutex
	subs int
}

func (aw *announceWatcher) handleStream(s network.Stream) {
	defer s.Close()

	r := protoio.NewDelimitedReader(s, 1<<20)

	var rpc pb.RPC
	for {
		rpc.Reset()
		err := r.ReadMsg(&rpc)
		if err != nil {
			if err != io.EOF {
				s.Reset()
			}
			return
		}

		for _, sub := range rpc.GetSubscriptions() {
			if sub.GetSubscribe() && sub.GetTopicid() == "test" {
				aw.mx.Lock()
				aw.subs++
				aw.mx.Unlock()
			}
		}
	}
}

func (aw *announceWatcher) countSubs() int {
	aw.mx.Lock()
	defer aw.mx.Unlock()
	return aw.subs
}

func TestPubsubWithAssortedOptions(t *testing.T) {
	// this test uses assorted options that are not covered in other tests
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hashMsgID := func(m *pb.Message) string {
		hash := sha256.Sum256(m.Data)
		return string(hash[:])
	}

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts,
		WithMessageIdFn(hashMsgID),
		WithPeerOutboundQueueSize(10),
		WithMessageAuthor(""),
		WithBlacklist(NewMapBlacklist()))

	connect(t, hosts[0], hosts[1])

	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(time.Second)

	for i := 0; i < 2; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i].Publish("test", msg)

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}
}

func TestWithInvalidMessageAuthor(t *testing.T) {
	// this test exercises the failure path in the WithMessageAuthor option
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := bhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	_, err := NewFloodSub(ctx, h, WithMessageAuthor("bogotr0n"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPreconnectedNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// If this test fails it may hang so set a timeout
	ctx, cancel = context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	// Create hosts
	h1 := bhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := bhost.NewBlankHost(swarmt.GenSwarm(t, ctx))

	opts := []Option{WithDiscovery(&dummyDiscovery{})}
	// Setup first PubSub
	p1, err := NewFloodSub(ctx, h1, opts...)
	if err != nil {
		t.Fatal(err)
	}

	// Connect the two hosts together
	connect(t, h2, h1)

	// Setup the second DHT
	p2, err := NewFloodSub(ctx, h2, opts...)
	if err != nil {
		t.Fatal(err)
	}

	// See if it works
	p2Topic, err := p2.Join("test")
	if err != nil {
		t.Fatal(err)
	}

	p1Topic, err := p1.Join("test")
	if err != nil {
		t.Fatal(err)
	}

	testPublish := func(publisher, receiver *Topic, msg []byte) {
		receiverSub, err := receiver.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		if err := publisher.Publish(ctx, msg, WithReadiness(MinTopicSize(1))); err != nil {
			t.Fatal(err)
		}

		m, err := receiverSub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if receivedData := m.GetData(); !bytes.Equal(receivedData, msg) {
			t.Fatalf("expected message %v, got %v", msg, receivedData)
		}
	}

	// Test both directions since PubSub uses one directional streams
	testPublish(p1Topic, p2Topic, []byte("test1-to-2"))
	testPublish(p1Topic, p2Topic, []byte("test2-to-1"))
}

func TestDedupInboundStreams(t *testing.T) {
	t.Skip()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := bhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := bhost.NewBlankHost(swarmt.GenSwarm(t, ctx))

	_, err := NewFloodSub(ctx, h1)
	if err != nil {
		t.Fatal(err)
	}

	// Connect the two hosts together
	connect(t, h2, h1)

	// open a few streams and make sure all but the last one get reset
	s1, err := h2.NewStream(ctx, h1.ID(), FloodSubID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	s2, err := h2.NewStream(ctx, h1.ID(), FloodSubID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	s3, err := h2.NewStream(ctx, h1.ID(), FloodSubID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	// check that s1 and s2 have been reset
	_, err = s1.Read([]byte{0})
	if err == nil {
		t.Fatal("expected s1 to be reset")
	}

	_, err = s2.Read([]byte{0})
	if err == nil {
		t.Fatal("expected s2 to be reset")
	}

	// check that s3 is readable and simply times out
	s3.SetReadDeadline(time.Now().Add(time.Millisecond))
	_, err = s3.Read([]byte{0})
	err2, ok := err.(interface{ Timeout() bool })
	if !ok || !err2.Timeout() {
		t.Fatal(err)
	}
}
