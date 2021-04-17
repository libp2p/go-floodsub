package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/record"

	bhost "github.com/libp2p/go-libp2p-blankhost"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"

	"github.com/libp2p/go-msgio/protoio"
)

func getGossipsub(ctx context.Context, h host.Host, opts ...Option) *PubSub {
	ps, err := NewGossipSub(ctx, h, opts...)
	if err != nil {
		panic(err)
	}
	return ps
}

func getGossipsubs(ctx context.Context, hs []host.Host, opts ...Option) []*PubSub {
	var psubs []*PubSub
	for _, h := range hs {
		psubs = append(psubs, getGossipsub(ctx, h, opts...))
	}
	return psubs
}

func TestSparseGossipsub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	sparseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

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

func TestDenseGossipsub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

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

func TestGossipsubFanout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs[1:] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := 0

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

	// subscribe the owner
	subch, err := psubs[0].Subscribe("foobar")
	if err != nil {
		t.Fatal(err)
	}
	msgs = append(msgs, subch)

	// wait for a heartbeat
	time.Sleep(time.Second * 1)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := 0

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

func TestGossipsubFanoutMaintenance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs[1:] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := 0

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

	// unsubscribe all peers to exercise fanout maintenance
	for _, sub := range msgs {
		sub.Cancel()
	}
	msgs = nil

	// wait for heartbeats
	time.Sleep(time.Second * 2)

	// resubscribe and repeat
	for _, ps := range psubs[1:] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	time.Sleep(time.Second * 2)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := 0

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

func TestGossipsubFanoutExpiry(t *testing.T) {
	GossipSubFanoutTTL = 1 * time.Second
	defer func() {
		GossipSubFanoutTTL = 60 * time.Second
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 10)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs[1:] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	for i := 0; i < 5; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := 0

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

	psubs[0].eval <- func() {
		if len(psubs[0].rt.(*GossipSubRouter).fanout) == 0 {
			t.Fatal("owner has no fanout")
		}
	}

	// wait for TTL to expire fanout peers in owner
	time.Sleep(time.Second * 2)

	psubs[0].eval <- func() {
		if len(psubs[0].rt.(*GossipSubRouter).fanout) > 0 {
			t.Fatal("fanout hasn't expired")
		}
	}

	// wait for it to run in the event loop
	time.Sleep(10 * time.Millisecond)
}

func TestGossipsubGossip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

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

		// wait a bit to have some gossip interleaved
		time.Sleep(time.Millisecond * 100)
	}

	// and wait for some gossip flushing
	time.Sleep(time.Second * 2)
}

func TestGossipsubGossipPiggyback(t *testing.T) {
	t.Skip("test no longer relevant; gossip propagation has become eager")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	var xmsgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("bazcrux")
		if err != nil {
			t.Fatal(err)
		}

		xmsgs = append(xmsgs, subch)
	}

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish("foobar", msg)
		psubs[owner].Publish("bazcrux", msg)

		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}

		for _, sub := range xmsgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}

		// wait a bit to have some gossip interleaved
		time.Sleep(time.Millisecond * 100)
	}

	// and wait for some gossip flushing
	time.Sleep(time.Second * 2)
}

func TestGossipsubGossipPropagation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 20)
	psubs := getGossipsubs(ctx, hosts)

	hosts1 := hosts[:GossipSubD+1]
	hosts2 := append(hosts[GossipSubD+1:], hosts[0])

	denseConnect(t, hosts1)
	denseConnect(t, hosts2)

	var msgs1 []*Subscription
	for _, ps := range psubs[1 : GossipSubD+1] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs1 = append(msgs1, subch)
	}

	time.Sleep(time.Second * 1)

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := 0

		psubs[owner].Publish("foobar", msg)

		for _, sub := range msgs1 {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}
	}

	time.Sleep(time.Millisecond * 100)

	var msgs2 []*Subscription
	for _, ps := range psubs[GossipSubD+1:] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs2 = append(msgs2, subch)
	}

	var collect [][]byte
	for i := 0; i < 10; i++ {
		for _, sub := range msgs2 {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			collect = append(collect, got.Data)
		}
	}

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))
		gotit := false
		for j := 0; j < len(collect); j++ {
			if bytes.Equal(msg, collect[j]) {
				gotit = true
				break
			}
		}
		if !gotit {
			t.Fatalf("Didn't get message %s", string(msg))
		}
	}
}

func TestGossipsubPrune(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	// disconnect some peers from the mesh to get some PRUNEs
	for _, sub := range msgs[:5] {
		sub.Cancel()
	}

	// wait a bit to take effect
	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish("foobar", msg)

		for _, sub := range msgs[5:] {
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

func TestGossipsubGraft(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	sparseConnect(t, hosts)

	time.Sleep(time.Second * 1)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)

		// wait for announce to propagate
		time.Sleep(time.Millisecond * 100)
	}

	time.Sleep(time.Second * 1)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

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

func TestGossipsubRemovePeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getGossipsubs(ctx, hosts)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	// disconnect some peers to exercise RemovePeer paths
	for _, host := range hosts[:5] {
		host.Close()
	}

	// wait a heartbeat
	time.Sleep(time.Second * 1)

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := 5 + rand.Intn(len(psubs)-5)

		psubs[owner].Publish("foobar", msg)

		for _, sub := range msgs[5:] {
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

func TestGossipsubGraftPruneRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)
	psubs := getGossipsubs(ctx, hosts)
	denseConnect(t, hosts)

	var topics []string
	var msgs [][]*Subscription
	for i := 0; i < 35; i++ {
		topic := fmt.Sprintf("topic%d", i)
		topics = append(topics, topic)

		var subs []*Subscription
		for _, ps := range psubs {
			subch, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}

			subs = append(subs, subch)
		}
		msgs = append(msgs, subs)
	}

	// wait for heartbeats to build meshes
	time.Sleep(time.Second * 5)

	for i, topic := range topics {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish(topic, msg)

		for _, sub := range msgs[i] {
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

func TestGossipsubControlPiggyback(t *testing.T) {
	t.Skip("travis regularly fails on this test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)
	psubs := getGossipsubs(ctx, hosts)
	denseConnect(t, hosts)

	for _, ps := range psubs {
		subch, err := ps.Subscribe("flood")
		if err != nil {
			t.Fatal(err)
		}
		go func(sub *Subscription) {
			for {
				_, err := sub.Next(ctx)
				if err != nil {
					break
				}
			}
		}(subch)
	}

	time.Sleep(time.Second * 1)

	// create a background flood of messages that overloads the queues
	done := make(chan struct{})
	go func() {
		owner := rand.Intn(len(psubs))
		for i := 0; i < 10000; i++ {
			msg := []byte("background flooooood")
			psubs[owner].Publish("flood", msg)
		}
		done <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 20)

	// and subscribe to a bunch of topics in the meantime -- this should
	// result in some dropped control messages, with subsequent piggybacking
	// in the background flood
	var topics []string
	var msgs [][]*Subscription
	for i := 0; i < 5; i++ {
		topic := fmt.Sprintf("topic%d", i)
		topics = append(topics, topic)

		var subs []*Subscription
		for _, ps := range psubs {
			subch, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}

			subs = append(subs, subch)
		}
		msgs = append(msgs, subs)
	}

	// wait for the flood to stop
	<-done

	// and test that we have functional overlays
	for i, topic := range topics {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish(topic, msg)

		for _, sub := range msgs[i] {
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

func TestMixedGossipsub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 30)

	gsubs := getGossipsubs(ctx, hosts[:20])
	fsubs := getPubsubs(ctx, hosts[20:])
	psubs := append(gsubs, fsubs...)

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	sparseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

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

func TestGossipsubMultihops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 6)

	psubs := getGossipsubs(ctx, hosts)

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

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

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

func TestGossipsubTreeTopology(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)
	psubs := getGossipsubs(ctx, hosts)

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

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	assertPeerLists(t, hosts, psubs[0], 1, 5)
	assertPeerLists(t, hosts, psubs[1], 0, 2, 4)
	assertPeerLists(t, hosts, psubs[2], 1, 3)

	checkMessageRouting(t, "fizzbuzz", []*PubSub{psubs[9], psubs[3]}, chs)
}

// this tests overlay bootstrapping through px in Gossipsub v1.1
// we start with a star topology and rely on px through prune to build the mesh
func TestGossipsubStarTopology(t *testing.T) {
	originalGossipSubD := GossipSubD
	GossipSubD = 4
	originalGossipSubDhi := GossipSubDhi
	GossipSubDhi = GossipSubD + 1
	originalGossipSubDlo := GossipSubDlo
	GossipSubDlo = GossipSubD - 1
	originalGossipSubDscore := GossipSubDscore
	GossipSubDscore = GossipSubDlo
	defer func() {
		GossipSubD = originalGossipSubD
		GossipSubDhi = originalGossipSubDhi
		GossipSubDlo = originalGossipSubDlo
		GossipSubDscore = originalGossipSubDscore
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 20)
	psubs := getGossipsubs(ctx, hosts, WithPeerExchange(true), WithFloodPublish(true))

	// configure the center of the star with a very low D
	psubs[0].eval <- func() {
		gs := psubs[0].rt.(*GossipSubRouter)
		gs.D = 0
		gs.Dlo = 0
		gs.Dhi = 0
		gs.Dscore = 0
	}

	// add all peer addresses to the peerstores
	// this is necessary because we can't have signed address records witout identify
	// pushing them
	for i := range hosts {
		for j := range hosts {
			if i == j {
				continue
			}
			hosts[i].Peerstore().AddAddrs(hosts[j].ID(), hosts[j].Addrs(), peerstore.PermanentAddrTTL)
		}
	}

	// build the star
	for i := 1; i < 20; i++ {
		connect(t, hosts[0], hosts[i])
	}

	time.Sleep(time.Second)

	// build the mesh
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// wait a bit for the mesh to build
	time.Sleep(10 * time.Second)

	// check that all peers have > 1 connection
	for i, h := range hosts {
		if len(h.Network().Conns()) == 1 {
			t.Errorf("peer %d has ony a single connection", i)
		}
	}

	// send a message from each peer and assert it was propagated
	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i].Publish("test", msg)

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}
}

// this tests overlay bootstrapping through px in Gossipsub v1.1, with addresses
// exchanged in signed peer records.
// we start with a star topology and rely on px through prune to build the mesh
func TestGossipsubStarTopologyWithSignedPeerRecords(t *testing.T) {
	originalGossipSubD := GossipSubD
	GossipSubD = 4
	originalGossipSubDhi := GossipSubDhi
	GossipSubDhi = GossipSubD + 1
	originalGossipSubDlo := GossipSubDlo
	GossipSubDlo = GossipSubD - 1
	originalGossipSubDscore := GossipSubDscore
	GossipSubDscore = GossipSubDlo
	defer func() {
		GossipSubD = originalGossipSubD
		GossipSubDhi = originalGossipSubDhi
		GossipSubDlo = originalGossipSubDlo
		GossipSubDscore = originalGossipSubDscore
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 20)
	psubs := getGossipsubs(ctx, hosts, WithPeerExchange(true), WithFloodPublish(true))

	// configure the center of the star with a very low D
	psubs[0].eval <- func() {
		gs := psubs[0].rt.(*GossipSubRouter)
		gs.D = 0
		gs.Dlo = 0
		gs.Dhi = 0
		gs.Dscore = 0
	}

	// manually create signed peer records for each host and add them to the
	// peerstore of the center of the star, which is doing the bootstrapping
	for i := range hosts[1:] {
		privKey := hosts[i].Peerstore().PrivKey(hosts[i].ID())
		if privKey == nil {
			t.Fatalf("unable to get private key for host %s", hosts[i].ID().Pretty())
		}
		ai := host.InfoFromHost(hosts[i])
		rec := peer.PeerRecordFromAddrInfo(*ai)
		signedRec, err := record.Seal(rec, privKey)
		if err != nil {
			t.Fatalf("error creating signed peer record: %s", err)
		}

		cab, ok := peerstore.GetCertifiedAddrBook(hosts[0].Peerstore())
		if !ok {
			t.Fatal("peerstore does not implement CertifiedAddrBook")
		}
		_, err = cab.ConsumePeerRecord(signedRec, peerstore.PermanentAddrTTL)
		if err != nil {
			t.Fatalf("error adding signed peer record: %s", err)
		}
	}

	// build the star
	for i := 1; i < 20; i++ {
		connect(t, hosts[0], hosts[i])
	}

	time.Sleep(time.Second)

	// build the mesh
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// wait a bit for the mesh to build
	time.Sleep(10 * time.Second)

	// check that all peers have > 1 connection
	for i, h := range hosts {
		if len(h.Network().Conns()) == 1 {
			t.Errorf("peer %d has ony a single connection", i)
		}
	}

	// send a message from each peer and assert it was propagated
	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i].Publish("test", msg)

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}
}

func TestGossipsubDirectPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := getNetHosts(t, ctx, 3)
	psubs := []*PubSub{
		getGossipsub(ctx, h[0], WithDirectConnectTicks(2)),
		getGossipsub(ctx, h[1], WithDirectPeers([]peer.AddrInfo{{ID: h[2].ID(), Addrs: h[2].Addrs()}}), WithDirectConnectTicks(2)),
		getGossipsub(ctx, h[2], WithDirectPeers([]peer.AddrInfo{{ID: h[1].ID(), Addrs: h[1].Addrs()}}), WithDirectConnectTicks(2)),
	}

	connect(t, h[0], h[1])
	connect(t, h[0], h[2])

	// verify that the direct peers connected
	time.Sleep(2 * time.Second)
	if len(h[1].Network().ConnsToPeer(h[2].ID())) == 0 {
		t.Fatal("expected a connection between direct peers")
	}

	// build the mesh
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(time.Second)

	// publish some messages
	for i := 0; i < 3; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i].Publish("test", msg)

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}

	// disconnect the direct peers to test reconnection
	for _, c := range h[1].Network().ConnsToPeer(h[2].ID()) {
		c.Close()
	}

	time.Sleep(5 * time.Second)

	if len(h[1].Network().ConnsToPeer(h[2].ID())) == 0 {
		t.Fatal("expected a connection between direct peers")
	}

	// publish some messages
	for i := 0; i < 3; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i].Publish("test", msg)

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}
}

func TestGossipsubDirectPeersFanout(t *testing.T) {
	// regression test for #371
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := getNetHosts(t, ctx, 3)
	psubs := []*PubSub{
		getGossipsub(ctx, h[0]),
		getGossipsub(ctx, h[1], WithDirectPeers([]peer.AddrInfo{{ID: h[2].ID(), Addrs: h[2].Addrs()}})),
		getGossipsub(ctx, h[2], WithDirectPeers([]peer.AddrInfo{{ID: h[1].ID(), Addrs: h[1].Addrs()}})),
	}

	connect(t, h[0], h[1])
	connect(t, h[0], h[2])

	// Join all peers except h2
	var subs []*Subscription
	for _, ps := range psubs[:2] {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(time.Second)

	// h2 publishes some messages to build a fanout
	for i := 0; i < 3; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[2].Publish("test", msg)

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}

	// verify that h0 is in the fanout of h2, but not h1 who is a direct peer
	result := make(chan bool, 2)
	psubs[2].eval <- func() {
		rt := psubs[2].rt.(*GossipSubRouter)
		fanout := rt.fanout["test"]
		_, ok := fanout[h[0].ID()]
		result <- ok
		_, ok = fanout[h[1].ID()]
		result <- ok
	}

	inFanout := <-result
	if !inFanout {
		t.Fatal("expected peer 0 to be in fanout")
	}

	inFanout = <-result
	if inFanout {
		t.Fatal("expected peer 1 to not be in fanout")
	}

	// now subscribe h2 too and verify tht h0 is in the mesh but not h1
	_, err := psubs[2].Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	psubs[2].eval <- func() {
		rt := psubs[2].rt.(*GossipSubRouter)
		mesh := rt.mesh["test"]
		_, ok := mesh[h[0].ID()]
		result <- ok
		_, ok = mesh[h[1].ID()]
		result <- ok
	}

	inMesh := <-result
	if !inMesh {
		t.Fatal("expected peer 0 to be in mesh")
	}

	inMesh = <-result
	if inMesh {
		t.Fatal("expected peer 1 to not be in mesh")
	}
}

func TestGossipsubFloodPublish(t *testing.T) {
	// uses a star topology without PX and publishes from the star to verify that all
	// messages get received
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 20)
	psubs := getGossipsubs(ctx, hosts, WithFloodPublish(true))

	// build the star
	for i := 1; i < 20; i++ {
		connect(t, hosts[0], hosts[i])
	}

	// build the (partial, unstable) mesh
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(time.Second)

	// send a message from the star and assert it was received
	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[0].Publish("test", msg)

		for _, sub := range subs {
			assertReceive(t, sub, msg)
		}
	}
}

func TestGossipsubEnoughPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 20)
	psubs := getGossipsubs(ctx, hosts)

	for _, ps := range psubs {
		_, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
	}

	// at this point we have no connections and no mesh, so EnoughPeers should return false
	res := make(chan bool, 1)
	psubs[0].eval <- func() {
		res <- psubs[0].rt.EnoughPeers("test", 0)
	}
	enough := <-res
	if enough {
		t.Fatal("should not have enough peers")
	}

	// connect them densly to build up the mesh
	denseConnect(t, hosts)

	time.Sleep(3 * time.Second)

	psubs[0].eval <- func() {
		res <- psubs[0].rt.EnoughPeers("test", 0)
	}
	enough = <-res
	if !enough {
		t.Fatal("should have enough peers")
	}
}

func TestGossipsubNegativeScore(t *testing.T) {
	// in this test we score sinkhole a peer to exercise code paths relative to negative scores
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 20)
	psubs := getGossipsubs(ctx, hosts,
		WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore: func(p peer.ID) float64 {
					if p == hosts[0].ID() {
						return -1000
					} else {
						return 0
					}
				},
				AppSpecificWeight: 1,
				DecayInterval:     time.Second,
				DecayToZero:       0.01,
			},
			&PeerScoreThresholds{
				GossipThreshold:   -10,
				PublishThreshold:  -100,
				GraylistThreshold: -10000,
			}))

	denseConnect(t, hosts)

	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(3 * time.Second)

	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i%20].Publish("test", msg)
		time.Sleep(20 * time.Millisecond)
	}

	// let the sinkholed peer try to emit gossip as well
	time.Sleep(2 * time.Second)

	// checks:
	// 1. peer 0 should only receive its own message
	// 2. peers 1-20 should not receive a message from peer 0, because it's not part of the mesh
	//    and its gossip is rejected
	collectAll := func(sub *Subscription) []*Message {
		var res []*Message
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				break
			}

			res = append(res, msg)
		}

		return res
	}

	count := len(collectAll(subs[0]))
	if count != 1 {
		t.Fatalf("expected 1 message but got %d instead", count)
	}

	for _, sub := range subs[1:] {
		all := collectAll(sub)
		for _, m := range all {
			if m.ReceivedFrom == hosts[0].ID() {
				t.Fatal("received message from sinkholed peer")
			}
		}
	}
}

func TestGossipsubScoreValidatorEx(t *testing.T) {
	// this is a test that of the two message drop responses from a validator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)
	psubs := getGossipsubs(ctx, hosts,
		WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore: func(p peer.ID) float64 { return 0 },
				DecayInterval:    time.Second,
				DecayToZero:      0.01,
				Topics: map[string]*TopicScoreParams{
					"test": {
						TopicWeight:                    1,
						TimeInMeshQuantum:              time.Second,
						InvalidMessageDeliveriesWeight: -1,
						InvalidMessageDeliveriesDecay:  0.9999,
					},
				},
			},
			&PeerScoreThresholds{
				GossipThreshold:   -10,
				PublishThreshold:  -100,
				GraylistThreshold: -10000,
			}))

	connectAll(t, hosts)

	err := psubs[0].RegisterTopicValidator("test", func(ctx context.Context, p peer.ID, msg *Message) ValidationResult {
		// we ignore host1 and reject host2
		if p == hosts[1].ID() {
			return ValidationIgnore
		}
		if p == hosts[2].ID() {
			return ValidationReject
		}

		return ValidationAccept
	})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := psubs[0].Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	expectNoMessage := func(sub *Subscription) {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		m, err := sub.Next(ctx)
		if err == nil {
			t.Fatal("expected no message, but got ", string(m.Data))
		}
	}

	psubs[1].Publish("test", []byte("i am not a walrus"))
	psubs[2].Publish("test", []byte("i am not a walrus either"))

	// assert no messages
	expectNoMessage(sub)

	// assert that peer1's score is still 0 (its message was ignored) while peer2 should have
	// a negative score (its message got rejected)
	res := make(chan float64, 1)
	psubs[0].eval <- func() {
		res <- psubs[0].rt.(*GossipSubRouter).score.Score(hosts[1].ID())
	}
	score := <-res
	if score != 0 {
		t.Fatalf("expected 0 score for peer1, but got %f", score)
	}

	psubs[0].eval <- func() {
		res <- psubs[0].rt.(*GossipSubRouter).score.Score(hosts[2].ID())
	}
	score = <-res
	if score >= 0 {
		t.Fatalf("expected negative score for peer2, but got %f", score)
	}
}

func TestGossipsubPiggybackControl(t *testing.T) {
	// this is a direct test of the piggybackControl function as we can't reliably
	// trigger it on travis
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := bhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	ps := getGossipsub(ctx, h)

	blah := peer.ID("bogotr0n")

	res := make(chan *RPC, 1)
	ps.eval <- func() {
		gs := ps.rt.(*GossipSubRouter)
		test1 := "test1"
		test2 := "test2"
		test3 := "test3"
		gs.mesh[test1] = make(map[peer.ID]struct{})
		gs.mesh[test2] = make(map[peer.ID]struct{})
		gs.mesh[test1][blah] = struct{}{}

		rpc := &RPC{RPC: pb.RPC{}}
		gs.piggybackControl(blah, rpc, &pb.ControlMessage{
			Graft: []*pb.ControlGraft{{TopicID: &test1}, {TopicID: &test2}, {TopicID: &test3}},
			Prune: []*pb.ControlPrune{{TopicID: &test1}, {TopicID: &test2}, {TopicID: &test3}},
		})
		res <- rpc
	}

	rpc := <-res
	if rpc.Control == nil {
		t.Fatal("expected non-nil control message")
	}
	if len(rpc.Control.Graft) != 1 {
		t.Fatal("expected 1 GRAFT")
	}
	if rpc.Control.Graft[0].GetTopicID() != "test1" {
		t.Fatal("expected test1 as graft topic ID")
	}
	if len(rpc.Control.Prune) != 2 {
		t.Fatal("expected 2 PRUNEs")
	}
	if rpc.Control.Prune[0].GetTopicID() != "test2" {
		t.Fatal("expected test2 as prune topic ID")
	}
	if rpc.Control.Prune[1].GetTopicID() != "test3" {
		t.Fatal("expected test3 as prune topic ID")
	}
}

func TestGossipsubMultipleGraftTopics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getGossipsubs(ctx, hosts)
	sparseConnect(t, hosts)

	time.Sleep(time.Second * 1)

	firstTopic := "topic1"
	secondTopic := "topic2"
	thirdTopic := "topic3"

	firstPeer := hosts[0].ID()
	secondPeer := hosts[1].ID()

	p2Sub := psubs[1]
	p1Router := psubs[0].rt.(*GossipSubRouter)
	p2Router := psubs[1].rt.(*GossipSubRouter)

	finChan := make(chan struct{})

	p2Sub.eval <- func() {
		// Add topics to second peer
		p2Router.mesh[firstTopic] = map[peer.ID]struct{}{}
		p2Router.mesh[secondTopic] = map[peer.ID]struct{}{}
		p2Router.mesh[thirdTopic] = map[peer.ID]struct{}{}

		finChan <- struct{}{}
	}
	<-finChan

	// Send multiple GRAFT messages to second peer from
	// 1st peer
	p1Router.sendGraftPrune(map[peer.ID][]string{
		secondPeer: {firstTopic, secondTopic, thirdTopic},
	}, map[peer.ID][]string{}, map[peer.ID]bool{})

	time.Sleep(time.Second * 1)

	p2Sub.eval <- func() {
		if _, ok := p2Router.mesh[firstTopic][firstPeer]; !ok {
			t.Errorf("First peer wasnt added to mesh of the second peer for the topic %s", firstTopic)
		}
		if _, ok := p2Router.mesh[secondTopic][firstPeer]; !ok {
			t.Errorf("First peer wasnt added to mesh of the second peer for the topic %s", secondTopic)
		}
		if _, ok := p2Router.mesh[thirdTopic][firstPeer]; !ok {
			t.Errorf("First peer wasnt added to mesh of the second peer for the topic %s", thirdTopic)
		}
		finChan <- struct{}{}
	}
	<-finChan
}

func TestGossipsubOpportunisticGrafting(t *testing.T) {
	originalGossipSubPruneBackoff := GossipSubPruneBackoff
	GossipSubPruneBackoff = 500 * time.Millisecond
	originalGossipSubGraftFloodThreshold := GossipSubGraftFloodThreshold
	GossipSubGraftFloodThreshold = 100 * time.Millisecond
	originalGossipSubOpportunisticGraftTicks := GossipSubOpportunisticGraftTicks
	GossipSubOpportunisticGraftTicks = 2
	defer func() {
		GossipSubPruneBackoff = originalGossipSubPruneBackoff
		GossipSubGraftFloodThreshold = originalGossipSubGraftFloodThreshold
		GossipSubOpportunisticGraftTicks = originalGossipSubOpportunisticGraftTicks
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 50)
	// pubsubs for the first 10 hosts
	psubs := getGossipsubs(ctx, hosts[:10],
		WithFloodPublish(true),
		WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore:  func(peer.ID) float64 { return 0 },
				AppSpecificWeight: 0,
				DecayInterval:     time.Second,
				DecayToZero:       0.01,
				Topics: map[string]*TopicScoreParams{
					"test": {
						TopicWeight:                   1,
						TimeInMeshWeight:              0.0002777,
						TimeInMeshQuantum:             time.Second,
						TimeInMeshCap:                 3600,
						FirstMessageDeliveriesWeight:  1,
						FirstMessageDeliveriesDecay:   0.9997,
						FirstMessageDeliveriesCap:     100,
						InvalidMessageDeliveriesDecay: 0.99997,
					},
				},
			},
			&PeerScoreThresholds{
				GossipThreshold:             -10,
				PublishThreshold:            -100,
				GraylistThreshold:           -10000,
				OpportunisticGraftThreshold: 1,
			}))

	// connect the real hosts with degree 5
	connectSome(t, hosts[:10], 5)

	// sybil squatters for the remaining 40 hosts

	for _, h := range hosts[10:] {
		squatter := &sybilSquatter{h: h}
		h.SetStreamHandler(GossipSubID_v10, squatter.handleStream)
	}

	// connect all squatters to every real host
	for _, squatter := range hosts[10:] {
		for _, real := range hosts[:10] {
			connect(t, squatter, real)
		}
	}

	// wait a bit for the connections to propagate events to the pubsubs
	time.Sleep(time.Second)

	// ask the real pubsus to join the topic
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		// consume the messages
		go func(sub *Subscription) {
			for {
				_, err := sub.Next(ctx)
				if err != nil {
					return
				}
			}
		}(sub)
	}

	// publish a bunch of messages from the real hosts
	for i := 0; i < 1000; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i%10].Publish("test", msg)
		time.Sleep(20 * time.Millisecond)
	}

	// now wait a few of oppgraft cycles
	time.Sleep(7 * time.Second)

	// check the honest peer meshes, they should have at least 3 honest peers each
	res := make(chan int, 1)
	for _, ps := range psubs {
		ps.eval <- func() {
			gs := ps.rt.(*GossipSubRouter)
			count := 0
			for _, h := range hosts[:10] {
				_, ok := gs.mesh["test"][h.ID()]
				if ok {
					count++
				}
			}
			res <- count
		}

		count := <-res
		if count < 3 {
			t.Fatalf("expected at least 3 honest peers, got %d", count)
		}
	}
}

type sybilSquatter struct {
	h host.Host
}

func (sq *sybilSquatter) handleStream(s network.Stream) {
	defer s.Close()

	os, err := sq.h.NewStream(context.Background(), s.Conn().RemotePeer(), GossipSubID_v10)
	if err != nil {
		panic(err)
	}

	// send a subscription for test in the output stream to become candidate for GRAFT
	// and then just read and ignore the incoming RPCs
	r := protoio.NewDelimitedReader(s, 1<<20)
	w := protoio.NewDelimitedWriter(os)
	truth := true
	topic := "test"
	err = w.WriteMsg(&pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Subscribe: &truth, Topicid: &topic}}})
	if err != nil {
		panic(err)
	}

	var rpc pb.RPC
	for {
		rpc.Reset()
		err = r.ReadMsg(&rpc)
		if err != nil {
			if err != io.EOF {
				s.Reset()
			}
			return
		}
	}
}

func TestGossipsubPeerScoreInspect(t *testing.T) {
	// this test exercises the code path sof peer score inspection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)

	inspector := &mockPeerScoreInspector{}
	psub1 := getGossipsub(ctx, hosts[0],
		WithPeerScore(
			&PeerScoreParams{
				Topics: map[string]*TopicScoreParams{
					"test": {
						TopicWeight:                    1,
						TimeInMeshQuantum:              time.Second,
						FirstMessageDeliveriesWeight:   1,
						FirstMessageDeliveriesDecay:    0.999,
						FirstMessageDeliveriesCap:      100,
						InvalidMessageDeliveriesWeight: -1,
						InvalidMessageDeliveriesDecay:  0.9999,
					},
				},
				AppSpecificScore: func(peer.ID) float64 { return 0 },
				DecayInterval:    time.Second,
				DecayToZero:      0.01,
			},
			&PeerScoreThresholds{
				GossipThreshold:   -1,
				PublishThreshold:  -10,
				GraylistThreshold: -1000,
			}),
		WithPeerScoreInspect(inspector.inspect, time.Second))
	psub2 := getGossipsub(ctx, hosts[1])
	psubs := []*PubSub{psub1, psub2}

	connect(t, hosts[0], hosts[1])

	for _, ps := range psubs {
		_, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second)

	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i%2].Publish("test", msg)
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(time.Second + 200*time.Millisecond)

	score2 := inspector.score(hosts[1].ID())
	if score2 < 9 {
		t.Fatalf("expected score to be at least 9, instead got %f", score2)
	}
}

func TestGossipsubPeerScoreResetTopicParams(t *testing.T) {
	// this test exercises the code path sof peer score inspection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 1)

	ps := getGossipsub(ctx, hosts[0],
		WithPeerScore(
			&PeerScoreParams{
				Topics: map[string]*TopicScoreParams{
					"test": {
						TopicWeight:                    1,
						TimeInMeshQuantum:              time.Second,
						FirstMessageDeliveriesWeight:   1,
						FirstMessageDeliveriesDecay:    0.999,
						FirstMessageDeliveriesCap:      100,
						InvalidMessageDeliveriesWeight: -1,
						InvalidMessageDeliveriesDecay:  0.9999,
					},
				},
				AppSpecificScore: func(peer.ID) float64 { return 0 },
				DecayInterval:    time.Second,
				DecayToZero:      0.01,
			},
			&PeerScoreThresholds{
				GossipThreshold:   -1,
				PublishThreshold:  -10,
				GraylistThreshold: -1000,
			}))

	topic, err := ps.Join("test")
	if err != nil {
		t.Fatal(err)
	}

	err = topic.SetScoreParams(
		&TopicScoreParams{
			TopicWeight:                    1,
			TimeInMeshQuantum:              time.Second,
			FirstMessageDeliveriesWeight:   1,
			FirstMessageDeliveriesDecay:    0.999,
			FirstMessageDeliveriesCap:      200,
			InvalidMessageDeliveriesWeight: -1,
			InvalidMessageDeliveriesDecay:  0.9999,
		})
	if err != nil {
		t.Fatal(err)
	}
}

type mockPeerScoreInspector struct {
	mx     sync.Mutex
	scores map[peer.ID]float64
}

func (ps *mockPeerScoreInspector) inspect(scores map[peer.ID]float64) {
	ps.mx.Lock()
	defer ps.mx.Unlock()
	ps.scores = scores
}

func (ps *mockPeerScoreInspector) score(p peer.ID) float64 {
	ps.mx.Lock()
	defer ps.mx.Unlock()
	return ps.scores[p]
}

func TestGossipsubRPCFragmentation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	ps := getGossipsub(ctx, hosts[0])

	// make a fake peer that requests everything through IWANT gossip
	iwe := iwantEverything{h: hosts[1]}
	iwe.h.SetStreamHandler(GossipSubID_v10, iwe.handleStream)

	connect(t, hosts[0], hosts[1])

	// have the real pubsub join the test topic
	_, err := ps.Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	// wait for the real pubsub to connect and try to graft to the faker
	time.Sleep(time.Second)

	// publish a bunch of fairly large messages from the real host
	nMessages := 1000
	msgSize := 20000
	for i := 0; i < nMessages; i++ {
		msg := make([]byte, msgSize)
		rand.Read(msg)
		ps.Publish("test", msg)
		time.Sleep(20 * time.Millisecond)
	}

	// wait a bit for them to be received via gossip by the fake peer
	time.Sleep(5 * time.Second)
	iwe.lk.Lock()
	defer iwe.lk.Unlock()

	// we should have received all the messages
	if iwe.msgsReceived != nMessages {
		t.Fatalf("expected fake gossipsub peer to receive all messages, got %d / %d", iwe.msgsReceived, nMessages)
	}

	// and we should have seen an IHAVE message for each of them
	if iwe.ihavesReceived != nMessages {
		t.Fatalf("expected to get IHAVEs for every message, got %d / %d", iwe.ihavesReceived, nMessages)
	}

	// If everything were fragmented with maximum efficiency, we would expect to get
	// (nMessages * msgSize) / ps.maxMessageSize total RPCs containing the messages we sent IWANTs for.
	// The actual number will probably be larger, since there's some overhead for the RPC itself, and
	// we probably aren't packing each RPC to it's maximum size
	minExpectedRPCS := (nMessages * msgSize) / ps.maxMessageSize
	if iwe.rpcsWithMessages < minExpectedRPCS {
		t.Fatalf("expected to receive at least %d RPCs containing messages, got %d", minExpectedRPCS, iwe.rpcsWithMessages)
	}
}

// iwantEverything is a simple gossipsub client that never grafts onto a mesh,
// instead requesting everything through IWANT gossip messages. It is used to
// test that large responses to IWANT requests are fragmented into multiple RPCs.
type iwantEverything struct {
	h                host.Host
	lk               sync.Mutex
	rpcsWithMessages int
	msgsReceived     int
	ihavesReceived   int
}

func (iwe *iwantEverything) handleStream(s network.Stream) {
	defer s.Close()

	os, err := iwe.h.NewStream(context.Background(), s.Conn().RemotePeer(), GossipSubID_v10)
	if err != nil {
		panic(err)
	}

	msgIdsReceived := make(map[string]struct{})
	gossipMsgIdsReceived := make(map[string]struct{})

	// send a subscription for test in the output stream to become candidate for gossip
	r := protoio.NewDelimitedReader(s, 1<<20)
	w := protoio.NewDelimitedWriter(os)
	truth := true
	topic := "test"
	err = w.WriteMsg(&pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Subscribe: &truth, Topicid: &topic}}})
	if err != nil {
		s.Reset()
		return
	}

	var rpc pb.RPC
	for {
		rpc.Reset()
		err = r.ReadMsg(&rpc)
		if err != nil {
			if err != io.EOF {
				s.Reset()
			}
			return
		}

		iwe.lk.Lock()
		if len(rpc.Publish) != 0 {
			iwe.rpcsWithMessages++
		}
		// keep track of unique message ids received
		for _, msg := range rpc.Publish {
			id := string(msg.Seqno)
			if _, seen := msgIdsReceived[id]; !seen {
				iwe.msgsReceived++
			}
			msgIdsReceived[id] = struct{}{}
		}

		if rpc.Control != nil {
			// send a PRUNE for all grafts, so we don't get direct message deliveries
			var prunes []*pb.ControlPrune
			for _, graft := range rpc.Control.Graft {
				prunes = append(prunes, &pb.ControlPrune{TopicID: graft.TopicID})
			}

			var iwants []*pb.ControlIWant
			for _, ihave := range rpc.Control.Ihave {
				iwants = append(iwants, &pb.ControlIWant{MessageIDs: ihave.MessageIDs})
				for _, msgId := range ihave.MessageIDs {
					if _, seen := gossipMsgIdsReceived[msgId]; !seen {
						iwe.ihavesReceived++
					}
					gossipMsgIdsReceived[msgId] = struct{}{}
				}
			}

			out := rpcWithControl(nil, nil, iwants, nil, prunes)
			err = w.WriteMsg(out)
			if err != nil {
				panic(err)
			}
		}
		iwe.lk.Unlock()
	}
}

func TestFragmentRPCFunction(t *testing.T) {
	p := peer.ID("some-peer")
	topic := "test"
	rpc := &RPC{from: p}
	limit := 1024

	mkMsg := func(size int) *pb.Message {
		msg := &pb.Message{}
		msg.Data = make([]byte, size-4) // subtract the protobuf overhead, so msg.Size() returns requested size
		rand.Read(msg.Data)
		return msg
	}

	ensureBelowLimit := func(rpcs []*RPC) {
		for _, r := range rpcs {
			if r.Size() > limit {
				t.Fatalf("expected fragmented RPC to be below %d bytes, was %d", limit, r.Size())
			}
		}
	}

	// it should not fragment if everything fits in one RPC
	rpc.Publish = []*pb.Message{}
	rpc.Publish = []*pb.Message{mkMsg(10), mkMsg(10)}
	results, err := fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected single RPC if input is < limit, got %d", len(results))
	}

	// if there's a message larger than the limit, we should fail
	rpc.Publish = []*pb.Message{mkMsg(10), mkMsg(limit * 2)}
	results, err = fragmentRPC(rpc, limit)
	if err == nil {
		t.Fatalf("expected an error if a message exceeds limit, got %d RPCs instead", len(results))
	}

	// if the individual messages are below the limit, but the RPC as a whole is larger, we should fragment
	nMessages := 100
	msgSize := 200
	truth := true
	rpc.Subscriptions = []*pb.RPC_SubOpts{
		{
			Subscribe: &truth,
			Topicid:   &topic,
		},
	}
	rpc.Publish = make([]*pb.Message, nMessages)
	for i := 0; i < nMessages; i++ {
		rpc.Publish[i] = mkMsg(msgSize)
	}
	results, err = fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	ensureBelowLimit(results)
	msgsPerRPC := limit / msgSize
	expectedRPCs := nMessages / msgsPerRPC
	if len(results) != expectedRPCs {
		t.Fatalf("expected %d RPC messages in output, got %d", expectedRPCs, len(results))
	}
	var nMessagesFragmented int
	var nSubscriptions int
	for _, r := range results {
		nMessagesFragmented += len(r.Publish)
		nSubscriptions += len(r.Subscriptions)
	}
	if nMessagesFragmented != nMessages {
		t.Fatalf("expected fragemented RPCs to contain same number of messages as input, got %d / %d", nMessagesFragmented, nMessages)
	}
	if nSubscriptions != 1 {
		t.Fatal("expected subscription to be present in one of the fragmented messages, but not found")
	}

	// if we're fragmenting, and the input RPC has control messages,
	// the control messages should be in a separate RPC at the end
	// reuse RPC from prev test, but add a control message
	rpc.Control = &pb.ControlMessage{
		Graft: []*pb.ControlGraft{{TopicID: &topic}},
		Prune: []*pb.ControlPrune{{TopicID: &topic}},
		Ihave: []*pb.ControlIHave{{MessageIDs: []string{"foo"}}},
		Iwant: []*pb.ControlIWant{{MessageIDs: []string{"bar"}}},
	}
	results, err = fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	ensureBelowLimit(results)
	// we expect one more RPC than last time, with the final one containing the control messages
	expectedCtrl := 1
	expectedRPCs = (nMessages / msgsPerRPC) + expectedCtrl
	if len(results) != expectedRPCs {
		t.Fatalf("expected %d RPC messages in output, got %d", expectedRPCs, len(results))
	}
	ctl := results[len(results)-1].Control
	if ctl == nil {
		t.Fatal("expected final fragmented RPC to contain control messages, but .Control was nil")
	}
	// since it was not altered, the original control message should be identical to the output control message
	originalBytes, err := rpc.Control.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	receivedBytes, err := ctl.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(originalBytes, receivedBytes) {
		t.Fatal("expected control message to be unaltered if it fits within one RPC message")
	}

	// if the control message is too large to fit into a single RPC, it should be split into multiple RPCs
	nTopics := 5 // pretend we're subscribed to multiple topics and sending IHAVE / IWANTs for each
	messageIdSize := 32
	msgsPerTopic := 100 // enough that a single IHAVE or IWANT will exceed the limit
	rpc.Control.Ihave = make([]*pb.ControlIHave, nTopics)
	rpc.Control.Iwant = make([]*pb.ControlIWant, nTopics)
	for i := 0; i < nTopics; i++ {
		messageIds := make([]string, msgsPerTopic)
		for m := 0; m < msgsPerTopic; m++ {
			mid := make([]byte, messageIdSize)
			rand.Read(mid)
			messageIds[m] = string(mid)
		}
		rpc.Control.Ihave[i] = &pb.ControlIHave{MessageIDs: messageIds}
		rpc.Control.Iwant[i] = &pb.ControlIWant{MessageIDs: messageIds}
	}
	results, err = fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	ensureBelowLimit(results)
	minExpectedCtl := rpc.Control.Size() / limit
	minExpectedRPCs := (nMessages / msgsPerRPC) + minExpectedCtl
	if len(results) < minExpectedRPCs {
		t.Fatalf("expected at least %d total RPCs (at least %d with control messages), got %d total", expectedRPCs, expectedCtrl, len(results))
	}

	// Test the pathological case where a single gossip message ID exceeds the limit.
	// It should not be present in the fragmented messages, but smaller IDs should be
	rpc.Reset()
	giantIdBytes := make([]byte, limit*2)
	rand.Read(giantIdBytes)
	rpc.Control = &pb.ControlMessage{
		Iwant: []*pb.ControlIWant{
			{MessageIDs: []string{"hello", string(giantIdBytes)}},
		},
	}
	results, err = fragmentRPC(rpc, limit)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 RPC, got %d", len(results))
	}
	if len(results[0].Control.Iwant) != 1 {
		t.Fatalf("expected 1 IWANT, got %d", len(results[0].Control.Iwant))
	}
	if results[0].Control.Iwant[0].MessageIDs[0] != "hello" {
		t.Fatalf("expected small message ID to be included unaltered, got %s instead",
			results[0].Control.Iwant[0].MessageIDs[0])
	}
}
