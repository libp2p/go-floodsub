package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
)

func getRandomsub(ctx context.Context, h host.Host, size int, opts ...Option) *PubSub {
	ps, err := NewRandomSub(ctx, h, size, opts...)
	if err != nil {
		panic(err)
	}
	return ps
}

func getRandomsubs(ctx context.Context, hs []host.Host, size int, opts ...Option) []*PubSub {
	var psubs []*PubSub
	for _, h := range hs {
		psubs = append(psubs, getRandomsub(ctx, h, size, opts...))
	}
	return psubs
}

func tryReceive(sub *Subscription) *Message {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	m, err := sub.Next(ctx)
	if err != nil {
		return nil
	} else {
		return m
	}
}

func TestRandomsubSmall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)
	psubs := getRandomsubs(ctx, hosts, 10)

	connectAll(t, hosts)

	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(time.Second)

	count := 0
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i].Publish("test", msg)

		for _, sub := range subs {
			if tryReceive(sub) != nil {
				count++
			}
		}
	}

	if count < 7*len(hosts) {
		t.Fatalf("received too few messages; expected at least %d but got %d", 9*len(hosts), count)
	}
}

func TestRandomsubBig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 50)
	psubs := getRandomsubs(ctx, hosts, 50)

	connectSome(t, hosts, 12)

	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(time.Second)

	count := 0
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i].Publish("test", msg)

		for _, sub := range subs {
			if tryReceive(sub) != nil {
				count++
			}
		}
	}

	if count < 7*len(hosts) {
		t.Fatalf("received too few messages; expected at least %d but got %d", 9*len(hosts), count)
	}
}

func TestRandomsubMixed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 40)
	fsubs := getPubsubs(ctx, hosts[:10])
	rsubs := getRandomsubs(ctx, hosts[10:], 30)
	psubs := append(fsubs, rsubs...)

	connectSome(t, hosts, 12)

	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	time.Sleep(time.Second)

	count := 0
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message %d", i))
		psubs[i].Publish("test", msg)

		for _, sub := range subs {
			if tryReceive(sub) != nil {
				count++
			}
		}
	}

	if count < 7*len(hosts) {
		t.Fatalf("received too few messages; expected at least %d but got %d", 9*len(hosts), count)
	}
}

func TestRandomsubEnoughPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 40)
	fsubs := getPubsubs(ctx, hosts[:10])
	rsubs := getRandomsubs(ctx, hosts[10:], 30)
	psubs := append(fsubs, rsubs...)

	connectSome(t, hosts, 12)

	for _, ps := range psubs {
		_, err := ps.Subscribe("test")
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second)

	res := make(chan bool, 1)
	rsubs[0].eval <- func() {
		rs := rsubs[0].rt.(*RandomSubRouter)
		res <- rs.EnoughPeers("test", 0)
	}

	enough := <-res
	if !enough {
		t.Fatal("expected enough peers")
	}

	rsubs[0].eval <- func() {
		rs := rsubs[0].rt.(*RandomSubRouter)
		res <- rs.EnoughPeers("test", 100)
	}

	enough = <-res
	if !enough {
		t.Fatal("expected enough peers")
	}
}
