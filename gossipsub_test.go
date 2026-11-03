package pubsub

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	mrand "math/rand"
	mrand2 "math/rand/v2"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/internal/gologshim"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-msgio"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/record"

	"github.com/libp2p/go-msgio/pbio"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
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
	optsCopy := make([]Option, len(opts)+1)
	copy(optsCopy[1:], opts)
	logger := gologshim.Logger("pubsub")
	for _, h := range hs {
		// Set WithLogger option first so that later options can override it
		optsCopy[0] = WithLogger(logger.With("id", h.ID()))
		psubs = append(psubs, getGossipsub(ctx, h, optsCopy...))
	}
	return psubs
}

func TestGossipSubParamsValidate(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		params := DefaultGossipSubParams()
		params.Dhi = 1
		params.Dscore = 10
		if params.validate() == nil {
			t.Fatal("Params should be invalid")
		}
	})
}

func TestGossipSubBootstrapParamsValidate(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		params := DefaultGossipSubParams()
		params.D = 0
		params.Dlo = 0
		params.Dhi = 0
		params.Dout = 0
		params.Dscore = 0
		if err := params.validate(); err != nil {
			t.Fatalf("Params should be valid: %v", err)
		}
	})
}

func getGossipsubsOptFn(ctx context.Context, hs []host.Host, optFn func(int, host.Host) []Option) []*PubSub {
	var psubs []*PubSub
	for i, h := range hs {
		opts := optFn(i, h)
		psubs = append(psubs, getGossipsub(ctx, h, opts...))
	}
	return psubs
}

func TestConfigurableMaxControlMessageSize(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)

		defaultPubsub := getGossipsub(ctx, hosts[0])
		if defaultPubsub.maxControlMessageSize != DefaultMaxControlMessageSize {
			t.Fatalf("expected default max control message size %d, got %d", DefaultMaxControlMessageSize, defaultPubsub.maxControlMessageSize)
		}

		const maxControlMessageSize = 1234
		configuredPubsub := getGossipsub(ctx, hosts[1], WithMaxControlMessageSize(maxControlMessageSize))
		if configuredPubsub.maxControlMessageSize != maxControlMessageSize {
			t.Fatalf("expected configured max control message size %d, got %d", maxControlMessageSize, configuredPubsub.maxControlMessageSize)
		}
	})
}

func TestSparseGossipsub(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			lh := slog.NewJSONHandler(os.Stdout, nil)
			logger := slog.New(lh.WithAttrs([]slog.Attr{slog.String("id", h.ID().String())}))
			return []Option{
				WithRPCLogger(logger),
			}
		})

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestDenseGossipsub(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestGossipsubFanout(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

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
	})
}

func TestGossipsubFanoutMaintenance(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

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
	})
}

func TestGossipsubFanoutExpiry(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		GossipSubFanoutTTL = 1 * time.Second
		defer func() {
			GossipSubFanoutTTL = 60 * time.Second
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 10)

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

		for i := range 5 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

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
	})
}

func TestGossipsubGossip(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestGossipsubGossipPiggyback(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		t.Skip("test no longer relevant; gossip propagation has become eager")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestGossipsubGossipPropagation(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 20)
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

		for i := range 10 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

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
		for range 10 {
			for _, sub := range msgs2 {
				got, err := sub.Next(ctx)
				if err != nil {
					t.Fatal(sub.err)
				}
				collect = append(collect, got.Data)
			}
		}

		for i := range 10 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)
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
	})
}

func TestGossipsubPrune(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

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

		for i := range 10 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestGossipsubPruneBackoffTime(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 10)

		// App specific score that we'll change later.
		currentScoreForHost0 := int32(0)

		params := DefaultGossipSubParams()
		params.HeartbeatInitialDelay = time.Millisecond * 10
		params.HeartbeatInterval = time.Millisecond * 100

		psubs := getGossipsubs(ctx, hosts, WithGossipSubParams(params), WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore: func(p peer.ID) float64 {
					if p == hosts[0].ID() {
						return float64(atomic.LoadInt32(&currentScoreForHost0))
					} else {
						return 0
					}
				},
				AppSpecificWeight: 1,
				DecayInterval:     time.Second,
				DecayToZero:       0.01,
			},
			&PeerScoreThresholds{
				GossipThreshold:   -1,
				PublishThreshold:  -1,
				GraylistThreshold: -1,
			}))

		var msgs []*Subscription
		for _, ps := range psubs {
			subch, err := ps.Subscribe("foobar")
			if err != nil {
				t.Fatal(err)
			}

			msgs = append(msgs, subch)
		}

		connectAll(t, hosts)

		// wait for heartbeats to build mesh
		time.Sleep(time.Second)

		pruneTime := time.Now()
		// Flip the score. Host 0 should be pruned from everyone
		atomic.StoreInt32(&currentScoreForHost0, -1000)

		// wait for heartbeats to run and prune
		time.Sleep(time.Second)

		wg := sync.WaitGroup{}
		var missingBackoffs uint32 = 0
		for i := 1; i < 10; i++ {
			wg.Add(1)
			// Copy i so this func keeps the correct value in the closure.
			var idx = i
			// Run this check in the eval thunk so that we don't step over the heartbeat goroutine and trigger a race.
			psubs[idx].rt.(*GossipSubRouter).p.eval <- func() {
				defer wg.Done()
				backoff, ok := psubs[idx].rt.(*GossipSubRouter).backoff["foobar"][hosts[0].ID()]
				if !ok {
					atomic.AddUint32(&missingBackoffs, 1)
				}
				if ok && backoff.Sub(pruneTime)-params.PruneBackoff > time.Second {
					t.Errorf("backoff time should be equal to prune backoff (with some slack) was %v", backoff.Sub(pruneTime)-params.PruneBackoff)
				}
			}
		}
		wg.Wait()

		// Sometimes not all the peers will have updated their backoffs by this point. If the majority haven't we'll fail this test.
		if missingBackoffs >= 5 {
			t.Errorf("missing too many backoffs: %v", missingBackoffs)
		}

		for i := range 10 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			// Don't publish from host 0, since everyone should have pruned it.
			owner := mrand.Intn(len(psubs)-1) + 1

			psubs[owner].Publish("foobar", msg)

			for _, sub := range msgs[1:] {
				got, err := sub.Next(ctx)
				if err != nil {
					t.Fatal(sub.err)
				}
				if !bytes.Equal(msg, got.Data) {
					t.Fatal("got wrong message!")
				}
			}
		}
	})
}

func TestGossipsubGraft(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestGossipsubRemovePeer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 20)

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

		for i := range 10 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := 5 + mrand.Intn(len(psubs)-5)

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
	})
}

func TestGossipsubGraftPruneRetry(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 10)
		psubs := getGossipsubs(ctx, hosts)
		denseConnect(t, hosts)

		var topics []string
		var msgs [][]*Subscription
		for i := range 35 {
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
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestGossipsubControlPiggyback(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 10)
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
			owner := mrand.Intn(len(psubs))
			for range 10000 {
				msg := []byte("background flooooood")
				psubs[owner].Publish("flood", msg)
			}
			done <- struct{}{}
		}()

		// subscribe to a bunch of topics in the meantime -- this should
		// result in some dropped control messages, with subsequent piggybacking
		// in the background flood
		var topics []string
		var msgs [][]*Subscription
		for i := range 5 {
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

		// wait for heartbeats to establish meshes for the new topics
		time.Sleep(time.Second * 2)

		// and test that we have functional overlays
		for i, topic := range topics {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestMixedGossipsub(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 30)

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

		for i := range 100 {
			msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)

			owner := mrand.Intn(len(psubs))

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
	})
}

func TestGossipsubMultihops(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 6)

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
	})
}

func TestGossipsubTreeTopology(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 10)
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
	})
}

// this tests overlay bootstrapping through px in Gossipsub v1.1
// we start with a star topology and rely on px through prune to build the mesh
func TestGossipsubStarTopology(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
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

		hosts := getDefaultHosts(t, 20)
		psubs := getGossipsubs(ctx, hosts, WithPeerExchange(true), WithFloodPublish(true))

		// configure the center of the star with a very low D
		psubs[0].eval <- func() {
			gs := psubs[0].rt.(*GossipSubRouter)
			gs.params.D = 0
			gs.params.Dlo = 0
			gs.params.Dhi = 0
			gs.params.Dscore = 0
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
		for i := range 20 {
			msg := fmt.Appendf(nil, "message %d", i)
			psubs[i].Publish("test", msg)

			for _, sub := range subs {
				assertReceive(t, sub, msg)
			}
		}
	})
}

// this tests overlay bootstrapping through px in Gossipsub v1.1, with addresses
// exchanged in signed peer records.
// we start with a star topology and rely on px through prune to build the mesh
func TestGossipsubStarTopologyWithSignedPeerRecords(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
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

		hosts := getDefaultHosts(t, 20)
		psubs := getGossipsubs(ctx, hosts, WithPeerExchange(true), WithFloodPublish(true))

		// configure the center of the star with a very low D
		psubs[0].eval <- func() {
			gs := psubs[0].rt.(*GossipSubRouter)
			gs.params.D = 0
			gs.params.Dlo = 0
			gs.params.Dhi = 0
			gs.params.Dscore = 0
		}

		// manually create signed peer records for each host and add them to the
		// peerstore of the center of the star, which is doing the bootstrapping
		for i := range hosts[1:] {
			privKey := hosts[i].Peerstore().PrivKey(hosts[i].ID())
			if privKey == nil {
				t.Fatalf("unable to get private key for host %s", hosts[i].ID())
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
		for i := range 20 {
			msg := fmt.Appendf(nil, "message %d", i)
			psubs[i].Publish("test", msg)

			for _, sub := range subs {
				assertReceive(t, sub, msg)
			}
		}
	})
}

func TestGossipsubDirectPeers(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h := getDefaultHosts(t, 3)
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
		for i := range 3 {
			msg := fmt.Appendf(nil, "message %d", i)
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
		for i := range 3 {
			msg := fmt.Appendf(nil, "message %d", i)
			psubs[i].Publish("test", msg)

			for _, sub := range subs {
				assertReceive(t, sub, msg)
			}
		}
	})
}

func TestGossipsubDynamicDirectPeers(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h := getDefaultHosts(t, 3)
		psubs := []*PubSub{
			getGossipsub(ctx, h[0], WithDirectConnectTicks(2)),
			getGossipsub(ctx, h[1], WithDirectConnectTicks(2)),
			getGossipsub(ctx, h[2], WithDirectConnectTicks(2)),
		}

		listDirectPeers := func(psb *PubSub) int {
			directPeers := 0
			gspRt, _ := psb.rt.(*GossipSubRouter)
			fn := func() {
				directPeers = len(gspRt.direct)
			}
			psb.syncEval(fn)
			return directPeers
		}

		// test dinamic addition of direct-peers to h[1] and h[2]
		psubs[1].AddDirectPeer(peer.AddrInfo{ID: h[2].ID(), Addrs: h[2].Addrs()})
		psubs[2].AddDirectPeer(peer.AddrInfo{ID: h[1].ID(), Addrs: h[1].Addrs()})

		// give enough time to the state machine to process the direct additions
		time.Sleep(time.Second)

		if listDirectPeers(psubs[1]) < 1 || listDirectPeers(psubs[2]) < 1 {
			t.Fatal("expected 1 direct peer at both gsp rts")
		}

		// remove peer from direct from directPeers
		psubs[1].RemoveDirectPeer(h[2].ID())
		psubs[2].RemoveDirectPeer(h[1].ID())

		// give enough time to the state machine to process the direct additions
		time.Sleep(time.Second)

		if listDirectPeers(psubs[1]) > 0 || listDirectPeers(psubs[2]) > 0 {
			t.Fatal("expected no direct peers both gsp rts")
		}
	})
}

func TestGossipSubPeerFilter(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h := getDefaultHosts(t, 3)
		psubs := []*PubSub{
			getGossipsub(ctx, h[0], WithPeerFilter(func(pid peer.ID, topic string) bool {
				return pid == h[1].ID()
			})),
			getGossipsub(ctx, h[1], WithPeerFilter(func(pid peer.ID, topic string) bool {
				return pid == h[0].ID()
			})),
			getGossipsub(ctx, h[2]),
		}

		connect(t, h[0], h[1])
		connect(t, h[0], h[2])

		// Join all peers
		var subs []*Subscription
		for _, ps := range psubs {
			sub, err := ps.Subscribe("test")
			if err != nil {
				t.Fatal(err)
			}
			subs = append(subs, sub)
		}

		time.Sleep(time.Second)

		msg := []byte("message")

		psubs[0].Publish("test", msg)
		assertReceive(t, subs[1], msg)
		assertNeverReceives(t, subs[2], time.Second)

		psubs[1].Publish("test", msg)
		assertReceive(t, subs[0], msg)
		assertNeverReceives(t, subs[2], time.Second)
	})
}

func TestGossipsubDirectPeersFanout(t *testing.T) {
	// regression test for #371
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h := getDefaultHosts(t, 3)
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
		for i := range 3 {
			msg := fmt.Appendf(nil, "message %d", i)
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
	})
}

func TestGossipsubFloodPublish(t *testing.T) {
	// uses a star topology without PX and publishes from the star to verify that all
	// messages get received
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 20)
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

		// Flood publish only sends to peers currently in the topic map. With
		// simlibp2p latency, a fixed sleep can resume before subscription
		// RPCs are applied, so wait until the hub sees every leaf.
		for len(psubs[0].ListPeers("test")) < len(hosts)-1 {
			time.Sleep(10 * time.Millisecond)
		}

		// send a message from the star and assert it was received
		for i := range 20 {
			msg := fmt.Appendf(nil, "message %d", i)
			psubs[0].Publish("test", msg)

			for _, sub := range subs {
				assertReceive(t, sub, msg)
			}
		}
	})
}

func TestGossipsubEnoughPeers(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 20)
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
	})
}

func TestGossipsubCustomParams(t *testing.T) {
	// in this test we score sinkhole a peer to exercise code paths relative to negative scores
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		params := DefaultGossipSubParams()

		wantedFollowTime := 1 * time.Second
		params.IWantFollowupTime = wantedFollowTime

		customGossipFactor := 0.12
		params.GossipFactor = customGossipFactor

		wantedMaxPendingConns := 23
		params.MaxPendingConnections = wantedMaxPendingConns
		hosts := getDefaultHosts(t, 1)
		psubs := getGossipsubs(ctx, hosts,
			WithGossipSubParams(params))

		if len(psubs) != 1 {
			t.Fatalf("incorrect number of pusbub objects received: wanted %d but got %d", 1, len(psubs))
		}

		rt, ok := psubs[0].rt.(*GossipSubRouter)
		if !ok {
			t.Fatal("Did not get gossip sub router from pub sub object")
		}

		if rt.params.IWantFollowupTime != wantedFollowTime {
			t.Errorf("Wanted %d of param GossipSubIWantFollowupTime but got %d", wantedFollowTime, rt.params.IWantFollowupTime)
		}
		if rt.params.GossipFactor != customGossipFactor {
			t.Errorf("Wanted %f of param GossipSubGossipFactor but got %f", customGossipFactor, rt.params.GossipFactor)
		}
		if rt.params.MaxPendingConnections != wantedMaxPendingConns {
			t.Errorf("Wanted %d of param GossipSubMaxPendingConnections but got %d", wantedMaxPendingConns, rt.params.MaxPendingConnections)
		}
	})
}

func TestGossipsubNegativeScore(t *testing.T) {
	// in this test we score sinkhole a peer to exercise code paths relative to negative scores
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 20)
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

		for i := range 20 {
			msg := fmt.Appendf(nil, "message %d", i)
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
	})
}

func TestGossipsubScoreValidatorEx(t *testing.T) {
	// this is a test that of the two message drop responses from a validator
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 3)
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
	})
}

func TestGossipsubPiggybackControl(t *testing.T) {
	// this is a direct test of the piggybackControl function as we can't reliably
	// trigger it on travis
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h := getDefaultHosts(t, 1)[0]
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
	})
}

func TestGossipsubMultipleGraftTopics(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)
		psubs := getGossipsubs(ctx, hosts)
		sparseConnect(t, hosts)

		time.Sleep(time.Second * 1)

		firstTopic := "topic1"
		secondTopic := "topic2"
		thirdTopic := "topic3"

		firstPeer := hosts[0].ID()
		secondPeer := hosts[1].ID()

		p1Sub := psubs[0]
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
		wait := make(chan struct{})
		p1Sub.eval <- func() {
			defer close(wait)
			p1Router.sendGraftPrune(map[peer.ID][]string{
				secondPeer: {firstTopic, secondTopic, thirdTopic},
			}, map[peer.ID][]string{}, map[peer.ID]bool{})
		}
		<-wait

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
	})
}

func TestGossipsubOpportunisticGrafting(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
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

		hosts := getDefaultHosts(t, 50)
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
		for i := range 1000 {
			msg := fmt.Appendf(nil, "message %d", i)
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
	})
}
func TestGossipSubLeaveTopic(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h := getDefaultHosts(t, 2)
		psubs := []*PubSub{
			getGossipsub(ctx, h[0]),
			getGossipsub(ctx, h[1]),
		}

		connect(t, h[0], h[1])

		// Join all peers
		for _, ps := range psubs {
			_, err := ps.Subscribe("test")
			if err != nil {
				t.Fatal(err)
			}
		}

		time.Sleep(time.Second)

		leaveTime := time.Now()
		done := make(chan struct{})

		psubs[0].rt.(*GossipSubRouter).p.eval <- func() {
			defer close(done)
			psubs[0].rt.Leave("test")
			time.Sleep(time.Second)
			peerMap := psubs[0].rt.(*GossipSubRouter).backoff["test"]
			if len(peerMap) != 1 {
				t.Fatalf("No peer is populated in the backoff map for peer 0")
			}
			_, ok := peerMap[h[1].ID()]
			if !ok {
				t.Errorf("Expected peer does not exist in the backoff map")
			}

			backoffTime := peerMap[h[1].ID()].Sub(leaveTime)
			// Check that the backoff time is roughly the unsubscribebackoff time (with a slack of 1s)
			if backoffTime-GossipSubUnsubscribeBackoff > time.Second {
				t.Error("Backoff time should be set to GossipSubUnsubscribeBackoff.")
			}
		}
		<-done

		done = make(chan struct{})
		// Ensure that remote peer 1 also applies the backoff appropriately
		// for peer 0.
		psubs[1].rt.(*GossipSubRouter).p.eval <- func() {
			defer close(done)
			peerMap2 := psubs[1].rt.(*GossipSubRouter).backoff["test"]
			if len(peerMap2) != 1 {
				t.Fatalf("No peer is populated in the backoff map for peer 1")
			}
			_, ok := peerMap2[h[0].ID()]
			if !ok {
				t.Errorf("Expected peer does not exist in the backoff map")
			}

			backoffTime := peerMap2[h[0].ID()].Sub(leaveTime)
			// Check that the backoff time is roughly the unsubscribebackoff time (with a slack of 1s)
			if backoffTime-GossipSubUnsubscribeBackoff > time.Second {
				t.Error("Backoff time should be set to GossipSubUnsubscribeBackoff.")
			}
		}
		<-done
	})
}

// withRouter is a race-free way of accessing state from the PubSubRouter.
// It runs the callback synchronously
func withRouter(p *PubSub, f func(r PubSubRouter)) {
	done := make(chan struct{})
	p.eval <- func() {
		defer close(done)
		router := p.rt
		f(router)
	}
	<-done
}

// withGSRouter is a race-free way of accessing state from the GossipSubRouter.
// It runs the callback synchronously
func withGSRouter(p *PubSub, f func(r *GossipSubRouter)) {
	withRouter(p, func(r PubSubRouter) {
		router := p.rt.(*GossipSubRouter)
		f(router)
	})
}

func TestGossipSubJoinTopic(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h := getDefaultHosts(t, 3)
		psubs := []*PubSub{
			getGossipsub(ctx, h[0]),
			getGossipsub(ctx, h[1]),
			getGossipsub(ctx, h[2]),
		}

		connect(t, h[0], h[1])
		connect(t, h[0], h[2])

		// Add in backoff for peer.
		peerMap := make(map[peer.ID]time.Time)
		withGSRouter(psubs[0], func(router0 *GossipSubRouter) {
			peerMap[h[1].ID()] = time.Now().Add(router0.params.UnsubscribeBackoff)
		})

		withGSRouter(psubs[0], func(router0 *GossipSubRouter) {
			router0.backoff["test"] = peerMap
		})

		// Join all peers
		for _, ps := range psubs {
			_, err := ps.Subscribe("test")
			if err != nil {
				t.Fatal(err)
			}
		}

		time.Sleep(time.Second)

		withGSRouter(psubs[0], func(router0 *GossipSubRouter) {
			meshMap := router0.mesh["test"]
			if len(meshMap) != 1 {
				t.Fatalf("Unexpect peer included in the mesh")
			}
			_, ok := meshMap[h[1].ID()]
			if ok {
				t.Fatalf("Peer that was to be backed off is included in the mesh")
			}
		})
	})
}

type sybilSquatter struct {
	h            host.Host
	ignoreErrors bool // set to false to ignore connection/stream errors.
}

func (sq *sybilSquatter) handleStream(s network.Stream) {
	defer s.Close()

	os, err := sq.h.NewStream(context.Background(), s.Conn().RemotePeer(), GossipSubID_v10)
	if err != nil {
		if !sq.ignoreErrors {
			panic(err)
		}
		return
	}

	// send a subscription for test in the output stream to become candidate for GRAFT
	// and then just read and ignore the incoming RPCs
	r := pbio.NewDelimitedReader(s, 1<<20)
	w := pbio.NewDelimitedWriter(os)
	truth := true
	topic := "test"
	err = w.WriteMsg(&pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Subscribe: &truth, Topicid: &topic}}})
	if err != nil {
		if !sq.ignoreErrors {
			panic(err)
		}
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
	}
}

func TestGossipsubPeerScoreInspect(t *testing.T) {
	// this test exercises the code path sof peer score inspection
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)

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

		for i := range 20 {
			msg := fmt.Appendf(nil, "message %d", i)
			psubs[i%2].Publish("test", msg)
			time.Sleep(20 * time.Millisecond)
		}

		time.Sleep(time.Second + 200*time.Millisecond)

		score2 := inspector.score(hosts[1].ID())
		if score2 < 9 {
			t.Fatalf("expected score to be at least 9, instead got %f", score2)
		}
	})
}

func TestGossipsubPeerFeedback(t *testing.T) {
	// this test exercises the code path sof peer score inspection
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)

		inspector := &mockPeerScoreInspector{}
		psub1 := getGossipsub(ctx, hosts[0],
			WithPeerScore(
				&PeerScoreParams{
					Topics: map[string]*TopicScoreParams{
						"test": {
							TopicWeight:                    1,
							TimeInMeshQuantum:              time.Second,
							FirstMessageDeliveriesWeight:   10,
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
		_ = getGossipsub(ctx, hosts[1])

		connect(t, hosts[0], hosts[1])
		time.Sleep(500 * time.Millisecond) // Wait for nodes to connect

		var err error
		err = errors.Join(err, psub1.PeerFeedback("test", hosts[1].ID(), PeerFeedbackUsefulMessage))
		err = errors.Join(err, psub1.PeerFeedback("test", hosts[1].ID(), PeerFeedbackUsefulMessage))
		err = errors.Join(err, psub1.PeerFeedback("test", hosts[1].ID(), PeerFeedbackUsefulMessage))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond) // Wait for feedback to be incorporated

		score2 := inspector.score(hosts[1].ID())
		if score2 < 9 {
			t.Fatalf("expected score to be at least 9, instead got %f", score2)
		}
	})
}

func TestSyncEvalAmidstCancellation(t *testing.T) {
	t.Run("After Cancellation", func(t *testing.T) {
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			hosts := getDefaultHosts(t, 1)
			psub := getGossipsub(ctx, hosts[0])

			cancel()
			synctest.Wait()

			if err := psub.syncEval(func() {}); !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		})
	})

	t.Run("During Cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		hosts := getDefaultHosts(t, 1)
		psub := getGossipsub(ctx, hosts[0])

		evalStarted := make(chan struct{})
		unblockEval := make(chan struct{})
		defer close(unblockEval)

		errCh := make(chan error, 1)
		go func() {
			errCh <- psub.syncEval(func() {
				close(evalStarted)
				<-unblockEval
			})
		}()

		select {
		case <-evalStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for pubsub eval loop")
		}

		// Cancel the pubsub's context while the evaluation function is running.
		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("PeerFeedback blocked after pubsub shutdown")
		}
	})
}

func TestGossipsubPeerScoreResetTopicParams(t *testing.T) {
	// this test exercises the code path sof peer score inspection
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 1)

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
	})
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
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)
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
		for range nMessages {
			msg := make([]byte, msgSize)
			crand.Read(msg)
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
	})
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
	r := pbio.NewDelimitedReader(s, 1<<20)
	w := pbio.NewDelimitedWriter(os)
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

			out := rpcWithControl(nil, nil, iwants, nil, prunes, nil)
			err = w.WriteMsg(out)
			if err != nil {
				panic(err)
			}
		}
		iwe.lk.Unlock()
	}
}

func validRPCSizes(slice []*RPC, limit int) bool {
	for _, rpc := range slice {
		if proto.Size(&rpc.RPC) > limit {
			return false
		}
	}
	return true
}

func TestFragmentRPCFunction(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		fragmentRPC := func(rpc *RPC, limit int) ([]*RPC, error) {
			rpcs := slices.Collect(rpc.split(limit, limit))
			if allValid := validRPCSizes(rpcs, limit); !allValid {
				return rpcs, fmt.Errorf("RPC size exceeds limit")
			}
			return rpcs, nil
		}

		p := peer.ID("some-peer")
		topic := "test"
		rpc := &RPC{from: p}
		limit := 1024

		mkMsg := func(size int) *pb.Message {
			msg := &pb.Message{}
			msg.Data = make([]byte, size-4) // subtract the protobuf overhead, so proto.Size(msg) returns requested size
			crand.Read(msg.Data)
			return msg
		}

		ensureBelowLimit := func(rpcs []*RPC) {
			for _, r := range rpcs {
				if proto.Size(&r.RPC) > limit {
					t.Fatalf("expected fragmented RPC to be below %d bytes, was %d", limit, proto.Size(&r.RPC))
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
			t.Fatalf("expected single RPC if input is < limit, got %d %#v", len(results), results)
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
		for i := range nMessages {
			rpc.Publish[i] = mkMsg(msgSize)
		}
		results, err = fragmentRPC(rpc, limit)
		if err != nil {
			t.Fatal(err)
		}
		ensureBelowLimit(results)
		msgsPerRPC := limit / msgSize
		expectedRPCs := nMessages / msgsPerRPC
		if len(results) > expectedRPCs+1 {
			t.Fatalf("expected around %d RPC messages in output, got %d", expectedRPCs, len(results))
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
		originalBytes, err := proto.Marshal(rpc.Control)
		if err != nil {
			t.Fatal(err)
		}
		receivedBytes, err := proto.Marshal(ctl)
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
		for i := range nTopics {
			messageIds := make([]string, msgsPerTopic)
			for m := range msgsPerTopic {
				mid := make([]byte, messageIdSize)
				crand.Read(mid)
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
		minExpectedCtl := proto.Size(rpc.Control) / limit
		minExpectedRPCs := (nMessages / msgsPerRPC) + minExpectedCtl
		if len(results) < minExpectedRPCs {
			t.Fatalf("expected at least %d total RPCs (at least %d with control messages), got %d total", expectedRPCs, expectedCtrl, len(results))
		}

		// Control messages should split on the control limit even if the
		// full RPC would fit within the RPC size limit.
		rpc.Reset()
		controlLimit := 256
		maxRPCSize := limit * 8
		messageIds := make([]string, 64)
		for i := range messageIds {
			mid := make([]byte, 16)
			crand.Read(mid)
			messageIds[i] = string(mid)
		}
		rpc.Control = &pb.ControlMessage{
			Iwant:     []*pb.ControlIWant{{MessageIDs: messageIds}},
			Idontwant: []*pb.ControlIDontWant{{MessageIDs: messageIds}},
		}
		results = slices.Collect(rpc.split(maxRPCSize, controlLimit))
		if len(results) < 2 {
			t.Fatalf("expected control message to be split by control limit, got %d RPCs", len(results))
		}
		var fragmentedMessageIds []string
		var fragmentedIDontWantMessageIds []string
		for _, r := range results {
			if size := proto.Size(&r.RPC); size > maxRPCSize {
				t.Fatalf("expected fragmented RPC to be below %d bytes, was %d", maxRPCSize, size)
			}
			if size := controlRPCSize(r); size > controlLimit {
				t.Fatalf("expected fragmented control message to be below %d bytes, was %d", controlLimit, size)
			}
			for _, iwant := range r.Control.GetIwant() {
				fragmentedMessageIds = append(fragmentedMessageIds, iwant.MessageIDs...)
			}
			for _, idontwant := range r.Control.GetIdontwant() {
				fragmentedIDontWantMessageIds = append(fragmentedIDontWantMessageIds, idontwant.MessageIDs...)
			}
		}
		if !slices.Equal(messageIds, fragmentedMessageIds) {
			t.Fatal("expected fragmented IWANT control messages to contain the same message IDs as input")
		}
		if !slices.Equal(messageIds, fragmentedIDontWantMessageIds) {
			t.Fatal("expected fragmented IDONTWANT control messages to contain the same message IDs as input")
		}

		rpc.Reset()
		rpc.Subscriptions = make([]*pb.RPC_SubOpts, 32)
		for i := range rpc.Subscriptions {
			topic := fmt.Sprintf("subscription-topic-%d", i)
			rpc.Subscriptions[i] = &pb.RPC_SubOpts{Topicid: &topic}
		}
		results = slices.Collect(rpc.split(maxRPCSize, controlLimit))
		if len(results) < 2 {
			t.Fatalf("expected subscriptions to be split by control limit, got %d RPCs", len(results))
		}
		var nSubscriptionsFragmented int
		for _, r := range results {
			if size := controlRPCSize(r); size > controlLimit {
				t.Fatalf("expected fragmented subscriptions to be below %d bytes, was %d", controlLimit, size)
			}
			nSubscriptionsFragmented += len(r.Subscriptions)
		}
		if nSubscriptionsFragmented != len(rpc.Subscriptions) {
			t.Fatalf("expected fragmented RPCs to contain same number of subscriptions as input, got %d / %d",
				nSubscriptionsFragmented, len(rpc.Subscriptions))
		}

		rpc.Reset()
		rpc.Partial = &pb.PartialMessagesExtension{PartialMessage: make([]byte, controlLimit*2)}
		results = slices.Collect(rpc.split(maxRPCSize, controlLimit))
		if len(results) != 1 {
			t.Fatalf("expected partial message extension to be excluded from control limit, got %d RPCs", len(results))
		}
		if results[0].Partial == nil {
			t.Fatal("expected partial message extension to be preserved")
		}
		if size := controlRPCSize(results[0]); size != 0 {
			t.Fatalf("expected partial-only RPC control size to be 0, got %d", size)
		}

		// Test the pathological case where a single gossip message ID exceeds the limit.
		// It should not be present in the fragmented messages, but smaller IDs should be
		rpc.Reset()
		giantIdBytes := make([]byte, limit*2)
		crand.Read(giantIdBytes)
		rpc.Control = &pb.ControlMessage{
			Iwant: []*pb.ControlIWant{
				{MessageIDs: []string{"hello", string(giantIdBytes)}},
			},
		}
		results, _ = fragmentRPC(rpc, limit)

		// The old behavior would silently drop the giant ID.
		// Now we return a the giant ID in a RPC by itself so that it can be
		// dropped before actually sending the RPC. This lets us log the anamoly.
		// To keep this test useful, we implement the old behavior here.
		filtered := make([]*RPC, 0, len(results))
		for _, r := range results {
			if proto.Size(&r.RPC) < limit {
				filtered = append(filtered, r)
			}
		}
		results = filtered
		err = nil
		if !validRPCSizes(results, limit) {
			err = fmt.Errorf("RPC size exceeds limit")
		}

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
	})
}

func FuzzRPCSplit(f *testing.F) {
	minMaxMsgSize := 100
	maxMaxMsgSize := 2048
	f.Fuzz(func(t *testing.T, data []byte) {
		maxSize := max(int(generateU16(&data))%maxMaxMsgSize, minMaxMsgSize)
		rpc := generateRPC(data, maxSize)

		originalControl := compressedRPC{ihave: make(map[string][]string)}
		originalControl.append(&rpc.RPC)
		mergedControl := compressedRPC{ihave: make(map[string][]string)}

		for rpc := range rpc.split(maxSize, maxSize) {
			if proto.Size(&rpc.RPC) > maxSize {
				t.Fatalf("invalid RPC size %v %d (max=%d)", rpc, proto.Size(&rpc.RPC), maxSize)
			}
			mergedControl.append(&rpc.RPC)
		}

		if !originalControl.equal(&mergedControl) {
			t.Fatalf("control mismatch: \n%#v\n%#v\n", originalControl, mergedControl)

		}
	})
}

type compressedRPC struct {
	msgs      [][]byte
	iwant     []string
	ihave     map[string][]string // topic -> []string
	idontwant []string
	prune     [][]byte
	graft     []string // list of topic
}

func (c *compressedRPC) equal(o *compressedRPC) bool {
	equalBytesSlices := func(a, b [][]byte) bool {
		return slices.EqualFunc(a, b, func(e1 []byte, e2 []byte) bool {
			return bytes.Equal(e1, e2)
		})
	}
	if !equalBytesSlices(c.msgs, o.msgs) {
		return false
	}

	if !slices.Equal(c.iwant, o.iwant) ||
		!slices.Equal(c.idontwant, o.idontwant) ||
		!equalBytesSlices(c.prune, o.prune) ||
		!slices.Equal(c.graft, o.graft) {
		return false
	}

	if len(c.ihave) != len(o.ihave) {
		return false
	}
	for topic, ids := range c.ihave {
		if !slices.Equal(ids, o.ihave[topic]) {
			return false
		}
	}

	return true

}

func (c *compressedRPC) append(rpc *pb.RPC) {
	for _, m := range rpc.Publish {
		d, err := proto.Marshal(m)
		if err != nil {
			panic(err)
		}
		c.msgs = append(c.msgs, d)
	}

	ctrl := rpc.Control
	if ctrl == nil {
		return
	}
	for _, iwant := range ctrl.Iwant {
		c.iwant = append(c.iwant, iwant.MessageIDs...)
		c.iwant = slices.DeleteFunc(c.iwant, func(e string) bool { return len(e) == 0 })
	}
	for _, ihave := range ctrl.Ihave {
		c.ihave[*ihave.TopicID] = append(c.ihave[*ihave.TopicID], ihave.MessageIDs...)
		c.ihave[*ihave.TopicID] = slices.DeleteFunc(c.ihave[*ihave.TopicID], func(e string) bool { return len(e) == 0 })
	}
	for _, idontwant := range ctrl.Idontwant {
		c.idontwant = append(c.idontwant, idontwant.MessageIDs...)
		c.idontwant = slices.DeleteFunc(c.idontwant, func(e string) bool { return len(e) == 0 })
	}
	for _, prune := range ctrl.Prune {
		d, err := proto.Marshal(prune)
		if err != nil {
			panic(err)
		}
		c.prune = append(c.prune, d)
	}
	for _, graft := range ctrl.Graft {
		c.graft = append(c.graft, *graft.TopicID)
		c.graft = slices.DeleteFunc(c.graft, func(e string) bool { return len(e) == 0 })
	}
}

func TestGossipsubManagesAnAddressBook(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Create a pair of hosts
		hosts := getDefaultHosts(t, 2)

		psubs := getGossipsubs(ctx, hosts)
		connectAll(t, hosts)

		// wait for identify events to propagate
		time.Sleep(time.Second)

		// Check that the address book is populated
		cab, ok := peerstore.GetCertifiedAddrBook(psubs[0].rt.(*GossipSubRouter).cab)
		if !ok {
			t.Fatalf("expected a certified address book")
		}

		env := cab.GetPeerRecord(hosts[1].ID())
		if env == nil {
			t.Fatalf("expected a peer record for host 1")
		}

		// Disconnect host 1. Host 0 should then update the TTL of the address book
		for _, c := range hosts[1].Network().Conns() {
			c.Close()
		}
		time.Sleep(time.Second)

		// This only updates addrs that are marked as recently connected, which should be all of them
		psubs[0].rt.(*GossipSubRouter).cab.UpdateAddrs(hosts[1].ID(), peerstore.RecentlyConnectedAddrTTL, 0)
		addrs := psubs[0].rt.(*GossipSubRouter).cab.Addrs(hosts[1].ID())
		// There should be no Addrs left because we cleared all recently connected ones.
		if len(addrs) != 0 {
			t.Fatalf("expected no addrs, got %d addrs", len(addrs))
		}
	})
}

func TestGossipsubIdontwantSend(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 3)

		msgID := func(pmsg *pb.Message) string {
			// silly content-based test message-ID: just use the data as whole
			return base64.URLEncoding.EncodeToString(pmsg.Data)
		}

		var validated atomic.Bool
		validate := func(context.Context, peer.ID, *Message) bool {
			time.Sleep(100 * time.Millisecond)
			validated.Store(true)
			return true
		}

		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 16

		psubs := make([]*PubSub, 2)
		psubs[0] = getGossipsub(ctx, hosts[0],
			WithGossipSubParams(params),
			WithMessageIdFn(msgID))
		psubs[1] = getGossipsub(ctx, hosts[1],
			WithGossipSubParams(params),
			WithMessageIdFn(msgID),
			WithDefaultValidator(validate))

		topic := "foobar"
		for _, ps := range psubs {
			_, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}
		}

		var midsMu sync.Mutex
		var expMids []string
		var actMids []string

		// Used to publish a message with random data
		publishMsg := func() {
			midsMu.Lock()
			defer midsMu.Unlock()
			data := make([]byte, 16)
			crand.Read(data)
			m := &pb.Message{Data: data}
			expMids = append(expMids, msgID(m))

			if err := psubs[0].Publish(topic, data); err != nil {
				t.Fatal(err)
			}
		}

		// Wait a bit after the last message before checking we got the right messages
		msgWaitMax := time.Second
		msgTimer := time.NewTimer(msgWaitMax)

		// Checks we received the right IDONTWANT messages
		checkMsgs := func() {
			midsMu.Lock()
			defer midsMu.Unlock()
			sort.Strings(actMids)
			sort.Strings(expMids)

			if len(actMids) != len(expMids) {
				t.Fatalf("Expected %d IDONTWANT messages, got %d", len(expMids), len(actMids))
			}
			for i, expMid := range expMids {
				actMid := actMids[i]
				if actMid != expMid {
					t.Fatalf("Expected the id of %s in the %d'th IDONTWANT messages, got %s", expMid, i+1, actMid)
				}
			}
		}

		// Wait for the timer to expire
		go func() {
			select {
			case <-msgTimer.C:
				checkMsgs()
				cancel()
				return
			case <-ctx.Done():
				checkMsgs()
			}
		}()

		newMockGS(ctx, t, hosts[2], func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
			// When the middle peer connects it will send us its subscriptions
			for _, sub := range irpc.GetSubscriptions() {
				if sub.GetSubscribe() {
					// Reply by subcribing to the topic and grafting to the middle peer
					writeMsg(&pb.RPC{
						Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
						Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
					})

					go func() {
						// Wait for a short interval to make sure the middle peer
						// received and processed the subscribe + graft
						time.Sleep(100 * time.Millisecond)

						// Publish messages from the first peer
						for range 10 {
							publishMsg()
						}
					}()
				}
			}

			// Each time the middle peer sends an IDONTWANT message
			for _, idonthave := range irpc.GetControl().GetIdontwant() {
				// If true, it means that, when we get IDONTWANT, the middle peer has done validation
				// already, which should not be the case
				if validated.Load() {
					t.Fatalf("IDONTWANT should be sent before doing validation")
				}
				for _, mid := range idonthave.GetMessageIDs() {
					// Add the message to the list and reset the timer
					actMids = append(actMids, mid)
					msgTimer.Reset(msgWaitMax)
				}
			}
		})

		connect(t, hosts[0], hosts[1])
		connect(t, hosts[1], hosts[2])

		<-ctx.Done()
	})
}

func TestGossipsubIdontwantReceive(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 3)

		msgID := func(pmsg *pb.Message) string {
			// silly content-based test message-ID: just use the data as whole
			return base64.URLEncoding.EncodeToString(pmsg.Data)
		}

		psubs := make([]*PubSub, 2)
		psubs[0] = getGossipsub(ctx, hosts[0], WithMessageIdFn(msgID))
		psubs[1] = getGossipsub(ctx, hosts[1], WithMessageIdFn(msgID))

		topic := "foobar"
		for _, ps := range psubs {
			_, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Wait a bit after the last message before checking the result
		msgWaitMax := time.Second
		msgTimer := time.NewTimer(msgWaitMax)

		// Checks we received no messages
		received := false
		checkMsgs := func() {
			if received {
				t.Fatalf("Expected no messages received after IDONWANT")
			}
		}

		// Wait for the timer to expire
		go func() {
			select {
			case <-msgTimer.C:
				checkMsgs()
				cancel()
				return
			case <-ctx.Done():
				checkMsgs()
			}
		}()

		newMockGS(ctx, t, hosts[2], func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
			// Check if it receives any message
			if len(irpc.GetPublish()) > 0 {
				received = true
			}
			// When the middle peer connects it will send us its subscriptions
			for _, sub := range irpc.GetSubscriptions() {
				if sub.GetSubscribe() {
					// Reply by subcribing to the topic and grafting to the middle peer
					writeMsg(&pb.RPC{
						Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
						Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
					})

					go func() {
						// Wait for a short interval to make sure the middle peer
						// received and processed the subscribe + graft
						time.Sleep(100 * time.Millisecond)

						// Generate a message and send IDONTWANT to the middle peer
						data := make([]byte, 16)
						crand.Read(data)
						mid := msgID(&pb.Message{Data: data})
						writeMsg(&pb.RPC{
							Control: &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{mid}}}},
						})

						// Wait for a short interval to make sure the middle peer
						// received and processed the IDONTWANTs
						time.Sleep(100 * time.Millisecond)

						// Publish the message from the first peer
						if err := psubs[0].Publish(topic, data); err != nil {
							t.Error(err)
							return // cannot call t.Fatal in a non-test goroutine
						}
					}()
				}
			}
		})

		connect(t, hosts[0], hosts[1])
		connect(t, hosts[1], hosts[2])

		<-ctx.Done()
	})
}

type mockRawTracer struct {
	onRecvRPC func(*RPC)
}

func (m *mockRawTracer) RecvRPC(rpc *RPC) {
	if m.onRecvRPC != nil {
		m.onRecvRPC(rpc)
	}
}

func (m *mockRawTracer) OnNewOutboundStream(p peer.ID, proto protocol.ID) {}
func (m *mockRawTracer) DeliverMessage(msg *Message)                      {}
func (m *mockRawTracer) DropRPC(rpc *RPC, p peer.ID)                      {}
func (m *mockRawTracer) DuplicateMessage(msg *Message)                    {}
func (m *mockRawTracer) Graft(p peer.ID, topic string)                    {}
func (m *mockRawTracer) Join(topic string)                                {}
func (m *mockRawTracer) Leave(topic string)                               {}
func (m *mockRawTracer) Prune(p peer.ID, topic string)                    {}
func (m *mockRawTracer) RejectMessage(msg *Message, reason string)        {}
func (m *mockRawTracer) OnClosedOutboundStream(p peer.ID)                 {}
func (m *mockRawTracer) SendRPC(rpc *RPC, p peer.ID)                      {}
func (m *mockRawTracer) SendRPCCompleted(*RPC, peer.ID, time.Duration)    {}
func (m *mockRawTracer) ThrottlePeer(p peer.ID)                           {}
func (m *mockRawTracer) UndeliverableMessage(msg *Message)                {}
func (m *mockRawTracer) ValidateMessage(msg *Message)                     {}

var _ RawTracer = &mockRawTracer{}

func TestGossipsubNoIDONTWANTToMessageSender(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		denseConnect(t, hosts)

		psubs := make([]*PubSub, 2)

		receivedIDONTWANT := make(chan struct{})
		psubs[0] = getGossipsub(ctx, hosts[0], WithRawTracer(&mockRawTracer{
			onRecvRPC: func(rpc *RPC) {
				if len(rpc.GetControl().GetIdontwant()) > 0 {
					close(receivedIDONTWANT)
				}
			},
		}))
		psubs[1] = getGossipsub(ctx, hosts[1])

		topicString := "foobar"
		var topics []*Topic
		for _, ps := range psubs {
			topic, err := ps.Join(topicString)
			if err != nil {
				t.Fatal(err)
			}
			topics = append(topics, topic)

			_, err = ps.Subscribe(topicString)
			if err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)

		msg := make([]byte, GossipSubIDontWantMessageThreshold+1)
		topics[0].Publish(ctx, msg)

		select {
		case <-receivedIDONTWANT:
			t.Fatal("IDONTWANT should not be sent to the message sender")
		case <-time.After(time.Second):
		}
	})
}

func TestGossipsubIDONTWANTBeforeFirstPublish(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		denseConnect(t, hosts)

		psubs := make([]*PubSub, 2)

		psubs[0] = getGossipsub(ctx, hosts[0])
		rpcsReceived := make(chan string)
		psubs[1] = getGossipsub(ctx, hosts[1], WithRawTracer(&mockRawTracer{
			onRecvRPC: func(rpc *RPC) {
				if len(rpc.GetControl().GetIdontwant()) > 0 {
					rpcsReceived <- "idontwant"
				}
				if len(rpc.GetPublish()) > 0 {
					rpcsReceived <- "publish"
				}
			},
		}))

		topicString := "foobar"
		var topics []*Topic
		for _, ps := range psubs {
			topic, err := ps.Join(topicString)
			if err != nil {
				t.Fatal(err)
			}
			topics = append(topics, topic)

			_, err = ps.Subscribe(topicString)
			if err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(2 * time.Second)

		msg := make([]byte, GossipSubIDontWantMessageThreshold+1)
		_ = topics[0].Publish(ctx, msg)

		timeout := time.After(5 * time.Second)

		select {
		case kind := <-rpcsReceived:
			if kind == "publish" {
				t.Fatal("IDONTWANT should be sent before publish")
			}
		case <-timeout:
			t.Fatal("IDONTWANT should be sent on first publish")
		}

		select {
		case kind := <-rpcsReceived:
			if kind != "publish" {
				t.Fatal("Expected publish after IDONTWANT")
			}
		case <-timeout:
			t.Fatal("Expected publish after IDONTWANT")
		}
	})

}

// Test that non-mesh peers will not get IDONTWANT
func TestGossipsubIdontwantNonMesh(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 3)

		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 16
		psubs := getGossipsubs(ctx, hosts[:2], WithGossipSubParams(params))

		topic := "foobar"
		for _, ps := range psubs {
			_, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Used to publish a message with random data
		publishMsg := func() {
			data := make([]byte, 16)
			crand.Read(data)

			if err := psubs[0].Publish(topic, data); err != nil {
				t.Fatal(err)
			}
		}

		// Wait a bit after the last message before checking we got the right messages
		msgWaitMax := time.Second
		msgTimer := time.NewTimer(msgWaitMax)
		received := false

		// Checks if we received any IDONTWANT
		checkMsgs := func() {
			if received {
				t.Fatalf("No IDONTWANT is expected")
			}
		}

		// Wait for the timer to expire
		go func() {
			select {
			case <-msgTimer.C:
				checkMsgs()
				cancel()
				return
			case <-ctx.Done():
				checkMsgs()
			}
		}()

		newMockGS(ctx, t, hosts[2], func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
			// When the middle peer connects it will send us its subscriptions
			for _, sub := range irpc.GetSubscriptions() {
				if sub.GetSubscribe() {
					// Reply by subcribing to the topic and pruning to the middle peer to make sure
					// that it's not in the mesh
					writeMsg(&pb.RPC{
						Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
						Control:       &pb.ControlMessage{Prune: []*pb.ControlPrune{{TopicID: sub.Topicid}}},
					})

					go func() {
						// Wait for a short interval to make sure the middle peer
						// received and processed the subscribe
						time.Sleep(100 * time.Millisecond)

						// Publish messages from the first peer
						for range 10 {
							publishMsg()
						}
					}()
				}
			}

			// Each time the middle peer sends an IDONTWANT message
			for range irpc.GetControl().GetIdontwant() {
				received = true
			}
		})

		connect(t, hosts[0], hosts[1])
		connect(t, hosts[1], hosts[2])

		<-ctx.Done()
	})
}

// Test that peers with incompatible versions will not get IDONTWANT
func TestGossipsubIdontwantIncompat(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 3)

		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 16
		psubs := getGossipsubs(ctx, hosts[:2], WithGossipSubParams(params))

		topic := "foobar"
		for _, ps := range psubs {
			_, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Used to publish a message with random data
		publishMsg := func() {
			data := make([]byte, 16)
			crand.Read(data)

			if err := psubs[0].Publish(topic, data); err != nil {
				t.Fatal(err)
			}
		}

		// Wait a bit after the last message before checking we got the right messages
		msgWaitMax := time.Second
		msgTimer := time.NewTimer(msgWaitMax)
		received := false

		// Checks if we received any IDONTWANT
		checkMsgs := func() {
			if received {
				t.Fatalf("No IDONTWANT is expected")
			}
		}

		// Wait for the timer to expire
		go func() {
			select {
			case <-msgTimer.C:
				checkMsgs()
				cancel()
				return
			case <-ctx.Done():
				checkMsgs()
			}
		}()

		// Use the old GossipSub version
		newMockGSWithVersion(ctx, t, hosts[2], protocol.ID("/meshsub/1.1.0"), func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
			// When the middle peer connects it will send us its subscriptions
			for _, sub := range irpc.GetSubscriptions() {
				if sub.GetSubscribe() {
					// Reply by subcribing to the topic and grafting to the middle peer
					writeMsg(&pb.RPC{
						Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
						Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
					})

					go func() {
						// Wait for a short interval to make sure the middle peer
						// received and processed the subscribe + graft
						time.Sleep(100 * time.Millisecond)

						// Publish messages from the first peer
						for range 10 {
							publishMsg()
						}
					}()
				}
			}

			// Each time the middle peer sends an IDONTWANT message
			for range irpc.GetControl().GetIdontwant() {
				received = true
			}
		})

		connect(t, hosts[0], hosts[1])
		connect(t, hosts[1], hosts[2])

		<-ctx.Done()
	})
}

// Test that IDONTWANT will not be sent for small messages
func TestGossipsubIdontwantSmallMessage(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 3)

		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 16
		psubs := getGossipsubs(ctx, hosts[:2], WithGossipSubParams(params))

		topic := "foobar"
		for _, ps := range psubs {
			_, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Used to publish a message with random data
		publishMsg := func() {
			data := make([]byte, 8)
			crand.Read(data)

			if err := psubs[0].Publish(topic, data); err != nil {
				t.Fatal(err)
			}
		}

		// Wait a bit after the last message before checking we got the right messages
		msgWaitMax := time.Second
		msgTimer := time.NewTimer(msgWaitMax)
		received := false

		// Checks if we received any IDONTWANT
		checkMsgs := func() {
			if received {
				t.Fatalf("No IDONTWANT is expected")
			}
		}

		// Wait for the timer to expire
		go func() {
			select {
			case <-msgTimer.C:
				checkMsgs()
				cancel()
				return
			case <-ctx.Done():
				checkMsgs()
			}
		}()

		newMockGS(ctx, t, hosts[2], func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
			// When the middle peer connects it will send us its subscriptions
			for _, sub := range irpc.GetSubscriptions() {
				if sub.GetSubscribe() {
					// Reply by subcribing to the topic and pruning to the middle peer to make sure
					// that it's not in the mesh
					writeMsg(&pb.RPC{
						Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
						Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
					})

					go func() {
						// Wait for a short interval to make sure the middle peer
						// received and processed the subscribe
						time.Sleep(100 * time.Millisecond)

						// Publish messages from the first peer
						for range 10 {
							publishMsg()
						}
					}()
				}
			}

			// Each time the middle peer sends an IDONTWANT message
			for range irpc.GetControl().GetIdontwant() {
				received = true
			}
		})

		connect(t, hosts[0], hosts[1])
		connect(t, hosts[1], hosts[2])

		<-ctx.Done()
	})
}

// Test that IWANT will have no effect after IDONTWANT is sent
func TestGossipsubIdontwantBeforeIwant(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 3)

		msgID := func(pmsg *pb.Message) string {
			// silly content-based test message-ID: just use the data as whole
			return base64.URLEncoding.EncodeToString(pmsg.Data)
		}

		psubs := make([]*PubSub, 2)
		psubs[0] = getGossipsub(ctx, hosts[0], WithMessageIdFn(msgID))
		psubs[1] = getGossipsub(ctx, hosts[1], WithMessageIdFn(msgID))

		topic := "foobar"
		for _, ps := range psubs {
			_, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Wait a bit after the last message before checking the result
		msgWaitMax := 2 * time.Second
		msgTimer := time.NewTimer(msgWaitMax)

		// Checks we received right messages
		var msgReceived atomic.Bool
		var ihaveReceived atomic.Bool
		checkMsgs := func() {
			if msgReceived.Load() {
				t.Fatalf("Expected no messages received after IDONWANT")
			}
			if !ihaveReceived.Load() {
				t.Fatalf("Expected IHAVE received")
			}
		}

		// Wait for the timer to expire
		go func() {
			select {
			case <-msgTimer.C:
				checkMsgs()
				cancel()
				return
			case <-ctx.Done():
				checkMsgs()
			}
		}()

		newMockGS(ctx, t, hosts[2], func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
			// Check if it receives any message
			if len(irpc.GetPublish()) > 0 {
				msgReceived.Store(true)
			}
			// The middle peer is supposed to send IHAVE
			for _, ihave := range irpc.GetControl().GetIhave() {
				ihaveReceived.Store(true)
				mids := ihave.GetMessageIDs()

				writeMsg(&pb.RPC{
					Control: &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: mids}}},
				})
				// Wait for the middle peer to process IDONTWANT
				time.Sleep(100 * time.Millisecond)
				writeMsg(&pb.RPC{
					Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: mids}}},
				})
			}
			// When the middle peer connects it will send us its subscriptions
			for _, sub := range irpc.GetSubscriptions() {
				if sub.GetSubscribe() {
					// Reply by subcribing to the topic and pruning to the middle peer to make sure
					// that it's not in the mesh
					writeMsg(&pb.RPC{
						Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
						Control:       &pb.ControlMessage{Prune: []*pb.ControlPrune{{TopicID: sub.Topicid}}},
					})

					go func() {
						// Wait for an interval to make sure the middle peer
						// received and processed the subscribe
						time.Sleep(100 * time.Millisecond)

						data := make([]byte, 16)
						crand.Read(data)

						// Publish the message from the first peer
						if err := psubs[0].Publish(topic, data); err != nil {
							t.Error(err)
							return // cannot call t.Fatal in a non-test goroutine
						}
					}()
				}
			}
		})

		connect(t, hosts[0], hosts[1])
		connect(t, hosts[1], hosts[2])

		<-ctx.Done()
	})
}

// Test that IDONTWANT will cleared when it's old enough
func TestGossipsubIdontwantClear(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 3)

		msgID := func(pmsg *pb.Message) string {
			// silly content-based test message-ID: just use the data as whole
			return base64.URLEncoding.EncodeToString(pmsg.Data)
		}

		psubs := make([]*PubSub, 2)
		psubs[0] = getGossipsub(ctx, hosts[0], WithMessageIdFn(msgID))
		psubs[1] = getGossipsub(ctx, hosts[1], WithMessageIdFn(msgID))

		topic := "foobar"
		for _, ps := range psubs {
			_, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Wait a bit after the last message before checking the result
		msgWaitMax := 5 * time.Second
		msgTimer := time.NewTimer(msgWaitMax)

		// Checks we received some message after the IDONTWANT is cleared
		var received atomic.Bool
		checkMsgs := func() {
			if !received.Load() {
				t.Fatalf("Expected some message after the IDONTWANT is cleared")
			}
		}

		// Wait for the timer to expire
		go func() {
			select {
			case <-msgTimer.C:
				checkMsgs()
				cancel()
				return
			case <-ctx.Done():
				checkMsgs()
			}
		}()

		newMockGS(ctx, t, hosts[2], func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
			// Check if it receives any message
			if len(irpc.GetPublish()) > 0 {
				received.Store(true)
			}
			// When the middle peer connects it will send us its subscriptions
			for _, sub := range irpc.GetSubscriptions() {
				if sub.GetSubscribe() {
					// Reply by subcribing to the topic and grafting to the middle peer
					writeMsg(&pb.RPC{
						Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
						Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
					})

					go func() {
						// Wait for a short interval to make sure the middle peer
						// received and processed the subscribe + graft
						time.Sleep(100 * time.Millisecond)

						// Generate a message and send IDONTWANT to the middle peer
						data := make([]byte, 16)
						crand.Read(data)
						mid := msgID(&pb.Message{Data: data})
						writeMsg(&pb.RPC{
							Control: &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{mid}}}},
						})

						// Wait for a short interval to make sure the middle peer
						// received and processed the IDONTWANTs
						time.Sleep(100 * time.Millisecond)

						// Wait for 4 heartbeats to make sure the IDONTWANT is cleared
						time.Sleep(4 * time.Second)

						// Publish the message from the first peer
						if err := psubs[0].Publish(topic, data); err != nil {
							t.Error(err)
							return // cannot call t.Fatal in a non-test goroutine
						}
					}()
				}
			}
		})

		connect(t, hosts[0], hosts[1])
		connect(t, hosts[1], hosts[2])

		<-ctx.Done()
	})
}

func TestGossipsubPruneMeshCorrectly(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 9)

		msgID := func(pmsg *pb.Message) string {
			// silly content-based test message-ID: just use the data as whole
			return base64.URLEncoding.EncodeToString(pmsg.Data)
		}

		params := DefaultGossipSubParams()
		params.Dhi = 8

		psubs := make([]*PubSub, 9)
		for i := range 9 {
			psubs[i] = getGossipsub(ctx, hosts[i],
				WithGossipSubParams(params),
				WithMessageIdFn(msgID))
		}

		topic := "foobar"
		for _, ps := range psubs {
			_, err := ps.Subscribe(topic)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Connect first peer with the rest of the 8 other
		// peers.
		for i := 1; i < 9; i++ {
			connect(t, hosts[0], hosts[i])
		}

		// Wait for 2 heartbeats to be able to prune excess peers back down to D.
		totalTimeToWait := params.HeartbeatInitialDelay + 2*params.HeartbeatInterval
		time.Sleep(totalTimeToWait)

		withGSRouter(psubs[0], func(rt *GossipSubRouter) {
			meshPeers, ok := rt.mesh[topic]
			if !ok {
				t.Fatal("mesh does not exist for topic")
			}
			if len(meshPeers) != params.D {
				t.Fatalf("mesh does not have the correct number of peers. Wanted %d but got %d", params.D, len(meshPeers))
			}
		})
	})
}

func BenchmarkAllocDoDropRPC(b *testing.B) {
	gs := GossipSubRouter{tracer: &pubsubTracer{}}

	for i := 0; i < b.N; i++ {
		gs.doDropRPC(&RPC{}, "peerID", "reason")
	}
}

func TestRoundRobinMessageIDScheduler(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		const maxNumPeers = 256
		const maxNumMessages = 1_000

		err := quick.Check(func(numPeers uint16, numMessages uint16) bool {
			numPeers = numPeers % maxNumPeers
			numMessages = numMessages % maxNumMessages

			output := make([]pendingRPC, 0, numMessages*numPeers)

			var strategy RoundRobinMessageIDScheduler

			peers := make([]peer.ID, numPeers)
			for i := 0; i < int(numPeers); i++ {
				peers[i] = peer.ID(fmt.Sprintf("peer%d", i))
			}

			getID := func(r pendingRPC) string {
				return string(r.rpc.Publish[0].Data)
			}

			for i := range int(numMessages) {
				for j := range int(numPeers) {
					strategy.AddRPC(peers[j], fmt.Sprintf("msg%d", i), &RPC{
						RPC: pb.RPC{
							Publish: []*pb.Message{
								{
									Data: fmt.Appendf(nil, "msg%d", i),
								},
							},
						},
					})
				}
			}

			for p, rpc := range strategy.All() {
				output = append(output, pendingRPC{
					peer: p,
					rpc:  rpc,
				})
			}

			// Check invariants
			// 1. The published rpcs count is the same as the number of messages added
			// 2. Before all message IDs are seen, no message ID may be repeated
			// 3. The set of message ID + peer ID combinations should be the same as the input

			// 1.
			expectedCount := int(numMessages) * int(numPeers)
			if len(output) != expectedCount {
				t.Logf("Expected %d RPCs, got %d", expectedCount, len(output))
				return false
			}

			// 2.
			seen := make(map[string]bool)
			expected := make(map[string]bool)
			for i := 0; i < int(numMessages); i++ {
				expected[fmt.Sprintf("msg%d", i)] = true
			}

			for _, rpc := range output {
				if expected[getID(rpc)] {
					delete(expected, getID(rpc))
				}
				if seen[getID(rpc)] && len(expected) > 0 {
					t.Logf("Message ID %s repeated before all message IDs are seen", getID(rpc))
					return false
				}
				seen[getID(rpc)] = true
			}

			// 3.
			inputSet := make(map[string]bool)
			for i := range int(numMessages) {
				for j := range int(numPeers) {
					inputSet[fmt.Sprintf("msg%d:peer%d", i, j)] = true
				}
			}
			for _, rpc := range output {
				if !inputSet[getID(rpc)+":"+string(rpc.peer)] {
					t.Logf("Message ID %s not in input", getID(rpc))
					return false
				}
			}
			return true
		}, &quick.Config{MaxCount: 32})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkRoundRobinMessageIDScheduler(b *testing.B) {
	const numPeers = 1_000
	const numMessages = 1_000
	var strategy RoundRobinMessageIDScheduler

	peers := make([]peer.ID, numPeers)
	for i := range int(numPeers) {
		peers[i] = peer.ID(fmt.Sprintf("peer%d", i))
	}
	msgs := make([]string, numMessages)
	for i := range numMessages {
		msgs[i] = fmt.Sprintf("msg%d", i)
	}

	emptyRPC := &RPC{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j := i % len(peers)
		msgIdx := i % numMessages
		strategy.AddRPC(peers[j], msgs[msgIdx], emptyRPC)
		if i%100 == 0 {
			for range strategy.All() {
			}
		}
	}
}

func TestMessageBatchPublish(t *testing.T) {
	concurrentAdds := []bool{false, true}
	for _, concurrentAdd := range concurrentAdds {
		t.Run(fmt.Sprintf("WithConcurrentAdd=%v", concurrentAdd), func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				hosts := getDefaultHosts(t, 20)

				msgIDFn := func(msg *pb.Message) string {
					hdr := string(msg.Data[0:16])
					msgID := strings.SplitN(hdr, " ", 2)
					return msgID[0]
				}
				const numMessages = 100
				// +8 to account for the gossiping overhead
				psubs := getGossipsubs(ctx, hosts, WithMessageIdFn(msgIDFn), WithPeerOutboundQueueSize(numMessages+8), WithValidateQueueSize(numMessages+8))

				var topics []*Topic
				var msgs []*Subscription
				for _, ps := range psubs {
					topic, err := ps.Join("foobar")
					if err != nil {
						t.Fatal(err)
					}
					topics = append(topics, topic)

					subch, err := topic.Subscribe(WithBufferSize(numMessages + 8))
					if err != nil {
						t.Fatal(err)
					}

					msgs = append(msgs, subch)
				}

				sparseConnect(t, hosts)

				// wait for heartbeats to build mesh
				time.Sleep(time.Second * 2)

				var batch MessageBatch
				var wg sync.WaitGroup
				for i := range numMessages {
					msg := fmt.Appendf(nil, "%d it's not a floooooood %d", i, i)
					if concurrentAdd {
						wg.Go(func() {
							err := topics[0].AddToBatch(ctx, &batch, msg)
							if err != nil {
								t.Log(err)
								t.Fail()
							}
						})
					} else {
						err := topics[0].AddToBatch(ctx, &batch, msg)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				wg.Wait()
				err := psubs[0].PublishBatch(&batch)
				if err != nil {
					t.Fatal(err)
				}

				for range numMessages {
					for _, sub := range msgs {
						got, err := sub.Next(ctx)
						if err != nil {
							t.Fatal(err)
						}
						id := msgIDFn(got.Message)
						expected := fmt.Appendf(nil, "%s it's not a floooooood %s", id, id)
						if !bytes.Equal(expected, got.Data) {
							t.Fatal("got wrong message!")
						}
					}
				}
			})
		})
	}
}

func TestPublishDuplicateMessage(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 1)
		psubs := getGossipsubs(ctx, hosts, WithMessageIdFn(func(msg *pb.Message) string {
			return string(msg.Data)
		}))
		topic, err := psubs[0].Join("foobar")
		if err != nil {
			t.Fatal(err)
		}
		err = topic.Publish(ctx, []byte("hello"))
		if err != nil {
			t.Fatal(err)
		}

		err = topic.Publish(ctx, []byte("hello"))
		if err != nil {
			t.Fatal("Duplicate message should not return an error")
		}
	})
}

func genNRpcs(tb testing.TB, n int, maxSize int) []*RPC {
	r := mrand2.NewChaCha8([32]byte{})
	rpcs := make([]*RPC, n)
	for i := range rpcs {
		var data [64]byte
		_, err := r.Read(data[:])
		if err != nil {
			tb.Fatal(err)
		}
		rpcs[i] = generateRPC(data[:], maxSize)
	}
	return rpcs
}

func BenchmarkSplitRPC(b *testing.B) {
	maxSize := 2048
	rpcs := genNRpcs(b, 100, maxSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rpc := rpcs[i%len(rpcs)]
		for range rpc.split(maxSize, maxSize) {
		}
	}
}

func BenchmarkSplitRPCLargeMessages(b *testing.B) {
	addToRPC := func(rpc *RPC, numMsgs int, msgSize int) {
		msgs := make([]*pb.Message, numMsgs)
		payload := make([]byte, msgSize)
		for i := range msgs {
			rpc.Publish = append(rpc.Publish, &pb.Message{
				Data: payload,
				From: []byte(strconv.Itoa(i)),
			})
		}
	}

	b.Run("Many large messages", func(b *testing.B) {
		r := mrand.New(mrand.NewSource(99))
		const numRPCs = 30
		const msgSize = 50 * 1024
		rpc := &RPC{}
		for range numRPCs {
			addToRPC(rpc, 20, msgSize+r.Intn(100))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for range rpc.split(DefaultMaxMessageSize, DefaultMaxControlMessageSize) {

			}
		}
	})

	b.Run("2 large messages", func(b *testing.B) {
		const numRPCs = 2
		const msgSize = DefaultMaxMessageSize - 100
		rpc := &RPC{}
		for range numRPCs {
			addToRPC(rpc, 1, msgSize)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count := 0
			for range rpc.split(DefaultMaxMessageSize, DefaultMaxControlMessageSize) {
				count++
			}
			if count != 2 {
				b.Fatalf("expected 2 RPCs, got %d", count)
			}
		}
	})
}

func TestCheckPubAddrsOnSignedPeerRecord(t *testing.T) {
	// generate keypair and peer ID
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.Default()

	t.Run("private addresses only", func(t *testing.T) {
		addr := "/ip4/192.168.1.1/tcp/4001"
		signed := makeSignedPeerRecord(t, id, []string{addr}, priv)
		if checkPubAddrsOnSignedPeerRecord(logger, id, signed) {
			t.Errorf("expected false for private addrs")
		}
	})

	t.Run("public address present", func(t *testing.T) {
		addr := "/ip4/8.8.8.8/tcp/4001"
		signed := makeSignedPeerRecord(t, id, []string{addr}, priv)
		if !checkPubAddrsOnSignedPeerRecord(logger, id, signed) {
			t.Errorf("expected true for public addr")
		}
	})

	t.Run("malformed record", func(t *testing.T) {
		if checkPubAddrsOnSignedPeerRecord(logger, id, &record.Envelope{}) {
			t.Errorf("expected false for malformed record")
		}
		if checkPubAddrsOnSignedPeerRecord(logger, id, nil) {
			t.Errorf("expected false for malformed record")
		}
	})
}

// helper to sign and marshal PeerRecord
func makeSignedPeerRecord(t *testing.T, id peer.ID, addrs []string, priv crypto.PrivKey) *record.Envelope {
	var maddrs []ma.Multiaddr
	for _, s := range addrs {
		m, err := ma.NewMultiaddr(s)
		if err != nil {
			t.Fatalf("failed to parse maddr: %v", err)
		}
		maddrs = append(maddrs, m)
	}
	rec := &peer.PeerRecord{PeerID: id, Addrs: maddrs}
	signed, err := record.Seal(rec, priv)
	if err != nil {
		t.Fatalf("failed to sign peer record: %v", err)
	}
	return signed
}

func TestDefaultPXRecordReducer(t *testing.T) {
	// generate keypair and peer ID
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.Default()

	t.Run("private addresses records only", func(t *testing.T) {
		addrs := []string{
			"/ip4/192.168.1.1/tcp/4001",
			"/ip4/192.168.1.2/tcp/4001",
		}
		px := make([]*pb.PeerInfo, 0)
		for _, addr := range addrs {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				t.Fatal(err)
			}
			signed := makeSignedPeerRecord(t, id, []string{addr}, priv)
			px = defaultPXRecordReducer(px, logger, id, []ma.Multiaddr{maddr}, signed)
		}
		if len(px) < len(addrs) {
			t.Fatalf("Expected as many px records (%d) as addresses (%d)", len(px), len(addrs))
		}
	})

	t.Run("private and public records", func(t *testing.T) {
		addrs := []string{
			"/ip4/192.168.1.1/tcp/4001",
			"/ip4/8.8.8.8/tcp/4001",
		}
		px := make([]*pb.PeerInfo, 0)
		for _, addr := range addrs {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				t.Fatal(err)
			}
			signed := makeSignedPeerRecord(t, id, []string{addr}, priv)
			px = defaultPXRecordReducer(px, logger, id, []ma.Multiaddr{maddr}, signed)
		}
		if len(px) < len(addrs) {
			t.Fatalf("Expected as many px records (%d) as addresses (%d)", len(px), len(addrs))
		}
	})

	t.Run("private and public but no records", func(t *testing.T) {
		addrs := []string{
			"/ip4/192.168.1.1/tcp/4001",
			"/ip4/8.8.8.8/tcp/4001",
		}
		px := make([]*pb.PeerInfo, 0)
		for _, addr := range addrs {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				t.Fatal(err)
			}
			px = defaultPXRecordReducer(px, logger, id, []ma.Multiaddr{maddr}, nil)
		}
		if len(px) < len(addrs) {
			t.Fatalf("Expected as many px records (%d) as addresses (%d)", len(px), len(addrs))
		}
		for _, indvPx := range px {
			if len(indvPx.SignedPeerRecord) > 0 {
				t.Fatalf("Expected no records as they are invalid, got %d", len(indvPx.SignedPeerRecord))
			}
		}
	})
}

func TestPublicAddrsPXRecordReducer(t *testing.T) {
	// generate keypair and peer ID
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.Default()

	t.Run("private records only", func(t *testing.T) {
		// should trigger the address public check before checking the record
		addrs := []string{
			"/ip4/192.168.1.1/tcp/4001",
			"/ip4/192.168.1.2/tcp/4001",
		}
		px := make([]*pb.PeerInfo, 0)
		for _, addr := range addrs {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				t.Fatal(err)
			}
			signed := makeSignedPeerRecord(t, id, []string{addr}, priv)
			px = publicAddrsPXRecordReducer(px, logger, id, []ma.Multiaddr{maddr}, signed)
		}
		if len(px) < len(addrs) {
			t.Fatalf("Expected as many px records (%d) as addresses (%d)", len(px), len(addrs))
		}
	})

	t.Run("public records only", func(t *testing.T) {
		addrs := []string{
			"/ip4/8.8.8.8/tcp/4001",
			"/ip4/8.8.8.8/tcp/4002",
		}
		px := make([]*pb.PeerInfo, 0)
		for _, addr := range addrs {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				t.Fatal(err)
			}
			signed := makeSignedPeerRecord(t, id, []string{addr}, priv)
			px = publicAddrsPXRecordReducer(px, logger, id, []ma.Multiaddr{maddr}, signed)
		}
		if len(px) < len(addrs) {
			t.Fatalf("Expected as many px records (%d) as addresses (%d)", len(px), len(addrs))
		}
		for _, indvPx := range px {
			if len(indvPx.SignedPeerRecord) == 0 {
				t.Fatalf("Expected some bytes on signed records, got %d", len(indvPx.SignedPeerRecord))
			}
		}
	})

	t.Run("private and public records", func(t *testing.T) {
		addrs := []string{
			"/ip4/192.168.1.1/tcp/4001",
			"/ip4/8.8.8.8/tcp/4001",
		}
		px := make([]*pb.PeerInfo, 0)
		for _, addr := range addrs {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				t.Fatal(err)
			}
			signed := makeSignedPeerRecord(t, id, []string{addr}, priv)
			px = publicAddrsPXRecordReducer(px, logger, id, []ma.Multiaddr{maddr}, signed)
		}
		if len(px) < len(addrs) {
			t.Fatalf("Expected as many px records (%d) as addresses (%d)", len(px), len(addrs))
		}
		if len(px[0].SignedPeerRecord) > 0 {
			t.Fatalf("Expected no bytes from priv addrs, got %d", len(px[0].SignedPeerRecord))
		}
		if len(px[1].SignedPeerRecord) == 0 {
			t.Fatalf("Expected some bytes from pub addrs, got %d", len(px[1].SignedPeerRecord))
		}
	})

	t.Run("public addrs but no records", func(t *testing.T) {
		addrs := []string{
			"/ip4/8.8.8.8/tcp/4001",
			"/ip4/8.8.8.8/tcp/4002",
		}
		px := make([]*pb.PeerInfo, 0)
		for _, addr := range addrs {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				t.Fatal(err)
			}
			px = publicAddrsPXRecordReducer(px, logger, id, []ma.Multiaddr{maddr}, nil)
		}
		if len(px) < len(addrs) {
			t.Fatalf("Expected as many px records (%d) as addresses (%d)", len(px), len(addrs))
		}
		for _, indvPx := range px {
			if len(indvPx.SignedPeerRecord) > 0 {
				t.Fatalf("Expected no records, got %d", len(indvPx.SignedPeerRecord))
			}
		}
	})

	t.Run("public addrs but prc records", func(t *testing.T) {
		privAddr := "/ip4/192.168.1.1/tcp/4001"
		addrs := []string{
			privAddr,
			"/ip4/8.8.8.8/tcp/4001",
		}
		px := make([]*pb.PeerInfo, 0, 2)
		for range addrs {
			maddr, err := ma.NewMultiaddr(privAddr)
			if err != nil {
				t.Fatal(err)
			}
			signed := makeSignedPeerRecord(t, id, []string{privAddr}, priv)
			px = publicAddrsPXRecordReducer(px, logger, id, []ma.Multiaddr{maddr}, signed)
		}
		if len(px) < len(addrs) {
			t.Fatalf("Expected as many px records (%d) as addresses (%d)", len(px), len(addrs))
		}
		for _, indvPx := range px {
			if len(indvPx.SignedPeerRecord) > 0 {
				t.Fatalf("Expected no records, got %d", len(indvPx.SignedPeerRecord))
			}
		}
	})
}

type skeletonGossipsub struct {
	outRPC <-chan *pb.RPC
	inRPC  chan<- *pb.RPC
}

func newSkeletonGossipsub(ctx context.Context, h host.Host) *skeletonGossipsub {
	recvRPC := make(chan *pb.RPC, 16)
	sendRPC := make(chan *pb.RPC, 16)
	logger := slog.Default().With("id", h.ID())

	h.SetStreamHandler(GossipSubID_v13, func(s network.Stream) {
		// Open outbound stream to send writes too
		outboundStream, err := h.NewStream(context.Background(), s.Conn().RemotePeer(), GossipSubID_v13)
		if err != nil {
			panic(err)
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func(ctx context.Context) {
			defer outboundStream.Close()
			w := msgio.NewVarintWriter(outboundStream)
			for {
				select {
				case <-ctx.Done():
					return
				case r := <-sendRPC:
					b, err := proto.Marshal(r)
					if err != nil {
						panic(err)
					}
					err = w.WriteMsg(b)
					if err != nil {
						panic(err)
					}
				}
			}
		}(ctx)

		r := msgio.NewVarintReaderSize(s, DefaultMaxMessageSize)
		for {
			msgbytes, err := r.ReadMsg()
			if err != nil {
				r.ReleaseMsg(msgbytes)
				if err != io.EOF {
					s.Reset()
					logger.Debug("error reading rpc", "from", s.Conn().RemotePeer(), "err", err)
				} else {
					// Just be nice. They probably won't read this
					// but it doesn't hurt to send it.
					s.Close()
				}

				return
			}
			if len(msgbytes) == 0 {
				continue
			}

			rpc := new(pb.RPC)
			err = proto.Unmarshal(msgbytes, rpc)
			r.ReleaseMsg(msgbytes)
			if err != nil {
				s.Reset()
				panic(err)
			}
			recvRPC <- rpc
		}
	})

	return &skeletonGossipsub{
		outRPC: recvRPC,
		inRPC:  sendRPC,
	}
}

func TestExtensionsControlMessage(t *testing.T) {
	for _, wellBehaved := range []bool{true, false} {
		t.Run(fmt.Sprintf("wellBehaved=%t", wellBehaved), func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				hosts := getDefaultHosts(t, 2)
				psub0 := getGossipsub(ctx, hosts[0],
					WithPeerScore(
						&PeerScoreParams{
							AppSpecificScore:       func(peer.ID) float64 { return 0 },
							BehaviourPenaltyWeight: -1,
							BehaviourPenaltyDecay:  ScoreParameterDecay(time.Minute),
							DecayInterval:          DefaultDecayInterval,
							DecayToZero:            DefaultDecayToZero,
						},
						&PeerScoreThresholds{
							GossipThreshold:   -100,
							PublishThreshold:  -500,
							GraylistThreshold: -1000,
						}),
					WithMessageIdFn(func(msg *pb.Message) string {
						return string(msg.Data)
					}))

				psub1 := newSkeletonGossipsub(ctx, hosts[1])

				connect(t, hosts[0], hosts[1])
				time.Sleep(time.Second)

				loopTimes := 3

				for i := range loopTimes {
					rpcToSend := &pb.RPC{
						Control: &pb.ControlMessage{
							Extensions: &pb.ControlExtensions{},
						},
					}
					if wellBehaved && i > 0 {
						// A well behaved node does not repeat the control
						// extension message
						rpcToSend.Control.Extensions = nil
					}
					psub1.inRPC <- rpcToSend
				}

				time.Sleep(time.Second)

				peerScore := psub0.rt.(*GossipSubRouter).score.Score(hosts[1].ID())
				t.Log("Peer score:", peerScore)
				if wellBehaved {
					if peerScore < 0 {
						t.Fatal("Peer score should not be negative")
					}
				} else {
					if peerScore >= 0 {
						t.Fatal("Peer score should not be positive")
					}
				}
			})
		})
	}
}

func TestTestExtension(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		hosts := getDefaultHosts(t, 2)
		var receivedTestExtension atomic.Bool
		c := TestExtensionConfig{
			OnReceiveTestExtension: func(_ peer.ID) {
				receivedTestExtension.Store(true)
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		psub := getGossipsub(ctx, hosts[0], WithTestExtension(c))
		_ = psub
		psub1 := newSkeletonGossipsub(ctx, hosts[1])

		connect(t, hosts[0], hosts[1])

		const timeout = 3 * time.Second
		select {
		case <-time.After(timeout):
			t.Fatal("Timeout")
		case r := <-psub1.outRPC:
			if !*r.Control.Extensions.TestExtension {
				t.Fatal("Unexpected RPC. First RPC should be the Extensions Control Message")
			}
		}

		truePtr := true
		psub1.inRPC <- &pb.RPC{
			Control: &pb.ControlMessage{
				Extensions: &pb.ControlExtensions{
					TestExtension: &truePtr,
				},
			},
		}

		select {
		case <-time.After(timeout):
			t.Fatal("Timeout")
		case r := <-psub1.outRPC:
			if r.TestExtension == nil {
				t.Fatal("Unexpected RPC. Next RPC should be the TestExtension Message")
			}
		}

		if !receivedTestExtension.Load() {
			t.Fatal("TestExtension not received")
		}
	})
}

type minimalTestPartialMessage struct {
	Group []byte
	Parts [2][]byte
}

func (m *minimalTestPartialMessage) complete() bool {
	return len(m.Parts[0]) > 0 && len(m.Parts[1]) > 0
}

// PartsMetadata implements partialmessages.PartialMessage.
func (m *minimalTestPartialMessage) PartsMetadata() bitmap.Bitmap {
	out := make(bitmap.Bitmap, 1)
	for i := range m.Parts {
		if len(m.Parts[i]) > 0 {
			out.Set(i)
		}
	}
	return out
}

func (m *minimalTestPartialMessage) extendFromEncodedPartialMessage(_ peer.ID, data []byte) (extended bool) {
	var temp minimalTestPartialMessage
	json.Unmarshal(data, &temp)
	for i := range m.Parts {
		if len(temp.Parts[i]) > 0 && m.Parts[i] == nil {
			extended = true
			m.Parts[i] = temp.Parts[i]
		}
	}
	return
}

// onIncomingRPC handle an incoming rpc and will return a non-nil publish
// options if the caller should republish this partial message.
func (m *minimalTestPartialMessage) onIncomingRPC(from peer.ID, rpc *pb.PartialMessagesExtension) bool {
	if len(rpc.PartialMessage) > 0 && m.extendFromEncodedPartialMessage(from, rpc.PartialMessage) {
		return true
	}

	var peerHasUsefulData, iHaveUsefulData bool
	// Only do these checks if we didn't extend our partial message.
	// Since, otherwise, we simply publish again to all peers.
	if len(rpc.PartsMetadata) > 0 {
		iHave := m.PartsMetadata()[0]
		iWant := ^iHave

		peerHas := rpc.PartsMetadata[0]
		peerWants := ^peerHas

		iHaveUsefulData = iHave&peerWants != 0
		peerHasUsefulData = iWant&peerHas != 0
	}
	return peerHasUsefulData || iHaveUsefulData
}

// GroupID implements partialmessages.PartialMessage.
func (m *minimalTestPartialMessage) GroupID() []byte {
	return m.Group
}

func (m *minimalTestPartialMessage) publishActions(peerStates map[peer.ID]peerState, peerRequestsPartial func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
	myPartsMeta := m.PartsMetadata()
	return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
		for p, ps := range peerStates {
			var encodedMsg []byte
			if peerRequestsPartial(p) && ps.recvd != nil {
				peerHas := ps.recvd
				var temp minimalTestPartialMessage
				temp.Group = m.Group
				if !peerHas.Get(0) && m.Parts[0] != nil {
					temp.Parts[0] = m.Parts[0]
				}
				if !peerHas.Get(1) && m.Parts[1] != nil {
					temp.Parts[1] = m.Parts[1]
				}
				if temp.Parts[0] != nil || temp.Parts[1] != nil {
					b, err := json.Marshal(temp)
					if err != nil {
						panic(err)
					}
					encodedMsg = b
				}
				ps.recvd = bitmap.Merge(ps.recvd, bitmap.Bitmap(myPartsMeta))
			}

			var partsMetadataToSend partialmessages.PartsMetadata
			if !bytes.Equal(myPartsMeta, ps.sent) {
				partsMetadataToSend = partialmessages.PartsMetadata(slices.Clone(myPartsMeta))
				ps.sent = slices.Clone(myPartsMeta)
			}
			peerStates[p] = ps

			if !yield(p, partialmessages.PublishAction{
				EncodedPartialMessage: encodedMsg,
				EncodedPartsMetadata:  partsMetadataToSend,
			}) {
				return
			}
		}
	}
}

type peerState struct {
	recvd bitmap.Bitmap
	sent  bitmap.Bitmap
}

func TestPartialMessages(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		topic := "test-topic"
		const hostCount = 5
		hosts := getDefaultHosts(t, hostCount)
		psubs := make([]*PubSub, 0, len(hosts))

		gossipsubCtx, closeGossipsub := context.WithCancel(context.Background())
		go func() {
			<-gossipsubCtx.Done()
			for _, h := range hosts {
				h.Close()
			}
		}()

		partialExt := make([]*partialmessages.PartialMessagesExtension[peerState], hostCount)
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

		// A list of maps from topic+groupID to partialMessage. One map per peer
		// var partialMessageStoreMu sync.Mutex
		partialMessageStore := make([]map[string]*minimalTestPartialMessage, hostCount)
		for i := range hostCount {
			partialMessageStore[i] = make(map[string]*minimalTestPartialMessage)
		}

		for i := range partialExt {
			partialExt[i] = &partialmessages.PartialMessagesExtension[peerState]{
				Logger: logger.With("id", i),
				OnEmitGossip: func(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
					pm := partialMessageStore[i][topic+string(groupID)]
					if pm == nil {
						return
					}
					partialExt[i].PublishPartial(topic, groupID, pm.publishActions)
				},
				OnIncomingRPC: func(from peer.ID, peerStates map[peer.ID]peerState, rpc *pb.PartialMessagesExtension) error {
					peerState := peerStates[from]
					groupID := rpc.GroupID
					pm, ok := partialMessageStore[i][topic+string(groupID)]
					if !ok {
						pm = &minimalTestPartialMessage{
							Group: groupID,
						}
						partialMessageStore[i][topic+string(groupID)] = pm
					}
					if rpc.PartsMetadata != nil {
						peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
					}
					prevMeta := slices.Clone(pm.PartsMetadata())
					shouldRepublish := pm.onIncomingRPC(from, rpc)
					if !bytes.Equal(prevMeta, pm.PartsMetadata()) {
						peerState.sent = bitmap.Merge(peerState.sent, pm.PartsMetadata())
					}
					peerStates[from] = peerState
					if shouldRepublish {
						go PublishPartial(psubs[i], topic, pm.GroupID(), pm.publishActions)
					}
					return nil
				},
			}
		}

		for i, h := range hosts {
			psub := getGossipsub(gossipsubCtx, h, WithPartialMessagesExtension(partialExt[i]))
			topic, err := psub.Join(topic, RequestPartialMessages())
			if err != nil {
				t.Fatal(err)
			}
			_, err = topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			psubs = append(psubs, psub)
		}

		denseConnect(t, hosts)
		time.Sleep(2 * time.Second)

		group := []byte("test-group")
		msg1 := &minimalTestPartialMessage{
			Group: group,
			Parts: [2][]byte{
				[]byte("Hello"),
				[]byte("World"),
			},
		}
		partialMessageStore[0][topic+string(group)] = msg1
		err := PublishPartial(psubs[0], topic, msg1.GroupID(), msg1.publishActions)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(2 * time.Second)

		// Close gossipsub before we inspect the state to avoid race conditions
		closeGossipsub()
		time.Sleep(1 * time.Second)

		if len(partialMessageStore) != hostCount {
			t.Errorf("One host is missing the partial message")
		}

		for i, msgStore := range partialMessageStore {
			if len(msgStore) == 0 {
				t.Errorf("Host %d is missing the partial message", i)
			}
			for _, partialMessage := range msgStore {
				if !partialMessage.complete() {
					t.Errorf("expected complete message, but %v is incomplete", partialMessage)
				}
			}
		}
	})
}

// TestPartialMessagesEmitGossipWhenAllPeersPartial verifies that emitGossip
// still drives the partial messages extension's EmitGossip when all gossip
// candidates are partial-message peers and the regular mcache for the topic is
// empty. Previously, emitGossip returned early on an empty mcache, so the
// partial-message gossip path was never exercised in that case.
func TestPartialMessagesEmitGossipWhenAllPeersPartial(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		topic := "test-topic"
		// Use enough hosts that, after mesh formation, every peer still has
		// non-mesh peers in its gossip candidate set. emitGossip excludes mesh
		// peers, so with too few hosts the loop has nothing to gossip to.
		const hostCount = 30
		hosts := getDefaultHosts(t, hostCount)
		psubs := make([]*PubSub, 0, len(hosts))

		gossipsubCtx, closeGossipsub := context.WithCancel(context.Background())
		go func() {
			<-gossipsubCtx.Done()
			for _, h := range hosts {
				h.Close()
			}
		}()

		partialExt := make([]*partialmessages.PartialMessagesExtension[peerState], hostCount)
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

		partialMessageStore := make([]map[string]*minimalTestPartialMessage, hostCount)
		for i := range hostCount {
			partialMessageStore[i] = make(map[string]*minimalTestPartialMessage)
		}

		// Counts per-host OnEmitGossip invocations. emitGossip runs on the
		// gossipsub goroutine inside the synctest bubble, so atomic ops are used
		// for safety even though the bubble serializes.
		var emitGossipCalls [hostCount]atomic.Int64

		for i := range partialExt {
			partialExt[i] = &partialmessages.PartialMessagesExtension[peerState]{
				Logger: logger.With("id", i),
				OnEmitGossip: func(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
					emitGossipCalls[i].Add(1)
					pm := partialMessageStore[i][topic+string(groupID)]
					if pm == nil {
						return
					}
					partialExt[i].PublishPartial(topic, groupID, pm.publishActions)
				},
				OnIncomingRPC: func(from peer.ID, peerStates map[peer.ID]peerState, rpc *pb.PartialMessagesExtension) error {
					peerState := peerStates[from]
					groupID := rpc.GroupID
					pm, ok := partialMessageStore[i][topic+string(groupID)]
					if !ok {
						pm = &minimalTestPartialMessage{
							Group: groupID,
						}
						partialMessageStore[i][topic+string(groupID)] = pm
					}
					if rpc.PartsMetadata != nil {
						peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
					}
					prevMeta := slices.Clone(pm.PartsMetadata())
					shouldRepublish := pm.onIncomingRPC(from, rpc)
					if !bytes.Equal(prevMeta, pm.PartsMetadata()) {
						peerState.sent = bitmap.Merge(peerState.sent, pm.PartsMetadata())
					}
					peerStates[from] = peerState
					if shouldRepublish {
						go PublishPartial(psubs[i], topic, pm.GroupID(), pm.publishActions)
					}
					return nil
				},
			}
		}

		for i, h := range hosts {
			psub := getGossipsub(gossipsubCtx, h, WithPartialMessagesExtension(partialExt[i]))
			topic, err := psub.Join(topic, RequestPartialMessages())
			if err != nil {
				t.Fatal(err)
			}
			_, err = topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			psubs = append(psubs, psub)
		}

		denseConnect(t, hosts)
		time.Sleep(2 * time.Second)

		group := []byte("test-group")
		msg1 := &minimalTestPartialMessage{
			Group: group,
			Parts: [2][]byte{
				[]byte("Hello"),
				[]byte("World"),
			},
		}
		partialMessageStore[0][topic+string(group)] = msg1
		err := PublishPartial(psubs[0], topic, msg1.GroupID(), msg1.publishActions)
		if err != nil {
			t.Fatal(err)
		}

		// Allow several heartbeats to fire so emitGossip runs on every host.
		// The regular mcache stays empty (we never call pubsub.Publish for this
		// topic), so the partial-message gossip path is the only way
		// OnEmitGossip can fire.
		time.Sleep(5 * time.Second)

		closeGossipsub()
		time.Sleep(1 * time.Second)

		for i, msgStore := range partialMessageStore {
			if len(msgStore) == 0 {
				t.Errorf("Host %d is missing the partial message", i)
			}
			for _, partialMessage := range msgStore {
				if !partialMessage.complete() {
					t.Errorf("expected complete message, but %v is incomplete", partialMessage)
				}
			}
		}

		var totalEmitGossipCalls int64
		for i := range hostCount {
			totalEmitGossipCalls += emitGossipCalls[i].Load()
		}
		if totalEmitGossipCalls == 0 {
			t.Errorf("expected OnEmitGossip to be called at least once when all peers are partial-message capable, got 0")
		}
	})
}

func TestPeerSupportsPartialMessages(t *testing.T) {
	// N peers connected in a ring:
	// peer 0 requests partial messages
	// peer 1 does not support partial messages
	// peer 2..N-1 support partial messages
	//
	// Peer 0 first requests a partial message by doing a partial publish with an
	// empty (no parts) message.
	//
	// The rest of the peers then publish the full message. The peer that
	// supports partial messages should have received the request from peer 0,
	// and can sent the missing parts right away.
	synctestTest(t, func(t *testing.T) {

		topic := "test-topic"
		const hostCount = 5
		hosts := getDefaultHosts(t, hostCount)
		psubs := make([]*PubSub, 0, len(hosts))
		topics := make([]*Topic, 0, len(hosts))
		subs := make([]*Subscription, 0, len(hosts))

		gossipsubCtx, closeGossipsub := context.WithCancel(context.Background())
		go func() {
			<-gossipsubCtx.Done()
			for _, h := range hosts {
				h.Close()
			}
		}()

		partialExt := make([]*partialmessages.PartialMessagesExtension[peerState], hostCount)
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

		// A list of maps from topic+groupID to partialMessage. One map per peer
		// var partialMessageStoreMu sync.Mutex
		partialMessageStore := make([]map[string]*minimalTestPartialMessage, hostCount)
		for i := range hostCount {
			partialMessageStore[i] = make(map[string]*minimalTestPartialMessage)
		}

		for i := range partialExt {
			partialExt[i] = &partialmessages.PartialMessagesExtension[peerState]{
				Logger: logger.With("id", i),
				OnEmitGossip: func(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
					pm := partialMessageStore[i][topic+string(groupID)]
					if pm == nil {
						return
					}
					partialExt[i].PublishPartial(topic, groupID, pm.publishActions)
				},
				OnIncomingRPC: func(from peer.ID, peerStates map[peer.ID]peerState, rpc *pb.PartialMessagesExtension) error {
					peerState := peerStates[from]
					if from == hosts[1].ID() {
						panic("peer 1 does not support partial messages, so should not send a partial message RPC")
					}

					if i == 0 && rpc.PartialMessage == nil {
						// The first incoming rpc to the peer requesting a partial
						// message should contain data since we made sure to send
						// the request first.
						panic("expected to receive a partial message from a supporting peer")
					}

					groupID := rpc.GroupID
					pm, ok := partialMessageStore[i][topic+string(groupID)]
					if !ok {
						pm = &minimalTestPartialMessage{
							Group: groupID,
						}
						partialMessageStore[i][topic+string(groupID)] = pm
					}
					if rpc.PartsMetadata != nil {
						peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
					}
					prevMeta := slices.Clone(pm.PartsMetadata())
					shouldRepublish := pm.onIncomingRPC(from, rpc)
					if !bytes.Equal(prevMeta, pm.PartsMetadata()) {
						peerState.sent = bitmap.Merge(peerState.sent, pm.PartsMetadata())
					}
					peerStates[from] = peerState
					if shouldRepublish {
						go PublishPartial(psubs[i], topic, pm.GroupID(), pm.publishActions)
						if pm.complete() {
							encoded, _ := json.Marshal(pm)
							go func() {
								err := psubs[i].Publish(topic, encoded)
								if err != nil {
									panic(err)
								}
							}()
						}
					}
					return nil
				},
			}
		}

		for i, h := range hosts {
			var topicOpts []TopicOpt
			if i == 0 {
				topicOpts = append(topicOpts, RequestPartialMessages())
			} else if i == 1 {
				// The right neighbor doesn't support partial messages
			} else {
				topicOpts = append(topicOpts, SupportsPartialMessages())
			}

			psub := getGossipsub(gossipsubCtx, h, WithPartialMessagesExtension(partialExt[i]))
			topic, err := psub.Join(topic, topicOpts...)
			if err != nil {
				t.Fatal(err)
			}
			sub, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			psubs = append(psubs, psub)
			topics = append(topics, topic)
			subs = append(subs, sub)
		}

		ringConnect(t, hosts)
		time.Sleep(2 * time.Second)

		group := []byte("test-group")
		emptyMsg := &minimalTestPartialMessage{
			Group: group,
		}
		fullMsg := &minimalTestPartialMessage{
			Group: group,
			Parts: [2][]byte{
				[]byte("Hello"),
				[]byte("World"),
			},
		}

		for i := range hosts {
			if i <= 1 {
				continue
			}
			copiedMsg := *fullMsg
			partialMessageStore[i][topic+string(group)] = &copiedMsg
		}

		// Have the first host publish the empty partial message to send a partial
		// message request to peers that support partial messages.
		partialMessageStore[0][topic+string(group)] = emptyMsg
		// first host has no data
		err := PublishPartial(psubs[0], topic, emptyMsg.GroupID(), emptyMsg.publishActions)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second)

		for i := range hosts {
			if i == 0 {
				continue
			} else {
				if i != 1 {
					pm := partialMessageStore[i][topic+string(group)]
					err := PublishPartial(psubs[i], topic, pm.GroupID(), pm.publishActions)
					if err != nil {
						t.Fatal(err)
					}
				}
				encoded, err := json.Marshal(fullMsg)
				if err != nil {
					t.Fatal(err)
				}
				err = topics[i].Publish(context.Background(), encoded)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		time.Sleep(2 * time.Second)

		// Close gossipsub before we inspect the state to avoid race conditions
		closeGossipsub()
		time.Sleep(1 * time.Second)

		if len(partialMessageStore) != hostCount {
			t.Errorf("One host is missing the partial message")
		}

		for i, msgStore := range partialMessageStore {
			if i == 1 {
				// Host 1 doesn't support partial messages
				continue
			}
			if len(msgStore) == 0 {
				t.Errorf("Host %d is missing the partial message", i)
			}
			for _, partialMessage := range msgStore {
				if !partialMessage.complete() {
					t.Errorf("expected complete message, but %v is incomplete at host %d", partialMessage, i)
				}
			}
		}

		for i, sub := range subs {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			_, err := sub.Next(ctx)
			if err != nil {
				t.Errorf("failed to receive message: %v at host %d", err, i)
			}
		}
	})
}

func TestSkipPublishingToPeersRequestingPartialMessages(t *testing.T) {
	synctestTest(t, func(t *testing.

		// 3 hosts.
		// hosts[0]: Publisher. Requests partial messages
		// hosts[1]: Subscriber. Requests partial messages
		// hosts[2]: Alternate publisher. Does not support partial messages. Only
		//   connected to hosts[0]
		T) {
		topicName := "test-topic"
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 3)

		const hostsWithPartialMessageSupport = 2
		partialExt := make([]*partialmessages.PartialMessagesExtension[peerState], hostsWithPartialMessageSupport)
		// A list of maps from topic+groupID to partialMessage. One map per peer
		partialMessageStore := make([]map[string]*minimalTestPartialMessage, hostsWithPartialMessageSupport)
		for i := range hostsWithPartialMessageSupport {
			partialMessageStore[i] = make(map[string]*minimalTestPartialMessage)
		}

		// Only hosts with partial message support
		psubs := make([]*PubSub, 0, len(hosts)-1)

		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		for i := range partialExt {
			partialExt[i] = &partialmessages.PartialMessagesExtension[peerState]{
				Logger: logger,
				OnEmitGossip: func(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
					pm := partialMessageStore[i][topic+string(groupID)]
					if pm == nil {
						return
					}
					partialExt[i].PublishPartial(topic, groupID, pm.publishActions)
				},
				OnIncomingRPC: func(from peer.ID, peerStates map[peer.ID]peerState, rpc *pb.PartialMessagesExtension) error {
					peerState := peerStates[from]
					topicID := rpc.GetTopicID()
					groupID := rpc.GetGroupID()
					pm, ok := partialMessageStore[i][topicID+string(groupID)]
					if !ok {
						pm = &minimalTestPartialMessage{
							Group: groupID,
						}
						partialMessageStore[i][topicID+string(groupID)] = pm
					}
					if rpc.PartsMetadata != nil {
						peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
					}
					prevMeta := slices.Clone(pm.PartsMetadata())
					shouldRepublish := pm.onIncomingRPC(from, rpc)
					if !bytes.Equal(prevMeta, pm.PartsMetadata()) {
						peerState.sent = bitmap.Merge(peerState.sent, pm.PartsMetadata())
					}
					peerStates[from] = peerState
					if shouldRepublish {
						go PublishPartial(psubs[i], topicID, pm.GroupID(), pm.publishActions)
					}
					return nil
				},
			}
		}

		for i, h := range hosts[:2] {
			psub := getGossipsub(ctx, h, WithPartialMessagesExtension(partialExt[i]))
			psubs = append(psubs, psub)
		}

		nonPartialPubsub := getGossipsub(ctx, hosts[2])

		denseConnect(t, hosts[:2])
		time.Sleep(2 * time.Second)

		// Connect nonPartialPubsub to the publisher
		connect(t, hosts[0], hosts[2])

		var topics []*Topic
		var subs []*Subscription
		for _, psub := range psubs {
			topic, err := psub.Join(topicName, RequestPartialMessages())
			if err != nil {
				t.Fatal(err)
			}
			topics = append(topics, topic)
			s, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			subs = append(subs, s)
		}

		topicForNonPartial, err := nonPartialPubsub.Join(topicName)
		if err != nil {
			t.Fatal(err)
		}

		// Wait for subscriptions to propagate
		time.Sleep(time.Second)

		topics[0].Publish(ctx, []byte("Hello"))

		// Publish from another peer, the publisher (psub[0]) should not forward this to psub[1].
		// The application has to handle the interaction of getting a standard
		// gossipsub message and republishing it with partial messages.
		topicForNonPartial.Publish(ctx, []byte("from non-partial"))

		recvdMessage := make(chan struct{}, 1)
		go func() {
			msg, err := subs[1].Next(ctx)
			if err == context.Canceled {
				return
			}
			if err != nil {
				t.Log(err)
				t.Fail()
				return
			}
			t.Log("Received msg", string(msg.Data))
			recvdMessage <- struct{}{}
		}()

		select {
		case <-recvdMessage:
			t.Fatal("Received message")
		case <-time.After(2 * time.Second):
		}
	})
}

func TestPairwiseInteractionWithPartialMessages(t *testing.T) {
	type PartialMessageStatus int
	const (
		NoPartialMessages PartialMessageStatus = iota
		// The peer has partial messages extension, but doesn't support it for the topic
		PartialMessagesExtensionNoSupport
		PeerSupportsPartialMessages
		PeerRequestsPartialMessages
	)

	type TestCase struct {
		hostSupport  []PartialMessageStatus
		publisherIdx int
	}

	var tcs []TestCase
	for _, a := range []PartialMessageStatus{NoPartialMessages, PartialMessagesExtensionNoSupport, PeerSupportsPartialMessages, PeerRequestsPartialMessages} {
		for _, b := range []PartialMessageStatus{NoPartialMessages, PartialMessagesExtensionNoSupport, PeerSupportsPartialMessages, PeerRequestsPartialMessages} {
			for i := range 2 {
				tcs = append(tcs, TestCase{hostSupport: []PartialMessageStatus{a, b}, publisherIdx: i})
			}
		}
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("Host Support: %v. Publisher: %d", tc.hostSupport, tc.publisherIdx), func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				topic := "test-topic"
				hostCount := len(tc.hostSupport)
				hosts := getDefaultHosts(t, hostCount)
				topics := make([]*Topic, 0, len(hosts))
				psubs := make([]*PubSub, 0, len(hosts))

				gossipsubCtx, closeGossipsub := context.WithCancel(context.Background())
				defer closeGossipsub()
				go func() {
					<-gossipsubCtx.Done()
					for _, h := range hosts {
						h.Close()
					}
				}()

				partialExt := make([]*partialmessages.PartialMessagesExtension[peerState], hostCount)
				logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

				// A list of maps from topic+groupID to partialMessage. One map per peer
				// var partialMessageStoreMu sync.Mutex
				partialMessageStore := make([]map[string]*minimalTestPartialMessage, hostCount)
				for i := range hostCount {
					partialMessageStore[i] = make(map[string]*minimalTestPartialMessage)
				}

				receivedMessage := make(chan struct{}, hostCount)

				for i := range partialExt {
					if tc.hostSupport[i] == NoPartialMessages {
						continue
					}
					partialExt[i] = &partialmessages.PartialMessagesExtension[peerState]{
						Logger: logger.With("id", i),
						OnEmitGossip: func(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
							pm := partialMessageStore[i][topic+string(groupID)]
							if pm == nil {
								return
							}
							partialExt[i].PublishPartial(topic, groupID, pm.publishActions)
						},
						OnIncomingRPC: func(from peer.ID, peerStates map[peer.ID]peerState, rpc *pb.PartialMessagesExtension) error {
							peerState := peerStates[from]
							if tc.hostSupport[i] == PeerSupportsPartialMessages && len(rpc.PartialMessage) > 0 {
								panic("This host should not have received partial message data")
							}

							groupID := rpc.GroupID
							pm, ok := partialMessageStore[i][topic+string(groupID)]
							if !ok {
								pm = &minimalTestPartialMessage{
									Group: groupID,
								}
								partialMessageStore[i][topic+string(groupID)] = pm
							}
							if rpc.PartsMetadata != nil {
								peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
							}
							prevMeta := slices.Clone(pm.PartsMetadata())
							prevComplete := pm.complete()
							shouldRepublish := pm.onIncomingRPC(from, rpc)
							if !bytes.Equal(prevMeta, pm.PartsMetadata()) {
								peerState.sent = bitmap.Merge(peerState.sent, pm.PartsMetadata())
							}
							peerStates[from] = peerState
							if shouldRepublish {
								if !prevComplete && pm.complete() {
									t.Log("host", i, "received partial message")

									receivedMessage <- struct{}{}
								}

								go PublishPartial(psubs[i], topic, pm.GroupID(), pm.publishActions)
							}
							return nil
						},
					}
				}

				for i, h := range hosts {
					var opts []Option
					// For debugging:
					// slog.SetLogLoggerLevel(slog.LevelDebug)
					// opts = append(opts, WithRPCLogger(slog.Default().With("host_idx", i)))
					var topicOpts []TopicOpt
					switch tc.hostSupport[i] {
					case NoPartialMessages:
					case PartialMessagesExtensionNoSupport:
						opts = append(opts, WithPartialMessagesExtension(partialExt[i]))
					case PeerSupportsPartialMessages:
						opts = append(opts, WithPartialMessagesExtension(partialExt[i]))
						topicOpts = append(topicOpts, SupportsPartialMessages())
					case PeerRequestsPartialMessages:
						opts = append(opts, WithPartialMessagesExtension(partialExt[i]))
						topicOpts = append(topicOpts, RequestPartialMessages())
					}

					psub := getGossipsub(gossipsubCtx, h, opts...)
					topic, err := psub.Join(topic, topicOpts...)
					if err != nil {
						t.Fatal(err)
					}
					topics = append(topics, topic)
					sub, err := topic.Subscribe()
					if err != nil {
						t.Fatal(err)
					}
					psubs = append(psubs, psub)
					go func() {
						_, err := sub.Next(gossipsubCtx)
						if err == context.Canceled {
							return
						}
						if err != nil {
							panic(err)
						}

						t.Log("host", i, "received message")
						receivedMessage <- struct{}{}
					}()
				}

				denseConnect(t, hosts)
				time.Sleep(time.Second)

				group := []byte("test-group")
				msg1 := &minimalTestPartialMessage{
					Group: group,
					Parts: [2][]byte{
						[]byte("Hello"),
						[]byte("World"),
					},
				}

				for i := range hostCount {
					if i != tc.publisherIdx {
						continue
					}

					partialMessageStore[i][topic+string(group)] = msg1

					encoded, err := json.Marshal(msg1)
					if err != nil {
						t.Fatal(err)
					}
					err = topics[i].Publish(context.Background(), encoded)
					if err != nil {
						t.Fatal(err)
					}

					switch tc.hostSupport[i] {
					case PeerSupportsPartialMessages, PeerRequestsPartialMessages:
						err = PublishPartial(psubs[i], topic, msg1.GroupID(), msg1.publishActions)
						if err != nil {
							t.Fatal(err)
						}
					}
				}

				for range hostCount {
					select {
					case <-receivedMessage:
					case <-time.After(time.Second):
						t.Fatalf("At least one message was not received")
					}
				}

				select {
				case <-receivedMessage:
					t.Fatalf("An extra message was received")
				case <-time.After(100 * time.Millisecond):
				}
			})
		})
	}
}

func TestNoIDONTWANTWithPartialMessage(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		hs := getDefaultHosts(t, 3)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		type rpcWithMeta struct {
			from, to   peer.ID
			idontwants []*pb.ControlIDontWant
		}

		receivedIDONTWANTs := make(chan *rpcWithMeta, 20)

		pubsubs := getGossipsubsOptFn(ctx, hs,
			func(i int, h host.Host) []Option {
				tracer := &mockRawTracer{
					onRecvRPC: func(r *RPC) {
						if r.Control != nil {
							receivedIDONTWANTs <- &rpcWithMeta{
								from:       r.from,
								to:         h.ID(),
								idontwants: r.Control.Idontwant,
							}
						}
					},
				}
				return []Option{
					WithPartialMessagesExtension(
						&partialmessages.PartialMessagesExtension[peerState]{
							OnEmitGossip: func(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
							},
							Logger: slog.Default(),
							OnIncomingRPC: func(from peer.ID, peerStates map[peer.ID]peerState, rpc *pb.PartialMessagesExtension) error {
								return nil
							},
						},
					),
					WithRawTracer(tracer),
				}
			})

		for i, h := range hs {
			t.Logf("Host ID of node %v: %s\n", i, h.ID())
		}

		hs[0].Connect(ctx, peer.AddrInfo{
			ID:    hs[1].ID(),
			Addrs: hs[1].Addrs(),
		})
		hs[1].Connect(ctx, peer.AddrInfo{
			ID:    hs[2].ID(),
			Addrs: hs[2].Addrs(),
		})

		var topics []*Topic
		var receivedMessage [3]atomic.Bool
		const topicStr = "test-topic"
		for i, ps := range pubsubs {
			var topicOpts []TopicOpt
			switch i {
			case 0:
				topicOpts = append(topicOpts, SupportsPartialMessages())
			case 1:
				topicOpts = append(topicOpts, RequestPartialMessages())
			}
			topic, err := ps.Join(topicStr, topicOpts...)
			if err != nil {
				t.Fatal(err)
			}
			topics = append(topics, topic)
			sub, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				for {
					msg, err := sub.Next(ctx)
					if errors.Is(err, context.Canceled) {
						return
					}
					if err != nil {
						panic(err)
					}
					t.Logf("%v Received message from %s\n", i, msg.GetFrom())
					receivedMessage[i].Store(true)
				}
			}()
		}

		// Let mesh form
		time.Sleep(time.Second)
		msg := make([]byte, GossipSubIDontWantMessageThreshold)
		err := topics[2].Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}

	outer:
		for {
			select {
			case rpc := <-receivedIDONTWANTs:
				// Peer 1 shouldn't send any IDONTWANTS.
				// because:
				//   - Peer 2 send the message, so shouldn't receive an IDONTWANT.
				//   - Peer 0 supports partial messages and peer 1 requested partial
				//   messages. It will not receive a full message from peer 1.
				if rpc.from == hs[1].ID() && len(rpc.idontwants) > 0 {
					t.Fatalf("Received unexpected IDONTWANT from %s", rpc.from)
				}
				t.Logf("Received RPC: %s->%s %+v\n", rpc.from, rpc.to, rpc.idontwants)
			case <-time.After(2 * time.Second):
				break outer
			}
		}

		for i := range receivedMessage {
			if !receivedMessage[i].Load() {
				t.Fatal("Peer did not receive a full message")
			}
		}
	})
}

func TestGossipsubFanoutOnly(t *testing.T) {
	// Test that a fanout-only topic can publish to the network but its
	// subscriber only receives locally published messages.
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 5)
		psubs := getGossipsubs(ctx, hosts)

		topicID := "foobar"

		// hosts[0] joins with FanoutOnly - it should be able to publish but never
		// join the mesh, so its subscriber should not receive remote messages.
		fanoutTopic, err := psubs[0].Join(topicID, FanoutOnly())
		if err != nil {
			t.Fatal(err)
		}

		// The rest subscribe normally.
		var normalSubs []*Subscription
		for _, ps := range psubs[1:] {
			sub, err := ps.Subscribe(topicID)
			if err != nil {
				t.Fatal(err)
			}
			normalSubs = append(normalSubs, sub)
		}

		// Also subscribe on the fanout-only topic. Since it's fanout-only, this
		// must not trigger a p2p subscription.
		fanoutSub, err := fanoutTopic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		denseConnect(t, hosts)

		// Wait for heartbeats to build mesh.
		time.Sleep(2 * time.Second)

		// Publish from the fanout-only node. Normal subscribers should receive it
		// because the router uses fanout to forward the message.
		fanoutMsg := []byte("from fanout-only node")
		if err := fanoutTopic.Publish(ctx, fanoutMsg); err != nil {
			t.Fatal(err)
		}

		for i, sub := range normalSubs {
			tctx, tcancel := context.WithTimeout(ctx, 5*time.Second)
			got, err := sub.Next(tctx)
			tcancel()
			if err != nil {
				t.Fatalf("normal sub %d did not receive fanout message: %v", i, err)
			}
			if !bytes.Equal(got.Data, fanoutMsg) {
				t.Fatalf("normal sub %d got wrong message", i)
			}
		}

		// The fanout subscriber should also get the locally published message.
		tctx, tcancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := fanoutSub.Next(tctx)
		tcancel()
		if err != nil {
			t.Fatal("fanout subscriber did not receive locally published message:", err)
		}
		if !bytes.Equal(got.Data, fanoutMsg) {
			t.Fatal("fanout subscriber got wrong message")
		}

		// Now publish from a normal node. The fanout subscriber must NOT receive
		// it because the fanout-only topic never joined the mesh.
		remoteMsg := []byte("from normal node")
		if err := psubs[1].Publish(topicID, remoteMsg); err != nil {
			t.Fatal(err)
		}

		// The other normal subscribers should receive it.
		for i, sub := range normalSubs[1:] {
			tctx, tcancel := context.WithTimeout(ctx, 5*time.Second)
			got, err := sub.Next(tctx)
			tcancel()
			if err != nil {
				t.Fatalf("normal sub %d did not receive remote message: %v", i+1, err)
			}
			if !bytes.Equal(got.Data, remoteMsg) {
				t.Fatalf("normal sub %d got wrong message", i+1)
			}
		}

		// The fanout subscriber should NOT receive the remote message.
		tctx, tcancel = context.WithTimeout(ctx, time.Second)
		_, err = fanoutSub.Next(tctx)
		tcancel()
		if err == nil {
			t.Fatal("fanout subscriber received a remote message but should not have")
		}
	})
}

func TestGossipsubFanoutOnlyRelay(t *testing.T) {
	// Test that Relay() returns an error on a fanout-only topic.
	synctestTest(t, func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 1)
		ps := getGossipsub(ctx, hosts[0])

		topic, err := ps.Join("foobar", FanoutOnly())
		if err != nil {
			t.Fatal(err)
		}

		_, err = topic.Relay()
		if !errors.Is(err, ErrFanoutOnlyTopic) {
			t.Fatalf("expected ErrFanoutOnlyTopic, got: %v", err)
		}
	})
}

func TestGossipsubDuplicatePublishError(t *testing.T) {
	// republishing a rejected message must rerun validation and return the
	// rejection error again, not report success as a duplicate
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 1)
		psubs := getGossipsubs(ctx, hosts)

		allReject := func(ctx context.Context, p peer.ID, msg *Message) ValidationResult {
			return ValidationReject
		}
		err := psubs[0].RegisterTopicValidator("test", allReject)
		if err != nil {
			t.Fatal(err)
		}

		topic, err := psubs[0].Join("test", WithTopicMessageIdFn(func(pmsg *pb.Message) string {
			return "message-1"
		}))
		if err != nil {
			t.Fatal(err)
		}

		if err := topic.Publish(ctx, []byte("hello")); err == nil {
			t.Fatal("expected rejection error on publish")
		}

		if err := topic.Publish(ctx, []byte("hello")); err == nil {
			t.Fatal("expected rejection error on republish")
		}
	})
}

func TestGossipsubForwardAfterIgnore(t *testing.T) {
	// uses a linear chain 0-1-2-3-4 where node k=2 ignores relayed messages, so the
	// message stops at k until k republishes it locally and it reaches the rest
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const N = 5
		hosts := getDefaultHosts(t, N)
		psubs := getGossipsubs(ctx, hosts)
		for i := range N - 1 {
			connect(t, hosts[i], hosts[i+1])
		}
		var topics []*Topic
		for i := range N {
			topic, err := psubs[i].Join("test", WithTopicMessageIdFn(func(pmsg *pb.Message) string {
				hash := sha256.Sum256(pmsg.Data)
				return string(hash[:])
			}))
			if err != nil {
				t.Fatal(err)
			}
			topics = append(topics, topic)
		}

		var subs []*Subscription
		for i := range N {
			sub, err := topics[i].Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			subs = append(subs, sub)
		}

		time.Sleep(2 * time.Second)

		// node k ignores the message and republishes it later
		const k = 2
		ignoreAllNonLocal := func(ctx context.Context, p peer.ID, msg *Message) ValidationResult {
			// only accept locally published messages
			if p == hosts[k].ID() {
				return ValidationAccept
			}
			return ValidationIgnore
		}
		err := psubs[k].RegisterTopicValidator("test", ignoreAllNonLocal)
		if err != nil {
			t.Fatal(err)
		}

		publishedMessage := []byte("hello")
		if err := topics[0].Publish(ctx, publishedMessage); err != nil {
			t.Fatal(err)
		}

		for i := range k {
			assertReceive(t, subs[i], publishedMessage)
		}
		for i := k; i < N; i++ {
			assertNeverReceives(t, subs[i], time.Second)
		}

		if err := topics[k].Publish(ctx, publishedMessage); err != nil {
			t.Fatal(err)
		}

		for i := k; i < N; i++ {
			assertReceive(t, subs[i], publishedMessage)
		}
		// nodes before k must not receive the republished message again
		for i := range k {
			assertNeverReceives(t, subs[i], time.Second)
		}
	})
}

func TestGossipsubDuplicatePublishDeliveredOnce(t *testing.T) {
	// republishing an already-delivered message must not deliver it to local
	// subscribers or the network a second time
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)
		psubs := getGossipsubs(ctx, hosts)
		connect(t, hosts[0], hosts[1])

		var topics []*Topic
		var subs []*Subscription
		for i := range 2 {
			topic, err := psubs[i].Join("test", WithTopicMessageIdFn(func(pmsg *pb.Message) string {
				hash := sha256.Sum256(pmsg.Data)
				return string(hash[:])
			}))
			if err != nil {
				t.Fatal(err)
			}
			topics = append(topics, topic)

			sub, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			subs = append(subs, sub)
		}

		time.Sleep(2 * time.Second)

		publishedMessage := []byte("hello")
		if err := topics[0].Publish(ctx, publishedMessage); err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			assertReceive(t, subs[i], publishedMessage)
		}

		// republishing a delivered message is a no-op reported as success
		if err := topics[0].Publish(ctx, publishedMessage); err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			assertNeverReceives(t, subs[i], time.Second)
		}
	})
}

func TestGossipsubLimitIWANT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getDefaultHosts(t, 3)
	denseConnect(t, hosts)

	psubs := make([]*PubSub, 3)

	defaultParams := DefaultGossipSubParams()
	defaultParams.MaxIWantsPerMessageIDPerHeartbeat = 1
	// Delay the heartbeat so we don't reset our count of allowed IWANTS for the
	// first part of the test.
	defaultParams.HeartbeatInitialDelay = 4 * time.Second
	defaultParams.HeartbeatInterval = time.Second

	psubs[0] = getGossipsub(ctx, hosts[0], WithGossipSubParams(defaultParams))

	var iwantsRecvd atomic.Int32

	copy(psubs[1:], getGossipsubs(ctx, hosts[1:], WithRawTracer(&mockRawTracer{
		onRecvRPC: func(rpc *RPC) {
			if len(rpc.GetControl().GetIwant()) > 0 {
				iwantsRecvd.Add(1)
			}
		},
	})))

	topicString := "foobar"
	for _, ps := range psubs {
		topic, err := ps.Join(topicString)
		if err != nil {
			t.Fatal(err)
		}

		_, err = topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * time.Second)

	publishIWant := func() {
		psubs[1].eval <- func() {
			psubs[1].rt.(*GossipSubRouter).sendRPC(hosts[0].ID(), &RPC{
				RPC: pb.RPC{
					Control: &pb.ControlMessage{
						Ihave: []*pb.ControlIHave{
							{
								TopicID:    &topicString,
								MessageIDs: []string{"1"},
							},
						},
					},
				},
			}, false)
		}

		psubs[2].eval <- func() {
			psubs[2].rt.(*GossipSubRouter).sendRPC(hosts[0].ID(), &RPC{
				RPC: pb.RPC{
					Control: &pb.ControlMessage{
						Ihave: []*pb.ControlIHave{
							{
								TopicID:    &topicString,
								MessageIDs: []string{"1"},
							},
						},
					},
				},
			}, false)
		}
	}

	publishIWant()
	time.Sleep(time.Second)

	if iwantsRecvd.Swap(0) != 1 {
		t.Fatal("Expected exactly 1 IWANT due to limits")
	}

	// Wait for heartbeat to reset limit
	time.Sleep(2 * time.Second)

	publishIWant()
	time.Sleep(time.Second)

	if iwantsRecvd.Swap(0) != 1 {
		t.Fatal("Expected exactly 1 IWANT due to limits")
	}
}
