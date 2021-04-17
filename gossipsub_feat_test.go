package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/protocol"
)

func TestDefaultGossipSubFeatures(t *testing.T) {
	if GossipSubDefaultFeatures(GossipSubFeatureMesh, FloodSubID) {
		t.Fatal("floodsub should not support Mesh")
	}
	if !GossipSubDefaultFeatures(GossipSubFeatureMesh, GossipSubID_v10) {
		t.Fatal("gossipsub-v1.0 should support Mesh")
	}
	if !GossipSubDefaultFeatures(GossipSubFeatureMesh, GossipSubID_v11) {
		t.Fatal("gossipsub-v1.1 should support Mesh")
	}

	if GossipSubDefaultFeatures(GossipSubFeaturePX, FloodSubID) {
		t.Fatal("floodsub should not support PX")
	}
	if GossipSubDefaultFeatures(GossipSubFeaturePX, GossipSubID_v10) {
		t.Fatal("gossipsub-v1.0 should not support PX")
	}
	if !GossipSubDefaultFeatures(GossipSubFeatureMesh, GossipSubID_v11) {
		t.Fatal("gossipsub-v1.1 should support PX")
	}
}

func TestGossipSubCustomProtocols(t *testing.T) {
	customsub := protocol.ID("customsub/1.0.0")
	protos := []protocol.ID{customsub, FloodSubID}
	features := func(feat GossipSubFeature, proto protocol.ID) bool { return proto == customsub }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 3)

	gsubs := getGossipsubs(ctx, hosts[:2], WithGossipSubProtocols(protos, features))
	fsub := getPubsub(ctx, hosts[2])
	psubs := append(gsubs, fsub)

	connectAll(t, hosts)

	topic := "test"
	var subs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}

		subs = append(subs, subch)
	}

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	// check the meshes of the gsubs, the gossipsub meshes should include each other but not the
	// floddsub peer
	gsubs[0].eval <- func() {
		gs := gsubs[0].rt.(*GossipSubRouter)

		_, ok := gs.mesh[topic][hosts[1].ID()]
		if !ok {
			t.Fatal("expected gs0 to have gs1 in its mesh")
		}

		_, ok = gs.mesh[topic][hosts[2].ID()]
		if ok {
			t.Fatal("expected gs0 to not have fs in its mesh")
		}
	}

	gsubs[1].eval <- func() {
		gs := gsubs[1].rt.(*GossipSubRouter)

		_, ok := gs.mesh[topic][hosts[0].ID()]
		if !ok {
			t.Fatal("expected gs1 to have gs0 in its mesh")
		}

		_, ok = gs.mesh[topic][hosts[2].ID()]
		if ok {
			t.Fatal("expected gs1 to not have fs in its mesh")
		}
	}

	// send some messages
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d it's not quite a floooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish(topic, msg)

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
}
