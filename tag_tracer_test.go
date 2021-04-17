package pubsub

import (
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	connmgri "github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/peer"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func TestTagTracerMeshTags(t *testing.T) {
	// test that tags are applied when the tagTracer sees graft and prune events

	cmgr := connmgr.NewConnManager(5, 10, time.Minute)
	tt := newTagTracer(cmgr)

	p := peer.ID("a-peer")
	topic := "a-topic"

	tt.Join(topic)
	tt.Graft(p, topic)

	tag := "pubsub:" + topic
	if !cmgr.IsProtected(p, tag) {
		t.Fatal("expected the mesh peer to be protected")
	}

	tt.Prune(p, topic)
	if cmgr.IsProtected(p, tag) {
		t.Fatal("expected the former mesh peer to be unprotected")
	}
}

func TestTagTracerDirectPeerTags(t *testing.T) {
	// test that we add a tag to direct peers
	cmgr := connmgr.NewConnManager(5, 10, time.Minute)
	tt := newTagTracer(cmgr)

	p1 := peer.ID("1")
	p2 := peer.ID("2")
	p3 := peer.ID("3")

	// in the real world, tagTracer.direct is set in the WithDirectPeers option function
	tt.direct = make(map[peer.ID]struct{})
	tt.direct[p1] = struct{}{}

	tt.AddPeer(p1, GossipSubID_v10)
	tt.AddPeer(p2, GossipSubID_v10)
	tt.AddPeer(p3, GossipSubID_v10)

	tag := "pubsub:<direct>"
	if !cmgr.IsProtected(p1, tag) {
		t.Fatal("expected direct peer to be protected")
	}

	for _, p := range []peer.ID{p2, p3} {
		if cmgr.IsProtected(p, tag) {
			t.Fatal("expected non-direct peer to be unprotected")
		}
	}
}

func TestTagTracerDeliveryTags(t *testing.T) {
	t.Skip("flaky test temporarily disabled; TODO: fixme")
	// test decaying delivery tags

	// use fake time to test the tag decay
	clk := clock.NewMock()
	decayCfg := &connmgr.DecayerCfg{
		Clock:      clk,
		Resolution: time.Minute,
	}
	cmgr := connmgr.NewConnManager(5, 10, time.Minute, connmgr.DecayerConfig(decayCfg))

	tt := newTagTracer(cmgr)

	topic1 := "topic-1"
	topic2 := "topic-2"

	p := peer.ID("a-peer")

	tt.Join(topic1)
	tt.Join(topic2)

	for i := 0; i < 20; i++ {
		// deliver only 5 messages to topic 2 (less than the cap)
		topic := &topic1
		if i < 5 {
			topic = &topic2
		}
		msg := &Message{
			ReceivedFrom: p,
			Message: &pb.Message{
				From:  []byte(p),
				Data:  []byte("hello"),
				Topic: topic,
			},
		}
		tt.DeliverMessage(msg)
	}

	// we have to tick the fake clock once to apply the bump
	clk.Add(time.Minute)

	tag1 := "pubsub-deliveries:topic-1"
	tag2 := "pubsub-deliveries:topic-2"

	// the tag value for topic-1 should be capped at GossipSubConnTagMessageDeliveryCap (default 15)
	val := getTagValue(cmgr, p, tag1)
	expected := GossipSubConnTagMessageDeliveryCap
	if val != expected {
		t.Errorf("expected delivery tag to be capped at %d, was %d", expected, val)
	}

	// the value for topic-2 should equal the number of messages delivered (5), since it was less than the cap
	val = getTagValue(cmgr, p, tag2)
	expected = 5
	if val != expected {
		t.Errorf("expected delivery tag value = %d, got %d", expected, val)
	}

	// if we jump forward a few minutes, we should see the tags decrease by 1 / 10 minutes
	clk.Add(50 * time.Minute)
	time.Sleep(2 * time.Second)

	val = getTagValue(cmgr, p, tag1)
	expected = GossipSubConnTagMessageDeliveryCap - 5
	// the actual expected value should be GossipSubConnTagMessageDeliveryCap - 5,
	// however due to timing issues on Travis, we consistently get GossipSubConnTagMessageDeliveryCap - 4
	// there instead. So our assertion checks for the expected value +/- 1
	if val > expected+1 || val < expected-1 {
		t.Errorf("expected delivery tag value = %d ± 1, got %d", expected, val)
	}

	// the tag for topic-2 should have reset to zero by now, but again we add one for Travis since it's slow...
	val = getTagValue(cmgr, p, tag2)
	expected = 0
	if val > expected+1 || val < expected-1 {
		t.Errorf("expected delivery tag value = %d ± 1, got %d", expected, val)
	}

	// leaving the topic should remove the tag
	if !tagExists(cmgr, p, tag1) {
		t.Errorf("expected delivery tag %s to be applied to peer %s", tag1, p)
	}
	tt.Leave(topic1)
	// advance the real clock a bit to allow the connmgr to remove the tag async
	time.Sleep(time.Second)
	if tagExists(cmgr, p, tag1) {
		t.Errorf("expected delivery tag %s to be removed after leaving the topic", tag1)
	}
}

func TestTagTracerDeliveryTagsNearFirst(t *testing.T) {
	// use fake time to test the tag decay
	clk := clock.NewMock()
	decayCfg := &connmgr.DecayerCfg{
		Clock:      clk,
		Resolution: time.Minute,
	}
	cmgr := connmgr.NewConnManager(5, 10, time.Minute, connmgr.DecayerConfig(decayCfg))

	tt := newTagTracer(cmgr)

	topic := "test"

	p := peer.ID("a-peer")
	p2 := peer.ID("another-peer")
	p3 := peer.ID("slow-peer")

	tt.Join(topic)

	for i := 0; i < GossipSubConnTagMessageDeliveryCap+5; i++ {
		msg := &Message{
			ReceivedFrom: p,
			Message: &pb.Message{
				From:  []byte(p),
				Data:  []byte(fmt.Sprintf("msg-%d", i)),
				Topic: &topic,
				Seqno: []byte(fmt.Sprintf("%d", i)),
			},
		}

		// a duplicate of the message, received from p2
		dup := &Message{
			ReceivedFrom: p2,
			Message:      msg.Message,
		}

		// the message starts validating as soon as we receive it from p
		tt.ValidateMessage(msg)
		// p2 should get near-first credit for the duplicate message that arrives before
		// validation is complete
		tt.DuplicateMessage(dup)
		// DeliverMessage gets called when validation is complete
		tt.DeliverMessage(msg)

		// p3 delivers a duplicate after validation completes & gets no credit
		dup.ReceivedFrom = p3
		tt.DuplicateMessage(dup)
	}

	clk.Add(time.Minute)

	// both p and p2 should get delivery tags equal to the cap
	tag := "pubsub-deliveries:test"
	val := getTagValue(cmgr, p, tag)
	if val != GossipSubConnTagMessageDeliveryCap {
		t.Errorf("expected tag %s to have val %d, was %d", tag, GossipSubConnTagMessageDeliveryCap, val)
	}
	val = getTagValue(cmgr, p2, tag)
	if val != GossipSubConnTagMessageDeliveryCap {
		t.Errorf("expected tag %s for near-first peer to have val %d, was %d", tag, GossipSubConnTagMessageDeliveryCap, val)
	}

	// p3 should have no delivery tag credit
	val = getTagValue(cmgr, p3, tag)
	if val != 0 {
		t.Errorf("expected tag %s for slow peer to have val %d, was %d", tag, 0, val)
	}
}

func getTagValue(mgr connmgri.ConnManager, p peer.ID, tag string) int {
	info := mgr.GetTagInfo(p)
	if info == nil {
		return 0
	}
	val, ok := info.Tags[tag]
	if !ok {
		return 0
	}
	return val
}

// staticcheck says this function is unused because the tests that use it are skipped.
// TODO: remove the lint directive when tests are unskipped.

//lint:ignore U1000 unused function
func tagExists(mgr connmgri.ConnManager, p peer.ID, tag string) bool {
	info := mgr.GetTagInfo(p)
	if info == nil {
		return false
	}
	_, exists := info.Tags[tag]
	return exists
}
