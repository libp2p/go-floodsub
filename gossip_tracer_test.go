package pubsub

import (
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestBrokenPromises(t *testing.T) {
	// tests that unfullfilled promises are tracked correctly
	originalGossipSubIWantFollowupTime := GossipSubIWantFollowupTime
	GossipSubIWantFollowupTime = 100 * time.Millisecond
	defer func() {
		GossipSubIWantFollowupTime = originalGossipSubIWantFollowupTime
	}()

	gt := newGossipTracer()

	peerA := peer.ID("A")
	peerB := peer.ID("B")
	peerC := peer.ID("C")

	var mids []string
	for i := 0; i < 100; i++ {
		m := makeTestMessage(i)
		m.From = []byte(peerA)
		mid := DefaultMsgIdFn(m)
		mids = append(mids, mid)
	}

	gt.AddPromise(peerA, mids)
	gt.AddPromise(peerB, mids)
	gt.AddPromise(peerC, mids)

	// no broken promises yet
	brokenPromises := gt.GetBrokenPromises()
	if brokenPromises != nil {
		t.Fatal("expected no broken promises")
	}

	// throttle one of the peers to save his promises
	gt.ThrottlePeer(peerC)

	// make promises break
	time.Sleep(GossipSubIWantFollowupTime + 10*time.Millisecond)

	brokenPromises = gt.GetBrokenPromises()
	if len(brokenPromises) != 2 {
		t.Fatalf("expected 2 broken prmises, got %d", len(brokenPromises))
	}

	brokenPromisesA := brokenPromises[peerA]
	if brokenPromisesA != 1 {
		t.Fatalf("expected 1 broken promise from A, got %d", brokenPromisesA)
	}

	brokenPromisesB := brokenPromises[peerB]
	if brokenPromisesB != 1 {
		t.Fatalf("expected 1 broken promise from A, got %d", brokenPromisesB)
	}
}

func TestNoBrokenPromises(t *testing.T) {
	// like above, but this time we deliver messages to fullfil the promises
	originalGossipSubIWantFollowupTime := GossipSubIWantFollowupTime
	GossipSubIWantFollowupTime = 100 * time.Millisecond
	defer func() {
		GossipSubIWantFollowupTime = originalGossipSubIWantFollowupTime
	}()

	gt := newGossipTracer()

	peerA := peer.ID("A")
	peerB := peer.ID("B")

	var msgs []*pb.Message
	var mids []string
	for i := 0; i < 100; i++ {
		m := makeTestMessage(i)
		m.From = []byte(peerA)
		msgs = append(msgs, m)
		mid := DefaultMsgIdFn(m)
		mids = append(mids, mid)
	}

	gt.AddPromise(peerA, mids)
	gt.AddPromise(peerB, mids)

	for _, m := range msgs {
		gt.DeliverMessage(&Message{Message: m})
	}

	time.Sleep(GossipSubIWantFollowupTime + 10*time.Millisecond)

	// there should be no broken promises
	brokenPromises := gt.GetBrokenPromises()
	if brokenPromises != nil {
		t.Fatal("expected no broken promises")
	}

}
