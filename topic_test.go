package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func getTopics(psubs []*PubSub, topicID string, opts ...TopicOpt) []*Topic {
	topics := make([]*Topic, len(psubs))

	for i, ps := range psubs {
		t, err := ps.Join(topicID, opts...)
		if err != nil {
			panic(err)
		}
		topics[i] = t
	}

	return topics
}

func getTopicEvts(topics []*Topic, opts ...TopicEventHandlerOpt) []*TopicEventHandler {
	handlers := make([]*TopicEventHandler, len(topics))

	for i, t := range topics {
		h, err := t.EventHandler(opts...)
		if err != nil {
			panic(err)
		}
		handlers[i] = h
	}

	return handlers
}

func TestTopicCloseWithOpenSubscription(t *testing.T) {
	var sub *Subscription
	var err error
	testTopicCloseWithOpenResource(t,
		func(topic *Topic) {
			sub, err = topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
		},
		func() {
			sub.Cancel()
		},
	)
}

func TestTopicCloseWithOpenEventHandler(t *testing.T) {
	var evts *TopicEventHandler
	var err error
	testTopicCloseWithOpenResource(t,
		func(topic *Topic) {
			evts, err = topic.EventHandler()
			if err != nil {
				t.Fatal(err)
			}
		},
		func() {
			evts.Cancel()
		},
	)
}

func TestTopicCloseWithOpenRelay(t *testing.T) {
	var relayCancel RelayCancelFunc
	var err error
	testTopicCloseWithOpenResource(t,
		func(topic *Topic) {
			relayCancel, err = topic.Relay()
			if err != nil {
				t.Fatal(err)
			}
		},
		func() {
			relayCancel()
		},
	)
}

func testTopicCloseWithOpenResource(t *testing.T, openResource func(topic *Topic), closeResource func()) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numHosts = 1
	topicID := "foobar"
	hosts := getNetHosts(t, ctx, numHosts)
	ps := getPubsub(ctx, hosts[0])

	// Try create and cancel topic
	topic, err := ps.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	if err := topic.Close(); err != nil {
		t.Fatal(err)
	}

	// Try create and cancel topic while there's an outstanding subscription/event handler
	topic, err = ps.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	openResource(topic)

	if err := topic.Close(); err == nil {
		t.Fatal("expected an error closing a topic with an open resource")
	}

	// Check if the topic closes properly after closing the resource
	closeResource()
	time.Sleep(time.Millisecond * 100)

	if err := topic.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTopicReuse(t *testing.T) {
	t.Skip()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numHosts = 2
	topicID := "foobar"
	hosts := getNetHosts(t, ctx, numHosts)

	sender := getPubsub(ctx, hosts[0], WithDiscovery(&dummyDiscovery{}))
	receiver := getPubsub(ctx, hosts[1])

	connectAll(t, hosts)

	// Sender creates topic
	sendTopic, err := sender.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// Receiver creates and subscribes to the topic
	receiveTopic, err := receiver.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	sub, err := receiveTopic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	firstMsg := []byte("1")
	if err := sendTopic.Publish(ctx, firstMsg, WithReadiness(MinTopicSize(1))); err != nil {
		t.Fatal(err)
	}

	msg, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(msg.GetData(), firstMsg) {
		t.Fatal("received incorrect message")
	}

	if err := sendTopic.Close(); err != nil {
		t.Fatal(err)
	}

	// Recreate the same topic
	newSendTopic, err := sender.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	// Try sending data with original topic
	illegalSend := []byte("illegal")
	if err := sendTopic.Publish(ctx, illegalSend); err != ErrTopicClosed {
		t.Fatal(err)
	}

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second*2)
	defer timeoutCancel()
	msg, err = sub.Next(timeoutCtx)
	if err != context.DeadlineExceeded {
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(msg.GetData(), illegalSend) {
			t.Fatal("received incorrect message from illegal topic")
		}
		t.Fatal("received message sent by illegal topic")
	}
	timeoutCancel()

	// Try cancelling the new topic by using the original topic
	if err := sendTopic.Close(); err != nil {
		t.Fatal(err)
	}

	secondMsg := []byte("2")
	if err := newSendTopic.Publish(timeoutCtx, secondMsg); err != nil {
		t.Fatal(err)
	}

	timeoutCtx, timeoutCancel = context.WithTimeout(ctx, time.Second*2)
	defer timeoutCancel()
	msg, err = sub.Next(timeoutCtx)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(msg.GetData(), secondMsg) {
		t.Fatal("received incorrect message")
	}
}

func TestTopicEventHandlerCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numHosts = 5
	topicID := "foobar"
	hosts := getNetHosts(t, ctx, numHosts)
	ps := getPubsub(ctx, hosts[0])

	// Try create and cancel topic
	topic, err := ps.Join(topicID)
	if err != nil {
		t.Fatal(err)
	}

	evts, err := topic.EventHandler()
	if err != nil {
		t.Fatal(err)
	}
	evts.Cancel()
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second*2)
	defer timeoutCancel()
	connectAll(t, hosts)
	_, err = evts.NextPeerEvent(timeoutCtx)
	if err != context.DeadlineExceeded {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("received event after cancel")
	}
}

func TestSubscriptionJoinNotification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numLateSubscribers = 10
	const numHosts = 20
	hosts := getNetHosts(t, ctx, numHosts)
	topics := getTopics(getPubsubs(ctx, hosts), "foobar")
	evts := getTopicEvts(topics)

	subs := make([]*Subscription, numHosts)
	topicPeersFound := make([]map[peer.ID]struct{}, numHosts)

	// Have some peers subscribe earlier than other peers.
	// This exercises whether we get subscription notifications from
	// existing peers.
	for i, topic := range topics[numLateSubscribers:] {
		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		subs[i] = subch
	}

	connectAll(t, hosts)

	time.Sleep(time.Millisecond * 100)

	// Have the rest subscribe
	for i, topic := range topics[:numLateSubscribers] {
		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		subs[i+numLateSubscribers] = subch
	}

	wg := sync.WaitGroup{}
	for i := 0; i < numHosts; i++ {
		peersFound := make(map[peer.ID]struct{})
		topicPeersFound[i] = peersFound
		evt := evts[i]
		wg.Add(1)
		go func(peersFound map[peer.ID]struct{}) {
			defer wg.Done()
			for len(peersFound) < numHosts-1 {
				event, err := evt.NextPeerEvent(ctx)
				if err != nil {
					panic(err)
				}
				if event.Type == PeerJoin {
					peersFound[event.Peer] = struct{}{}
				}
			}
		}(peersFound)
	}

	wg.Wait()
	for _, peersFound := range topicPeersFound {
		if len(peersFound) != numHosts-1 {
			t.Fatal("incorrect number of peers found")
		}
	}
}

func TestSubscriptionLeaveNotification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numHosts = 20
	hosts := getNetHosts(t, ctx, numHosts)
	psubs := getPubsubs(ctx, hosts)
	topics := getTopics(psubs, "foobar")
	evts := getTopicEvts(topics)

	subs := make([]*Subscription, numHosts)
	topicPeersFound := make([]map[peer.ID]struct{}, numHosts)

	// Subscribe all peers and wait until they've all been found
	for i, topic := range topics {
		subch, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		subs[i] = subch
	}

	connectAll(t, hosts)

	time.Sleep(time.Millisecond * 100)

	wg := sync.WaitGroup{}
	for i := 0; i < numHosts; i++ {
		peersFound := make(map[peer.ID]struct{})
		topicPeersFound[i] = peersFound
		evt := evts[i]
		wg.Add(1)
		go func(peersFound map[peer.ID]struct{}) {
			defer wg.Done()
			for len(peersFound) < numHosts-1 {
				event, err := evt.NextPeerEvent(ctx)
				if err != nil {
					panic(err)
				}
				if event.Type == PeerJoin {
					peersFound[event.Peer] = struct{}{}
				}
			}
		}(peersFound)
	}

	wg.Wait()
	for _, peersFound := range topicPeersFound {
		if len(peersFound) != numHosts-1 {
			t.Fatal("incorrect number of peers found")
		}
	}

	// Test removing peers and verifying that they cause events
	subs[1].Cancel()
	hosts[2].Close()
	psubs[0].BlacklistPeer(hosts[3].ID())

	leavingPeers := make(map[peer.ID]struct{})
	for len(leavingPeers) < 3 {
		event, err := evts[0].NextPeerEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if event.Type == PeerLeave {
			leavingPeers[event.Peer] = struct{}{}
		}
	}

	if _, ok := leavingPeers[hosts[1].ID()]; !ok {
		t.Fatal(fmt.Errorf("canceling subscription did not cause a leave event"))
	}
	if _, ok := leavingPeers[hosts[2].ID()]; !ok {
		t.Fatal(fmt.Errorf("closing host did not cause a leave event"))
	}
	if _, ok := leavingPeers[hosts[3].ID()]; !ok {
		t.Fatal(fmt.Errorf("blacklisting peer did not cause a leave event"))
	}
}

func TestSubscriptionManyNotifications(t *testing.T) {
	t.Skip("flaky test disabled")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "foobar"

	const numHosts = 33
	hosts := getNetHosts(t, ctx, numHosts)
	topics := getTopics(getPubsubs(ctx, hosts), topic)
	evts := getTopicEvts(topics)

	subs := make([]*Subscription, numHosts)
	topicPeersFound := make([]map[peer.ID]struct{}, numHosts)

	// Subscribe all peers except one and wait until they've all been found
	for i := 1; i < numHosts; i++ {
		subch, err := topics[i].Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		subs[i] = subch
	}

	connectAll(t, hosts)

	time.Sleep(time.Millisecond * 100)

	wg := sync.WaitGroup{}
	for i := 1; i < numHosts; i++ {
		peersFound := make(map[peer.ID]struct{})
		topicPeersFound[i] = peersFound
		evt := evts[i]
		wg.Add(1)
		go func(peersFound map[peer.ID]struct{}) {
			defer wg.Done()
			for len(peersFound) < numHosts-2 {
				event, err := evt.NextPeerEvent(ctx)
				if err != nil {
					panic(err)
				}
				if event.Type == PeerJoin {
					peersFound[event.Peer] = struct{}{}
				}
			}
		}(peersFound)
	}

	wg.Wait()
	for _, peersFound := range topicPeersFound[1:] {
		if len(peersFound) != numHosts-2 {
			t.Fatalf("found %d peers, expected %d", len(peersFound), numHosts-2)
		}
	}

	// Wait for remaining peer to find other peers
	remPeerTopic, remPeerEvts := topics[0], evts[0]
	for len(remPeerTopic.ListPeers()) < numHosts-1 {
		time.Sleep(time.Millisecond * 100)
	}

	// Subscribe the remaining peer and check that all the events came through
	sub, err := remPeerTopic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	subs[0] = sub

	peerState := readAllQueuedEvents(ctx, t, remPeerEvts)

	if len(peerState) != numHosts-1 {
		t.Fatal("incorrect number of peers found")
	}

	for _, e := range peerState {
		if e != PeerJoin {
			t.Fatal("non Join event occurred")
		}
	}

	// Unsubscribe all peers except one and check that all the events came through
	for i := 1; i < numHosts; i++ {
		subs[i].Cancel()
	}

	// Wait for remaining peer to disconnect from the other peers
	for len(topics[0].ListPeers()) != 0 {
		time.Sleep(time.Millisecond * 100)
	}

	peerState = readAllQueuedEvents(ctx, t, remPeerEvts)

	if len(peerState) != numHosts-1 {
		t.Fatal("incorrect number of peers found")
	}

	for _, e := range peerState {
		if e != PeerLeave {
			t.Fatal("non Leave event occurred")
		}
	}
}

func TestSubscriptionNotificationSubUnSub(t *testing.T) {
	// Resubscribe and Unsubscribe a peers and check the state for consistency
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "foobar"

	const numHosts = 35
	hosts := getNetHosts(t, ctx, numHosts)
	topics := getTopics(getPubsubs(ctx, hosts), topic)

	for i := 1; i < numHosts; i++ {
		connect(t, hosts[0], hosts[i])
	}
	time.Sleep(time.Millisecond * 100)

	notifSubThenUnSub(ctx, t, topics)
}

func TestTopicRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const topic = "foobar"
	const numHosts = 5

	hosts := getNetHosts(t, ctx, numHosts)
	topics := getTopics(getPubsubs(ctx, hosts), topic)

	// [0.Rel] - [1.Rel] - [2.Sub]
	//             |
	//           [3.Rel] - [4.Sub]

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[1], hosts[3])
	connect(t, hosts[3], hosts[4])

	time.Sleep(time.Millisecond * 100)

	var subs []*Subscription

	for i, topic := range topics {
		if i == 2 || i == 4 {
			sub, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}

			subs = append(subs, sub)
		} else {
			_, err := topic.Relay()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 100; i++ {
		msg := []byte("message")

		owner := rand.Intn(len(topics))

		err := topics[owner].Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}

		for _, sub := range subs {
			received, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(msg, received.Data) {
				t.Fatal("received message is other than expected")
			}
		}
	}
}

func TestTopicRelayReuse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "foobar"
	const numHosts = 1

	hosts := getNetHosts(t, ctx, numHosts)
	pubsubs := getPubsubs(ctx, hosts)
	topics := getTopics(pubsubs, topic)

	relay1Cancel, err := topics[0].Relay()
	if err != nil {
		t.Fatal(err)
	}

	relay2Cancel, err := topics[0].Relay()
	if err != nil {
		t.Fatal(err)
	}

	relay3Cancel, err := topics[0].Relay()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	res := make(chan bool, 1)
	pubsubs[0].eval <- func() {
		res <- pubsubs[0].myRelays[topic] == 3
	}

	isCorrectNumber := <-res
	if !isCorrectNumber {
		t.Fatal("incorrect number of relays")
	}

	// only the first invocation should take effect
	relay1Cancel()
	relay1Cancel()
	relay1Cancel()

	pubsubs[0].eval <- func() {
		res <- pubsubs[0].myRelays[topic] == 2
	}

	isCorrectNumber = <-res
	if !isCorrectNumber {
		t.Fatal("incorrect number of relays")
	}

	relay2Cancel()
	relay3Cancel()

	time.Sleep(time.Millisecond * 100)

	pubsubs[0].eval <- func() {
		res <- pubsubs[0].myRelays[topic] == 0
	}

	isCorrectNumber = <-res
	if !isCorrectNumber {
		t.Fatal("incorrect number of relays")
	}
}

func TestTopicRelayOnClosedTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "foobar"
	const numHosts = 1

	hosts := getNetHosts(t, ctx, numHosts)
	topics := getTopics(getPubsubs(ctx, hosts), topic)

	err := topics[0].Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = topics[0].Relay()
	if err == nil {
		t.Fatalf("error should be returned")
	}
}

func notifSubThenUnSub(ctx context.Context, t *testing.T, topics []*Topic) {
	primaryTopic := topics[0]
	msgs := make([]*Subscription, len(topics))
	checkSize := len(topics) - 1

	// Subscribe all peers to the topic
	var err error
	for i, topic := range topics {
		msgs[i], err = topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for the primary peer to be connected to the other peers
	for len(primaryTopic.ListPeers()) < checkSize {
		time.Sleep(time.Millisecond * 100)
	}

	// Unsubscribe all peers except the primary
	for i := 1; i < checkSize+1; i++ {
		msgs[i].Cancel()
	}

	// Wait for the unsubscribe messages to reach the primary peer
	for len(primaryTopic.ListPeers()) < 0 {
		time.Sleep(time.Millisecond * 100)
	}

	// read all available events and verify that there are no events to process
	// this is because every peer that joined also left
	primaryEvts, err := primaryTopic.EventHandler()
	if err != nil {
		t.Fatal(err)
	}
	peerState := readAllQueuedEvents(ctx, t, primaryEvts)

	if len(peerState) != 0 {
		for p, s := range peerState {
			fmt.Println(p, s)
		}
		t.Fatalf("Received incorrect events. %d extra events", len(peerState))
	}
}

func readAllQueuedEvents(ctx context.Context, t *testing.T, evt *TopicEventHandler) map[peer.ID]EventType {
	peerState := make(map[peer.ID]EventType)
	for {
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
		event, err := evt.NextPeerEvent(ctx)
		cancel()

		if err == context.DeadlineExceeded {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		e, ok := peerState[event.Peer]
		if !ok {
			peerState[event.Peer] = event.Type
		} else if e != event.Type {
			delete(peerState, event.Peer)
		}
	}
	return peerState
}
