package pubsub

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-msgio/protoio"
)

// Test that when Gossipsub receives too many IWANT messages from a peer
// for the same message ID, it cuts off the peer
func TestGossipsubAttackSpamIWANT(t *testing.T) {
	t.Skip()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create legitimate and attacker hosts
	hosts := getNetHosts(t, ctx, 2)
	legit := hosts[0]
	attacker := hosts[1]

	// Set up gossipsub on the legit host
	ps, err := NewGossipSub(ctx, legit)
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe to mytopic on the legit host
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	// Used to publish a message with random data
	publishMsg := func() {
		data := make([]byte, 16)
		rand.Read(data)

		if err = ps.Publish(mytopic, data); err != nil {
			t.Fatal(err)
		}
	}

	// Wait a bit after the last message before checking we got the
	// right number of messages
	msgWaitMax := time.Second
	msgCount := 0
	msgTimer := time.NewTimer(msgWaitMax)

	// Checks we received the right number of messages
	checkMsgCount := func() {
		// After the original message from the legit host, we keep sending
		// IWANT until it stops replying. So the number of messages is
		// <original message> + GossipSubGossipRetransmission
		exp := 1 + GossipSubGossipRetransmission
		if msgCount != exp {
			t.Fatalf("Expected %d messages, got %d", exp, msgCount)
		}
	}

	// Wait for the timer to expire
	go func() {
		select {
		case <-msgTimer.C:
			checkMsgCount()
			cancel()
			return
		case <-ctx.Done():
			checkMsgCount()
		}
	}()

	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// When the legit host connects it will send us its subscriptions
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// Reply by subcribing to the topic and grafting to the peer
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
				})

				go func() {
					// Wait for a short interval to make sure the legit host
					// received and processed the subscribe + graft
					time.Sleep(100 * time.Millisecond)

					// Publish a message from the legit host
					publishMsg()
				}()
			}
		}

		// Each time the legit host sends a message
		for _, msg := range irpc.GetPublish() {
			// Increment the number of messages and reset the timer
			msgCount++
			msgTimer.Reset(msgWaitMax)

			// Shouldn't get more than the expected number of messages
			exp := 1 + GossipSubGossipRetransmission
			if msgCount > exp {
				cancel()
				t.Fatal("Received too many responses")
			}

			// Send an IWANT with the message ID, causing the legit host
			// to send another message (until it cuts off the attacker for
			// being spammy)
			iwantlst := []string{DefaultMsgIdFn(msg)}
			iwant := []*pb.ControlIWant{{MessageIDs: iwantlst}}
			orpc := rpcWithControl(nil, nil, iwant, nil, nil)
			writeMsg(&orpc.RPC)
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// Test that Gossipsub only responds to IHAVE with IWANT once per heartbeat
func TestGossipsubAttackSpamIHAVE(t *testing.T) {
	t.Skip()
	originalGossipSubIWantFollowupTime := GossipSubIWantFollowupTime
	GossipSubIWantFollowupTime = 10 * time.Second
	defer func() {
		GossipSubIWantFollowupTime = originalGossipSubIWantFollowupTime
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create legitimate and attacker hosts
	hosts := getNetHosts(t, ctx, 2)
	legit := hosts[0]
	attacker := hosts[1]

	// Set up gossipsub on the legit host
	ps, err := NewGossipSub(ctx, legit,
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
			}))
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe to mytopic on the legit host
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	iWantCount := 0
	iWantCountMx := sync.Mutex{}
	getIWantCount := func() int {
		iWantCountMx.Lock()
		defer iWantCountMx.Unlock()
		return iWantCount
	}
	addIWantCount := func(i int) {
		iWantCountMx.Lock()
		defer iWantCountMx.Unlock()
		iWantCount += i
	}

	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// When the legit host connects it will send us its subscriptions
		errs := make(chan error)
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// Reply by subcribing to the topic and grafting to the peer
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
				})

				go func() {
					defer close(errs)
					defer cancel()

					// Wait for a short interval to make sure the legit host
					// received and processed the subscribe + graft
					time.Sleep(20 * time.Millisecond)

					// Send a bunch of IHAVEs
					for i := 0; i < 3*GossipSubMaxIHaveLength; i++ {
						ihavelst := []string{"someid" + strconv.Itoa(i)}
						ihave := []*pb.ControlIHave{{TopicID: sub.Topicid, MessageIDs: ihavelst}}
						orpc := rpcWithControl(nil, ihave, nil, nil, nil)
						writeMsg(&orpc.RPC)
					}

					time.Sleep(GossipSubHeartbeatInterval)

					// Should have hit the maximum number of IWANTs per peer
					// per heartbeat
					iwc := getIWantCount()
					if iwc > GossipSubMaxIHaveLength {
						errs <- fmt.Errorf("Expecting max %d IWANTs per heartbeat but received %d", GossipSubMaxIHaveLength, iwc)
						return
					}
					firstBatchCount := iwc

					// the score should still be 0 because we haven't broken any promises yet
					score := ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score != 0 {
						errs <- fmt.Errorf("Expected 0 score, but got %f", score)
						return
					}

					// Send a bunch of IHAVEs
					for i := 0; i < 3*GossipSubMaxIHaveLength; i++ {
						ihavelst := []string{"someid" + strconv.Itoa(i+100)}
						ihave := []*pb.ControlIHave{{TopicID: sub.Topicid, MessageIDs: ihavelst}}
						orpc := rpcWithControl(nil, ihave, nil, nil, nil)
						writeMsg(&orpc.RPC)
					}

					time.Sleep(GossipSubHeartbeatInterval)

					// Should have sent more IWANTs after the heartbeat
					iwc = getIWantCount()
					if iwc == firstBatchCount {
						errs <- fmt.Errorf("Expecting to receive more IWANTs after heartbeat but did not")
						return
					}
					// Should not be more than the maximum per heartbeat
					if iwc-firstBatchCount > GossipSubMaxIHaveLength {
						errs <- fmt.Errorf("Expecting max %d IWANTs per heartbeat but received %d", GossipSubMaxIHaveLength, iwc-firstBatchCount)
						return
					}

					time.Sleep(GossipSubIWantFollowupTime)

					// The score should now be negative because of broken promises
					score = ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score >= 0 {
						errs <- fmt.Errorf("Expected negative score, but got %f", score)
						return
					}
				}()
			}
		}

		for err := range errs {
			t.Fatal(err)
		}

		// Record the count of received IWANT messages
		if ctl := irpc.GetControl(); ctl != nil {
			addIWantCount(len(ctl.GetIwant()))
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// Test that when Gossipsub receives GRAFT for an unknown topic, it ignores
// the request
func TestGossipsubAttackGRAFTNonExistentTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create legitimate and attacker hosts
	hosts := getNetHosts(t, ctx, 2)
	legit := hosts[0]
	attacker := hosts[1]

	// Set up gossipsub on the legit host
	ps, err := NewGossipSub(ctx, legit)
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe to mytopic on the legit host
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	// Checks that we haven't received any PRUNE message
	pruneCount := 0
	checkForPrune := func() {
		// We send a GRAFT for a non-existent topic so we shouldn't
		// receive a PRUNE in response
		if pruneCount != 0 {
			t.Fatalf("Got %d unexpected PRUNE messages", pruneCount)
		}
	}

	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// When the legit host connects it will send us its subscriptions
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// Reply by subcribing to the topic and grafting to the peer
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
				})

				// Graft to the peer on a non-existent topic
				nonExistentTopic := "non-existent"
				writeMsg(&pb.RPC{
					Control: &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &nonExistentTopic}}},
				})

				go func() {
					// Wait for a short interval to make sure the legit host
					// received and processed the subscribe + graft
					time.Sleep(100 * time.Millisecond)

					// We shouldn't get any prune messages becaue the topic
					// doesn't exist
					checkForPrune()
					cancel()
				}()
			}
		}

		// Record the count of received PRUNE messages
		if ctl := irpc.GetControl(); ctl != nil {
			pruneCount += len(ctl.GetPrune())
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// Test that when Gossipsub receives GRAFT for a peer that has been PRUNED,
// it penalizes through P7 and eventually graylists and ignores the requests if the
// GRAFTs are coming too fast
func TestGossipsubAttackGRAFTDuringBackoff(t *testing.T) {
	t.Skip()
	originalGossipSubPruneBackoff := GossipSubPruneBackoff
	GossipSubPruneBackoff = 200 * time.Millisecond
	originalGossipSubGraftFloodThreshold := GossipSubGraftFloodThreshold
	GossipSubGraftFloodThreshold = 100 * time.Millisecond
	defer func() {
		GossipSubPruneBackoff = originalGossipSubPruneBackoff
		GossipSubGraftFloodThreshold = originalGossipSubGraftFloodThreshold
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create legitimate and attacker hosts
	hosts := getNetHosts(t, ctx, 2)
	legit := hosts[0]
	attacker := hosts[1]

	// Set up gossipsub on the legit host
	ps, err := NewGossipSub(ctx, legit,
		WithPeerScore(
			&PeerScoreParams{
				AppSpecificScore:       func(peer.ID) float64 { return 0 },
				BehaviourPenaltyWeight: -100,
				BehaviourPenaltyDecay:  ScoreParameterDecay(time.Minute),
				DecayInterval:          DefaultDecayInterval,
				DecayToZero:            DefaultDecayToZero,
			},
			&PeerScoreThresholds{
				GossipThreshold:   -100,
				PublishThreshold:  -500,
				GraylistThreshold: -1000,
			}))
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe to mytopic on the legit host
	mytopic := "mytopic"
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	pruneCount := 0
	pruneCountMx := sync.Mutex{}
	getPruneCount := func() int {
		pruneCountMx.Lock()
		defer pruneCountMx.Unlock()
		return pruneCount
	}
	addPruneCount := func(i int) {
		pruneCountMx.Lock()
		defer pruneCountMx.Unlock()
		pruneCount += i
	}

	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// When the legit host connects it will send us its subscriptions
		errs := make(chan error)
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// Reply by subcribing to the topic and grafting to the peer
				graft := []*pb.ControlGraft{{TopicID: sub.Topicid}}
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: graft},
				})

				go func() {
					defer close(errs)
					defer cancel()

					// Wait for a short interval to make sure the legit host
					// received and processed the subscribe + graft
					time.Sleep(20 * time.Millisecond)

					// No PRUNE should have been sent at this stage
					pc := getPruneCount()
					if pc != 0 {
						errs <- fmt.Errorf("Expected %d PRUNE messages but got %d", 0, pc)
						return
					}

					// Send a PRUNE to remove the attacker node from the legit
					// host's mesh
					var prune []*pb.ControlPrune
					prune = append(prune, &pb.ControlPrune{TopicID: sub.Topicid})
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Prune: prune},
					})

					time.Sleep(20 * time.Millisecond)

					// No PRUNE should have been sent at this stage
					pc = getPruneCount()
					if pc != 0 {
						errs <- fmt.Errorf("Expected %d PRUNE messages but got %d", 0, pc)
						return
					}

					// wait for the GossipSubGraftFloodThreshold to pass before attempting another graft
					time.Sleep(GossipSubGraftFloodThreshold + time.Millisecond)

					// Send a GRAFT to attempt to rejoin the mesh
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					time.Sleep(20 * time.Millisecond)

					// We should have been peanalized by the peer for sending before the backoff has expired
					// but should still receive a PRUNE because we haven't dropped below GraylistThreshold
					// yet.
					pc = getPruneCount()
					if pc != 1 {
						errs <- fmt.Errorf("Expected %d PRUNE messages but got %d", 1, pc)
						return
					}

					score1 := ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score1 >= 0 {
						errs <- fmt.Errorf("Expected negative score, but got %f", score1)
						return
					}

					// Send a GRAFT again to attempt to rejoin the mesh
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					time.Sleep(20 * time.Millisecond)

					// we are before the flood threshold so we should be penalized twice, but still get
					// a PRUNE because we are before the flood threshold
					pc = getPruneCount()
					if pc != 2 {
						errs <- fmt.Errorf("Expected %d PRUNE messages but got %d", 2, pc)
						return
					}

					score2 := ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score2 >= score1 {
						errs <- fmt.Errorf("Expected score below %f, but got %f", score1, score2)
						return
					}

					// Send another GRAFT; this should get us a PRUNE, but penalize us below the graylist threshold
					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					time.Sleep(20 * time.Millisecond)

					pc = getPruneCount()
					if pc != 3 {
						errs <- fmt.Errorf("Expected %d PRUNE messages but got %d", 3, pc)
						return
					}

					score3 := ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
					if score3 >= score2 {
						errs <- fmt.Errorf("Expected score below %f, but got %f", score2, score3)
						return
					}
					if score3 >= -1000 {
						errs <- fmt.Errorf("Expected score below %f, but got %f", -1000.0, score3)
						return
					}

					// Wait for the PRUNE backoff to expire and try again; this time we should fail
					// because we are below the graylist threshold, so our RPC should be ignored and
					// we should get no PRUNE back
					time.Sleep(GossipSubPruneBackoff + time.Millisecond)

					writeMsg(&pb.RPC{
						Control: &pb.ControlMessage{Graft: graft},
					})

					time.Sleep(20 * time.Millisecond)

					pc = getPruneCount()
					if pc != 3 {
						errs <- fmt.Errorf("Expected %d PRUNE messages but got %d", 3, pc)
						return
					}

					// make sure we are _not_ in the mesh
					res := make(chan bool)
					ps.eval <- func() {
						mesh := ps.rt.(*GossipSubRouter).mesh[mytopic]
						_, inMesh := mesh[attacker.ID()]
						res <- inMesh
					}

					inMesh := <-res
					if inMesh {
						errs <- fmt.Errorf("Expected to not be in the mesh of the legitimate host")
						return
					}
				}()
			}
		}

		for err := range errs {
			t.Fatal(err)
		}

		if ctl := irpc.GetControl(); ctl != nil {
			addPruneCount(len(ctl.GetPrune()))
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

type gsAttackInvalidMsgTracer struct {
	rejectCount int
}

func (t *gsAttackInvalidMsgTracer) Trace(evt *pb.TraceEvent) {
	// fmt.Printf("    %s %s\n", evt.Type, evt)
	if evt.GetType() == pb.TraceEvent_REJECT_MESSAGE {
		t.rejectCount++
	}
}

// Test that when Gossipsub receives a lot of invalid messages from
// a peer it should graylist the peer
func TestGossipsubAttackInvalidMessageSpam(t *testing.T) {
	t.Skip()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create legitimate and attacker hosts
	hosts := getNetHosts(t, ctx, 2)
	legit := hosts[0]
	attacker := hosts[1]

	mytopic := "mytopic"

	// Create parameters with reasonable default values
	params := &PeerScoreParams{
		AppSpecificScore:            func(peer.ID) float64 { return 0 },
		IPColocationFactorWeight:    0,
		IPColocationFactorThreshold: 1,
		DecayInterval:               5 * time.Second,
		DecayToZero:                 0.01,
		RetainScore:                 10 * time.Second,
		Topics:                      make(map[string]*TopicScoreParams),
	}
	params.Topics[mytopic] = &TopicScoreParams{
		TopicWeight:                     0.25,
		TimeInMeshWeight:                0.0027,
		TimeInMeshQuantum:               time.Second,
		TimeInMeshCap:                   3600,
		FirstMessageDeliveriesWeight:    0.664,
		FirstMessageDeliveriesDecay:     0.9916,
		FirstMessageDeliveriesCap:       1500,
		MeshMessageDeliveriesWeight:     -0.25,
		MeshMessageDeliveriesDecay:      0.97,
		MeshMessageDeliveriesCap:        400,
		MeshMessageDeliveriesThreshold:  100,
		MeshMessageDeliveriesActivation: 30 * time.Second,
		MeshMessageDeliveriesWindow:     5 * time.Minute,
		MeshFailurePenaltyWeight:        -0.25,
		MeshFailurePenaltyDecay:         0.997,
		InvalidMessageDeliveriesWeight:  -99,
		InvalidMessageDeliveriesDecay:   0.9994,
	}
	thresholds := &PeerScoreThresholds{
		GossipThreshold:   -100,
		PublishThreshold:  -200,
		GraylistThreshold: -300,
		AcceptPXThreshold: 0,
	}

	// Set up gossipsub on the legit host
	tracer := &gsAttackInvalidMsgTracer{}
	ps, err := NewGossipSub(ctx, legit,
		WithEventTracer(tracer),
		WithPeerScore(params, thresholds),
	)
	if err != nil {
		t.Fatal(err)
	}

	attackerScore := func() float64 {
		return ps.rt.(*GossipSubRouter).score.Score(attacker.ID())
	}

	// Subscribe to mytopic on the legit host
	_, err = ps.Subscribe(mytopic)
	if err != nil {
		t.Fatal(err)
	}

	pruneCount := 0
	pruneCountMx := sync.Mutex{}
	getPruneCount := func() int {
		pruneCountMx.Lock()
		defer pruneCountMx.Unlock()
		return pruneCount
	}
	addPruneCount := func(i int) {
		pruneCountMx.Lock()
		defer pruneCountMx.Unlock()
		pruneCount += i
	}

	newMockGS(ctx, t, attacker, func(writeMsg func(*pb.RPC), irpc *pb.RPC) {
		// When the legit host connects it will send us its subscriptions
		errs := make(chan error)
		for _, sub := range irpc.GetSubscriptions() {
			if sub.GetSubscribe() {
				// Reply by subcribing to the topic and grafting to the peer
				writeMsg(&pb.RPC{
					Subscriptions: []*pb.RPC_SubOpts{{Subscribe: sub.Subscribe, Topicid: sub.Topicid}},
					Control:       &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: sub.Topicid}}},
				})

				go func() {
					defer close(errs)
					defer cancel()

					// Attacker score should start at zero
					if attackerScore() != 0 {
						errs <- fmt.Errorf("Expected attacker score to be zero but it's %f", attackerScore())
						return
					}

					// Send a bunch of messages with no signature (these will
					// fail validation and reduce the attacker's score)
					for i := 0; i < 100; i++ {
						msg := &pb.Message{
							Data:  []byte("some data" + strconv.Itoa(i)),
							Topic: &mytopic,
							From:  []byte(attacker.ID()),
							Seqno: []byte{byte(i + 1)},
						}
						writeMsg(&pb.RPC{
							Publish: []*pb.Message{msg},
						})
					}

					// Wait for the initial heartbeat, plus a bit of padding
					time.Sleep(100*time.Millisecond + GossipSubHeartbeatInitialDelay)

					// The attackers score should now have fallen below zero
					if attackerScore() >= 0 {
						errs <- fmt.Errorf("Expected attacker score to be less than zero but it's %f", attackerScore())
						return
					}
					// There should be several rejected messages (because the signature was invalid)
					if tracer.rejectCount == 0 {
						errs <- fmt.Errorf("Expected message rejection but got none")
						return
					}
					// The legit node should have sent a PRUNE message
					pc := getPruneCount()
					if pc == 0 {
						errs <- fmt.Errorf("Expected attacker node to be PRUNED when score drops low enough")
						return
					}
				}()
			}
		}

		for err := range errs {
			t.Fatal(err)
		}

		if ctl := irpc.GetControl(); ctl != nil {
			addPruneCount(len(ctl.GetPrune()))
		}
	})

	connect(t, hosts[0], hosts[1])

	<-ctx.Done()
}

// func turnOnPubsubDebug() {
// 	logging.SetLogLevel("pubsub", "debug")
// }

type mockGSOnRead func(writeMsg func(*pb.RPC), irpc *pb.RPC)

func newMockGS(ctx context.Context, t *testing.T, attacker host.Host, onReadMsg mockGSOnRead) {
	// Listen on the gossipsub protocol
	const gossipSubID = protocol.ID("/meshsub/1.0.0")
	const maxMessageSize = 1024 * 1024
	attacker.SetStreamHandler(gossipSubID, func(stream network.Stream) {
		// When an incoming stream is opened, set up an outgoing stream
		p := stream.Conn().RemotePeer()
		ostream, err := attacker.NewStream(ctx, p, gossipSubID)
		if err != nil {
			t.Fatal(err)
		}

		r := protoio.NewDelimitedReader(stream, maxMessageSize)
		w := protoio.NewDelimitedWriter(ostream)

		var irpc pb.RPC

		writeMsg := func(rpc *pb.RPC) {
			if err = w.WriteMsg(rpc); err != nil {
				t.Fatalf("error writing RPC: %s", err)
			}
		}

		// Keep reading messages and responding
		for {
			// Bail out when the test finishes
			if ctx.Err() != nil {
				return
			}

			irpc.Reset()

			err := r.ReadMsg(&irpc)

			// Bail out when the test finishes
			if ctx.Err() != nil {
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			onReadMsg(writeMsg, &irpc)
		}
	})
}
