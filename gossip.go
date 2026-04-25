package blurry

import (
	"context"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

var log = logging.Logger("gossip")

const gossipTopic = "/gossip/1.0.0"

// GossipBroadcaster broadcasts and receives CRDT updates over a single
// libp2p GossipSub topic. It satisfies the go-ds-crdt Broadcaster interface.
type GossipBroadcaster struct {
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

// NewGossipBroadcaster joins the gossip topic and starts a subscription.
// The broadcaster lifecycle is bound to ctx via the underlying PubSub.
func NewGossipBroadcaster(ctx context.Context, node host.Host) (*GossipBroadcaster, error) {
	ps, err := pubsub.NewGossipSub(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("gossip: new pubsub: %w", err)
	}

	topic, err := ps.Join(gossipTopic)
	if err != nil {
		return nil, fmt.Errorf("gossip: join %s: %w", gossipTopic, err)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		_ = topic.Close()
		return nil, fmt.Errorf("gossip: subscribe: %w", err)
	}

	log.Infof("gossip: subscribed to topic %s", gossipTopic)

	return &GossipBroadcaster{topic: topic, sub: sub}, nil
}

// Broadcast publishes data to all peers on the gossip topic.
func (gb *GossipBroadcaster) Broadcast(ctx context.Context, data []byte) error {
	return gb.topic.Publish(ctx, data)
}

// Next returns the next message received on the gossip topic.
func (gb *GossipBroadcaster) Next(ctx context.Context) ([]byte, error) {
	msg, err := gb.sub.Next(ctx)
	if err != nil {
		return nil, err
	}
	return msg.GetData(), nil
}

// Close cancels the subscription and closes the topic.
func (gb *GossipBroadcaster) Close() error {
	gb.sub.Cancel()
	return gb.topic.Close()
}
