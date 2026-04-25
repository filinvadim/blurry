package blurry

import (
	"context"
)

func NewBlurry(ctx context.Context) error {
	gossip, err := NewGossipBroadcaster(ctx, NewGossip(ctx))
	if err != nil {
		return err
	}
	NewCRDT(ctx, gossip, ds.newda, nil, nil, "")
}
