package blurry

import (
	"context"

	"github.com/libp2p/go-libp2p/core/host"
)

func NewBlurry(ctx context.Context, node host.Host) error {
	gossip, err := NewGossipBroadcaster(ctx, node)
	if err != nil {
		return err
	}
	NewCRDT(ctx, gossip, ds.newda, nil, nil, "")
}
