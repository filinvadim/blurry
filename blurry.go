package blurry

import (
	"context"
	"io"
)

type Blurry struct {
	node   io.Closer
	crdt   io.Closer
	ds     io.Closer
	gossip io.Closer
}

func NewBlurry(ctx context.Context, path string, clusterPeers ...string) (*Blurry, error) {
	node, err := NewNode(ctx, clusterPeers...)
	if err != nil {
		return nil, err
	}

	gossip, err := NewGossipBroadcaster(ctx, node.Host())
	if err != nil {
		return nil, err
	}

	ds, err := NewDatastore(path, DefaultOptions)
	if err != nil {
		return nil, err
	}

	crdt, err := NewCRDT(ctx, gossip, ds, node.Host(), node, "")
	if err != nil {
		return nil, err
	}

	return &Blurry{crdt: crdt, ds: ds, gossip: gossip, node: node}, nil
}

func (n *Blurry) Close() error {
	n.gossip.Close()
	n.node.Close()
	n.crdt.Close()
	return n.ds.Close()
}
