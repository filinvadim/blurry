// Package blurry is a CRDT-backed key/value store running on top of
// libp2p (gossip + DHT) and badger. After the v0.x → KV refactor the
// surface is intentionally tiny: NewBlurry wires libp2p + go-ds-crdt +
// badger and exposes a KV facade. The previous Chotki-shaped class /
// object / field machinery and the chotki wire-protocol bridge live
// in legacy/chotki/ and only build with the chotki_legacy tag.
package blurry

import (
	"context"
	"errors"
	"fmt"
	"io"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
)

var blurryLog = logging.Logger("blurry")

// Blurry is the top-level handle that wires together the libp2p node,
// the gossip broadcaster, the badger-backed CRDT store, the KV
// facade, and the HTTP API.
type Blurry struct {
	settings *Settings

	node    *Node
	crdt    *CRDT
	ds      io.Closer
	gossip  *GossipBroadcaster
	kv      *KV
	httpSrv *HTTPServer
}

// NewBlurry creates a Blurry instance configured by Settings, opens the
// badger datastore at path, joins the libp2p network, and connects to
// every replica listed in Settings.ClusterPeers.
func NewBlurry(ctx context.Context, path string, s *Settings) (*Blurry, error) {
	if s == nil {
		s = DefaultSettings()
	}
	s.SetDefaults()
	if err := s.Validate(); err != nil {
		return nil, err
	}

	node, err := NewNode(ctx, s)
	if err != nil {
		return nil, err
	}

	gossip, err := NewGossipBroadcaster(ctx, node.Host())
	if err != nil {
		_ = node.Close()
		return nil, err
	}

	dsOpts := s.Datastore
	if dsOpts == nil {
		opts := *DefaultOptions
		opts.SyncWrites = s.SyncWrites
		dsOpts = &opts
	}
	dataStore, err := NewDatastore(path, dsOpts)
	if err != nil {
		_ = gossip.Close()
		_ = node.Close()
		return nil, err
	}

	crdt, err := NewCRDT(ctx, gossip, dataStore, node.Host(), node, CRDTSettings{
		VersionPrefix:       s.VersionPrefix,
		RebroadcastInterval: s.RebroadcastInterval,
		DAGSyncerTimeout:    s.DAGSyncerTimeout,
	})
	if err != nil {
		_ = dataStore.Close()
		_ = gossip.Close()
		_ = node.Close()
		return nil, err
	}

	b := &Blurry{
		settings: s,
		node:     node,
		gossip:   gossip,
		ds:       dataStore,
		crdt:     crdt,
		kv:       NewKV(crdt.Datastore()),
	}

	// Connect to the whole cluster on start. Errors are logged per-peer
	// and not fatal: missing peers can still join later.
	if err := node.ConnectCluster(ctx); err != nil {
		blurryLog.Warnf("blurry: cluster connect: %v", err)
	}

	if s.HTTPPort > 0 {
		b.httpSrv = NewHTTPServer(b)
		addr := fmt.Sprintf("%s:%d", s.HTTPHost, s.HTTPPort)
		if err := b.httpSrv.Start(addr); err != nil {
			blurryLog.Warnf("blurry: http start %s: %v", addr, err)
		}
	}

	return b, nil
}

// Settings returns the active configuration.
func (b *Blurry) Settings() *Settings { return b.settings }

// KV returns the key/value façade. All writes go through the
// CRDT-wrapped datastore via ds.Batching so they replicate via
// libp2p pubsub.
func (b *Blurry) KV() *KV { return b.kv }

// Listen toggles a listen address on the underlying libp2p host.
func (b *Blurry) Listen(addr string) error {
	h := b.libp2pHost()
	if h == nil {
		return errors.New("blurry: not started")
	}
	mAddr, err := dialAddr(addr, b.settings.ListenPort)
	if err != nil {
		return err
	}
	if err := h.Network().Listen(mAddr); err != nil {
		return fmt.Errorf("blurry: listen %s: %w", addr, err)
	}
	return nil
}

// Connect dials a single replica.
func (b *Blurry) Connect(ctx context.Context, addr string) error {
	h := b.libp2pHost()
	if h == nil {
		return errors.New("blurry: not started")
	}
	info, err := ResolvePeerAddr(addr, b.settings.ListenPort)
	if err != nil && !errors.Is(err, ErrNoPeerID) {
		return err
	}
	if info.ID == "" {
		return errors.New("blurry: peer id is required to connect (use multiaddr with /p2p/<id>)")
	}
	return h.Connect(ctx, info)
}

func (b *Blurry) libp2pHost() host.Host {
	if b == nil || b.node == nil {
		return nil
	}
	return b.node.Host()
}

// Close gracefully tears down all components.
func (b *Blurry) Close() error {
	if b == nil {
		return nil
	}
	var firstErr error
	if b.httpSrv != nil {
		if err := b.httpSrv.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.gossip != nil {
		if err := b.gossip.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.crdt != nil {
		if err := b.crdt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.node != nil {
		if err := b.node.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.ds != nil {
		if err := b.ds.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
