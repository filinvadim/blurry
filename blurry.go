// Package blurry is a Chotki-compatible CRDT-backed key-value store
// running on top of libp2p (gossip + DHT) and badger.
package blurry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	ma "github.com/multiformats/go-multiaddr"
)

var blurryLog = logging.Logger("blurry")

// Blurry is the top-level handle that wires together the libp2p node,
// the gossip broadcaster, the badger-backed CRDT store and the
// Chotki-compatible HTTP API.
type Blurry struct {
	settings *Settings

	node    *Node
	crdt    *CRDT
	ds      io.Closer
	gossip  *GossipBroadcaster
	store   *Store
	httpSrv *HTTPServer
}

// NewBlurry creates a Blurry instance configured by Settings, opens the
// badger datastore at path, joins the libp2p network and connects to
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
		// Mirror Chotki's pebble Sync:true default for ACID durability.
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

	// Pick the store's "source" string in priority order:
	//   1. user-provided Src (chotki parity)
	//   2. deterministic Src derived from PrivateKeySeed
	//   3. fall back to the libp2p peer id
	storeSource := node.Host().ID().String()
	if src := s.EffectiveSrc(); src != 0 {
		storeSource = fmt.Sprintf("%x", src)
	}
	store := NewStore(dataStore, storeSource)

	b := &Blurry{
		settings: s,
		node:     node,
		gossip:   gossip,
		ds:       dataStore,
		crdt:     crdt,
		store:    store,
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

// Store returns the Chotki-compatible class/object store.
func (b *Blurry) Store() *Store { return b.store }

// Listen toggles a listen address on the underlying libp2p host. Accepts
// the same forms ResolvePeerAddr does (multiaddr, host:port, bare IP).
func (b *Blurry) Listen(addr string) error {
	if b == nil || b.node == nil || b.node.node == nil {
		return errors.New("blurry: not started")
	}
	mAddr, err := dialAddr(addr, b.settings.ListenPort)
	if err != nil {
		return err
	}
	if err := b.node.node.Network().Listen(mAddr); err != nil {
		return fmt.Errorf("blurry: listen %s: %w", addr, err)
	}
	return nil
}

// Connect dials a single replica. The host's connection manager keeps
// the link alive afterwards. Use Settings.ClusterPeers for the boot set.
func (b *Blurry) Connect(ctx context.Context, addr string) error {
	if b == nil || b.node == nil || b.node.node == nil {
		return errors.New("blurry: not started")
	}
	info, err := ResolvePeerAddr(addr, b.settings.ListenPort)
	if err != nil && !errors.Is(err, ErrNoPeerID) {
		return err
	}
	if info.ID == "" {
		return errors.New("blurry: peer id is required to connect (use multiaddr with /p2p/<id>)")
	}
	return b.node.node.Connect(ctx, info)
}

// resolveRef accepts a name (registered via /name) or a raw id and
// returns a canonical Store ID.
func (b *Blurry) resolveRef(ctx context.Context, ref string) (ID, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", fmt.Errorf("empty reference")
	}
	// Heuristic: a registered name has no '/' or '0x' prefix.
	if !strings.Contains(ref, "/") && !strings.HasPrefix(ref, "0x") {
		if id, err := b.store.LookupName(ctx, ref); err == nil {
			return id, nil
		}
	}
	return ID(ref), nil
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

// dialAddr converts an addr to a single multiaddr; helper for Listen.
func dialAddr(addr string, defaultPort int) (ma.Multiaddr, error) {
	info, err := ResolvePeerAddr(addr, defaultPort)
	if err != nil && !errors.Is(err, ErrNoPeerID) {
		return nil, err
	}
	if len(info.Addrs) == 0 {
		return nil, fmt.Errorf("blurry: no addresses parsed from %q", addr)
	}
	return info.Addrs[0], nil
}
