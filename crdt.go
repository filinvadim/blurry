package blurry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockservice"
	bs "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	crdt "github.com/ipfs/go-ds-crdt"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var crdtLog = logging.Logger("crdt")

type CRDTStorer interface {
	ds.Datastore
}

type Broadcaster interface {
	Broadcast(ctx context.Context, data []byte) error
	Next(ctx context.Context) ([]byte, error)
}

type CRDTRouter interface {
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo
}

type CRDT struct {
	crdt        *crdt.Datastore
	broadcaster Broadcaster
	ctx         context.Context
	cancel      context.CancelFunc

	hookMu  sync.RWMutex
	putHook func(k ds.Key, v []byte)
}

// SetPutHook installs a callback that fires after every datastore Put
// — including ones merged in from remote replicas. The hook is the
// only way for higher layers (e.g. the chotki bridge) to learn about
// CRDT-applied writes that did not originate from this node's HTTP
// API. Pass nil to clear.
func (s *CRDT) SetPutHook(hook func(k ds.Key, v []byte)) {
	s.hookMu.Lock()
	s.putHook = hook
	s.hookMu.Unlock()
}

func (s *CRDT) firePut(k ds.Key, v []byte) {
	s.hookMu.RLock()
	hook := s.putHook
	s.hookMu.RUnlock()
	if hook != nil {
		hook(k, v)
	}
}

// CRDTSettings carries the subset of Settings the CRDT layer needs.
type CRDTSettings struct {
	VersionPrefix       string
	RebroadcastInterval time.Duration
	DAGSyncerTimeout    time.Duration
}

func NewCRDT(
	ctx context.Context,
	broadcaster Broadcaster,
	datastore CRDTStorer,
	node host.Host,
	router CRDTRouter,
	cs CRDTSettings,
) (*CRDT, error) {
	ctx, cancel := context.WithCancel(ctx)

	baseStore := dssync.MutexWrap(datastore)

	// Match the blockstore wiring used by ipfs-lite (the canonical
	// reference for go-ds-crdt deployments):
	//
	//   - WriteThrough(true): skip the redundant Has() check on every
	//     Put. The CRDT writes blocks once and never overwrites them,
	//     so the check just slows commits down.
	//
	//   - NewIdStore: synthesise blocks for "identity" multihashes
	//     (small payloads encoded directly in the CID). Without it
	//     bitswap can't satisfy WANTs for inline blocks, which the
	//     CRDT layer occasionally produces for tiny deltas.
	blockstore := bs.NewIdStore(bs.NewBlockstore(baseStore, bs.WriteThrough(true)))

	bitswapNetwork := bsnet.NewFromIpfsHost(node)
	// ProviderSearchDelay tells the bitswap client how long to wait
	// before kicking off a content-router lookup. Lowering it speeds up
	// cold reads in a small cluster where peers are already connected.
	bitswapExchange := bitswap.New(
		ctx,
		bitswapNetwork,
		router,
		blockstore,
		bitswap.ProviderSearchDelay(time.Second),
	)

	// Replay any libp2p connections that were already established when
	// bitswap registered as a network notifier. libp2p's swarm.Notify
	// only fires for FUTURE events, so peers that connected during the
	// window between libp2p.New() (host starts listening) and
	// bitswap.New() (handlers wired) would otherwise be invisible to
	// the bitswap peer manager — leading to "No peers - broadcasting"
	// loops that never converge in a small cluster. Same pattern that
	// kubo / ipfs-lite avoid by ensuring nothing inbound can connect
	// before bitswap is up; we have to do it explicitly because the
	// host is already exposed by the time NewCRDT is reached.
	for _, p := range node.Network().Peers() {
		bitswapExchange.PeerConnected(p)
	}

	blockService := blockservice.New(blockstore, bitswapExchange)
	dagService := merkledag.NewDAGService(blockService)

	rebroadcast := cs.RebroadcastInterval
	if rebroadcast <= 0 {
		rebroadcast = time.Minute
	}
	dagTimeout := cs.DAGSyncerTimeout
	if dagTimeout <= 0 {
		dagTimeout = time.Minute
	}

	store := &CRDT{
		broadcaster: broadcaster,
		ctx:         ctx,
		cancel:      cancel,
	}

	opts := crdt.DefaultOptions()
	opts.Logger = crdtLog
	opts.PutHook = func(k ds.Key, v []byte) {
		crdtLog.Debugf("crdt: item put: %s", k.String())
		store.firePut(k, v)
	}
	opts.DeleteHook = func(k ds.Key) {
		crdtLog.Debugf("crdt: item deleted: %s", k.String())
	}
	opts.RebroadcastInterval = rebroadcast
	opts.DAGSyncerTimeout = dagTimeout
	opts.MultiHeadProcessing = true

	crdtStore, err := crdt.New(
		baseStore,
		ds.NewKey(cs.VersionPrefix),
		dagService,
		broadcaster,
		opts,
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create CRDT store: %w", err)
	}
	store.crdt = crdtStore

	return store, nil
}

// Datastore returns the CRDT-wrapped datastore. Writes through this
// datastore are merged into the local store and broadcast to other
// replicas via the configured Broadcaster.
func (s *CRDT) Datastore() ds.Datastore {
	return s.crdt
}

func (s *CRDT) Close() error {
	if s == nil {
		return nil
	}
	s.cancel()
	return s.crdt.Close()
}
