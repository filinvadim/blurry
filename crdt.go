package blurry

import (
	"context"
	"fmt"
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
	blockstore := bs.NewBlockstore(baseStore)
	bitswapNetwork := bsnet.NewFromIpfsHost(node)
	bitswapExchange := bitswap.New(ctx, bitswapNetwork, router, blockstore)
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

	opts := crdt.DefaultOptions()
	opts.Logger = crdtLog
	opts.PutHook = func(k ds.Key, _ []byte) {
		crdtLog.Debugf("crdt: item put: %s", k.String())
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

	store := &CRDT{
		crdt:        crdtStore,
		broadcaster: broadcaster,
		ctx:         ctx,
		cancel:      cancel,
	}

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
