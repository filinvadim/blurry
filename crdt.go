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

var logger = logging.Logger("crdt")

type CRDTStorer interface {
	ds.Datastore
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

func NewCRDT(
	ctx context.Context,
	broadcaster Broadcaster,
	datastore CRDTStorer,
	node host.Host,
	router CRDTRouter,
	versionPrefix string, // default: empty string
) (*CRDT, error) {
	ctx, cancel := context.WithCancel(ctx)

	baseStore := dssync.MutexWrap(datastore)
	blockstore := bs.NewBlockstore(baseStore)
	bitswapNetwork := bsnet.NewFromIpfsHost(node)
	bitswapExchange := bitswap.New(ctx, bitswapNetwork, router, blockstore)
	blockService := blockservice.New(blockstore, bitswapExchange)
	dagService := merkledag.NewDAGService(blockService)

	opts := crdt.DefaultOptions()
	opts.Logger = logger
	opts.PutHook = func(k ds.Key, _ []byte) {
		logger.Debugf("crdt: item put: %s", k.String())
	}
	opts.DeleteHook = func(k ds.Key) {
		logger.Debugf("crdt: item deleted: %s", k.String())
	}
	opts.RebroadcastInterval = time.Minute
	opts.DAGSyncerTimeout = time.Minute
	opts.MultiHeadProcessing = true

	crdtStore, err := crdt.New(
		baseStore,
		ds.NewKey(versionPrefix),
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

func (s *CRDT) Close() error {
	if s == nil {
		return nil
	}
	s.cancel()
	return s.crdt.Close()
}
