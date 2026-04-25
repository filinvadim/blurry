package blurry

import (
	"context"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"math"
	"time"
)

var dhtLog = logging.Logger("dht")

func NewDHT(
	ctx context.Context,
	h host.Host,
	store ds.Batching,
	peers ...peer.AddrInfo,
) (*dht.IpfsDHT, error) {
	providerStore, err := records.NewProviderManager(
		ctx, h.ID(), h.Peerstore(), store,
	)
	if err != nil {
		return nil, err
	}

	table, err := dht.New(
		ctx, h,
		dht.Mode(dht.ModeAutoServer),
		dht.ProtocolPrefix("/blurry/1.0.0"),
		dht.Datastore(store),
		dht.MaxRecordAge(time.Hour),
		dht.RoutingTableRefreshPeriod(time.Hour),
		dht.RoutingTableRefreshQueryTimeout(time.Minute*5),
		dht.BootstrapPeers(peers...),
		dht.ProviderStore(providerStore),
		dht.RoutingTableLatencyTolerance(time.Minute),
		dht.BucketSize(50),
	)
	if err != nil {
		dhtLog.Errorf("dht: new: %v", err)
		return nil, err
	}

	go bootstrapDHT(table, peers)
	dhtLog.Infoln("dht: routing started")
	return table, nil
}

func bootstrapDHT(table *dht.IpfsDHT, infos []peer.AddrInfo) {
	if table == nil {
		return
	}
	ownID := table.Host().ID()

	for _, info := range infos {
		if ownID == info.ID {
			continue
		}
		table.Host().Peerstore().AddAddrs(info.ID, info.Addrs, math.MaxInt64)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := table.Bootstrap(ctx); err != nil {
		dhtLog.Fatalf("bootstrap dht: %v", err)
	}

	<-table.RefreshRoutingTable()
	dhtLog.Infoln("dht: bootstrap complete")
}
