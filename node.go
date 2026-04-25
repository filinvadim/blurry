package blurry

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"

	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"

	"github.com/libp2p/go-libp2p/core/host"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	json "github.com/json-iterator/go"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/event"
)

var nodeLog = logging.Logger("node")

const (
	DefaultRelayDataLimit     = 32 << 20 // 32 MiB
	DefaultRelayDurationLimit = 5 * time.Minute
)

type Node struct {
	ctx  context.Context
	node host.Host

	startTime time.Time
	eventsSub event.Subscription

	closeF       func() error
	clusterInfos []peer.AddrInfo
	infosChan    chan peer.AddrInfo
}

func NewNode(
	ctx context.Context,
	peers ...string, // TODO Add setting for cluster peers
) (*Node, error) {
	limiter := rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale())

	manager, err := connmgr.NewConnManager( // TODO move to settings
		20,
		50,
		connmgr.WithGracePeriod(time.Hour),
	)
	if err != nil {
		return nil, err
	}
	rm, err := rcmgr.NewResourceManager(limiter) // TODO move to settings
	if err != nil {
		return nil, err
	}
	memoryStore, err := pstoremem.NewPeerstore()
	if err != nil {
		return nil, fmt.Errorf("bootstrap: fail creating memory peerstore: %w", err)
	}
	mapStore := datastore.NewMapDatastore()

	closeF := func() error {
		_ = memoryStore.Close()
		return mapStore.Close()
	}

	var infos []peer.AddrInfo
	for _, p := range peers {
		info, err := peer.AddrInfoFromString(p) // TODO convert also if it's plain IP
		if err != nil || info == nil {
			dhtLog.Warnf("failed to parse peer info from DHT: %s", err)
			continue
		}
		infos = append(infos, *info)
	}

	opts := []libp2p.Option{
		libp2p.Peerstore(memoryStore),
		libp2p.PrivateNetwork([]byte("blurry")),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip6/%s/tcp/%s", "", ""), // TODO add host and port
			fmt.Sprintf("/ip4/%s/tcp/%s", "", ""), // TODO add host and port
		),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			// TODO if DHT enabled setting
			return NewDHT(ctx, h, mapStore, infos...)
		}),
		libp2p.ResourceManager(rm),
		libp2p.ConnectionManager(manager),
		libp2p.DisableMetrics(),                       // TODO move to settings
		libp2p.EnableAutoRelayWithStaticRelays(infos), // TODO move to settings
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Ping(true),
		libp2p.Security(noise.ID, noise.New),
		libp2p.EnableAutoNATv2(), // TODO move to settings
		libp2p.EnableRelay(),     // TODO move to settings
		libp2p.EnableRelayService(relayv2.WithResources(relayv2.Resources{ // TODO move to settings
			Limit: &relayv2.RelayLimit{
				Duration: DefaultRelayDurationLimit,
				Data:     DefaultRelayDataLimit,
			},

			ReservationTTL: time.Hour,

			MaxReservations: 128,
			MaxCircuits:     16,
			BufferSize:      4096,

			MaxReservationsPerIP:  8,
			MaxReservationsPerASN: 32,
		})), // for member nodes that have static IP
		libp2p.EnableHolePunching(), // TODO move to settings
		libp2p.EnableNATService(),   // TODO move to settings
		libp2p.NATPortMap(),         // TODO move to settings
	}

	node, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("node: failed to init node: %w", err)
	}

	sub, err := node.EventBus().Subscribe(event.WildcardSubscription)
	if err != nil {
		return nil, fmt.Errorf("node: failed to subscribe: %w", err)
	}
	n := &Node{
		ctx:          ctx,
		node:         node,
		startTime:    time.Now(),
		eventsSub:    sub,
		closeF:       closeF,
		clusterInfos: infos,
		infosChan:    make(chan peer.AddrInfo, len(infos)),
	}

	go n.trackIncomingEvents()
	return n, nil
}

func (n *Node) Host() host.Host {
	return n.node
}
func (n *Node) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) (ch <-chan peer.AddrInfo) {
	for _, info := range n.clusterInfos {
		n.infosChan <- info
	}
	return n.infosChan
}

var localAddrActions = map[int]string{
	0: "unknown",
	1: "added",
	2: "maintained",
	3: "removed",
}

func (n *Node) trackIncomingEvents() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case ev, ok := <-n.eventsSub.Out():
			if !ok {
				return
			}
			switch typedEvent := ev.(type) {
			case event.EvtPeerProtocolsUpdated:
				if len(typedEvent.Added) != 0 {
					nodeLog.Infof("node: event: protocol added: %v", typedEvent.Added)
				}
				if len(typedEvent.Removed) != 0 {
					nodeLog.Infof("node: event: protocol removed: %v", typedEvent.Removed)
				}
			case event.EvtLocalProtocolsUpdated:
				if len(typedEvent.Added) != 0 {
					nodeLog.Infof("node: event: protocol added: %v", typedEvent.Added)
				} else {
					nodeLog.Infof("node: event: protocol removed: %v", typedEvent.Removed)
				}
			case event.EvtPeerConnectednessChanged:
				pid := typedEvent.Peer.String()
				nodeLog.Infof(
					"node: event: peer ...%s connectedness updated: %s",
					pid[len(pid)-6:],
					typedEvent.Connectedness.String(),
				)
			case event.EvtPeerIdentificationFailed:
				pid := typedEvent.Peer
				addrs := n.node.Peerstore().Addrs(pid)
				nodeLog.Errorf(
					"node: event: peer %s %v identification failed, reason: %s",
					pid.String(), addrs, typedEvent.Reason,
				)

			case event.EvtPeerIdentificationCompleted:
				pid := typedEvent.Peer.String()
				nodeLog.Debugf(
					"node: event: peer ...%s identification completed, observed address: %s",
					pid[len(pid)-6:], typedEvent.ObservedAddr.String(),
				)
			case event.EvtLocalReachabilityChanged:
				r := typedEvent.Reachability // it's int32 under the hood
				nodeLog.Infof(
					"node: event: own node reachability changed: %s",
					strings.ToLower(r.String()),
				)
			case event.EvtNATDeviceTypeChanged:
				nodeLog.Infof(
					"node: event: NAT device type changed: %s, transport: %s",
					typedEvent.NatDeviceType.String(), typedEvent.TransportProtocol.String(),
				)
			case event.EvtAutoRelayAddrsUpdated:
				if len(typedEvent.RelayAddrs) != 0 {
					nodeLog.Infoln("node: event: relay address added")
				}
			case event.EvtLocalAddressesUpdated:
				for _, addr := range typedEvent.Current {
					nodeLog.Debugf(
						"node: event: local address %s: %s",
						addr.Address.String(), localAddrActions[int(addr.Action)],
					)
				}
			case event.EvtHostReachableAddrsChanged:
				nodeLog.Infof(
					`node: event: peer reachability changed: reachable: %v, unreachable: %v, unknown: %v`,
					typedEvent.Reachable,
					typedEvent.Unreachable,
					typedEvent.Unknown,
				)
			default:
				bt, _ := json.Marshal(ev)
				nodeLog.Infof("node: event: %T %s", ev, bt)
			}
		}
	}
}

func (n *Node) Close() error {
	nodeLog.Infoln("node: shutting down node...")
	if n == nil || n.node == nil {
		return nil
	}
	_ = n.closeF()

	if n.eventsSub != nil {
		_ = n.eventsSub.Close()
	}
	nodeLog.Infoln("node: event sub closed")

	nodeLog.Infoln("node: relay closed")

	return n.node.Close()
}
