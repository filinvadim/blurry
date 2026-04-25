package blurry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	json "github.com/json-iterator/go"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

var nodeLog = logging.Logger("node")

const (
	DefaultRelayDataLimit     = 32 << 20 // 32 MiB
	DefaultRelayDurationLimit = 5 * time.Minute

	// DefaultPrivateNetworkID is used when Settings.PrivateNetworkPSK is
	// empty. It is padded/truncated to 32 bytes for libp2p PrivateNetwork.
	DefaultPrivateNetworkID = "blurry-default-private-network."
)

type Node struct {
	ctx  context.Context
	host host.Host

	startTime time.Time
	eventsSub event.Subscription

	closeF       func() error
	clusterInfos []peer.AddrInfo
	infosChan    chan peer.AddrInfo
}

// NewNode constructs a libp2p host wired up according to Settings.
// The same Settings always produce the same peer ID (deterministic identity).
func NewNode(ctx context.Context, s *Settings) (*Node, error) {
	if s == nil {
		s = DefaultSettings()
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}

	priv, err := s.IdentityKey()
	if err != nil {
		return nil, err
	}

	limiter := rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale())

	manager, err := connmgr.NewConnManager(
		s.ConnLowWater,
		s.ConnHighWater,
		connmgr.WithGracePeriod(s.ConnGracePeriod),
	)
	if err != nil {
		return nil, err
	}
	rm, err := rcmgr.NewResourceManager(limiter)
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

	infos := ResolvePeerAddrs(s.ClusterPeers, s.ListenPort, func(addr string, err error) {
		nodeLog.Warnf("node: invalid cluster peer %q: %v", addr, err)
	})

	psk := s.PrivateNetworkPSK
	if len(psk) == 0 {
		psk = padPSK(DefaultPrivateNetworkID)
	}

	listenAddrs := []string{
		fmt.Sprintf("/ip4/%s/tcp/%d", s.ListenHost, s.ListenPort),
	}
	// Add IPv6 wildcard listener when binding to all-interfaces IPv4.
	if s.ListenHost == "0.0.0.0" {
		listenAddrs = append(listenAddrs, fmt.Sprintf("/ip6/::/tcp/%d", s.ListenPort))
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.Peerstore(memoryStore),
		libp2p.PrivateNetwork(psk),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.ResourceManager(rm),
		libp2p.ConnectionManager(manager),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Ping(true),
		libp2p.Security(noise.ID, noise.New),
	}

	if !s.EnableMetrics {
		opts = append(opts, libp2p.DisableMetrics())
	}
	if s.EnableDHT {
		opts = append(opts, libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			return NewDHT(ctx, h, mapStore, infos...)
		}))
	}
	if s.EnableAutoRelay && len(infos) > 0 {
		opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays(infos))
	}
	if s.EnableAutoNATv2 {
		opts = append(opts, libp2p.EnableAutoNATv2())
	}
	if s.EnableRelay {
		opts = append(opts, libp2p.EnableRelay())
	}
	if s.EnableRelayService {
		opts = append(opts, libp2p.EnableRelayService(relayv2.WithResources(relayv2.Resources{
			Limit: &relayv2.RelayLimit{
				Duration: DefaultRelayDurationLimit,
				Data:     DefaultRelayDataLimit,
			},
			ReservationTTL:        time.Hour,
			MaxReservations:       128,
			MaxCircuits:           16,
			BufferSize:            4096,
			MaxReservationsPerIP:  8,
			MaxReservationsPerASN: 32,
		})))
	}
	if s.EnableHolePunching {
		opts = append(opts, libp2p.EnableHolePunching())
	}
	if s.EnableNATService {
		opts = append(opts, libp2p.EnableNATService())
	}
	if s.EnableNATPortMap {
		opts = append(opts, libp2p.NATPortMap())
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("node: failed to init node: %w", err)
	}

	sub, err := h.EventBus().Subscribe(event.WildcardSubscription)
	if err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("node: failed to subscribe: %w", err)
	}
	n := &Node{
		ctx:          ctx,
		host:         h,
		startTime:    time.Now(),
		eventsSub:    sub,
		closeF:       closeF,
		clusterInfos: infos,
		infosChan:    make(chan peer.AddrInfo, max(len(infos), 1)),
	}

	nodeLog.Infof("node: started with peer id %s, listen %v, cluster size %d",
		h.ID().String(), h.Addrs(), len(infos))

	go n.trackIncomingEvents()
	return n, nil
}

func (n *Node) Host() host.Host {
	return n.host
}

// connectBackoffMin/Max bound the per-peer retry interval used by
// ConnectCluster's reconnect loop.
const (
	connectBackoffMin = 500 * time.Millisecond
	connectBackoffMax = time.Minute
)

// ConnectCluster dials every configured cluster peer in the background.
// Each peer gets its own goroutine that keeps retrying with exponential
// backoff until either:
//   - the connection succeeds, or
//   - the parent context is cancelled (shutdown).
//
// This handles the common boot race where one replica starts before
// the others are ready, plus transient network glitches at runtime.
// libp2p's ConnManager keeps the connection alive once established.
func (n *Node) ConnectCluster(ctx context.Context) error {
	if n == nil || n.host == nil {
		return errors.New("node: not started")
	}
	ownID := n.host.ID()
	for _, info := range n.clusterInfos {
		if info.ID == ownID || info.ID == "" {
			continue
		}
		go n.keepConnecting(ctx, info)
	}
	return nil
}

// keepConnecting dials one peer with exponential backoff. It returns
// when the connection is established (and remains so) or ctx is done.
func (n *Node) keepConnecting(ctx context.Context, info peer.AddrInfo) {
	backoff := connectBackoffMin
	for ctx.Err() == nil {
		// Skip the dial if libp2p already has a live connection — the
		// ConnManager keeps it open for us.
		if n.host.Network().Connectedness(info.ID) == network.Connected {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}

		dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := n.host.Connect(dialCtx, info)
		cancel()
		if err == nil {
			nodeLog.Infof("node: connected to cluster peer %s", info.ID.String())
			backoff = connectBackoffMin
			continue
		}

		nodeLog.Debugf("node: connect %s: %v (retry in %s)", info.ID.String(), err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(connectBackoffMax, backoff*2)
	}
}

// FindProvidersAsync satisfies the bitswap router interface by returning
// the configured cluster peers. They are good content providers in a
// closed cluster topology.
func (n *Node) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) (ch <-chan peer.AddrInfo) {
	out := make(chan peer.AddrInfo, max(len(n.clusterInfos), 1))
	go func() {
		defer close(out)
		for _, info := range n.clusterInfos {
			select {
			case <-ctx.Done():
				return
			case out <- info:
			}
		}
	}()
	return out
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
					tail(pid, 6),
					typedEvent.Connectedness.String(),
				)
			case event.EvtPeerIdentificationFailed:
				pid := typedEvent.Peer
				addrs := n.host.Peerstore().Addrs(pid)
				nodeLog.Errorf(
					"node: event: peer %s %v identification failed, reason: %s",
					pid.String(), addrs, typedEvent.Reason,
				)
			case event.EvtPeerIdentificationCompleted:
				pid := typedEvent.Peer.String()
				nodeLog.Debugf(
					"node: event: peer ...%s identification completed, observed address: %s",
					tail(pid, 6), typedEvent.ObservedAddr.String(),
				)
			case event.EvtLocalReachabilityChanged:
				r := typedEvent.Reachability
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
	if n == nil || n.host == nil {
		return nil
	}
	_ = n.closeF()

	if n.eventsSub != nil {
		_ = n.eventsSub.Close()
	}
	nodeLog.Infoln("node: event sub closed")

	return n.host.Close()
}

// padPSK derives a 32-byte PSK from any input by repeating/truncating.
// libp2p's PrivateNetwork requires exactly 32 bytes.
func padPSK(s string) []byte {
	const want = 32
	out := make([]byte, want)
	for i := 0; i < want; i++ {
		out[i] = s[i%len(s)]
	}
	return out
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
