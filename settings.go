package blurry

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// Settings is the unified configuration for a Blurry replica.
//
// The fields below intentionally mirror github.com/drpcorg/chotki.Options
// (which itself embeds chotki.ClusterConfig in the matching commit on
// the Chotki side) so the same deployment descriptor can drive both
// backends. Fields that have no behavioural effect on the libp2p stack
// are still honoured wherever possible (e.g. broadcast queue limits
// translate to gossip parameters).
type Settings struct {
	// ---- identity ----------------------------------------------------
	//
	// A single Name is the canonical identifier for this replica.
	// Everything stable-identity-related is derived from it:
	//   * the libp2p peer id (Ed25519 key seeded by Name)
	//   * the chotki-style 32-bit Src (sha256(Name) → uint32 space)
	//   * the Store's id-prefix
	//   * the Y record on first boot
	//
	// Two replicas booted with the same Name are the same logical node.
	// PrivateKey is a programmatic override (used in tests / embedded
	// deployments where the caller already owns a key); when set it
	// wins over Name for the libp2p identity but Src/Name still drive
	// chotki-side bookkeeping.
	Name       string
	PrivateKey crypto.PrivKey

	// ---- libp2p network ---------------------------------------------

	// Local listen endpoint.
	ListenHost string // "0.0.0.0", "::", or a specific address
	ListenPort int    // e.g. 4001

	// HTTP API port (Chotki-compatible).
	HTTPHost string
	HTTPPort int

	// ClusterPeers contains addresses of other replicas to connect to
	// at start. Each entry can be:
	//   - a libp2p multiaddr ("/ip4/1.2.3.4/tcp/4001/p2p/<peerID>")
	//   - a libp2p AddrInfo string ("/ip4/.../p2p/<peerID>")
	//   - a plain "host:port" pair (resolved to /ip4/host/tcp/port)
	//   - a bare IP (uses the same TCP port as ListenPort)
	ClusterPeers []string

	// PrivateNetworkPSK enables a libp2p private-net pre-shared key.
	// Empty means the default Blurry network identifier is used.
	PrivateNetworkPSK []byte

	// libp2p feature toggles. They map onto github.com/libp2p/go-libp2p
	// options of the same name.
	EnableDHT          bool
	EnableRelay        bool
	EnableRelayService bool
	EnableHolePunching bool
	EnableNATService   bool
	EnableNATPortMap   bool
	EnableAutoNATv2    bool
	EnableAutoRelay    bool
	EnableMetrics      bool

	// Connection-manager limits.
	ConnLowWater    int
	ConnHighWater   int
	ConnGracePeriod time.Duration

	// ---- chotki.Options parity --------------------------------------
	//
	// These fields are accepted as-is from a chotki.Options-shaped
	// descriptor. They influence Blurry's gossip and libp2p layers
	// where there is a sensible mapping; otherwise they are stored on
	// the Settings struct so the same config can round-trip.

	PingPeriod time.Duration
	PingWait   time.Duration

	BroadcastQueueMaxSize      int
	BroadcastQueueMinBatchSize int
	BroadcastQueueTimeLimit    time.Duration

	ReadAccumTimeLimit         time.Duration
	ReadMaxBufferSize          int
	ReadMinBufferSizeToProcess int

	TcpReadBufferSize  int
	TcpWriteBufferSize int

	WriteTimeout    time.Duration
	MaxSyncDuration time.Duration

	// TlsConfig optionally enables a libp2p TLS transport (chotki uses
	// the same field for its tcp+tls listener).
	TlsConfig *tls.Config

	// ---- storage / CRDT ---------------------------------------------

	// Datastore is the badger options blob; nil → DefaultOptions.
	Datastore *Options

	// VersionPrefix namespaces the CRDT keys under a sub-tree (chotki
	// has no equivalent; default empty).
	VersionPrefix       string
	RebroadcastInterval time.Duration
	DAGSyncerTimeout    time.Duration

	// SyncWrites toggles badger fsync-on-commit. Mirrors chotki's
	// pebble.WriteOptions{Sync:true} default for ACID durability.
	SyncWrites bool
}

// DefaultSettings returns sensible defaults aligned with chotki's
// SetDefaults() in chotki.go. Boolean toggles default to "on" so a
// fresh Blurry node can discover and dial peers without further wiring.
func DefaultSettings() *Settings {
	s := &Settings{
		ListenHost:         "0.0.0.0",
		ListenPort:         4001,
		HTTPHost:           "127.0.0.1",
		HTTPPort:           8001,
		EnableDHT:          true,
		EnableRelay:        true,
		EnableRelayService: true,
		EnableHolePunching: true,
		EnableNATService:   true,
		EnableNATPortMap:   true,
		EnableAutoNATv2:    true,
		EnableAutoRelay:    true,
		SyncWrites:         true,
	}
	s.SetDefaults()
	return s
}

// SetDefaults fills numeric/duration zero values with the same defaults
// chotki.Options.SetDefaults uses. Boolean toggles are intentionally
// left alone (they default to their zero value when the struct is
// constructed manually; use DefaultSettings() for toggles-on defaults).
func (s *Settings) SetDefaults() {
	if s == nil {
		return
	}
	if s.ConnLowWater == 0 {
		s.ConnLowWater = 20
	}
	if s.ConnHighWater == 0 {
		s.ConnHighWater = 50
	}
	if s.ConnGracePeriod == 0 {
		s.ConnGracePeriod = time.Hour
	}
	if s.PingPeriod == 0 {
		s.PingPeriod = 30 * time.Second
	}
	if s.PingWait == 0 {
		s.PingWait = 10 * time.Second
	}
	if s.ReadMaxBufferSize == 0 {
		s.ReadMaxBufferSize = 1024 * 1024 * 1000 // 1000MB
	}
	if s.ReadMinBufferSizeToProcess == 0 {
		s.ReadMinBufferSizeToProcess = 10 * 1024 // 10kb
	}
	if s.BroadcastQueueTimeLimit == 0 {
		s.BroadcastQueueTimeLimit = time.Second
	}
	if s.BroadcastQueueMaxSize == 0 {
		s.BroadcastQueueMaxSize = 10 * 1024 * 1024 // 10MB
	}
	if s.ReadAccumTimeLimit == 0 {
		s.ReadAccumTimeLimit = 5 * time.Second
	}
	if s.WriteTimeout == 0 {
		s.WriteTimeout = 5 * time.Minute
	}
	if s.MaxSyncDuration == 0 {
		s.MaxSyncDuration = 10 * time.Minute
	}
	if s.RebroadcastInterval == 0 {
		s.RebroadcastInterval = time.Minute
	}
	if s.DAGSyncerTimeout == 0 {
		s.DAGSyncerTimeout = time.Minute
	}
}

// Validate normalizes and checks the settings, returning an error on
// invalid combinations. Safe to call multiple times.
func (s *Settings) Validate() error {
	if s == nil {
		return errors.New("settings: nil")
	}
	if s.ListenPort <= 0 || s.ListenPort > 0xFFFF {
		return fmt.Errorf("settings: invalid listen port %d", s.ListenPort)
	}
	if s.HTTPPort < 0 || s.HTTPPort > 0xFFFF {
		return fmt.Errorf("settings: invalid http port %d", s.HTTPPort)
	}
	if s.ListenHost == "" {
		s.ListenHost = "0.0.0.0"
	}
	if s.HTTPHost == "" {
		s.HTTPHost = "127.0.0.1"
	}
	if s.ConnHighWater > 0 && s.ConnHighWater < s.ConnLowWater {
		return fmt.Errorf("settings: ConnHighWater(%d) < ConnLowWater(%d)",
			s.ConnHighWater, s.ConnLowWater)
	}
	return nil
}

// MaxSrc bounds derived Source values to chotki's 32-bit Source space
// (chotki/rdx.MaxSrc).
const MaxSrc = uint64(1<<32 - 1)

// Src returns the chotki-style 32-bit replica id derived from Name.
// Two replicas configured with the same Name share the same Src; an
// empty Name returns 0.
func (s *Settings) Src() uint64 {
	if s == nil || s.Name == "" {
		return 0
	}
	sum := sha256.Sum256([]byte(s.Name))
	v := binary.BigEndian.Uint64(sum[:8]) & MaxSrc
	if v == 0 {
		v = 1
	}
	return v
}

// IdentityKey returns the libp2p private key for this replica. When
// PrivateKey is set it wins; otherwise an Ed25519 key is derived
// deterministically from Name. An empty Name and no PrivateKey produce
// a fresh random key.
func (s *Settings) IdentityKey() (crypto.PrivKey, error) {
	if s.PrivateKey != nil {
		return s.PrivateKey, nil
	}
	if s.Name != "" {
		// Hash Name down to ed25519's required 32-byte seed size.
		sum := sha256.Sum256([]byte(s.Name))
		seed := sum[:ed25519.SeedSize]
		edPriv := ed25519.NewKeyFromSeed(seed)
		priv, err := crypto.UnmarshalEd25519PrivateKey(edPriv)
		if err != nil {
			return nil, fmt.Errorf("settings: derive ed25519 key: %w", err)
		}
		return priv, nil
	}
	priv, _, err := crypto.GenerateEd25519Key(nil)
	return priv, err
}
