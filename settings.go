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

// Settings is the unified configuration for a Blurry replica. After
// the v0.x → KV refactor this struct only carries fields Blurry's
// runtime actually consumes: libp2p host options, the connection
// manager, the go-ds-crdt knobs, badger sync mode, and TLS. The
// chotki-parity timing/sizing block (BroadcastQueue*, ReadAccum*,
// PingPeriod, …) and the chotki-bridge fields (ChotkiPeer,
// ChotkiMirrorClass, ChotkiClassID) live in legacy/chotki/ now.
type Settings struct {
	// ---- identity ---------------------------------------------------
	//
	// A single Name is the canonical identifier for this replica:
	//   * the libp2p peer id (Ed25519 key seeded by Name)
	//   * the chotki-style 32-bit Src (sha256(Name) → uint32 space)
	//   * the Y record on first boot
	//
	// Two replicas booted with the same Name are the same logical node.
	// PrivateKey is a programmatic override (used in tests / embedded
	// deployments where the caller already owns a key); when set it
	// wins over Name for the libp2p identity.
	Name       string
	PrivateKey crypto.PrivKey

	// ---- libp2p network --------------------------------------------

	// Local listen endpoint.
	ListenHost string // "0.0.0.0", "::", or a specific address
	ListenPort int    // e.g. 4001

	// HTTP API endpoint (KV REST surface).
	HTTPHost string
	HTTPPort int

	// ClusterPeers contains addresses of other replicas to connect to
	// at start. Each entry can be a libp2p multiaddr, a libp2p
	// AddrInfo string, a plain "host:port" pair, or a bare IP.
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

	// TlsConfig optionally enables TLS for the HTTP API.
	TlsConfig *tls.Config

	// ---- storage / CRDT --------------------------------------------

	// Datastore is the badger options blob; nil → DefaultOptions.
	Datastore *Options

	// VersionPrefix namespaces the CRDT keys under a sub-tree.
	VersionPrefix       string
	RebroadcastInterval time.Duration
	DAGSyncerTimeout    time.Duration

	// SyncWrites toggles badger fsync-on-commit.
	SyncWrites bool
}

// DefaultSettings returns sensible defaults. Boolean toggles default
// to "on" so a fresh Blurry node can discover and dial peers without
// further wiring.
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

// SetDefaults fills numeric/duration zero values. Boolean toggles are
// intentionally left alone (they default to their zero value when the
// struct is constructed manually; use DefaultSettings() for toggles-on
// defaults).
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
	if s.RebroadcastInterval == 0 {
		s.RebroadcastInterval = time.Minute
	}
	if s.DAGSyncerTimeout == 0 {
		s.DAGSyncerTimeout = time.Minute
	}
}

// Validate normalizes and checks the settings.
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

// MaxSrc bounds derived Source values to a 32-bit Source space.
const MaxSrc = uint64(1<<32 - 1)

// Src returns a 32-bit replica id derived from Name. Two replicas
// configured with the same Name share the same Src; an empty Name
// returns 0.
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
