package blurry

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// Settings is the unified configuration for a Blurry replica.
// It is shared in shape with chotki.Options so a single deployment
// descriptor can drive both backends.
type Settings struct {
	// Identity. PrivateKey takes precedence; if nil, a deterministic
	// Ed25519 key is derived from PrivateKeySeed (32 bytes recommended).
	// If both are empty a random key is generated.
	PrivateKey     crypto.PrivKey
	PrivateKeySeed []byte

	// Replica name (for logging and Chotki-compatible Y record).
	Name string

	// Local listen endpoint.
	ListenHost string // e.g. "0.0.0.0"
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
	// Empty means no PSK (default Blurry network identifier is used).
	PrivateNetworkPSK []byte

	// libp2p feature toggles (mirrored in chotki.Options for parity).
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
	ConnLowWater   int
	ConnHighWater  int
	ConnGracePeriod time.Duration

	// Storage.
	Datastore *Options // badger options; nil → DefaultOptions

	// CRDT.
	VersionPrefix       string
	RebroadcastInterval time.Duration
	DAGSyncerTimeout    time.Duration

	// ACID. SyncWrites controls whether badger fsyncs on every write.
	// Mirrors pebble.WriteOptions{Sync:true} default in Chotki.
	SyncWrites bool
}

// DefaultSettings returns sensible defaults that mirror Chotki's defaults
// where applicable.
func DefaultSettings() *Settings {
	return &Settings{
		ListenHost:          "0.0.0.0",
		ListenPort:          4001,
		HTTPHost:            "127.0.0.1",
		HTTPPort:            8001,
		EnableDHT:           true,
		EnableRelay:         true,
		EnableRelayService:  true,
		EnableHolePunching:  true,
		EnableNATService:    true,
		EnableNATPortMap:    true,
		EnableAutoNATv2:     true,
		EnableAutoRelay:     true,
		EnableMetrics:       false,
		ConnLowWater:        20,
		ConnHighWater:       50,
		ConnGracePeriod:     time.Hour,
		RebroadcastInterval: time.Minute,
		DAGSyncerTimeout:    time.Minute,
		SyncWrites:          true,
	}
}

// Validate normalizes and checks the settings, returning an error on
// invalid combinations.
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
	return nil
}

// IdentityKey returns the libp2p private key derived from the configured
// identity material. The same Settings always produce the same peer ID,
// provided PrivateKey or PrivateKeySeed is set.
func (s *Settings) IdentityKey() (crypto.PrivKey, error) {
	if s.PrivateKey != nil {
		return s.PrivateKey, nil
	}
	if len(s.PrivateKeySeed) > 0 {
		// Hash the seed to ed25519's required 32-byte size.
		sum := sha256.Sum256(s.PrivateKeySeed)
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
