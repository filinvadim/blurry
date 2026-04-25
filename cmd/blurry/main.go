// Command blurry runs a single Blurry replica.
//
// Configuration is layered (lowest precedence first):
//
//  1. Built-in defaults (DefaultSettings).
//  2. Optional config file (--config <path> or $BLURRY_CONFIG, formats:
//     yaml/json/toml/env).
//  3. Environment variables, prefixed BLURRY_ (e.g. BLURRY_LISTEN_PORT).
//  4. Command-line flags.
//
// Run "blurry --help" for the flag listing.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/filinvadim/blurry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const envPrefix = "BLURRY"

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "blurry:", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "blurry",
		Short: "Run a Blurry replica with Chotki-compatible HTTP API",
		Long: `Blurry is a CRDT-backed key/value store running on libp2p with a
Chotki-compatible REST API. Settings are read from flags, environment
variables (BLURRY_*) and an optional config file.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			v, err := initViper(cmd, configPath)
			if err != nil {
				return err
			}
			s, err := settingsFromViper(v)
			if err != nil {
				return err
			}
			return run(cmd.Context(), s, v.GetString("data_dir"))
		},
	}

	cmd.PersistentFlags().StringVar(&configPath, "config", "",
		"path to a config file (yaml/json/toml/env); also reads $BLURRY_CONFIG")

	registerFlags(cmd)
	return cmd
}

// registerFlags binds all Settings knobs as CLI flags. Each flag also
// has a corresponding BLURRY_<UPPER> env var (set up in initViper).
func registerFlags(cmd *cobra.Command) {
	f := cmd.PersistentFlags()

	// Storage / runtime.
	f.String("data_dir", "./data", "badger data directory")

	// Identity.
	f.String("name", "", "replica name (logging + Y record)")
	f.String("private_key_seed", "",
		"deterministic identity seed; empty = random key")
	f.Uint64("src", 0,
		"explicit chotki-style 32-bit replica id; 0 = derive from private_key_seed")

	// libp2p endpoint.
	f.String("listen_host", "0.0.0.0", "libp2p listen host")
	f.Int("listen_port", 4001, "libp2p listen TCP port")

	// HTTP API.
	f.String("http_host", "0.0.0.0", "HTTP API listen host")
	f.Int("http_port", 8001, "HTTP API listen port (0 disables HTTP)")

	// Cluster.
	f.StringSlice("cluster_peers", nil,
		"cluster peer addresses (multiaddr, host:port, or bare IP)")
	f.String("private_network_psk", "",
		"libp2p private-network PSK (any string; padded to 32 bytes)")

	// libp2p toggles.
	f.Bool("enable_dht", true, "enable Kademlia DHT")
	f.Bool("enable_relay", true, "enable circuit-relay client")
	f.Bool("enable_relay_service", true, "enable circuit-relay service")
	f.Bool("enable_hole_punching", true, "enable hole punching")
	f.Bool("enable_nat_service", true, "enable NAT service")
	f.Bool("enable_nat_port_map", true, "enable UPnP NAT port map")
	f.Bool("enable_auto_natv2", true, "enable AutoNAT v2")
	f.Bool("enable_auto_relay", true, "enable AutoRelay with static relays")
	f.Bool("enable_metrics", false, "enable libp2p metrics")

	// Connection manager.
	f.Int("conn_low_water", 20, "ConnManager low watermark")
	f.Int("conn_high_water", 50, "ConnManager high watermark")
	f.Duration("conn_grace_period", time.Hour, "ConnManager grace period")

	// chotki.Options-parity timing/sizing.
	f.Duration("ping_period", 30*time.Second, "ping interval to neighbour replicas")
	f.Duration("ping_wait", 10*time.Second, "ping response timeout")
	f.Int("broadcast_queue_max_size", 10*1024*1024, "broadcast queue max bytes")
	f.Int("broadcast_queue_min_batch_size", 0, "broadcast queue min batch bytes")
	f.Duration("broadcast_queue_time_limit", time.Second, "broadcast queue flush deadline")
	f.Duration("read_accum_time_limit", 5*time.Second, "read accumulator flush deadline")
	f.Int("read_max_buffer_size", 1024*1024*1000, "read max buffer bytes")
	f.Int("read_min_buffer_size_to_process", 10*1024, "read min buffer bytes before processing")
	f.Int("tcp_read_buffer_size", 0, "TCP read buffer size (0 = OS default)")
	f.Int("tcp_write_buffer_size", 0, "TCP write buffer size (0 = OS default)")
	f.Duration("write_timeout", 5*time.Minute, "write timeout")
	f.Duration("max_sync_duration", 10*time.Minute, "max sync duration")

	// CRDT.
	f.String("version_prefix", "", "CRDT version key prefix")
	f.Duration("rebroadcast_interval", time.Minute, "CRDT rebroadcast interval")
	f.Duration("dag_syncer_timeout", time.Minute, "CRDT DAG syncer timeout")

	// Storage.
	f.Bool("sync_writes", true, "fsync badger on every write (ACID)")

	// TLS for the HTTP server / libp2p TLS transport.
	f.String("tls_cert_file", "", "TLS certificate file (PEM)")
	f.String("tls_key_file", "", "TLS private key file (PEM)")
}

// initViper wires viper to the cobra flag set, the BLURRY_* env namespace
// and an optional config file.
func initViper(cmd *cobra.Command, configPath string) (*viper.Viper, error) {
	v := viper.New()

	// CLI flags take highest precedence after explicit overrides.
	if err := v.BindPFlags(cmd.PersistentFlags()); err != nil {
		return nil, fmt.Errorf("bind flags: %w", err)
	}

	// Env namespace: BLURRY_LISTEN_PORT etc.
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	if configPath == "" {
		configPath = os.Getenv(envPrefix + "_CONFIG")
	}
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config %q: %w", configPath, err)
		}
	}
	return v, nil
}

// settingsFromViper materializes a *blurry.Settings from a populated viper.
func settingsFromViper(v *viper.Viper) (*blurry.Settings, error) {
	s := &blurry.Settings{
		Name:                       v.GetString("name"),
		PrivateKeySeed:             []byte(v.GetString("private_key_seed")),
		Src:                        v.GetUint64("src"),
		ListenHost:                 v.GetString("listen_host"),
		ListenPort:                 v.GetInt("listen_port"),
		HTTPHost:                   v.GetString("http_host"),
		HTTPPort:                   v.GetInt("http_port"),
		ClusterPeers:               v.GetStringSlice("cluster_peers"),
		PrivateNetworkPSK:          []byte(v.GetString("private_network_psk")),
		EnableDHT:                  v.GetBool("enable_dht"),
		EnableRelay:                v.GetBool("enable_relay"),
		EnableRelayService:         v.GetBool("enable_relay_service"),
		EnableHolePunching:         v.GetBool("enable_hole_punching"),
		EnableNATService:           v.GetBool("enable_nat_service"),
		EnableNATPortMap:           v.GetBool("enable_nat_port_map"),
		EnableAutoNATv2:            v.GetBool("enable_auto_natv2"),
		EnableAutoRelay:            v.GetBool("enable_auto_relay"),
		EnableMetrics:              v.GetBool("enable_metrics"),
		ConnLowWater:               v.GetInt("conn_low_water"),
		ConnHighWater:              v.GetInt("conn_high_water"),
		ConnGracePeriod:            v.GetDuration("conn_grace_period"),
		PingPeriod:                 v.GetDuration("ping_period"),
		PingWait:                   v.GetDuration("ping_wait"),
		BroadcastQueueMaxSize:      v.GetInt("broadcast_queue_max_size"),
		BroadcastQueueMinBatchSize: v.GetInt("broadcast_queue_min_batch_size"),
		BroadcastQueueTimeLimit:    v.GetDuration("broadcast_queue_time_limit"),
		ReadAccumTimeLimit:         v.GetDuration("read_accum_time_limit"),
		ReadMaxBufferSize:          v.GetInt("read_max_buffer_size"),
		ReadMinBufferSizeToProcess: v.GetInt("read_min_buffer_size_to_process"),
		TcpReadBufferSize:          v.GetInt("tcp_read_buffer_size"),
		TcpWriteBufferSize:         v.GetInt("tcp_write_buffer_size"),
		WriteTimeout:               v.GetDuration("write_timeout"),
		MaxSyncDuration:            v.GetDuration("max_sync_duration"),
		VersionPrefix:              v.GetString("version_prefix"),
		RebroadcastInterval:        v.GetDuration("rebroadcast_interval"),
		DAGSyncerTimeout:           v.GetDuration("dag_syncer_timeout"),
		SyncWrites:                 v.GetBool("sync_writes"),
	}

	if cert, key := v.GetString("tls_cert_file"), v.GetString("tls_key_file"); cert != "" || key != "" {
		if cert == "" || key == "" {
			return nil, errors.New("tls_cert_file and tls_key_file must be set together")
		}
		c, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("load tls keypair: %w", err)
		}
		s.TlsConfig = &tls.Config{Certificates: []tls.Certificate{c}, MinVersion: tls.VersionTLS12}
	}

	s.SetDefaults()
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// run starts a Blurry instance and blocks until SIGINT/SIGTERM.
func run(ctx context.Context, s *blurry.Settings, dataDir string) error {
	if dataDir == "" {
		return errors.New("data_dir must be set")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create data_dir: %w", err)
	}

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	b, err := blurry.NewBlurry(ctx, dataDir, s)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr,
		"blurry: ready (listen=%s:%d http=%s:%d cluster_peers=%d data=%s)\n",
		s.ListenHost, s.ListenPort, s.HTTPHost, s.HTTPPort,
		len(s.ClusterPeers), dataDir)

	<-ctx.Done()
	fmt.Fprintln(os.Stderr, "blurry: shutting down")
	// b.Close() may surface context.Canceled from sub-components that
	// observed the same signal-driven cancellation; that's not a real
	// failure of the shutdown path.
	if err := b.Close(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
