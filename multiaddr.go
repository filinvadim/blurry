package blurry

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// ErrNoPeerID is returned when an address can be parsed as a multiaddr
// but contains no /p2p/<peerID> component, so libp2p can not dial it.
var ErrNoPeerID = errors.New("multiaddr: missing /p2p/<peerID> component")

// ResolvePeerAddr converts a user-provided cluster peer string into a
// libp2p AddrInfo. Accepted forms:
//
//   - libp2p multiaddr with peer ID:
//     /ip4/1.2.3.4/tcp/4001/p2p/12D3KooW...
//   - libp2p multiaddr without peer ID (returns ErrNoPeerID):
//     /ip4/1.2.3.4/tcp/4001
//   - "host:port" or bare IP — converted to /ip4|/ip6 + /tcp; peer ID is
//     unknown so AddrInfo.ID is empty (caller may use defaultPort to
//     fill the port for a bare IP)
func ResolvePeerAddr(s string, defaultPort int) (peer.AddrInfo, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return peer.AddrInfo{}, errors.New("multiaddr: empty address")
	}

	// Looks like a multiaddr already.
	if strings.HasPrefix(s, "/") {
		mAddr, err := ma.NewMultiaddr(s)
		if err != nil {
			return peer.AddrInfo{}, fmt.Errorf("multiaddr: parse %q: %w", s, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(mAddr)
		if err != nil {
			// Allow bare multiaddr without /p2p, but signal explicitly.
			if errors.Is(err, peer.ErrInvalidAddr) {
				return peer.AddrInfo{Addrs: []ma.Multiaddr{mAddr}}, ErrNoPeerID
			}
			return peer.AddrInfo{}, fmt.Errorf("multiaddr: addrinfo %q: %w", s, err)
		}
		return *info, nil
	}

	// Plain "host:port" or bare host.
	host, port := splitHostPort(s, defaultPort)
	if host == "" {
		return peer.AddrInfo{}, fmt.Errorf("multiaddr: cannot parse %q", s)
	}

	mAddr, err := hostPortToMultiaddr(host, port)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	return peer.AddrInfo{Addrs: []ma.Multiaddr{mAddr}}, nil
}

// ResolvePeerAddrs resolves a list of cluster peer strings, skipping
// (with the provided onErr callback) entries that fail to parse.
func ResolvePeerAddrs(peers []string, defaultPort int, onErr func(s string, err error)) []peer.AddrInfo {
	out := make([]peer.AddrInfo, 0, len(peers))
	for _, p := range peers {
		info, err := ResolvePeerAddr(p, defaultPort)
		if err != nil && !errors.Is(err, ErrNoPeerID) {
			if onErr != nil {
				onErr(p, err)
			}
			continue
		}
		out = append(out, info)
	}
	return out
}

func splitHostPort(s string, defaultPort int) (string, int) {
	// Handle bracketed IPv6.
	if strings.HasPrefix(s, "[") {
		host, portStr, err := net.SplitHostPort(s)
		if err == nil {
			port, _ := strconv.Atoi(portStr)
			return host, port
		}
	}
	if strings.Contains(s, ":") && strings.Count(s, ":") == 1 {
		host, portStr, err := net.SplitHostPort(s)
		if err == nil {
			port, _ := strconv.Atoi(portStr)
			return host, port
		}
	}
	return s, defaultPort
}

func hostPortToMultiaddr(host string, port int) (ma.Multiaddr, error) {
	if port <= 0 {
		return nil, fmt.Errorf("multiaddr: missing port for host %q", host)
	}
	ip := net.ParseIP(host)
	var proto string
	switch {
	case ip == nil:
		proto = "dns" // libp2p resolves at dial time
	case ip.To4() != nil:
		proto = "ip4"
		host = ip.To4().String()
	default:
		proto = "ip6"
		host = ip.To16().String()
	}
	return ma.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%d", proto, host, port))
}
