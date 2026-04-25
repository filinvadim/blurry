package blurry

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/replication"
)

var bridgeLog = logging.Logger("chotki-bridge")

// ChotkiBridge is a minimal one-connection bridge between a Blurry
// replica and a single Chotki replica. It is a PoC, not a full
// implementation:
//
//   - Dials a Chotki TCP listener and performs the chotki replication
//     handshake (H{T, M, V, S}).
//   - Reads chotki wire packets and applies "interesting" ones to the
//     local Blurry Store. Today: 'O' (object create) and 'E'
//     (object edit) for the Name field, mapped onto a fixed Blurry
//     class so the data round-trips through the Blurry HTTP API.
//   - Watches a single Blurry mirror class and, whenever a new object
//     is created or edited there, encodes a matching 'O'/'E' packet
//     and sends it back to Chotki.
//
// Limitations consciously accepted for the PoC:
//
//   - Only String fields are bridged (chotki rdt 'S').
//   - Object ids are NOT preserved across the bridge: each side mints
//     its own id; we maintain an in-memory translation table.
//   - The class on the chotki side must already exist and be
//     pre-registered via BridgeOptions.ChotkiClassID.
//   - No diff sync, no version-vector reconciliation. Live updates
//     only — peers that disconnect and reconnect lose history.
type ChotkiBridge struct {
	opts     BridgeOptions
	store    *Store
	classID  ID // local Blurry class id we mirror to/from chotki
	conn    net.Conn
	wmu     sync.Mutex
	closing chan struct{}
	wg      sync.WaitGroup
	// chotki/replication.TraceSize is 10. Anything else trips
	// chotki_errors.ErrBadHPacket inside DrainHandshake and the peer
	// times out the session.
	traceID [10]byte

	// chotkiToBlurry maps a chotki object rdx.ID → Blurry object id.
	// Used so subsequent E packets for the same chotki object go to
	// the right Blurry record.
	idmap   map[rdx.ID]ID
	idmapMu sync.Mutex

	// mirroredOut tracks Blurry object ids we've already forwarded to
	// chotki so a key write that triggers the CRDT PutHook a second
	// time becomes an 'E' packet rather than another 'O'.
	outMu       sync.Mutex
	mirroredOut map[ID]struct{}

	// mirroredIn tracks Blurry object ids that originated on the
	// chotki side. The CRDT PutHook fires for those when our local
	// applyChotkiObject does its Store write; without this set, we'd
	// echo every chotki write straight back as a fresh 'O' packet.
	inMu       sync.Mutex
	mirroredIn map[ID]struct{}

	// applyingDepth counts in-flight applyChotkiObject/applyChotkiEdit
	// calls. Increments before the Store write, decrements after.
	// MirrorFromCRDT skips when > 0 — the PutHook is firing because
	// of our own apply, not a real local write.
	applyingDepth int32
}

// BridgeOptions configures a ChotkiBridge.
type BridgeOptions struct {
	// Peer is the chotki "host:port" or "tcp://host:port" to dial.
	Peer string

	// MirrorClass is the human-readable name registered on the Blurry
	// side. The bridge looks up (or creates) this class with a single
	// String field "Name" — matching chotki's tutorial Student class
	// shape.
	MirrorClass string

	// ChotkiClassID is the chotki rdx.ID of the equivalent class on
	// the chotki side, in chotki's dotted hex form (e.g. "1f-2"). The
	// chotki replica must already have a class with this id.
	ChotkiClassID string

	// Source is this bridge's chotki replica id (32-bit Src). When 0,
	// derived from the Blurry settings.
	Source uint64
}

// NewChotkiBridge constructs but does not yet start a bridge. Call
// Start to dial chotki.
func NewChotkiBridge(store *Store, opts BridgeOptions) *ChotkiBridge {
	return &ChotkiBridge{
		opts:        opts,
		store:       store,
		closing:     make(chan struct{}),
		idmap:       map[rdx.ID]ID{},
		mirroredOut: map[ID]struct{}{},
		mirroredIn:  map[ID]struct{}{},
	}
}

// MirrorFromCRDT is the hook installed on the CRDT layer. It fires
// whenever a key/value is written, including ones that arrived from
// other libp2p replicas. The bridge inspects the key — if it's a
// "Name" field of an object under the configured mirror class, and
// the object hasn't been forwarded yet, encode it as a chotki 'O'
// packet and send.
//
// Key shape we care about (matches object-storage.go):
//
//	/<classID>/<objectID>/Name
func (b *ChotkiBridge) MirrorFromCRDT(k ds.Key, v []byte) {
	if b == nil || b.conn == nil {
		return
	}
	// Don't echo: this PutHook was triggered by our own
	// applyChotkiObject / applyChotkiEdit handling an incoming chotki
	// packet.
	if atomic.LoadInt32(&b.applyingDepth) > 0 {
		return
	}
	parts := strings.Split(strings.Trim(k.String(), "/"), "/")
	// expected: [<classID>, <objectID>, <fieldName>]
	if len(parts) != 3 {
		return
	}
	classID, objID, fieldName := ID(parts[0]), ID(parts[0]+"/"+parts[1]), parts[2]
	if classID != b.classID || fieldName != "Name" {
		return
	}
	// Also skip when this object id is one we created locally from a
	// chotki packet. Belt-and-braces complement to applyingDepth.
	b.inMu.Lock()
	_, fromChotki := b.mirroredIn[objID]
	b.inMu.Unlock()
	if fromChotki {
		return
	}
	b.outMu.Lock()
	_, already := b.mirroredOut[objID]
	if !already {
		b.mirroredOut[objID] = struct{}{}
	}
	b.outMu.Unlock()
	b.Mirror(context.Background(), classID, objID, map[string]string{"Name": string(v)})
}

// Start dials the chotki replica, performs the handshake and spawns
// reader/keepalive goroutines. Returns once the connection is up.
func (b *ChotkiBridge) Start(ctx context.Context) error {
	addr := normaliseChotkiAddr(b.opts.Peer)
	bridgeLog.Infof("bridge: dialing chotki at %s", addr)

	d := net.Dialer{Timeout: 10 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("bridge: dial %s: %w", addr, err)
	}
	b.conn = conn

	// Make sure the local Blurry mirror class exists and remember its id.
	classID, err := b.store.LookupName(ctx, b.opts.MirrorClass)
	if err != nil {
		classID, err = b.store.CreateClass(ctx, "", b.opts.MirrorClass, []FieldSpec{
			{Name: "Name", Kind: FieldString},
		})
		if err != nil {
			_ = conn.Close()
			return fmt.Errorf("bridge: create mirror class: %w", err)
		}
	}
	b.classID = classID

	if err := b.sendHandshake(); err != nil {
		_ = conn.Close()
		return err
	}

	b.wg.Add(1)
	go b.readLoop(ctx)

	bridgeLog.Infof("bridge: connected, mirror class %s on Blurry, %s on Chotki",
		b.classID, b.opts.ChotkiClassID)
	return nil
}

// Close tears the bridge down.
func (b *ChotkiBridge) Close() error {
	if b == nil {
		return nil
	}
	select {
	case <-b.closing:
		return nil
	default:
		close(b.closing)
	}
	if b.conn != nil {
		_ = b.conn.Close()
	}
	b.wg.Wait()
	return nil
}

// Mirror is invoked by Blurry's Store after a local create/edit so the
// change can be forwarded to chotki. cid is the Blurry class id, oid
// the Blurry object id. The bridge encodes a chotki 'O' (or 'E' if the
// object id has been seen before) and sends it.
func (b *ChotkiBridge) Mirror(ctx context.Context, cid, oid ID, fields map[string]string) {
	if b == nil || b.conn == nil {
		return
	}
	if cid != b.classID {
		return
	}
	name, ok := fields["Name"]
	if !ok {
		return
	}

	b.idmapMu.Lock()
	chotkiID, alreadyMirrored := b.findChotkiIDLocked(oid)
	b.idmapMu.Unlock()

	if !alreadyMirrored {
		// 'O' packet: create new chotki object of the configured class.
		// Chotki object ids are minted by the chotki replica itself;
		// we send the class as the parent rdx.ID and the field tlvs
		// in body order, the way replication.ParsePacket expects.
		classRef := rdx.IDFromString(b.opts.ChotkiClassID)
		if classRef == rdx.BadId || classRef == rdx.ID0 {
			bridgeLog.Warnf("bridge: bad chotki class id %q", b.opts.ChotkiClassID)
			return
		}
		// We don't know the chotki object id ahead of time. Send a
		// freshly minted "client-side" id; the chotki replica will
		// translate via its own commit machinery. For the PoC we just
		// generate something unique-ish.
		ourID := rdx.IDFromSrcSeqOff(b.source(), uint64(time.Now().UnixNano()&0x000fffffffffffff), 0)
		body := protocol.Records{
			protocol.Record(rdx.String, rdx.Stlv(name)),
		}
		pkt := protocol.Record('O',
			protocol.Record('I', ourID.ZipBytes()),
			protocol.Record('R', classRef.ZipBytes()),
			protocol.Concat(body...),
		)
		b.idmapMu.Lock()
		b.idmap[ourID] = oid
		b.idmapMu.Unlock()
		if err := b.write(pkt); err != nil {
			bridgeLog.Warnf("bridge: send O: %v", err)
		}
		return
	}

	// 'E' packet: edit the existing chotki object. Field offset 1 = Name.
	body := []byte{}
	body = append(body, protocol.Record('F', rdx.ZipUint64(1))...)
	body = append(body, protocol.Record(rdx.String, rdx.Stlv(name))...)
	pkt := protocol.Record('E',
		protocol.Record('I', rdx.IDFromSrcSeqOff(b.source(), uint64(time.Now().UnixNano()&0x000fffffffffffff), 0).ZipBytes()),
		protocol.Record('R', chotkiID.ZipBytes()),
		body,
	)
	if err := b.write(pkt); err != nil {
		bridgeLog.Warnf("bridge: send E: %v", err)
	}
}

// ---- internals --------------------------------------------------------

func (b *ChotkiBridge) source() uint64 {
	if b.opts.Source != 0 {
		return b.opts.Source
	}
	// As a last resort use a deterministic non-zero src derived from
	// MirrorClass; chotki rejects 0.
	h := sha1.Sum([]byte(b.opts.MirrorClass))
	v := uint64(h[0])<<24 | uint64(h[1])<<16 | uint64(h[2])<<8 | uint64(h[3])
	if v == 0 {
		v = 1
	}
	return v
}

// sendHandshake mirrors chotki/replication/sync.go FeedHandshake. We
// have no version vector and no snapshot, so we send empties.
func (b *ChotkiBridge) sendHandshake() error {
	mode := replication.SyncRWLive
	mzip := mode.Zip()

	// trace id is sha1 of a random uuid; for PoC just use the time.
	rand := time.Now().UnixNano()
	hash := sha1.Sum([]byte(fmt.Sprintf("blurry-bridge-%d", rand)))
	copy(b.traceID[:], hash[:10])

	// Empty version vector: V record with empty body. chotki accepts it.
	emptyVV := []byte{}

	hs := protocol.Record('H',
		protocol.TinyRecord('T', rdx.ID0.ZipBytes()),
		protocol.TinyRecord('M', mzip),
		protocol.Record('V', emptyVV),
		protocol.Record('S', b.traceID[:]),
	)
	bridgeLog.Debugf("bridge: handshake hex: %s", hex.EncodeToString(hs))
	return b.write(hs)
}

func (b *ChotkiBridge) write(pkt []byte) error {
	b.wmu.Lock()
	defer b.wmu.Unlock()
	_ = b.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := b.conn.Write(pkt)
	return err
}

func (b *ChotkiBridge) readLoop(ctx context.Context) {
	defer b.wg.Done()
	defer b.conn.Close()

	var buf bytes.Buffer
	chunk := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.closing:
			return
		default:
		}

		_ = b.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := b.conn.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				bridgeLog.Infof("bridge: chotki closed connection")
				return
			}
			var nerr net.Error
			if errors.As(err, &nerr) && nerr.Timeout() {
				continue
			}
			bridgeLog.Warnf("bridge: read: %v", err)
			return
		}

		recs, perr := protocol.Split(&buf)
		if perr != nil {
			bridgeLog.Warnf("bridge: split: %v", perr)
			continue
		}
		for _, r := range recs {
			b.handlePacket(ctx, r)
		}
	}
}

func (b *ChotkiBridge) handlePacket(ctx context.Context, pkt []byte) {
	lit, id, ref, body, err := replication.ParsePacket(pkt)
	if err != nil {
		bridgeLog.Debugf("bridge: skip unparsable packet: %v", err)
		return
	}
	switch lit {
	case 'H':
		bridgeLog.Infof("bridge: chotki handshake ack")
	case 'O':
		b.applyChotkiObject(ctx, id, ref, body)
	case 'E':
		b.applyChotkiEdit(ctx, ref, body)
	case 'C', 'Y', 'D', 'V', 'B', 'P':
		bridgeLog.Debugf("bridge: ignoring %c packet (id=%s)", lit, id)
	default:
		bridgeLog.Debugf("bridge: unknown packet %c", lit)
	}
}

// applyChotkiObject creates a Blurry object mirroring an incoming 'O'
// packet from chotki. We only look at the first String field.
func (b *ChotkiBridge) applyChotkiObject(ctx context.Context, id, ref rdx.ID, body []byte) {
	name := firstStringField(body)
	if name == "" {
		return
	}
	// Skip echoes of our own writes.
	b.idmapMu.Lock()
	if _, ours := b.idmap[id]; ours {
		b.idmapMu.Unlock()
		return
	}
	b.idmapMu.Unlock()

	// Tell MirrorFromCRDT to ignore the PutHook events fired by the
	// upcoming Store.CreateObject — they are echoes of an incoming
	// chotki write, not new local data.
	atomic.AddInt32(&b.applyingDepth, 1)
	bid, err := b.store.CreateObject(ctx, b.classID, map[string]string{"Name": name})
	atomic.AddInt32(&b.applyingDepth, -1)
	if err != nil {
		bridgeLog.Warnf("bridge: create blurry object from chotki %s: %v", id, err)
		return
	}
	b.idmapMu.Lock()
	b.idmap[id] = bid
	b.idmapMu.Unlock()
	b.inMu.Lock()
	b.mirroredIn[bid] = struct{}{}
	b.inMu.Unlock()
	bridgeLog.Infof("bridge: chotki %s → blurry %s (Name=%q)", id, bid, name)
}

func (b *ChotkiBridge) applyChotkiEdit(ctx context.Context, ref rdx.ID, body []byte) {
	b.idmapMu.Lock()
	bid, ok := b.idmap[ref]
	b.idmapMu.Unlock()
	if !ok {
		bridgeLog.Debugf("bridge: edit for unknown chotki id %s", ref)
		return
	}
	name := firstStringField(body)
	if name == "" {
		return
	}
	atomic.AddInt32(&b.applyingDepth, 1)
	_, err := b.store.EditObject(ctx, bid, map[string]string{"Name": name})
	atomic.AddInt32(&b.applyingDepth, -1)
	if err != nil {
		bridgeLog.Warnf("bridge: edit blurry %s: %v", bid, err)
		return
	}
	bridgeLog.Infof("bridge: chotki edit %s → blurry %s (Name=%q)", ref, bid, name)
}

func (b *ChotkiBridge) findChotkiIDLocked(oid ID) (rdx.ID, bool) {
	for cid, bid := range b.idmap {
		if bid == oid {
			return cid, true
		}
	}
	return rdx.ID0, false
}

// firstStringField walks an O/E body and returns the first 'S' (string)
// field's value. Other fields are skipped. Returns "" if none found.
func firstStringField(body []byte) string {
	for len(body) > 0 {
		lit, hlen, blen := protocol.ProbeHeader(body)
		if lit == 0 || hlen+blen > len(body) {
			return ""
		}
		fieldBody := body[hlen : hlen+blen]
		body = body[hlen+blen:]
		// 'F' records are field-offset markers in 'E' packets; skip.
		if lit == 'F' {
			continue
		}
		if lit == rdx.String || lit == 's' {
			return rdx.Snative(fieldBody)
		}
	}
	return ""
}

// normaliseChotkiAddr accepts "host:port" or "tcp://host:port" and
// returns "host:port" suitable for net.Dial.
func normaliseChotkiAddr(s string) string {
	const tcp = "tcp://"
	if len(s) > len(tcp) && s[:len(tcp)] == tcp {
		return s[len(tcp):]
	}
	return s
}
