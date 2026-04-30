package blurry

import (
	"context"
	"errors"
	"strings"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

// ErrKeyNotFound is returned by KV.Get when the key has no value in
// the local CRDT store yet.
var ErrKeyNotFound = errors.New("kv: key not found")

// KV is a thin key/value façade on top of the CRDT-wrapped datastore.
//
// Every write goes through ds.Batching: even a one-key Set turns into
// a single Batch.Commit, and bulk SetBatch collapses N writes into one
// CRDT delta + DAG node + pubsub broadcast. That's the path
// go-ds-crdt's documentation calls out as the difference between its
// "400 keys/s with batching" baseline and the much slower per-Put
// fallback.
//
// KV is intentionally minimal: bytes in, bytes out, optional prefix
// scan. Anything richer (schemas, classes, fields) belongs in a
// higher-level package or in legacy/chotki/ for the Chotki-shaped
// API.
type KV struct {
	ds ds.Datastore
}

// NewKV wraps a Datastore as a KV. The datastore is expected to be
// the CRDT-wrapped store from CRDT.Datastore() so writes propagate
// through the cluster; passing the raw badger store works too but
// keeps writes local.
func NewKV(d ds.Datastore) *KV {
	return &KV{ds: d}
}

// Get returns the value at key. Missing keys yield ErrKeyNotFound.
func (k *KV) Get(ctx context.Context, key string) ([]byte, error) {
	v, err := k.ds.Get(ctx, dsKey(key))
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return v, nil
}

// Has reports whether key has a value.
func (k *KV) Has(ctx context.Context, key string) (bool, error) {
	return k.ds.Has(ctx, dsKey(key))
}

// Set writes one key. It always goes through ds.Batch when the
// underlying datastore implements ds.Batching (the CRDT store does)
// so the wire path is identical to a one-element SetBatch.
func (k *KV) Set(ctx context.Context, key string, value []byte) error {
	return k.SetBatch(ctx, []KVPair{{Key: key, Value: value}})
}

// SetBatch atomically writes every pair in pairs as a single
// ds.Batch.Commit. With the CRDT-wrapped store this becomes one CRDT
// delta + DAG node + pubsub broadcast, regardless of len(pairs).
func (k *KV) SetBatch(ctx context.Context, pairs []KVPair) error {
	if len(pairs) == 0 {
		return nil
	}
	if b, ok := k.ds.(ds.Batching); ok {
		batch, err := b.Batch(ctx)
		if err != nil {
			return err
		}
		for _, p := range pairs {
			if err := batch.Put(ctx, dsKey(p.Key), p.Value); err != nil {
				return err
			}
		}
		return batch.Commit(ctx)
	}
	for _, p := range pairs {
		if err := k.ds.Put(ctx, dsKey(p.Key), p.Value); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes key. Falls back to a per-key Delete on non-Batching
// datastores; goes through Batch on the CRDT-wrapped store so the
// delete becomes one tombstone delta.
func (k *KV) Delete(ctx context.Context, key string) error {
	if b, ok := k.ds.(ds.Batching); ok {
		batch, err := b.Batch(ctx)
		if err != nil {
			return err
		}
		if err := batch.Delete(ctx, dsKey(key)); err != nil {
			return err
		}
		return batch.Commit(ctx)
	}
	return k.ds.Delete(ctx, dsKey(key))
}

// List streams every key/value whose key starts with prefix. The
// prefix is matched against application keys (the leading "/" used
// internally is added automatically). KeysOnly drops the value to
// save bandwidth on big scans.
func (k *KV) List(ctx context.Context, prefix string, keysOnly bool) ([]KVPair, error) {
	q := query.Query{
		Prefix:   "/" + strings.TrimPrefix(prefix, "/"),
		KeysOnly: keysOnly,
	}
	res, err := k.ds.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	var out []KVPair
	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		key := r.Key
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		var v []byte
		if !keysOnly {
			v = append([]byte(nil), r.Value...)
		}
		out = append(out, KVPair{Key: key, Value: v})
	}
	return out, nil
}

// KVPair is one key/value entry used by SetBatch and List.
type KVPair struct {
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// dsKey turns an application key into the leading-slash datastore
// path go-datastore expects.
func dsKey(k string) ds.Key {
	if len(k) > 0 && k[0] == '/' {
		return ds.NewKey(k)
	}
	return ds.NewKey("/" + k)
}
