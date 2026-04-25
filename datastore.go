package blurry

import (
	"context"
	"errors"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	json "github.com/json-iterator/go"
	"math"
	"runtime"
	"strings"
	"time"
)

const (
	requiredPrefixSlash = "/" // slash is required

	crdtPrefix = "CRDT"
)

var ErrNilNodeRepo = errors.New("node repo is nil")

func NewStatsRepo(db any) ds.Datastore {
	prefix := crdtPrefix
	if !strings.HasPrefix(prefix, requiredPrefixSlash) {
		prefix = requiredPrefixSlash + prefix
	}
	nr := &NodeRepo{
		db:       db,
		prefix:   prefix,
		stopChan: make(chan struct{}),
	}
	return nr
}

type NodeRepo struct {
	db       NodeStorer
	prefix   string
	stopChan chan struct{}

	BootstrapSelfHashHex string
}

func (d *NodeRepo) Put(ctx context.Context, key ds.Key, value []byte) error {
	if d == nil || d.db == nil {
		return ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if d.db.IsClosed() {
		return ErrNotRunning
	}

	rootKey := buildRootKey(key)

	prefix := NewPrefixBuilder(d.prefix).
		AddRootID(rootKey).
		Build()
	return d.db.Set(prefix, value)
}

func (d *NodeRepo) Sync(ctx context.Context, _ ds.Key) error {
	if d == nil || d.db == nil {
		return ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if d.db.IsClosed() {
		return ErrNotRunning
	}

	return d.db.Sync()
}

func (d *NodeRepo) PutWithTTL(ctx context.Context, key ds.Key, value []byte, ttl time.Duration) error {
	if d == nil || d.db == nil {
		return ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if d.db.IsClosed() {
		return ErrNotRunning
	}

	rootKey := buildRootKey(key)

	prefix := NewPrefixBuilder(d.prefix).
		AddRootID(rootKey).
		Build()

	return d.db.SetWithTTL(prefix, value, ttl)
}

func (d *NodeRepo) SetTTL(ctx context.Context, key ds.Key, ttl time.Duration) error {
	if d == nil || d.db == nil {
		return ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if d.db.IsClosed() {
		return ErrNotRunning
	}

	item, err := d.Get(ctx, key)
	if err != nil {
		return err
	}
	return d.PutWithTTL(ctx, key, item, ttl)
}

func (d *NodeRepo) GetExpiration(ctx context.Context, key ds.Key) (t time.Time, err error) {
	if d == nil || d.db == nil {
		return t, ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return t, ctx.Err()
	}
	if d.db.IsClosed() {
		return t, ErrNotRunning
	}

	rootKey := buildRootKey(key)

	prefix := NewPrefixBuilder(d.prefix).
		AddRootID(rootKey).
		Build()

	expiresAt, err := d.db.GetExpiration(prefix)
	if IsNotFoundError(err) {
		return t, ToDatastoreErrNotFound(err)
	}
	if err != nil {
		return t, err
	}

	if expiresAt > math.MaxInt64 {
		expiresAt = math.MaxInt64
	}
	expiration := time.Unix(int64(expiresAt), 0) //#nosec

	return expiration, err
}

func (d *NodeRepo) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	if d == nil || d.db == nil {
		return nil, ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if d.db.IsClosed() {
		return nil, ErrNotRunning
	}

	rootKey := buildRootKey(key)

	prefix := NewPrefixBuilder(d.prefix).
		AddRootID(rootKey).
		Build()

	value, err = d.db.Get(prefix)
	if IsNotFoundError(err) {
		return nil, ToDatastoreErrNotFound(err)
	}
	if err != nil {
		return nil, err
	}

	return value, err
}

func (d *NodeRepo) Has(ctx context.Context, key ds.Key) (_ bool, err error) {
	if d == nil || d.db == nil {
		return false, ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if d.db.IsClosed() {
		return false, ErrNotRunning
	}

	rootKey := buildRootKey(key)

	prefix := NewPrefixBuilder(d.prefix).
		AddRootID(rootKey).
		Build()

	_, err = d.db.Get(prefix)
	switch {
	case IsNotFoundError(err):
		return false, nil
	case err == nil:
		return true, nil
	default:
		return false, fmt.Errorf("has: %w", err)
	}
}

func (d *NodeRepo) GetSize(ctx context.Context, key ds.Key) (_ int, err error) {
	size := -1
	if d == nil || d.db == nil {
		return size, ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return size, ctx.Err()
	}
	if d.db.IsClosed() {
		return size, ErrNotRunning
	}

	rootKey := buildRootKey(key)

	prefix := NewPrefixBuilder(d.prefix).
		AddRootID(rootKey).
		Build()

	itemSize, err := d.db.GetSize(prefix)
	switch {
	case err == nil:
		return int(itemSize), nil
	case IsNotFoundError(err):
		return 0, ToDatastoreErrNotFound(err)
	default:
		return 0, fmt.Errorf("size: %w", err)
	}
}

func (d *NodeRepo) Delete(ctx context.Context, key ds.Key) error {
	if d == nil || d.db == nil {
		return ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if d.db.IsClosed() {
		return ErrNotRunning
	}

	rootKey := buildRootKey(key)

	prefix := NewPrefixBuilder(d.prefix).
		AddRootID(rootKey).
		Build()

	return d.db.Delete(prefix)
}

// DiskUsage implements the PersistentDatastore interface.
// It returns the sum of lsm and value log files sizes in bytes.
func (d *NodeRepo) DiskUsage(ctx context.Context) (uint64, error) {
	if d == nil || d.db == nil {
		return 0, ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if d.db.IsClosed() {
		return 0, ErrNotRunning
	}

	lsm, vlog := d.db.InnerDB().Size()
	if (lsm + vlog) < 0 {
		return 0, DBError("disk usage: malformed value")
	}
	return uint64(lsm + vlog), nil //#nosec
}

func (d *NodeRepo) Query(ctx context.Context, q ds.Query) (ds.Results, error) {
	if d == nil || d.db == nil {
		return nil, ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if d.db.IsClosed() {
		return nil, ErrNotRunning
	}

	tx := d.db.InnerDB().NewTransaction(true)
	return d.query(tx, q)
}

func (d *NodeRepo) query(tx *Txn, q ds.Query) (_ ds.Results, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = DBError("node repo: query recovered")
			err = fmt.Errorf("%w: %v", err, r)
		}
	}()

	if d.db.IsClosed() {
		return nil, ErrNotRunning
	}
	opt := DefaultIteratorOptions
	opt.PrefetchValues = !q.KeysOnly

	opt.Prefix = d.storageQueryPrefix(q.Prefix)

	// Handle ordering
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case ds.OrderByKey, *ds.OrderByKey:
		// We order by key by default.
		case ds.OrderByKeyDescending, *ds.OrderByKeyDescending:
			// Reverse order by key
			opt.Reverse = true
		default:
			// Ok, we have a weird order we can't handle. Let's
			// perform the _base_ query (prefix, filter, etc.), then
			// handle sort/offset/limit later.

			// Skip the stuff we can't apply.
			baseQuery := q
			baseQuery.Limit = 0
			baseQuery.Offset = 0
			baseQuery.Orders = nil

			// perform the base query.
			res, err := d.query(tx, baseQuery)
			if err != nil {
				return nil, err
			}

			res = ds.ResultsReplaceQuery(res, q)

			naiveQuery := q
			naiveQuery.Prefix = ""
			naiveQuery.Filters = nil

			return ds.NaiveQueryApply(naiveQuery, res), nil
		}
	}

	it := tx.NewIterator(opt)
	results := ds.ResultsWithContext(q, func(ctx context.Context, output chan<- ds.Result) {
		defer tx.Discard()
		defer it.Close()

		it.Rewind()

		for skipped := 0; skipped < q.Offset && it.Valid(); it.Next() {
			if d.db.IsClosed() {
				return
			}

			if len(q.Filters) == 0 {
				skipped++
				continue
			}
			item := it.Item()

			matches := true
			check := func(value []byte) error {
				e := ds.DsEntry{
					Key:   d.resultKeyFromStorageKey(string(item.Key())),
					Value: value,
					Size:  int(item.ValueSize()),
				}

				if q.ReturnExpirations {
					e.Expiration = expires(item)
				}
				matches = filter(q.Filters, e)
				return nil
			}

			var err error
			if q.KeysOnly {
				err = check(nil)
			} else {
				err = item.Value(check)
			}

			if err != nil {
				select {
				case output <- ds.Result{Error: err}:
				case <-d.stopChan:
					return
				case <-ctx.Done():
					return
				}
			}
			if !matches {
				skipped++
			}
		}

		for sent := 0; (q.Limit <= 0 || sent < q.Limit) && it.Valid(); it.Next() {
			if d.db.IsClosed() {
				return
			}
			item := it.Item()
			e := ds.DsEntry{Key: d.resultKeyFromStorageKey(string(item.Key()))}

			var result ds.Result
			if !q.KeysOnly {
				b, err := item.ValueCopy(nil)
				if err != nil {
					result = ds.Result{Error: err}
				} else {
					e.Value = b
					e.Size = len(b)
					result = ds.Result{Entry: e}
				}
			} else {
				e.Size = int(item.ValueSize())
				result = ds.Result{Entry: e}
			}

			if q.ReturnExpirations {
				result.Expiration = expires(item)
			}

			if result.Error == nil && filter(q.Filters, e) {
				continue
			}
			select {
			case output <- result:
				sent++
			case <-d.stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	})

	return results, nil
}

func (d *NodeRepo) storageQueryPrefix(queryPrefix string) []byte {
	prefix := strings.TrimSuffix(ds.NewKey(queryPrefix).String(), requiredPrefixSlash)
	base := strings.TrimSuffix(d.prefix, requiredPrefixSlash)

	if prefix == "" {
		return []byte(base + requiredPrefixSlash)
	}

	return []byte(base + prefix + requiredPrefixSlash)
}

func (d *NodeRepo) resultKeyFromStorageKey(storageKey string) string {
	if storageKey == d.prefix {
		return requiredPrefixSlash
	}

	trimPrefix := d.prefix + requiredPrefixSlash
	if strings.HasPrefix(storageKey, trimPrefix) { //nolint:modernize
		return requiredPrefixSlash + strings.TrimPrefix(storageKey, trimPrefix)
	}

	return storageKey
}

func filter(filters []ds.Filter, entry ds.DsEntry) bool {
	for _, f := range filters {
		if !f.Filter(entry) {
			return true
		}
	}
	return false
}

func expires(item *Item) time.Time {
	expiresAt := item.ExpiresAt()
	if expiresAt > math.MaxInt64 {
		expiresAt--
	}
	return time.Unix(int64(expiresAt), 0) //#nosec
}

func (d *NodeRepo) Close() (err error) {
	if d == nil || d.db == nil {
		return nil
	}
	if d.db.IsClosed() {
		return nil
	}

	close(d.stopChan)
	log.Infoln("node repo: closed")
	return nil
}

type batch struct {
	db         NodeStorer
	prefix     string
	writeBatch *WriteBatch
}

var _ ds.Batch = (*batch)(nil)

func (d *NodeRepo) Batch(ctx context.Context) (ds.Batch, error) {
	if d == nil || d.db == nil {
		return nil, ErrNilNodeRepo
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if d.db.IsClosed() {
		return nil, ErrNotRunning
	}
	b := &batch{d.db, d.prefix, d.db.InnerDB().NewWriteBatch()}
	runtime.SetFinalizer(b, func(b *batch) { _ = b.Cancel() })

	return b, nil
}

func (b *batch) Put(ctx context.Context, key ds.Key, value []byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	return b.put(key, value)
}

func (b *batch) put(key ds.Key, value []byte) error {
	if b == nil || b.db == nil || b.writeBatch == nil {
		return ErrNilNodeRepo
	}
	if b.db.IsClosed() {
		return ErrNotRunning
	}

	rootKey := buildRootKey(key)

	batchKey := NewPrefixBuilder(b.prefix).
		AddRootID(rootKey).
		Build()
	return b.writeBatch.Set(batchKey.Bytes(), value)
}

func (b *batch) putWithTTL(key ds.Key, value []byte, ttl time.Duration) error {
	if b == nil || b.db == nil || b.writeBatch == nil {
		return ErrNilNodeRepo
	}
	if b.db.IsClosed() {
		return ErrNotRunning
	}

	rootKey := buildRootKey(key)

	batchKey := NewPrefixBuilder(b.prefix).
		AddRootID(rootKey).
		Build()
	return b.writeBatch.SetEntry(&Entry{
		Key:       batchKey.Bytes(),
		Value:     value,
		ExpiresAt: uint64(time.Now().Add(ttl).Unix()), //#nosec
	})
}

func (b *batch) Delete(ctx context.Context, key ds.Key) error {
	if b == nil || b.db == nil || b.writeBatch == nil {
		return ErrNilNodeRepo
	}
	if b.db.IsClosed() {
		return ErrNotRunning
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	rootKey := buildRootKey(key)

	batchKey := NewPrefixBuilder(b.prefix).
		AddRootID(rootKey).
		Build()
	return b.writeBatch.Delete(batchKey.Bytes())
}

func (b *batch) Commit(ctx context.Context) error {
	if b == nil || b.db == nil || b.writeBatch == nil {
		return ErrNilNodeRepo
	}
	if b.db.IsClosed() {
		_ = b.Cancel()
		return nil
	}
	if ctx.Err() != nil {
		_ = b.Cancel()
		return ctx.Err()
	}

	err := b.writeBatch.Flush()
	_ = b.Cancel()
	return err
}

func (b *batch) Cancel() error {
	if b == nil || b.db == nil || b.writeBatch == nil {
		return nil
	}

	b.writeBatch.Cancel()
	b.writeBatch = nil
	runtime.SetFinalizer(b, nil)
	return nil
}

func buildRootKey(key ds.Key) string {
	rootKey := strings.TrimPrefix(key.String(), "/")
	if len(rootKey) == 0 {
		rootKey = key.String()
	}
	return rootKey
}

const (
	BlocklistSubNamespace     = "BLOCKLIST"
	BlocklistUserSubNamespace = "USER"
	BlocklistTermSubNamespace = "TERM"
)

type BlockLevel int

func (b BlockLevel) Next() BlockLevel {
	if b >= PermanentBlock {
		return PermanentBlock
	}
	return b + 1
}

const (
	InitialBlock BlockLevel = iota + 1
	MediumBlock
	AdvancedBlock
	PermanentBlock
)

const (
	noExpiryBlockDuration time.Duration = 0
	advancedBlockDuration               = 7 * 24 * time.Hour
	mediumBlockDuration                 = 24 * time.Hour
	initialBlockDuration                = time.Hour
)

var blockDurationMapping = map[BlockLevel]time.Duration{
	InitialBlock:   initialBlockDuration,
	MediumBlock:    mediumBlockDuration,
	AdvancedBlock:  advancedBlockDuration,
	PermanentBlock: noExpiryBlockDuration,
}

type BlocklistTerm struct {
	PeerID string
	Level  BlockLevel
}

func (d *NodeRepo) Blocklist(peerId string) error {
	if d == nil {
		return ErrNilNodeRepo
	}
	if peerId == "" {
		return DBError("empty peer ID")
	}

	txn, err := d.db.NewTxn()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	blocklistTermKey := NewPrefixBuilder(d.prefix).
		AddSubPrefix(BlocklistSubNamespace).
		AddSubPrefix(BlocklistTermSubNamespace).
		AddRootID(peerId).
		Build()

	bt, err := txn.Get(blocklistTermKey)
	if err != nil && !IsNotFoundError(err) {
		return err
	}

	var term BlocklistTerm
	if bt != nil {
		if err := json.Unmarshal(bt, &term); err != nil {
			return err
		}
	}

	if term.Level == 0 {
		term.Level = InitialBlock
	} else {
		term.Level = term.Level.Next()
	}

	dur := blockDurationMapping[term.Level]

	blocklistUserKey := NewPrefixBuilder(d.prefix).
		AddSubPrefix(BlocklistSubNamespace).
		AddSubPrefix(BlocklistUserSubNamespace).
		AddRootID(peerId).
		Build()

	if err := txn.SetWithTTL(blocklistUserKey, []byte{}, dur); err != nil {
		return err
	}

	bt, err = sonic.Marshal(term)
	if err != nil {
		return err
	}

	if err := txn.Set(blocklistTermKey, bt); err != nil {
		return err
	}
	return txn.Commit()
}

func (d *NodeRepo) IsBlocklisted(peerId string) bool {
	if d == nil {
		return false
	}
	if peerId == "" {
		return false
	}

	blocklistUserKey := NewPrefixBuilder(d.prefix).
		AddSubPrefix(BlocklistSubNamespace).
		AddSubPrefix(BlocklistUserSubNamespace).
		AddRootID(peerId).
		Build()
	_, err := d.db.Get(blocklistUserKey)
	return err == nil
}

func (d *NodeRepo) BlocklistTerm(peerId string) (*BlocklistTerm, error) {
	if d == nil {
		return nil, ErrNilNodeRepo
	}
	if peerId == "" {
		return nil, DBError("empty peer ID")
	}
	var term BlocklistTerm

	blocklistTermKey := NewPrefixBuilder(d.prefix).
		AddSubPrefix(BlocklistSubNamespace).
		AddSubPrefix(BlocklistTermSubNamespace).
		AddRootID(peerId).
		Build()
	bt, err := d.db.Get(blocklistTermKey)
	if IsNotFoundError(err) {
		return &term, nil
	}
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bt, &term); err != nil {
		return nil, err
	}
	return &term, nil
}

func (d *NodeRepo) BlocklistRemove(peerId string) error {
	if d == nil {
		return nil
	}
	if peerId == "" {
		return nil
	}
	txn, err := d.db.NewTxn()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	blocklistUserKey := NewPrefixBuilder(d.prefix).
		AddSubPrefix(BlocklistSubNamespace).
		AddSubPrefix(BlocklistUserSubNamespace).
		AddRootID(peerId).
		Build()
	_ = txn.Delete(blocklistUserKey)

	blocklistTermKey := NewPrefixBuilder(d.prefix).
		AddSubPrefix(BlocklistSubNamespace).
		AddSubPrefix(BlocklistTermSubNamespace).
		AddRootID(peerId).
		Build()
	term := BlocklistTerm{PeerID: peerId, Level: 0}
	bt, err := json.Marshal(term)
	if err != nil {
		return err
	}
	if err := txn.Set(blocklistTermKey, bt); err != nil {
		return err
	}
	return txn.Commit()
}
