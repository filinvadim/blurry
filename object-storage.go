package blurry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	json "github.com/json-iterator/go"
)

var storeLog = logging.Logger("store")

// ID is a hierarchical identifier composed of '/'-separated path
// components.  The string itself IS the badger key after a leading '/'
// is added.  Composition is direct concatenation:
//
//	root              "" (the empty path)
//	class             "<classID>"                   (one segment)
//	object            "<classID>/<objectID>"        (two segments)
//	field             "<classID>/<objectID>/<name>" (three segments)
//
// This form satisfies the requirement that the data hierarchy lives in
// the badger keys themselves ("parentID/childID/etc").
type ID string

const (
	keySep = "/"

	// classMarker / objectMarker disambiguate IDs that would otherwise
	// share the same path depth (e.g. a class definition vs. a top-level
	// object).  They are part of the ID and therefore part of the key,
	// preserving the literal "parentID/childID/..." layout.
	classMarker  = "c"
	objectMarker = "o"

	// nameKeyPrefix segregates {name -> id} mappings into their own
	// subtree so they don't collide with class/object ids.
	nameKeyPrefix = ".name"
)

// Errors returned by the Store.
var (
	ErrClassUnknown   = errors.New("store: class unknown")
	ErrObjectUnknown  = errors.New("store: object unknown")
	ErrFieldUnknown   = errors.New("store: field unknown")
	ErrNameUnknown    = errors.New("store: name unknown")
	ErrInvalidID      = errors.New("store: invalid id")
	ErrInvalidPayload = errors.New("store: invalid payload")
)

// FieldKind matches Chotki's RDX type codes (F/I/R/S/T/E/L/M/N/Z).
type FieldKind byte

const (
	FieldFloat    FieldKind = 'F'
	FieldInteger  FieldKind = 'I'
	FieldRef      FieldKind = 'R'
	FieldString   FieldKind = 'S'
	FieldTerm     FieldKind = 'T'
	FieldEulerian FieldKind = 'E'
	FieldLinear   FieldKind = 'L'
	FieldMapping  FieldKind = 'M'
	FieldNCounter FieldKind = 'N'
	FieldZCounter FieldKind = 'Z'
)

// FieldSpec describes one field in a class definition.
type FieldSpec struct {
	Name string    `json:"name"`
	Kind FieldKind `json:"kind"`
}

// Class is a list of named fields, mirroring chotki/classes.Fields.
type Class struct {
	ID     ID          `json:"id"`
	Parent ID          `json:"parent"`
	Name   string      `json:"name,omitempty"`
	Fields []FieldSpec `json:"fields"`
}

// Object is an instance of a Class. Field values are stored as their
// canonical string form, identical to what the Chotki HTTP API accepts.
type Object struct {
	ID      ID                `json:"id"`
	ClassID ID                `json:"class"`
	Fields  map[string]string `json:"fields"`
}

// WriteHook is called after a successful Create/Edit on the local
// Store. Used by ChotkiBridge to forward local changes to a Chotki
// replica over the chotki wire protocol. May be nil.
type WriteHook func(ctx context.Context, classID, objectID ID, fields map[string]string)

// Store is the Chotki-compatible class/object/field store.  Keys live
// directly in badger as "parentID/childID/etc" strings.  ACID semantics
// come from the underlying CRDT-wrapped badger datastore: writes are
// atomic per-key and durable when Settings.SyncWrites is true.
type Store struct {
	mu      sync.Mutex
	ds      ds.Datastore
	source  string // hex peer-id prefix used to mint new ids
	counter uint64

	hookMu      sync.RWMutex
	onCreateObj WriteHook
	onEditObj   WriteHook
}

// SetWriteHooks installs callbacks that fire after CreateObject /
// EditObject return successfully. Either may be nil.
func (s *Store) SetWriteHooks(onCreate, onEdit WriteHook) {
	s.hookMu.Lock()
	s.onCreateObj = onCreate
	s.onEditObj = onEdit
	s.hookMu.Unlock()
}

func (s *Store) fireCreate(ctx context.Context, cid, oid ID, fields map[string]string) {
	s.hookMu.RLock()
	hook := s.onCreateObj
	s.hookMu.RUnlock()
	if hook != nil {
		hook(ctx, cid, oid, fields)
	}
}

func (s *Store) fireEdit(ctx context.Context, cid, oid ID, fields map[string]string) {
	s.hookMu.RLock()
	hook := s.onEditObj
	s.hookMu.RUnlock()
	if hook != nil {
		hook(ctx, cid, oid, fields)
	}
}

// NewStore wires a Store on top of an existing CRDT-backed datastore.
// The source string is mixed into newly minted ids, keeping them unique
// per-replica without coordination. A short stable hash of source goes
// into every id so the same replica produces a consistent prefix.
func NewObjectStorage(d ds.Datastore, source string) *Store {
	if source == "" {
		source = "anon"
	}
	return &Store{ds: d, source: stableHash(source, 8)}
}

// MintID returns a fresh hierarchical id, using parent as the prefix.
// The id form is "<parent>/<marker><source>-<counter>" — i.e. a literal
// "parentID/childID" string that doubles as a badger key.
func (s *Store) MintID(parent ID, marker string) ID {
	s.mu.Lock()
	s.counter++
	n := s.counter
	s.mu.Unlock()
	child := marker + s.source + "-" + strconv.FormatUint(n, 16)
	if parent == "" {
		return ID(child)
	}
	return ID(string(parent) + keySep + child)
}

// CreateClass defines a new class. parent may be empty for top-level
// classes or another class id for inheritance.  The returned id is the
// full "parent/child" path.
func (s *Store) CreateClass(ctx context.Context, parent ID, name string, fields []FieldSpec) (ID, error) {
	for _, f := range fields {
		if f.Name == "" || f.Kind == 0 {
			return "", fmt.Errorf("%w: bad field %+v", ErrInvalidPayload, f)
		}
	}
	id := s.MintID(parent, classMarker)
	c := Class{ID: id, Parent: parent, Name: name, Fields: fields}
	if err := s.putJSON(ctx, dataKey(id), c); err != nil {
		return "", err
	}
	if name != "" {
		if err := s.SetName(ctx, name, id); err != nil {
			return "", err
		}
	}
	return id, nil
}

// GetClass loads a class definition by id.
func (s *Store) GetClass(ctx context.Context, id ID) (*Class, error) {
	var c Class
	if err := s.getJSON(ctx, dataKey(id), &c); err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return nil, ErrClassUnknown
		}
		return nil, err
	}
	return &c, nil
}

// CreateObject creates an Object of the given class. Field values not
// present in `values` keep their RDT zero value.  The new object id is
// rooted at the class id, so the badger key is literally
// "<classID>/<objectID>".
func (s *Store) CreateObject(ctx context.Context, classID ID, values map[string]string) (ID, error) {
	c, err := s.GetClass(ctx, classID)
	if err != nil {
		return "", err
	}

	for k := range values {
		if !classHasField(c, k) {
			return "", fmt.Errorf("%w: %q", ErrFieldUnknown, k)
		}
	}

	obj := Object{
		ID:      s.MintID(classID, objectMarker),
		ClassID: classID,
		Fields:  map[string]string{},
	}
	for _, f := range c.Fields {
		if v, ok := values[f.Name]; ok {
			obj.Fields[f.Name] = v
		} else {
			obj.Fields[f.Name] = defaultForKind(f.Kind)
		}
	}

	if err := s.putJSON(ctx, dataKey(obj.ID), obj); err != nil {
		return "", err
	}
	for name, val := range obj.Fields {
		if err := s.putString(ctx, fieldKey(obj.ID, name), val); err != nil {
			return "", err
		}
	}
	s.fireCreate(ctx, classID, obj.ID, obj.Fields)
	return obj.ID, nil
}

// EditObject merges the provided field values into the object.
// Each individual field write is a single badger key update; the
// underlying datastore guarantees per-key atomicity.
func (s *Store) EditObject(ctx context.Context, id ID, values map[string]string) (ID, error) {
	obj, err := s.GetObject(ctx, id)
	if err != nil {
		return "", err
	}
	c, err := s.GetClass(ctx, obj.ClassID)
	if err != nil {
		return "", err
	}
	known := map[string]struct{}{}
	for _, f := range c.Fields {
		known[f.Name] = struct{}{}
	}
	for name, v := range values {
		if _, ok := known[name]; !ok {
			return "", fmt.Errorf("%w: %q", ErrFieldUnknown, name)
		}
		obj.Fields[name] = v
		if err := s.putString(ctx, fieldKey(id, name), v); err != nil {
			return "", err
		}
	}
	if err := s.putJSON(ctx, dataKey(id), obj); err != nil {
		return "", err
	}
	s.fireEdit(ctx, obj.ClassID, id, values)
	return id, nil
}

// GetObject loads an Object together with its current field values.
// Field values are read from their leaf keys, which is where the most
// recent CRDT-merged version lives.
func (s *Store) GetObject(ctx context.Context, id ID) (*Object, error) {
	var o Object
	if err := s.getJSON(ctx, dataKey(id), &o); err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return nil, ErrObjectUnknown
		}
		return nil, err
	}
	if o.Fields == nil {
		o.Fields = map[string]string{}
	}
	for name := range o.Fields {
		v, err := s.getString(ctx, fieldKey(id, name))
		if err == nil {
			o.Fields[name] = v
		}
	}
	return &o, nil
}

// SetName creates or updates a {term -> id} mapping, mirroring chotki's
// `name` REPL command and PUT /name HTTP endpoint.
func (s *Store) SetName(ctx context.Context, term string, id ID) error {
	if term == "" {
		return fmt.Errorf("%w: empty term", ErrInvalidPayload)
	}
	return s.putString(ctx, nameKey(term), string(id))
}

// LookupName resolves a term to its id (or returns ErrNameUnknown).
func (s *Store) LookupName(ctx context.Context, term string) (ID, error) {
	v, err := s.getString(ctx, nameKey(term))
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return "", ErrNameUnknown
		}
		return "", err
	}
	return ID(v), nil
}

// ListNames returns a snapshot of all {term -> id} mappings.
func (s *Store) ListNames(ctx context.Context) (map[string]ID, error) {
	prefix := keySep + nameKeyPrefix + keySep
	res, err := s.ds.Query(ctx, query.Query{Prefix: prefix})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	out := map[string]ID{}
	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		out[strings.TrimPrefix(r.Key, prefix)] = ID(string(r.Value))
	}
	return out, nil
}

// Children iterates direct children of the given parent id (one
// hierarchy level only). Useful for "list all objects of class X" or
// "list all fields of object Y" queries.
func (s *Store) Children(ctx context.Context, parent ID) ([]ID, error) {
	prefix := keySep + string(parent) + keySep
	if parent == "" {
		prefix = keySep
	}
	res, err := s.ds.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	seen := map[string]struct{}{}
	out := []ID{}
	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		rel := strings.TrimPrefix(r.Key, prefix)
		// keep only the first segment, ignore deeper descendants
		if i := strings.Index(rel, keySep); i >= 0 {
			rel = rel[:i]
		}
		if rel == "" || strings.HasPrefix(rel, ".") {
			continue
		}
		full := rel
		if parent != "" {
			full = string(parent) + keySep + rel
		}
		if _, ok := seen[full]; ok {
			continue
		}
		seen[full] = struct{}{}
		out = append(out, ID(full))
	}
	return out, nil
}

// Close releases the underlying datastore. Safe to call multiple times.
func (s *Store) Close() error {
	if s == nil || s.ds == nil {
		return nil
	}
	return s.ds.Close()
}

// ---- key construction --------------------------------------------------

// dataKey returns the badger key that stores the JSON descriptor for a
// class or object id. The key is literally "/<id>".
func dataKey(id ID) ds.Key {
	return ds.NewKey(keySep + string(id))
}

// fieldKey returns the badger key for one field of one object. The key
// is literally "/<objectID>/<fieldName>" — exactly the
// "parentID/childID" composition the task asks for.
func fieldKey(objectID ID, name string) ds.Key {
	return ds.NewKey(keySep + string(objectID) + keySep + name)
}

// nameKey is "/.name/<term>".  The leading dot keeps it out of the
// regular id namespace while still being a valid badger key.
func nameKey(term string) ds.Key {
	return ds.NewKey(keySep + nameKeyPrefix + keySep + term)
}

// ---- helpers -----------------------------------------------------------

func (s *Store) putString(ctx context.Context, k ds.Key, v string) error {
	return s.ds.Put(ctx, k, []byte(v))
}

func (s *Store) getString(ctx context.Context, k ds.Key) (string, error) {
	b, err := s.ds.Get(ctx, k)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *Store) putJSON(ctx context.Context, k ds.Key, v any) error {
	bt, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.ds.Put(ctx, k, bt)
}

func (s *Store) getJSON(ctx context.Context, k ds.Key, v any) error {
	bt, err := s.ds.Get(ctx, k)
	if err != nil {
		return err
	}
	return json.Unmarshal(bt, v)
}

func defaultForKind(k FieldKind) string {
	switch k {
	case FieldFloat:
		return "0.0"
	case FieldInteger, FieldNCounter, FieldZCounter:
		return "0"
	case FieldString, FieldTerm:
		return ""
	case FieldRef:
		return ""
	case FieldEulerian, FieldLinear:
		return "[]"
	case FieldMapping:
		return "{}"
	default:
		return ""
	}
}

func classHasField(c *Class, name string) bool {
	for _, f := range c.Fields {
		if f.Name == name {
			return true
		}
	}
	return false
}

// stableHash returns a deterministic n-hex-char prefix derived from s.
// Unlike the previous implementation, this does NOT depend on wall-clock
// time, so the same source always produces the same prefix.
func stableHash(s string, n int) string {
	sum := sha256.Sum256([]byte(s))
	out := hex.EncodeToString(sum[:])
	if n <= 0 || n > len(out) {
		return out
	}
	return out[:n]
}
