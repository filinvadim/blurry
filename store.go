package blurry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	json "github.com/json-iterator/go"
)

var storeLog = logging.Logger("store")

// ID is a Chotki-compatible string identifier for classes, objects and
// fields. Internally it is just a hex-encoded byte sequence that preserves
// hierarchical "parent/child/field" composition when joined with '/'.
type ID string

const (
	// idRoot is the synthetic parent of top-level classes/objects.
	idRoot ID = "0"

	// keySep separates components in a badger hierarchical key.
	keySep = "/"

	// keyClassPrefix groups all class definitions under one subtree.
	keyClassPrefix = "class"

	// keyObjectPrefix groups all object instances under one subtree.
	keyObjectPrefix = "object"

	// keyNamePrefix groups Chotki-compatible {name -> id} mappings.
	keyNamePrefix = "name"

	// keyFieldPrefix is appended after an object id to store fields.
	keyFieldPrefix = "field"
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

// FieldKind describes a Chotki-compatible RDT family. The single-byte
// representation matches Chotki's RDX type codes (F/I/R/S/T/E/L/M/N/Z).
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
	ID     ID                `json:"id"`
	ClassID ID               `json:"class"`
	Fields map[string]string `json:"fields"`
}

// Store is the Chotki-compatible CRDT-backed key/value store.
//
// All keys use the hierarchical form "<prefix>/<parentID>/<childID>/...",
// stored in badger via the go-datastore facade. The CRDT layer
// guarantees causal consistency across replicas.
type Store struct {
	mu      sync.Mutex
	ds      ds.Datastore
	source  string // hex peer-id prefix, used to mint new ids
	counter uint64
}

// NewStore wires a Store on top of an existing CRDT-backed datastore.
// The source string is mixed into newly minted ids, keeping them unique
// per-replica without coordination.
func NewStore(d ds.Datastore, source string) *Store {
	if source == "" {
		source = "anon"
	}
	return &Store{ds: d, source: shortHash(source, 6)}
}

// MintID returns a fresh hierarchical id, prefixed by parent. Ids are
// monotonically increasing per replica; combined with the source prefix
// they are globally unique.
func (s *Store) MintID(parent ID) ID {
	s.mu.Lock()
	s.counter++
	n := s.counter
	s.mu.Unlock()
	if parent == "" {
		parent = idRoot
	}
	return ID(string(parent) + keySep + s.source + "-" + hex.EncodeToString(uint64Bytes(n)))
}

// CreateClass defines a new class under parent (use empty/idRoot for top
// level) with the given fields. Returns the freshly minted class id.
func (s *Store) CreateClass(ctx context.Context, parent ID, name string, fields []FieldSpec) (ID, error) {
	for _, f := range fields {
		if f.Name == "" || f.Kind == 0 {
			return "", fmt.Errorf("%w: bad field %+v", ErrInvalidPayload, f)
		}
	}
	id := s.MintID(parent)
	c := Class{ID: id, Parent: parent, Name: name, Fields: fields}
	if err := s.putJSON(ctx, classKey(id), c); err != nil {
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
	if err := s.getJSON(ctx, classKey(id), &c); err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return nil, ErrClassUnknown
		}
		return nil, err
	}
	return &c, nil
}

// CreateObject creates an Object of the given class. Field values not
// present in `values` keep their RDT zero value.
func (s *Store) CreateObject(ctx context.Context, classID ID, values map[string]string) (ID, error) {
	c, err := s.GetClass(ctx, classID)
	if err != nil {
		return "", err
	}

	obj := Object{
		ID:      s.MintID(classID),
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
	for k := range values {
		if _, ok := obj.Fields[k]; !ok {
			return "", fmt.Errorf("%w: unknown field %q", ErrFieldUnknown, k)
		}
	}

	if err := s.putJSON(ctx, objectKey(obj.ID), obj); err != nil {
		return "", err
	}
	for name, val := range obj.Fields {
		if err := s.putString(ctx, fieldKey(obj.ID, name), val); err != nil {
			return "", err
		}
	}
	return obj.ID, nil
}

// EditObject merges the provided field values into the object atomically.
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
	if err := s.putJSON(ctx, objectKey(id), obj); err != nil {
		return "", err
	}
	return id, nil
}

// GetObject loads an Object together with its current field values.
func (s *Store) GetObject(ctx context.Context, id ID) (*Object, error) {
	var o Object
	if err := s.getJSON(ctx, objectKey(id), &o); err != nil {
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

// SetName creates or updates a {term -> id} mapping, just like Chotki's
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
	res, err := s.ds.Query(ctx, query.Query{Prefix: keySep + keyNamePrefix})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	out := map[string]ID{}
	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		key := strings.TrimPrefix(r.Key, keySep+keyNamePrefix+keySep)
		out[key] = ID(string(r.Value))
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

// classKey returns the badger key for a class definition.
//
//	/class/<classID>
func classKey(id ID) ds.Key {
	return ds.NewKey(keySep + keyClassPrefix + keySep + string(id))
}

// objectKey returns the badger key for an object descriptor (header).
//
//	/object/<objectID>
//
// where objectID itself contains its parent class id thanks to MintID
// (the data hierarchy is therefore "classID/objectID" implicitly).
func objectKey(id ID) ds.Key {
	return ds.NewKey(keySep + keyObjectPrefix + keySep + string(id))
}

// fieldKey returns the badger key for one field of one object:
//
//	/object/<objectID>/field/<name>
func fieldKey(objectID ID, name string) ds.Key {
	return ds.NewKey(
		keySep + keyObjectPrefix + keySep + string(objectID) +
			keySep + keyFieldPrefix + keySep + name,
	)
}

func nameKey(term string) ds.Key {
	return ds.NewKey(keySep + keyNamePrefix + keySep + term)
}

// ---- low-level helpers -------------------------------------------------

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
	case FieldString:
		return ""
	case FieldTerm:
		return ""
	case FieldRef:
		return string(idRoot)
	case FieldEulerian, FieldLinear:
		return "[]"
	case FieldMapping:
		return "{}"
	default:
		return ""
	}
}

func shortHash(s string, n int) string {
	sum := sha256.Sum256([]byte(s + ":" + time.Now().Format(time.RFC3339Nano)))
	out := hex.EncodeToString(sum[:])
	if n <= 0 || n > len(out) {
		return out
	}
	return out[:n]
}

func uint64Bytes(n uint64) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(n & 0xff)
		n >>= 8
	}
	return b
}
