package blurry

import (
	"context"
	"errors"
	"strings"
	"testing"

	ds "github.com/ipfs/go-datastore"
)

func TestResolvePeerAddr(t *testing.T) {
	cases := []struct {
		name        string
		in          string
		defaultPort int
		wantErr     error
		wantAddr    string
		hasPeerID   bool
	}{
		{
			name:        "host_port",
			in:          "10.0.0.1:4001",
			defaultPort: 0,
			wantAddr:    "/ip4/10.0.0.1/tcp/4001",
		},
		{
			name:        "bare_ip_uses_default_port",
			in:          "10.0.0.2",
			defaultPort: 4001,
			wantAddr:    "/ip4/10.0.0.2/tcp/4001",
		},
		{
			name:     "multiaddr_with_peer_id",
			in:       "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw",
			wantAddr: "/ip4/127.0.0.1/tcp/4001",
			hasPeerID: true,
		},
		{
			name:    "multiaddr_no_peer_id_signals_err",
			in:      "/ip4/127.0.0.1/tcp/4001",
			wantErr: ErrNoPeerID,
		},
		{
			name:    "empty",
			in:      "",
			wantErr: errors.New("multiaddr: empty address"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info, err := ResolvePeerAddr(tc.in, tc.defaultPort)
			if tc.wantErr != nil {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !errors.Is(err, tc.wantErr) && !strings.Contains(err.Error(), tc.wantErr.Error()) {
					t.Fatalf("error mismatch: got %v want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(info.Addrs) == 0 {
				t.Fatalf("no addresses parsed")
			}
			if !strings.HasPrefix(info.Addrs[0].String(), tc.wantAddr) {
				t.Fatalf("addr mismatch: got %s want prefix %s", info.Addrs[0], tc.wantAddr)
			}
			if tc.hasPeerID && info.ID == "" {
				t.Fatalf("expected non-empty peer ID")
			}
		})
	}
}

func TestSettingsSrcFromName(t *testing.T) {
	a := &Settings{Name: "node-a"}
	b := &Settings{Name: "node-a"}
	c := &Settings{Name: "node-b"}
	if a.Src() == 0 {
		t.Fatal("non-empty name must never produce zero Src")
	}
	if a.Src() != b.Src() {
		t.Fatal("identical names → different Src")
	}
	if a.Src() == c.Src() {
		t.Fatal("different names collided")
	}
	if (&Settings{}).Src() != 0 {
		t.Fatal("empty name must yield 0")
	}
	if a.Src() > MaxSrc {
		t.Fatalf("Src overflowed 32-bit space: %x", a.Src())
	}
}

func TestSettingsSetDefaultsLeavesTogglesAlone(t *testing.T) {
	s := &Settings{} // all toggles false
	s.SetDefaults()
	if s.EnableDHT || s.EnableRelay || s.EnableNATService {
		t.Fatal("SetDefaults must not flip user-controlled booleans")
	}
	if s.PingPeriod == 0 || s.MaxSyncDuration == 0 || s.RebroadcastInterval == 0 {
		t.Fatal("SetDefaults must fill duration zero values")
	}
}

func TestSettingsValidateRejectsInverseConnLimits(t *testing.T) {
	s := DefaultSettings()
	s.ConnLowWater = 100
	s.ConnHighWater = 50
	if err := s.Validate(); err == nil {
		t.Fatal("expected error when high < low")
	}
}

func TestStoreHierarchicalKeys(t *testing.T) {
	store := NewStore(ds.NewMapDatastore(), "node-a")
	ctx := context.Background()

	classID, err := store.CreateClass(ctx, "", "Student", []FieldSpec{
		{Name: "Name", Kind: FieldString},
		{Name: "Score", Kind: FieldNCounter},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(classID), "/") {
		t.Fatalf("top-level class id should be one segment, got %q", classID)
	}

	objID, err := store.CreateObject(ctx, classID, map[string]string{
		"Name":  "Ivan",
		"Score": "10",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Object id must be literally "<classID>/<objectID>".
	if !strings.HasPrefix(string(objID), string(classID)+"/") {
		t.Fatalf("object id %q is not nested under class %q", objID, classID)
	}
	if strings.Count(string(objID), "/") != 1 {
		t.Fatalf("object id should be exactly two segments, got %q", objID)
	}

	got, err := store.GetObject(ctx, objID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Fields["Name"] != "Ivan" || got.Fields["Score"] != "10" {
		t.Fatalf("round-trip mismatch: %+v", got.Fields)
	}

	// Edits land in the same hierarchical leaf key.
	if _, err := store.EditObject(ctx, objID, map[string]string{"Score": "11"}); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetObject(ctx, objID)
	if got.Fields["Score"] != "11" {
		t.Fatalf("edit not visible: %+v", got.Fields)
	}

	// Unknown field on create rejected.
	if _, err := store.CreateObject(ctx, classID, map[string]string{"Bogus": "x"}); err == nil {
		t.Fatal("expected unknown-field error on create")
	}
	// Unknown field on edit rejected.
	if _, err := store.EditObject(ctx, objID, map[string]string{"Bogus": "x"}); err == nil {
		t.Fatal("expected unknown-field error on edit")
	}

	// Names round-trip.
	if err := store.SetName(ctx, "stu", classID); err != nil {
		t.Fatal(err)
	}
	if got, err := store.LookupName(ctx, "stu"); err != nil || got != classID {
		t.Fatalf("name lookup got %q err %v", got, err)
	}
	names, err := store.ListNames(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if names["stu"] != classID || names["Student"] != classID {
		t.Fatalf("names: %+v", names)
	}
}

func TestStoreIDDeterminismPerSource(t *testing.T) {
	a := NewStore(ds.NewMapDatastore(), "node-a")
	b := NewStore(ds.NewMapDatastore(), "node-a")
	// Same source string → same hex prefix.
	if a.source != b.source {
		t.Fatalf("source prefix unstable: %q vs %q", a.source, b.source)
	}
	c := NewStore(ds.NewMapDatastore(), "node-b")
	if a.source == c.source {
		t.Fatal("different sources collided")
	}
}

func TestSettingsIdentityKeyDeterminism(t *testing.T) {
	s1 := &Settings{Name: "same"}
	s2 := &Settings{Name: "same"}
	k1, err := s1.IdentityKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, err := s2.IdentityKey()
	if err != nil {
		t.Fatal(err)
	}
	if !k1.Equals(k2) {
		t.Fatalf("identical names produced different keys")
	}

	s3 := &Settings{Name: "different"}
	k3, err := s3.IdentityKey()
	if err != nil {
		t.Fatal(err)
	}
	if k1.Equals(k3) {
		t.Fatalf("different names produced identical keys")
	}
}
