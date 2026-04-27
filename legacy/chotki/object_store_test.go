//go:build ignore

package chotki_legacy

import (
	"context"
	"strings"
	"testing"

	ds "github.com/ipfs/go-datastore"
)

// Snapshot of the chotki-flavoured Store tests that ran in the live
// blurry package before the KV refactor. Compiled only with the
// chotki_legacy build tag.

func TestStoreHierarchicalKeys(t *testing.T) {
	store := NewObjectStorage(ds.NewMapDatastore(), "node-a")
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

	if _, err := store.EditObject(ctx, objID, map[string]string{"Score": "11"}); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetObject(ctx, objID)
	if got.Fields["Score"] != "11" {
		t.Fatalf("edit not visible: %+v", got.Fields)
	}

	if _, err := store.CreateObject(ctx, classID, map[string]string{"Bogus": "x"}); err == nil {
		t.Fatal("expected unknown-field error on create")
	}
	if _, err := store.EditObject(ctx, objID, map[string]string{"Bogus": "x"}); err == nil {
		t.Fatal("expected unknown-field error on edit")
	}

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
	a := NewObjectStorage(ds.NewMapDatastore(), "node-a")
	b := NewObjectStorage(ds.NewMapDatastore(), "node-a")
	if a.source != b.source {
		t.Fatalf("source prefix unstable: %q vs %q", a.source, b.source)
	}
	c := NewObjectStorage(ds.NewMapDatastore(), "node-b")
	if a.source == c.source {
		t.Fatal("different sources collided")
	}
}
