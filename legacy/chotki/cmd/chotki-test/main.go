//go:build ignore

// Command chotki-test runs a single chotki replica with a pre-created
// class for the blurry/chotki bridge smoke test. Bypasses the chotki
// REPL's class-creation bug (which loses the index byte and corrupts
// field names) by invoking chotki.NewClass directly.
//
// Usage:
//
//	chotki-test --dir /tmp/cho1 --src 0xb0b --listen "tcp://0.0.0.0:9999"
//
// On startup the program:
//  1. Opens (or creates) a chotki replica at --dir.
//  2. Ensures a class named "ChotkiMirror" with one String field "Name"
//     exists, printing its rdx.ID to stdout.
//  3. Starts listening for chotki replication peers at --listen.
//  4. Blocks until SIGINT/SIGTERM.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

func main() {
	dir := flag.String("dir", "/tmp/cho1", "chotki data directory")
	src := flag.Uint64("src", 0xb0b, "chotki replica Source id")
	listen := flag.String("listen", "tcp://0.0.0.0:9999", "chotki replication listen address")
	httpPort := flag.Int("http", 8090, "test HTTP port (POST /new with body \"FromChotki\" creates object)")
	flag.Parse()

	if err := run(*dir, *src, *listen, *httpPort); err != nil {
		fmt.Fprintln(os.Stderr, "chotki-test:", err)
		os.Exit(1)
	}
}

func run(dir string, src uint64, listen string, httpPort int) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	abs, _ := filepath.Abs(dir)

	cho, err := chotki.Open(abs, chotki.Options{
		Src:     src,
		Name:    "chotki-test",
		Options: pebble.Options{},
	})
	if err != nil {
		return fmt.Errorf("open chotki: %w", err)
	}
	defer cho.Close()

	classID, err := ensureClass(cho)
	if err != nil {
		return err
	}
	fmt.Printf("CHOTKI_CLASS_ID=%s\n", classID.String())

	if err := cho.Listen(listen); err != nil {
		return fmt.Errorf("listen %s: %w", listen, err)
	}
	fmt.Printf("CHOTKI_LISTEN=%s\n", listen)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Tiny HTTP shim:
	//   POST /new   body: "the name to set"  → creates a new object
	//   GET  /list                            → dumps all objects
	mux := http.NewServeMux()
	mux.HandleFunc("/new", func(w http.ResponseWriter, r *http.Request) {
		bt, _ := io.ReadAll(r.Body)
		name := strings.TrimSpace(string(bt))
		if name == "" {
			http.Error(w, "empty name", 400)
			return
		}
		body := protocol.Records{
			protocol.Record(rdx.String, rdx.Stlv(name)),
		}
		oid, err := cho.NewObjectTLV(r.Context(), classID, body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Fprintln(w, oid.String())
	})
	mux.HandleFunc("/list", func(w http.ResponseWriter, r *http.Request) {
		cho.DumpObjects(w)
	})
	srv := &http.Server{
		Addr: fmt.Sprintf("127.0.0.1:%d", httpPort),
		Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go srv.ListenAndServe()
	defer srv.Close()
	fmt.Printf("CHOTKI_HTTP=%s\n", srv.Addr)

	fmt.Println("READY")
	<-ctx.Done()
	return nil
}

// Schema is the canonical bridge mirror class: one String field "Name".
var Schema = []classes.Field{
	{Offset: 1, Name: "Name", RdxType: rdx.String},
}

// ensureClass creates the bridge mirror class once. The chotki replica
// is deterministic per (Src, sequence), so on first boot the class ends
// up at <Src>-1, which is what we want. On reopen we look it up by
// scanning for the first 'C' record under the configured Src.
func ensureClass(cho *chotki.Chotki) (rdx.ID, error) {
	expected := rdx.IDFromSrcSeqOff(cho.Source(), 1, 0)
	if _, err := cho.GetClassTLV(context.Background(), expected); err == nil {
		return expected, nil
	}
	id, err := cho.NewClass(context.Background(), rdx.ID0, Schema...)
	if err != nil {
		return rdx.BadId, fmt.Errorf("new class: %w", err)
	}
	return id, nil
}
