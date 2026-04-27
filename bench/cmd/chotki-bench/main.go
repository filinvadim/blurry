// chotki-bench is a thin HTTP front-end for a Chotki replica that mirrors
// the subset of endpoints used by the bench harness:
//
//	POST /class  - body: RDX mapping  → create a class
//	POST /new    - body: RDX mapping  → create an object
//	GET  /cat    - ?id=<rdx-id>       → object as text
//	GET  /healthz                     → 200 OK
//
// Configuration is read from environment variables. The replica listens
// for sync on CHOTKI_LISTEN, dials each peer in CHOTKI_PEERS (comma
// separated, "host:port"), and serves the HTTP API on CHOTKI_HTTP_LISTEN
// bound to all interfaces.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
	srcStr := getenv("CHOTKI_SRC", "1")
	src, err := strconv.ParseUint(srcStr, 16, 32)
	if err != nil {
		log.Fatalf("CHOTKI_SRC %q must be hex: %v", srcStr, err)
	}
	name := getenv("CHOTKI_NAME", "chotki-"+srcStr)
	dataDir := getenv("CHOTKI_DATA_DIR", "/data")
	syncAddr := getenv("CHOTKI_LISTEN", "0.0.0.0:4001")
	httpAddr := getenv("CHOTKI_HTTP_LISTEN", "0.0.0.0:8001")
	peers := splitCSV(os.Getenv("CHOTKI_PEERS"))

	cho, err := chotki.Open(dataDir, chotki.Options{
		Src:     src,
		Name:    name,
		Options: pebble.Options{},
	})
	if err != nil {
		log.Fatalf("chotki.Open(%s): %v", dataDir, err)
	}
	defer cho.Close()

	if err := cho.Listen(syncAddr); err != nil {
		log.Fatalf("chotki.Listen(%s): %v", syncAddr, err)
	}
	for _, p := range peers {
		if p == "" {
			continue
		}
		if err := cho.Connect(p); err != nil {
			log.Printf("chotki.Connect(%s): %v (will be retried)", p, err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/class", handleClass(cho))
	mux.HandleFunc("/new", handleNew(cho))
	mux.HandleFunc("/cat", handleCat(cho))
	mux.HandleFunc("/list", handleCat(cho))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{Addr: httpAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("chotki-bench: src=%x name=%s sync=%s http=%s peers=%v",
			src, name, syncAddr, httpAddr, peers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func handleClass(cho *chotki.Chotki) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		a, err := rdx.ParseRDX(body)
		if err != nil || a == nil {
			http.Error(w, fmt.Sprintf("parse: %v", err), http.StatusUnprocessableEntity)
			return
		}
		if a.RdxType != rdx.Mapping {
			http.Error(w, "expected mapping", http.StatusUnprocessableEntity)
			return
		}

		var parent rdx.ID
		var fields []classes.Field
		offset := int64(0)
		for i := 0; i+1 < len(a.Nested); i += 2 {
			k := a.Nested[i]
			v := a.Nested[i+1]
			if string(k.Text) == "_ref" {
				if v.RdxType == rdx.Reference {
					parent = rdx.IDFromText(v.Text)
				}
				continue
			}
			if v.RdxType != rdx.Term || len(v.Text) != 1 {
				http.Error(w, fmt.Sprintf("field %q must have a single-letter RDT", k.Text), http.StatusUnprocessableEntity)
				return
			}
			offset++
			fields = append(fields, classes.Field{
				Offset:  offset,
				Name:    string(k.Text),
				RdxType: v.Text[0],
			})
		}

		id, err := cho.NewClass(r.Context(), parent, fields...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(id.String()))
	}
}

func handleNew(cho *chotki.Chotki) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		a, err := rdx.ParseRDX(body)
		if err != nil || a == nil {
			http.Error(w, fmt.Sprintf("parse: %v", err), http.StatusUnprocessableEntity)
			return
		}
		if a.RdxType != rdx.Mapping {
			http.Error(w, "expected mapping", http.StatusUnprocessableEntity)
			return
		}

		pairs := a.Nested
		var tid rdx.ID
		if len(pairs) >= 2 && string(pairs[0].Text) == "_ref" {
			if pairs[1].RdxType != rdx.Reference {
				http.Error(w, "_ref must be a reference", http.StatusUnprocessableEntity)
				return
			}
			tid = rdx.IDFromText(pairs[1].Text)
			pairs = pairs[2:]
		}
		if tid == rdx.ID0 {
			http.Error(w, "missing _ref", http.StatusUnprocessableEntity)
			return
		}

		flds, err := cho.ClassFields(tid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tmp := make(protocol.Records, len(flds))
		for i := 0; i+1 < len(pairs); i += 2 {
			if pairs[i].RdxType != rdx.Term {
				http.Error(w, "field name must be a term", http.StatusUnprocessableEntity)
				return
			}
			name := string(pairs[i].Text)
			ndx := flds.FindName(name)
			if ndx == -1 {
				http.Error(w, "unknown field "+name, http.StatusUnprocessableEntity)
				return
			}
			val := &pairs[i+1]
			fieldType := flds[ndx].RdxType
			if val.RdxType != fieldType {
				if val.RdxType == rdx.Integer && (fieldType == rdx.Natural || fieldType == rdx.ZCounter) {
					val.RdxType = fieldType
				} else {
					http.Error(w, fmt.Sprintf("wrong type for %s", name), http.StatusUnprocessableEntity)
					return
				}
			}
			tmp[ndx] = rdx.FIRSTrdx2tlv(val)
		}

		var tlvs protocol.Records
		for i := 1; i < len(flds); i++ {
			rdt := flds[i].RdxType
			if tmp[i] == nil {
				tlvs = append(tlvs, protocol.Record(rdt, rdx.Xdefault(rdt)))
			} else {
				tlvs = append(tlvs, protocol.Record(rdt, tmp[i]))
			}
		}

		id, err := cho.CommitPacket(r.Context(), 'O', tid, tlvs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(id.String()))
	}
}

func handleCat(cho *chotki.Chotki) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		idStr := strings.TrimSpace(r.URL.Query().Get("id"))
		if idStr == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		oid := rdx.IDFromText([]byte(idStr))
		s, err := cho.ObjectString(oid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(s))
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
