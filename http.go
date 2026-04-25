package blurry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var httpLog = logging.Logger("http")

// HTTPServer exposes a Chotki-compatible REST API on top of a Blurry
// instance. The endpoints mirror chotki/repl/handlers.go so the same
// clients can target either backend interchangeably:
//
//	POST /listen     - body: "host:port"      → start listening
//	POST /connect    - body: "host:port"      → connect to a peer
//	POST /class      - body: {Name:S,...}     → create a class
//	PUT  /name       - body: {Term: ID}       → register a {name → id}
//	POST /new        - body: {_ref:Class,...} → create an object
//	PUT  /edit       - body: {_id:ID,...}     → edit an object
//	GET  /cat?id=ID                            → object as text
//	GET  /list?id=ID                           → object expanded
//
// All bodies are accepted as plain text in the loose RDX-style mapping
// format that Chotki's /class etc. handlers expect.
type HTTPServer struct {
	b      *Blurry
	srv    *http.Server
	listen string
}

// NewHTTPServer wires the handlers to mux but does not start a listener.
// Use Start to bind to an address.
func NewHTTPServer(b *Blurry) *HTTPServer {
	return &HTTPServer{b: b}
}

// Start binds and serves the API on addr (e.g. "127.0.0.1:8001"). When
// Settings.TlsConfig is non-nil the server is upgraded to HTTPS using
// the certificates in that config.
//
// Returns immediately; serving runs in a goroutine. Use Close() to stop.
func (h *HTTPServer) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/listen", withCORS(h.handleListen))
	mux.HandleFunc("/connect", withCORS(h.handleConnect))
	mux.HandleFunc("/class", withCORS(h.handleClass))
	mux.HandleFunc("/name", withCORS(h.handleName))
	mux.HandleFunc("/new", withCORS(h.handleNew))
	mux.HandleFunc("/edit", withCORS(h.handleEdit))
	mux.HandleFunc("/cat", withCORS(h.handleCat))
	mux.HandleFunc("/list", withCORS(h.handleList))

	h.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig:         h.b.settings.TlsConfig,
	}
	h.listen = addr
	useTLS := h.srv.TLSConfig != nil && len(h.srv.TLSConfig.Certificates) > 0
	go func() {
		var err error
		if useTLS {
			// Empty cert/key files because the certificates are already
			// loaded into TLSConfig by the caller (cmd/blurry).
			err = h.srv.ListenAndServeTLS("", "")
		} else {
			err = h.srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpLog.Errorf("http: serve %s: %v", addr, err)
		}
	}()
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	httpLog.Infof("http: serving on %s://%s", scheme, addr)
	return nil
}

// Close stops the HTTP server.
func (h *HTTPServer) Close() error {
	if h == nil || h.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return h.srv.Shutdown(ctx)
}

func withCORS(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Max-Age", "86400")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,OPTIONS")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		f(w, r)
	}
}

// ---- handlers ----------------------------------------------------------

func (h *HTTPServer) handleListen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.b.Listen(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.b.Connect(r.Context(), body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPServer) handleClass(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	parent, name, fields, err := parseClassBody(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	parentID, err := h.b.resolveRef(r.Context(), parent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := h.b.Store().CreateClass(r.Context(), parentID, name, fields)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(id))
}

func (h *HTTPServer) handleName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pairs, err := parseMapping(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	for term, ref := range pairs {
		id, err := h.b.resolveRef(r.Context(), ref)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := h.b.Store().SetName(r.Context(), term, id); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPServer) handleNew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ref, values, err := parseRefAndValues(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	classID, err := h.b.resolveRef(r.Context(), ref)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := h.b.Store().CreateObject(r.Context(), classID, values)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(id))
}

func (h *HTTPServer) handleEdit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, values, err := parseIDAndValues(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	objID, err := h.b.resolveRef(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	out, err := h.b.Store().EditObject(r.Context(), objID, values)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(out))
}

func (h *HTTPServer) handleCat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	idStr := r.URL.Query().Get("id")
	id, err := h.b.resolveRef(r.Context(), idStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	o, err := h.b.Store().GetObject(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(formatObject(o)))
}

func (h *HTTPServer) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	idStr := r.URL.Query().Get("id")
	id, err := h.b.resolveRef(r.Context(), idStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	o, err := h.b.Store().GetObject(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	bt, _ := json.MarshalIndent(o, "", "  ")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bt)
}

// ---- helpers -----------------------------------------------------------

func readText(r *http.Request) (string, error) {
	defer r.Body.Close()
	bt, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MiB cap
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bt)), nil
}

// parseMapping parses Chotki-flavoured "{Key:Val, Key2:Val2}" into a
// flat string→string map. Whitespace and quotes around values are
// stripped. Reserved keys (those starting with '_') are excluded.
func parseMapping(s string) (map[string]string, error) {
	body, err := stripBraces(s)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, kv := range splitTopLevel(body, ',') {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		i := strings.IndexByte(kv, ':')
		if i < 0 {
			return nil, fmt.Errorf("bad pair %q", kv)
		}
		k := strings.TrimSpace(kv[:i])
		v := unquote(strings.TrimSpace(kv[i+1:]))
		if k == "" {
			return nil, fmt.Errorf("empty key in %q", kv)
		}
		out[k] = v
	}
	return out, nil
}

// parseClassBody parses "{_ref: Parent, Name: S, Score: N}" → parent, name, fields.
func parseClassBody(s string) (parent string, name string, fields []FieldSpec, err error) {
	m, err := parseMapping(s)
	if err != nil {
		return "", "", nil, err
	}
	parent = m["_ref"]
	name = m["Name"]

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if strings.HasPrefix(k, "_") {
			continue
		}
		v := m[k]
		if k == "Name" && (v == "S" || v == "") {
			// Treat the special "Name:S" pair as both class name + first field.
			fields = append(fields, FieldSpec{Name: k, Kind: FieldString})
			continue
		}
		if len(v) != 1 {
			return "", "", nil, fmt.Errorf("field %q must have a single-letter RDT, got %q", k, v)
		}
		fields = append(fields, FieldSpec{Name: k, Kind: FieldKind(v[0])})
	}
	return
}

// parseRefAndValues parses {_ref:X, A:1, B:"foo"} → ref, {A:"1", B:"foo"}.
func parseRefAndValues(s string) (ref string, values map[string]string, err error) {
	m, err := parseMapping(s)
	if err != nil {
		return "", nil, err
	}
	ref = m["_ref"]
	if ref == "" {
		return "", nil, fmt.Errorf("missing _ref")
	}
	values = map[string]string{}
	for k, v := range m {
		if strings.HasPrefix(k, "_") {
			continue
		}
		values[k] = v
	}
	return
}

// parseIDAndValues parses {_id:X, A:1} → id, {A:"1"}.
func parseIDAndValues(s string) (id string, values map[string]string, err error) {
	m, err := parseMapping(s)
	if err != nil {
		return "", nil, err
	}
	id = m["_id"]
	if id == "" {
		// Chotki's edit also accepts _ref as alias.
		id = m["_ref"]
	}
	if id == "" {
		return "", nil, fmt.Errorf("missing _id")
	}
	values = map[string]string{}
	for k, v := range m {
		if strings.HasPrefix(k, "_") {
			continue
		}
		values[k] = v
	}
	return
}

func stripBraces(s string) (string, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return "", fmt.Errorf("expected mapping {...}, got %q", s)
	}
	return s[1 : len(s)-1], nil
}

// splitTopLevel splits by `sep` while respecting nested {} [] and quotes.
func splitTopLevel(s string, sep byte) []string {
	var out []string
	depth := 0
	inStr := false
	last := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' && (i == 0 || s[i-1] != '\\'):
			inStr = !inStr
		case !inStr && (c == '{' || c == '['):
			depth++
		case !inStr && (c == '}' || c == ']'):
			depth--
		case !inStr && c == sep && depth == 0:
			out = append(out, s[last:i])
			last = i + 1
		}
	}
	out = append(out, s[last:])
	return out
}

func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func formatObject(o *Object) string {
	keys := make([]string, 0, len(o.Fields))
	for k := range o.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(o.Fields[k])
	}
	b.WriteByte('}')
	return b.String()
}
