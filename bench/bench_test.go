// Bench harness: writes a stream of sequentially-numbered values
// (each carrying a fixed-size payload) to the first replica of a
// stack, reads them back from the third replica, and reports the
// wall-clock time the cluster needed to process the data.
//
// Two stacks are exercised back-to-back. They speak different APIs
// because Blurry is now a pure key/value store while Chotki keeps
// its class/object/field surface; per-stack hooks on stackCfg take
// care of the difference. The replication mechanics under each API
// are still directly comparable — sequential writes on replica 1,
// poll reads on replica 3, fixed payload size.
//
// Default load is light enough to run on a laptop (1000 × 64 KiB ≈
// 64 MiB end-to-end). Bump it via env vars when you want to push
// harder:
//
//	BENCH_REQUESTS=5000 \
//	BENCH_PAYLOAD_SIZE=131072 \
//	BENCH_PER_READ_TIMEOUT=2m \
//	  go test -timeout=2h -run TestBench .
//
// Usage (the bench/ directory is its own Go module, so cd into it):
//
//	cd bench
//	go test -timeout=2h -run TestBench .
//
// The test is skipped when `docker` is not on $PATH or when run with
// `-short`. SIGINT/SIGTERM trigger a best-effort `docker compose
// down -v` for both stacks before exit.
package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Bench load knobs.
const (
	defaultNumRequests     = 1000
	defaultPayloadSize     = 64 * 1024
	defaultPerReadDeadline = 60 * time.Second
)

const (
	composeUpWait  = 5 * time.Minute
	httpReqTimeout = 2 * time.Minute
	pollInterval   = 50 * time.Millisecond
)

var (
	numRequests     = envInt("BENCH_REQUESTS", defaultNumRequests)
	payloadSize     = envInt("BENCH_PAYLOAD_SIZE", defaultPayloadSize)
	perReadDeadline = envDuration("BENCH_PER_READ_TIMEOUT", defaultPerReadDeadline)
)

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

type stackCfg struct {
	name     string
	project  string
	compose  string
	writeURL string
	readURL  string

	// healthPath probes "is HTTP up". Stack-specific: blurry exposes
	// /v1/health, chotki only has /cat.
	healthPath string

	// setup is called once per run on the write replica before any
	// writes. It can return an opaque "schema id" the writer needs
	// (Chotki returns its class id, Blurry returns "").
	setup func(ctx context.Context, base string) (string, error)

	// write performs one application-level write. Returns the id the
	// reader should query to retrieve the value.
	write func(ctx context.Context, base, schemaID string, seq int, payload []byte) (string, error)

	// read fetches the body for id from the read replica.
	read func(ctx context.Context, base, id string) ([]byte, error)

	// verify validates that the body returned by read corresponds to
	// (seq, payload). Implementation depends on whether the body is
	// raw bytes (Blurry) or a wrapped object representation (Chotki).
	verify func(seq int, body, payload []byte) error
}

var stacks = []stackCfg{
	{
		name:       "blurry",
		project:    "bench-blurry",
		compose:    "docker-compose.blurry.yml",
		writeURL:   "http://127.0.0.1:8001",
		readURL:    "http://127.0.0.1:8003",
		healthPath: "/v1/health",
		setup:      blurrySetup,
		write:      blurryWrite,
		read:       blurryRead,
		verify:     blurryVerify,
	},
	{
		name:       "chotki",
		project:    "bench-chotki",
		compose:    "docker-compose.chotki.yml",
		writeURL:   "http://127.0.0.1:9001",
		readURL:    "http://127.0.0.1:9003",
		healthPath: "/cat?id=ping",
		setup:      chotkiSetup,
		write:      chotkiWrite,
		read:       chotkiRead,
		verify:     chotkiVerify,
	},
}

// payloadFiller is the static body shared by every request: allocated
// once at package init and never mutated. Each write builds a
// per-seq payload by stamping the 15-byte header into a copy.
var payloadFiller = bytes.Repeat([]byte{'a'}, payloadSize)

const payloadHeaderFmt = "SEQ=%010d:" // 15 bytes
const payloadHeaderLen = 15

// payloadFor returns a 256 KiB-sized body whose first 15 bytes encode
// `seq` and the rest are 'a'. Reuses payloadFiller for the suffix; a
// fresh buffer is allocated only for the seq header bytes since the
// HTTP body needs a stable view across the request lifetime.
func payloadFor(seq int) []byte {
	out := make([]byte, payloadSize)
	header := fmt.Sprintf(payloadHeaderFmt, seq)
	copy(out, header)
	copy(out[len(header):], payloadFiller[len(header):])
	return out
}

// TestMain installs a signal handler that tears both compose stacks
// down on SIGINT/SIGTERM. t.Cleanup hooks fire on test failures but
// not on Ctrl+C or external SIGTERM, so without this a killed run
// would leave containers and named volumes lying around.
//
// SIGKILL (kernel OOM, kill -9) still cannot be intercepted; if your
// run gets force-killed, run
//
//	docker compose --project-name bench-blurry -f bench/docker-compose.blurry.yml down -v
//	docker compose --project-name bench-chotki -f bench/docker-compose.chotki.yml down -v
//
// to clean up by hand.
func TestMain(m *testing.M) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigs
		fmt.Fprintf(os.Stderr, "bench: got %s, tearing down compose stacks...\n", s)
		teardownAllStacks()
		os.Exit(130)
	}()
	os.Exit(m.Run())
}

func teardownAllStacks() {
	for _, s := range stacks {
		composePath, err := filepath.Abs(s.compose)
		if err != nil {
			continue
		}
		_, _ = runCmd(2*time.Minute,
			"docker", "compose", "--project-name", s.project, "-f", composePath,
			"down", "-v")
	}
}

func TestBench(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping bench in short mode")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed; skipping bench")
	}
	for _, s := range stacks {
		s := s
		t.Run(s.name, func(t *testing.T) { runBench(t, s) })
	}
}

func runBench(t *testing.T, s stackCfg) {
	composePath, err := filepath.Abs(s.compose)
	if err != nil {
		t.Fatalf("abs compose path: %v", err)
	}

	t.Logf("[%s] bringing stack up via %s (project=%s)", s.name, composePath, s.project)
	dumpLogs := func(reason string) {
		out, lerr := runCmd(time.Minute,
			"docker", "compose", "--project-name", s.project, "-f", composePath,
			"logs", "--no-color", "--tail=200")
		if lerr != nil {
			t.Logf("[%s] %s: docker compose logs failed: %v\n%s", s.name, reason, lerr, out)
			return
		}
		t.Logf("[%s] %s — last 200 log lines per service:\n%s", s.name, reason, out)
	}
	t.Cleanup(func() {
		out, derr := runCmd(2*time.Minute,
			"docker", "compose", "--project-name", s.project, "-f", composePath,
			"down", "-v")
		if derr != nil {
			t.Logf("[%s] docker compose down: %v\n%s", s.name, derr, out)
		}
	})
	if out, err := runCmd(2*time.Minute,
		"docker", "compose", "--project-name", s.project, "-f", composePath,
		"down", "-v", "--remove-orphans"); err != nil {
		t.Logf("[%s] pre-up cleanup of stale state: %v\n%s", s.name, err, out)
	}
	if out, err := runCmd(composeUpWait,
		"docker", "compose", "--project-name", s.project, "-f", composePath,
		"up", "-d", "--build", "--wait"); err != nil {
		dumpLogs("up failed")
		t.Fatalf("docker compose up: %v\n%s", err, out)
	}

	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs("test failed")
		}
	})

	if err := waitReady(s.writeURL+s.healthPath, 2*time.Minute); err != nil {
		t.Fatalf("[%s] write replica not ready: %v", s.name, err)
	}
	if err := waitReady(s.readURL+s.healthPath, 2*time.Minute); err != nil {
		t.Fatalf("[%s] read replica not ready: %v", s.name, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schemaID, err := s.setup(ctx, s.writeURL)
	if err != nil {
		t.Fatalf("[%s] setup: %v", s.name, err)
	}
	if schemaID != "" {
		t.Logf("[%s] schema id: %s", s.name, schemaID)
	}

	type pair struct {
		seq int
		id  string
	}

	ch := make(chan pair, 256)
	writeErr := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	start := time.Now()

	go func() {
		defer close(ch)
		for seq := 0; seq < numRequests; seq++ {
			select {
			case <-done:
				writeErr <- nil
				return
			default:
			}
			id, err := s.write(ctx, s.writeURL, schemaID, seq, payloadFor(seq))
			if err != nil {
				writeErr <- fmt.Errorf("write seq=%d: %w", seq, err)
				return
			}
			select {
			case ch <- pair{seq, id}:
			case <-done:
				writeErr <- nil
				return
			}
		}
		writeErr <- nil
	}()

	read := 0
	var verifyErr error
	for p := range ch {
		deadline := time.Now().Add(perReadDeadline)
		if err := pollAndVerify(ctx, s, p.seq, p.id, deadline); err != nil {
			verifyErr = fmt.Errorf("read seq=%d id=%s: %w", p.seq, p.id, err)
			break
		}
		read++
		if read%1000 == 0 {
			t.Logf("[%s] verified %d/%d in %s", s.name, read, numRequests, time.Since(start))
		}
	}
	if verifyErr != nil {
		t.Fatalf("[%s] %v", s.name, verifyErr)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("[%s] writer: %v", s.name, err)
	}
	if read != numRequests {
		t.Fatalf("[%s] data lost: read %d, expected %d", s.name, read, numRequests)
	}
	elapsed := time.Since(start)
	t.Logf("[%s] processed %d x %d KiB through write→sync→read in %s (%.1f req/s, %.2f MiB/s)",
		s.name, numRequests, payloadSize/1024, elapsed,
		float64(numRequests)/elapsed.Seconds(),
		float64(numRequests*payloadSize)/(1024*1024)/elapsed.Seconds())
}

// ---- HTTP infrastructure ----------------------------------------------

var httpClient = &http.Client{Timeout: httpReqTimeout}

func waitReady(probeURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(probeURL)
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		if err == nil && resp != nil && resp.StatusCode > 0 && resp.StatusCode < 500 {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("not ready within %s: %s", timeout, probeURL)
}

func pollAndVerify(ctx context.Context, s stackCfg, seq int, id string, deadline time.Time) error {
	expected := payloadFor(seq)
	for {
		body, err := s.read(ctx, s.readURL, id)
		if err == nil {
			if verr := s.verify(seq, body, expected); verr != nil {
				return verr
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for sync; last err=%v", err)
		}
		time.Sleep(pollInterval)
	}
}

// ---- Blurry KV adapter ------------------------------------------------

func blurryKey(seq int) string {
	return fmt.Sprintf("bench/%010d", seq)
}

func blurrySetup(ctx context.Context, base string) (string, error) {
	return "", nil
}

// blurryWrite issues a single-key PUT /v1/batch with the seq's
// key/value pair. Going through /v1/batch (rather than the simpler
// /v1/kv/:key) exercises the same Batch.Commit path the docs cite as
// the "400 keys/s with batching" baseline, even when each request
// happens to carry one key.
func blurryWrite(ctx context.Context, base, _ string, seq int, payload []byte) (string, error) {
	key := blurryKey(seq)
	body, err := json.Marshal([]struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}{{Key: key, Value: payload}})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, base+"/v1/batch", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bt, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, bt)
	}
	return key, nil
}

func blurryRead(ctx context.Context, base, id string) ([]byte, error) {
	// Pass the key verbatim — Fiber's "/v1/kv/*" wildcard matches over
	// the raw URL path. url.PathEscape would percent-encode the "/"
	// inside `id` to "%2F", which fasthttp keeps verbatim in the
	// captured wildcard, producing "bench%2F0000000000" lookups that
	// never match the stored "bench/0000000000". Our keys are
	// alphanumeric + "/" so direct concatenation is safe.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/v1/kv/"+id, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bt, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.New("not found")
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, bt)
	}
	return bt, nil
}

func blurryVerify(seq int, body, expected []byte) error {
	if !bytes.Equal(body, expected) {
		return fmt.Errorf("body mismatch (got len=%d, want len=%d)", len(body), len(expected))
	}
	return nil
}

// ---- Chotki classic adapter ------------------------------------------

const chotkiClassBody = "{XSeq: I, XPayload: S}"

// chotkiSetup creates a class with two fields. The X-prefix workaround
// compensates for chotki's repl class encoding bug (it stores
// <RDT><name> while ParseClass reads <RDT><Index><name>, so the
// leading byte of every field name is eaten on read).
func chotkiSetup(ctx context.Context, base string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/class", strings.NewReader(chotkiClassBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bt, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, bt)
	}
	id := strings.TrimSpace(string(bt))
	if id == "" {
		return "", errors.New("empty class id")
	}
	return id, nil
}

func chotkiWrite(ctx context.Context, base, classID string, seq int, payload []byte) (string, error) {
	var buf bytes.Buffer
	buf.Grow(len(payload) + 128)
	buf.WriteString("{_ref: ")
	buf.WriteString(classID)
	buf.WriteString(", Seq: ")
	buf.WriteString(strconv.Itoa(seq))
	buf.WriteString(`, Payload: "`)
	buf.Write(payload)
	buf.WriteString(`"}`)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/new", bytes.NewReader(buf.Bytes()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bt, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, bt)
	}
	id := strings.TrimSpace(string(bt))
	if id == "" {
		return "", errors.New("empty object id")
	}
	return id, nil
}

func chotkiRead(ctx context.Context, base, id string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/cat?id="+url.QueryEscape(id), nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bt, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, bt)
	}
	return bt, nil
}

func chotkiVerify(seq int, body, expected []byte) error {
	payload, err := extractField(body, "Payload")
	if err != nil {
		return fmt.Errorf("extract Payload: %w (body len=%d)", err, len(body))
	}
	if !bytes.Equal(payload, expected) {
		return fmt.Errorf("Payload bytes mismatch (got len=%d, want len=%d)",
			len(payload), len(expected))
	}
	seqField, err := extractField(body, "Seq")
	if err != nil {
		return fmt.Errorf("extract Seq: %w", err)
	}
	gotSeq, err := strconv.Atoi(strings.TrimSpace(string(seqField)))
	if err != nil {
		return fmt.Errorf("parse Seq %q: %w", seqField, err)
	}
	if gotSeq != seq {
		return fmt.Errorf("Seq %d != expected %d", gotSeq, seq)
	}
	if !bytes.HasPrefix(payload, []byte(fmt.Sprintf(payloadHeaderFmt, seq))) {
		return fmt.Errorf("Payload header mismatch (first %d bytes %q)",
			payloadHeaderLen, payload[:payloadHeaderLen])
	}
	return nil
}

// extractField finds the value associated with `name:` in a flat
// `{a:1,b:2,c:"3"}` style mapping. Strips one layer of surrounding
// double quotes, otherwise returns everything up to the next `,` or
// `}`.
func extractField(body []byte, name string) ([]byte, error) {
	needle := []byte(name + ":")
	i := bytes.Index(body, needle)
	if i == -1 {
		return nil, fmt.Errorf("field %q not found", name)
	}
	val := body[i+len(needle):]
	if len(val) > 0 && val[0] == '"' {
		val = val[1:]
		end := bytes.IndexByte(val, '"')
		if end == -1 {
			return nil, fmt.Errorf("field %q has unclosed quote", name)
		}
		return val[:end], nil
	}
	end := bytes.IndexAny(val, ",}")
	if end == -1 {
		return nil, fmt.Errorf("field %q has no terminator", name)
	}
	return val[:end], nil
}

// ---- subprocess helper ------------------------------------------------

func runCmd(timeout time.Duration, name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	cmd.Env = append(os.Environ(), "DOCKER_BUILDKIT=1", "COMPOSE_DOCKER_CLI_BUILD=1")
	if timeout <= 0 {
		return cmd.CombinedOutput()
	}
	done := make(chan struct{})
	var out []byte
	var err error
	go func() {
		out, err = cmd.CombinedOutput()
		close(done)
	}()
	select {
	case <-done:
		return out, err
	case <-time.After(timeout):
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done
		return out, fmt.Errorf("command timed out after %s", timeout)
	}
}
