// Bench harness: writes 30000 sequentially-numbered objects (each
// carrying a 256 KiB Payload) to the first replica of a stack, reads
// them back from the third replica, and reports the wall-clock time
// the cluster needed to process the data.
//
// Two stacks are exercised back-to-back: first the Blurry compose file,
// then the Chotki one. Each subtest brings its own compose stack up
// (`docker compose -f ... up -d --build --wait`) and tears it down
// afterwards.
//
// Usage (the bench/ directory is its own Go module, so cd into it):
//
//	cd bench
//	go test -timeout=2h -run TestBench .
//
// The test is skipped when `docker` is not on $PATH or when run with
// `-short`.
package bench

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	numRequests     = 30000
	payloadSize     = 256 * 1024
	composeUpWait   = 5 * time.Minute
	httpReqTimeout  = 2 * time.Minute
	pollInterval    = 50 * time.Millisecond
	perReadDeadline = 5 * time.Minute
)

type stackCfg struct {
	name     string
	project  string // docker compose project name (isolates resources between stacks)
	compose  string
	writeURL string // base URL of "first" replica
	readURL  string // base URL of "third" replica

	// classBody is the body sent to POST /class to create the bench
	// schema. Both stacks must end up storing two fields whose names
	// are exactly "Seq" and "Payload": chotki's repl encodes class
	// fields as <RDT><name> while ParseClass expects <RDT><Index><name>,
	// so on chotki we prepend a one-byte padding char to each field
	// name to compensate. Both stacks then accept identical /new and
	// /cat traffic.
	classBody string
}

var stacks = []stackCfg{
	{
		name:      "blurry",
		project:   "bench-blurry",
		compose:   "docker-compose.blurry.yml",
		writeURL:  "http://127.0.0.1:8001",
		readURL:   "http://127.0.0.1:8003",
		classBody: "{Seq: I, Payload: S}",
	},
	{
		name:      "chotki",
		project:   "bench-chotki",
		compose:   "docker-compose.chotki.yml",
		writeURL:  "http://127.0.0.1:9001",
		readURL:   "http://127.0.0.1:9003",
		classBody: "{XSeq: I, XPayload: S}",
	},
}

// payloadFiller is the static 256 KiB body shared by every request:
// allocated once and never mutated. Each /new body is built by writing
// a 15-byte per-seq header into a reusable scratch buffer and then
// appending the suffix of payloadFiller, so we don't burn 30000 * 256
// KiB of GC churn.
var payloadFiller = bytes.Repeat([]byte{'a'}, payloadSize)

const payloadHeaderFmt = "SEQ=%010d:" // 15 bytes

const payloadHeaderLen = 15

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
	// Register the teardown before bringing the stack up so a failed
	// `up` (which can leave partially-created networks/volumes/containers
	// behind) still gets cleaned out.
	t.Cleanup(func() {
		out, derr := runCmd(2*time.Minute,
			"docker", "compose", "--project-name", s.project, "-f", composePath,
			"down", "-v")
		if derr != nil {
			t.Logf("[%s] docker compose down: %v\n%s", s.name, derr, out)
		}
	})
	if out, err := runCmd(composeUpWait,
		"docker", "compose", "--project-name", s.project, "-f", composePath,
		"up", "-d", "--build", "--wait"); err != nil {
		t.Fatalf("docker compose up: %v\n%s", err, out)
	}

	if err := waitReady(s.writeURL, 2*time.Minute); err != nil {
		t.Fatalf("[%s] write replica not ready: %v", s.name, err)
	}
	if err := waitReady(s.readURL, 2*time.Minute); err != nil {
		t.Fatalf("[%s] read replica not ready: %v", s.name, err)
	}

	classID, err := createClass(s.writeURL, s.classBody)
	if err != nil {
		t.Fatalf("[%s] create class: %v", s.name, err)
	}
	t.Logf("[%s] class id: %s", s.name, classID)

	type pair struct {
		seq int
		id  string
	}

	ch := make(chan pair, 256)
	writeErr := make(chan error, 1)
	// done is closed when the reader gives up (verification failure or
	// premature exit) so the writer goroutine can stop instead of
	// blocking on `ch <- pair` against a torn-down stack.
	done := make(chan struct{})
	defer close(done)

	start := time.Now()

	go func() {
		defer close(ch)
		bodyBuf := bytes.NewBuffer(make([]byte, 0, payloadSize+128))
		for seq := 0; seq < numRequests; seq++ {
			select {
			case <-done:
				writeErr <- nil
				return
			default:
			}
			id, err := postNew(s.writeURL, classID, seq, bodyBuf)
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
		if err := pollAndVerify(s.readURL, p.id, p.seq, deadline); err != nil {
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

// ---- HTTP helpers -----------------------------------------------------

var httpClient = &http.Client{Timeout: httpReqTimeout}

func waitReady(base string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(base + "/cat?id=ping")
		// http.Get can return a non-nil resp alongside a non-nil err
		// (redirect failures, etc.) — drain & close in either case.
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		// Any non-5xx status means the HTTP server is up and
		// answering; we tolerate 4xx because /cat?id=ping is an
		// intentionally invalid request used as a probe.
		if err == nil && resp != nil && resp.StatusCode > 0 && resp.StatusCode < 500 {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("not ready within %s", timeout)
}

func createClass(base, body string) (string, error) {
	resp, err := httpClient.Post(base+"/class", "text/plain", strings.NewReader(body))
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
		return "", errors.New("empty class id in response")
	}
	return id, nil
}

// postNew reuses a single bytes.Buffer per goroutine, avoiding a fresh
// 256 KiB allocation per request.
func postNew(base, classID string, seq int, buf *bytes.Buffer) (string, error) {
	buf.Reset()
	buf.WriteString("{_ref: ")
	buf.WriteString(classID)
	buf.WriteString(", Seq: ")
	buf.WriteString(strconv.Itoa(seq))
	buf.WriteString(`, Payload: "`)
	fmt.Fprintf(buf, payloadHeaderFmt, seq)
	buf.Write(payloadFiller[payloadHeaderLen:])
	buf.WriteString(`"}`)

	resp, err := httpClient.Post(base+"/new", "text/plain", bytes.NewReader(buf.Bytes()))
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
		return "", errors.New("empty object id in response")
	}
	return id, nil
}

func pollAndVerify(base, id string, seq int, deadline time.Time) error {
	url := base + "/cat?id=" + id
	for {
		resp, err := httpClient.Get(url)
		// http.Get can return a non-nil resp alongside a non-nil err
		// (redirect failures, etc.) — read & close in either case so
		// the keep-alive pool doesn't leak idle connections.
		var (
			body   []byte
			status string
		)
		if resp != nil {
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			status = resp.Status
		}
		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			if verr := verifyResponse(body, seq); verr != nil {
				return verr
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for sync; last status=%q err=%v", status, err)
		}
		time.Sleep(pollInterval)
	}
}

// verifyResponse extracts the Payload field from a /cat response body
// and checks every byte of it matches what postNew sent for `seq`. It
// also checks the Seq scalar field round-tripped intact.
//
// Both stacks return text approximating
//
//	{Payload:<value>,Seq:N}                (Blurry: keys sorted, no quotes)
//	{_ref:0-0,Seq:N,Payload:"<value>"}     (Chotki: ObjectString form)
//
// so we accept either quoted or unquoted Payload values.
func verifyResponse(body []byte, seq int) error {
	payload, err := extractField(body, "Payload")
	if err != nil {
		return fmt.Errorf("extract Payload: %w (body len=%d)", err, len(body))
	}
	if len(payload) != payloadSize {
		return fmt.Errorf("Payload len %d != expected %d", len(payload), payloadSize)
	}
	wantHeader := fmt.Sprintf(payloadHeaderFmt, seq)
	if !bytes.Equal(payload[:payloadHeaderLen], []byte(wantHeader)) {
		return fmt.Errorf("Payload header %q != expected %q",
			payload[:payloadHeaderLen], wantHeader)
	}
	if !bytes.Equal(payload[payloadHeaderLen:], payloadFiller[payloadHeaderLen:]) {
		return errors.New("Payload filler bytes mismatch")
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
	return nil
}

// extractField finds the value associated with `name:` in a flat
// `{a:1,b:2,c:"3"}` style mapping. It strips one layer of surrounding
// double quotes if present, and otherwise returns everything up to the
// next `,` or `}`.
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
