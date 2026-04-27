// Bench harness: writes 30000 sequentially-numbered objects (each
// carrying a 256 KiB payload) to the first replica of a stack, reads
// them back from the third replica, and reports the wall-clock time
// the cluster needed to process the data.
//
// Two stacks are exercised back-to-back: first the Blurry compose file,
// then the Chotki one. Each test brings its own compose stack up
// (`docker compose -f ... up -d --build --wait`) and tears it down
// afterwards.
//
// Usage:
//
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
}

var stacks = []stackCfg{
	{
		name:     "blurry",
		project:  "bench-blurry",
		compose:  "docker-compose.blurry.yml",
		writeURL: "http://127.0.0.1:8001",
		readURL:  "http://127.0.0.1:8003",
	},
	{
		name:     "chotki",
		project:  "bench-chotki",
		compose:  "docker-compose.chotki.yml",
		writeURL: "http://127.0.0.1:9001",
		readURL:  "http://127.0.0.1:9003",
	},
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
	if out, err := runCmd(composeUpWait,
		"docker", "compose", "--project-name", s.project, "-f", composePath,
		"up", "-d", "--build", "--wait"); err != nil {
		t.Fatalf("docker compose up: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		out, derr := runCmd(2*time.Minute,
			"docker", "compose", "--project-name", s.project, "-f", composePath,
			"down", "-v")
		if derr != nil {
			t.Logf("[%s] docker compose down: %v\n%s", s.name, derr, out)
		}
	})

	if err := waitReady(s.writeURL, 2*time.Minute); err != nil {
		t.Fatalf("[%s] write replica not ready: %v", s.name, err)
	}
	if err := waitReady(s.readURL, 2*time.Minute); err != nil {
		t.Fatalf("[%s] read replica not ready: %v", s.name, err)
	}

	classID, err := createClass(s.writeURL)
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

	start := time.Now()

	go func() {
		defer close(ch)
		for seq := 0; seq < numRequests; seq++ {
			id, err := postNew(s.writeURL, classID, seq, payloadFor(seq))
			if err != nil {
				writeErr <- fmt.Errorf("write seq=%d: %w", seq, err)
				return
			}
			ch <- pair{seq, id}
		}
		writeErr <- nil
	}()

	read := 0
	for p := range ch {
		deadline := time.Now().Add(perReadDeadline)
		if err := pollAndVerify(s.readURL, p.id, p.seq, deadline); err != nil {
			t.Fatalf("[%s] read seq=%d id=%s: %v", s.name, p.seq, p.id, err)
		}
		read++
		if read%1000 == 0 {
			t.Logf("[%s] verified %d/%d in %s", s.name, read, numRequests, time.Since(start))
		}
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

// ---- payload ----------------------------------------------------------

const payloadHeaderFmt = "SEQ=%010d:"

func payloadFor(seq int) string {
	head := fmt.Sprintf(payloadHeaderFmt, seq)
	if len(head) >= payloadSize {
		return head[:payloadSize]
	}
	tail := bytes.Repeat([]byte{'a'}, payloadSize-len(head))
	return head + string(tail)
}

func payloadHeader(seq int) string {
	return fmt.Sprintf(payloadHeaderFmt, seq)
}

// ---- HTTP helpers -----------------------------------------------------

var httpClient = &http.Client{Timeout: httpReqTimeout}

func waitReady(base string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(base + "/cat?id=ping")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode > 0 && resp.StatusCode < 600 {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("not ready within %s", timeout)
}

func createClass(base string) (string, error) {
	body := "{Seq: I, Payload: S}"
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

func postNew(base, classID string, seq int, payload string) (string, error) {
	var b strings.Builder
	b.Grow(payloadSize + 64)
	b.WriteString("{_ref: ")
	b.WriteString(classID)
	b.WriteString(", Seq: ")
	fmt.Fprintf(&b, "%d", seq)
	b.WriteString(`, Payload: "`)
	b.WriteString(payload)
	b.WriteString(`"}`)

	resp, err := httpClient.Post(base+"/new", "text/plain", strings.NewReader(b.String()))
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
	header := payloadHeader(seq)
	for {
		resp, err := httpClient.Get(url)
		if err == nil {
			bt, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				body := string(bt)
				if !strings.Contains(body, header) {
					return fmt.Errorf("payload header %q missing in body (len=%d)", header, len(body))
				}
				if len(body) < payloadSize {
					return fmt.Errorf("body too short: %d < %d", len(body), payloadSize)
				}
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for sync; last status=%v err=%v", statusOf(resp), err)
		}
		time.Sleep(pollInterval)
	}
}

func statusOf(r *http.Response) any {
	if r == nil {
		return nil
	}
	return r.Status
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
