package blurry

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	logging "github.com/ipfs/go-log/v2"
)

var httpLog = logging.Logger("http")

// HTTPServer exposes a minimal key/value REST API on top of a Blurry
// instance, served by Fiber v3:
//
//	GET    /v1/kv/*                                   read raw value
//	PUT    /v1/kv/*                                   write raw body as value
//	DELETE /v1/kv/*                                   tombstone the key
//	GET    /v1/kv?prefix=&keysOnly=                   range scan
//	PUT    /v1/batch                                  bulk set (one CRDT delta)
//	GET    /v1/health                                 200 OK
//
// "Always batch" — every mutation, even single-key Set/Delete, goes
// through ds.Batching internally. The /v1/batch endpoint exists so a
// client that knows it has many writes ahead of it can collapse them
// into a single CRDT delta + DAG node + pubsub broadcast.
//
// Values are raw byte sequences in/out. Keys in the URL path are
// percent-decoded by Fiber, so "/v1/kv/foo%2Fbar" stores under
// "foo/bar".
type HTTPServer struct {
	b      *Blurry
	app    *fiber.App
	listen string
}

// NewHTTPServer wires the routes but does not start a listener.
func NewHTTPServer(b *Blurry) *HTTPServer {
	app := fiber.New(fiber.Config{
		AppName:           "blurry",
		ReadBufferSize:    16 * 1024,
		WriteBufferSize:   16 * 1024,
		BodyLimit:         64 * 1024 * 1024,
		DisableKeepalive:  false,
		StreamRequestBody: true,
	})
	h := &HTTPServer{b: b, app: app}

	app.Get("/v1/health", h.handleHealth)

	app.Get("/v1/kv", h.handleList)
	app.Get("/v1/kv/*", h.handleGet)
	app.Put("/v1/kv/*", h.handleSet)
	app.Delete("/v1/kv/*", h.handleDelete)

	app.Put("/v1/batch", h.handleBatch)

	return h
}

// Start binds and serves the API on addr (e.g. "127.0.0.1:8001"). If
// Settings.TlsConfig has at least one certificate, the listener is
// wrapped with tls.NewListener so the same code path covers HTTPS.
//
// Fiber's startup banner ("Fiber" ASCII art + listening address) is
// suppressed — we already log a single "http: serving on …" line
// and the banner adds 16 lines of noise to test output and
// container logs.
func (h *HTTPServer) Start(addr string) error {
	h.listen = addr
	tlsCfg := h.b.settings.TlsConfig
	useTLS := tlsCfg != nil && len(tlsCfg.Certificates) > 0
	listenCfg := fiber.ListenConfig{DisableStartupMessage: true}
	go func() {
		var err error
		if useTLS {
			ln, lerr := tlsListener(addr, tlsCfg)
			if lerr != nil {
				httpLog.Errorf("http: tls listen %s: %v", addr, lerr)
				return
			}
			err = h.app.Listener(ln, listenCfg)
		} else {
			err = h.app.Listen(addr, listenCfg)
		}
		if err != nil && !errors.Is(err, fiber.ErrServiceUnavailable) {
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

func tlsListener(addr string, cfg *tls.Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(ln, cfg), nil
}

// Close stops the HTTP server.
func (h *HTTPServer) Close() error {
	if h == nil || h.app == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return h.app.ShutdownWithContext(ctx)
}

// ---- handlers ----------------------------------------------------------

func (h *HTTPServer) handleHealth(c fiber.Ctx) error {
	return c.SendStatus(fiber.StatusOK)
}

func (h *HTTPServer) handleGet(c fiber.Ctx) error {
	key, err := keyFromPath(c)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	v, err := h.b.KV().Get(c.Context(), key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return fiber.NewError(fiber.StatusNotFound, "key not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	c.Set(fiber.HeaderContentType, "application/octet-stream")
	return c.Send(v)
}

func (h *HTTPServer) handleSet(c fiber.Ctx) error {
	key, err := keyFromPath(c)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	body := c.Body()
	if err := h.b.KV().Set(c.Context(), key, body); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func (h *HTTPServer) handleDelete(c fiber.Ctx) error {
	key, err := keyFromPath(c)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	if err := h.b.KV().Delete(c.Context(), key); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func (h *HTTPServer) handleList(c fiber.Ctx) error {
	prefix := c.Query("prefix", "")
	keysOnly := c.Query("keysOnly", "") == "true" || c.Query("keys_only", "") == "true"
	pairs, err := h.b.KV().List(c.Context(), prefix, keysOnly)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.JSON(pairs)
}

// handleBatch accepts a JSON array of {"key":..., "value":...} pairs
// and commits them in a single ds.Batch. value is base64-encoded in
// JSON since it's raw bytes; an empty value is treated as a delete.
func (h *HTTPServer) handleBatch(c fiber.Ctx) error {
	var req []batchOp
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("decode body: %v", err))
	}
	pairs := make([]KVPair, 0, len(req))
	for _, op := range req {
		if op.Key == "" {
			return fiber.NewError(fiber.StatusBadRequest, "missing key in batch op")
		}
		pairs = append(pairs, KVPair{Key: op.Key, Value: op.Value})
	}
	if err := h.b.KV().SetBatch(c.Context(), pairs); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.SendStatus(fiber.StatusNoContent)
}

type batchOp struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// keyFromPath extracts the application key from a /v1/kv/* route. The
// wildcard match preserves slashes, so "GET /v1/kv/foo/bar" yields
// "foo/bar". Returns an error if the key is empty.
func keyFromPath(c fiber.Ctx) (string, error) {
	raw := c.Params("*", "")
	raw = strings.TrimPrefix(raw, "/")
	if raw == "" {
		return "", errors.New("empty key")
	}
	return raw, nil
}
