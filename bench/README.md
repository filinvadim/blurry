# bench

End-to-end benchmark that measures how long a three-replica cluster
needs to write → replicate → read a stream of fixed-size objects.
Two stacks are exercised back-to-back so their numbers are directly
comparable:

| stack    | image                                | HTTP write replica | HTTP read replica |
|----------|--------------------------------------|--------------------|-------------------|
| Blurry   | built from the repo-root Dockerfile  | `127.0.0.1:8001`   | `127.0.0.1:8003`  |
| Chotki   | upstream `drpcorg/chotki` repl       | `127.0.0.1:9001`   | `127.0.0.1:9003`  |

The chotki containers run the **official** chotki repl, pinned to a
specific upstream SHA — there's no bench-side wrapper between the
test and chotki. socat forwards `0.0.0.0:8001` to chotki's hardcoded
localhost `servehttp` inside each container.

## Prerequisites

* Docker Engine + the `docker compose` plugin (v2).
* Go 1.25+.
* Free host ports: `8001-8003`, `9001-9003`, `4001-4003`, `14001-14003`.
* Roughly `requests × payload_size` of free RAM/disk per stack
  (defaults: ~64 MiB; 30 000 × 256 KiB ≈ 7.5 GiB).

## Run

`bench/` is a nested Go module, so `cd` into it first:

```bash
cd bench
go test -v -timeout=2h -run TestBench .
```

Run a single stack:

```bash
go test -v -run TestBench/blurry .
go test -v -run TestBench/chotki .
```

## Tunables

The defaults are deliberately light so the harness fits on a laptop
without OOM-killing the host. Override via env vars:

| env var                  | default     | meaning                                  |
|--------------------------|-------------|------------------------------------------|
| `BENCH_REQUESTS`         | `1000`      | number of objects per stack              |
| `BENCH_PAYLOAD_SIZE`     | `65536`     | bytes per `Payload` field                |
| `BENCH_PER_READ_TIMEOUT` | `60s`       | how long to wait for one object to sync  |

Examples:

```bash
# Lighter still — useful while you iterate on the harness.
BENCH_REQUESTS=200 BENCH_PAYLOAD_SIZE=4096 \
  go test -v -timeout=10m -run TestBench .

# The original heavy spec (≈ 7.5 GiB through each stack).
BENCH_REQUESTS=30000 BENCH_PAYLOAD_SIZE=262144 BENCH_PER_READ_TIMEOUT=5m \
  go test -v -timeout=2h -run TestBench .
```

The `chotki` image SHA is pinned in `chotki/Dockerfile` via
`CHOTKI_REF` and the upstream URL via `CHOTKI_REPO`. Override either
when building against an internal mirror or a `file://` checkout:

```bash
docker buildx build \
  --build-arg CHOTKI_REPO=git://internal.example.com/chotki.git \
  --build-arg CHOTKI_REF=<sha> \
  bench/chotki
```

## What the test does

For each stack:

1. `docker compose up -d --build --wait` brings the three replicas up.
2. Polls `/cat?id=ping` on replicas 1 and 3 until the HTTP server
   answers with a non-5xx status.
3. `POST /class` on replica 1 to register a schema with `Seq:I` and
   `Payload:S` fields.
4. Writer goroutine pipelines `POST /new` to replica 1 with
   sequential `Seq` values and a deterministic 64 KiB Payload, sending
   each `(seq, id)` pair to a buffered channel.
5. Reader goroutine `GET /cat?id=<id>` on replica 3 with a 60 s
   per-object deadline, byte-compares the returned Payload against
   the static template, and verifies `Seq` round-tripped.
6. Reports total elapsed time + req/s + MiB/s.
7. `docker compose down -v` removes containers, volumes, and the
   compose network.

## Output

```
=== RUN   TestBench/blurry
    bench_test.go:139: [blurry] class id: ...
    bench_test.go:148: [blurry] verified 1000/1000 in 14.2s
    bench_test.go:156: [blurry] processed 1000 x 64 KiB through write→sync→read
                      in 14.2s (70.4 req/s, 4.41 MiB/s)
--- PASS: TestBench/blurry (15.4s)
=== RUN   TestBench/chotki
    bench_test.go:139: [chotki] class id: 1-104
    bench_test.go:148: [chotki] verified 1000/1000 in 11.0s
    bench_test.go:156: [chotki] processed 1000 x 64 KiB through write→sync→read
                      in 11.0s (90.9 req/s, 5.68 MiB/s)
--- PASS: TestBench/chotki (12.1s)
```

## Cleanup

A clean test exit (success or `t.Fatalf`) runs `docker compose down -v`
for both stacks via `t.Cleanup`. SIGINT/SIGTERM are caught by
`TestMain` and trigger the same teardown.

SIGKILL (`kill -9`, kernel OOM) cannot be intercepted. If a run
gets force-killed, blow the leftovers away by hand:

```bash
docker compose --project-name bench-blurry  -f bench/docker-compose.blurry.yml  down -v
docker compose --project-name bench-chotki  -f bench/docker-compose.chotki.yml  down -v
```

If you also want to free disk taken by the build cache:

```bash
docker image  prune -f
docker volume prune -f
docker buildx prune -f
```

## Troubleshooting

* **`open /tmp/blurry/LOCK: permission denied`** — a stale named volume
  from before the Dockerfile started pre-creating the data dir as
  `nonroot`. `docker compose ... down -v` drops it; the next `up`
  creates a fresh one with the right perms.
* **`image "chotki-bench:local": already exists`** during a chotki
  build — only `chotki-1` (and `blurry-1`) declare `build:`; if you
  added `build:` blocks to the other replicas they'll race on image
  export. Keep build blocks on the first replica only.
* **`chotki replica not ready`** — chotki's repl prints "listening"
  after `listen "tcp://0.0.0.0:4001"`. If you see startup logs but no
  HTTP, socat may be failing to forward to `127.0.0.1:18001`; bring
  one up by hand and `docker exec ... wget -qO- 127.0.0.1:18001/cat`
  to see whether the repl or the proxy is at fault.
* **Test gets killed mid-run with no log output** — the kernel
  OOM-killed the Go process. Reduce `BENCH_REQUESTS` /
  `BENCH_PAYLOAD_SIZE` or both, then re-run.

## Files

```
bench/
├── README.md                    (you are here)
├── bench_test.go                the harness
├── go.mod                       nested module (stdlib-only)
├── docker-compose.blurry.yml    three-replica Blurry stack
├── docker-compose.chotki.yml    three-replica Chotki stack
└── chotki/
    ├── Dockerfile               builds the upstream chotki repl + socat
    └── entrypoint.sh            wires create/listen/connect/servehttp args
```
