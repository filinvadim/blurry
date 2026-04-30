# Blurry

A simple embedded key-value store implementation that utilizes Merkle CRDTs.
Internally it uses a delta-CRDT Add-Wins Observed-Removed set. 
The current value for a key is the one with highest priority. 
Priorities are defined as the height of the Merkle-CRDT node in which the key was introduced.

# Benchmark

Write from first node, read from third:
```bash
go test -v -timeout=2h -count=1  -run TestBench .
    === RUN   TestBench
    === RUN   TestBench/blurry
    bench_test.go:226: [blurry] bringing stack up via /Users/vadim/go/src/github.com/filinvadim/blurry/bench/docker-compose.blurry.yml (project=bench-blurry)
    bench_test.go:327: [blurry] verified 1000/1000 in 3.707817958s
    bench_test.go:340: [blurry] processed 1000 x 64 KiB through
            write→sync→read in 3.70783625s (269.7 req/s, 16.86 MiB/s)

    === RUN   TestBench/chotki
    bench_test.go:226: [chotki] bringing stack up via /Users/vadim/go/src/github.com/filinvadim/blurry/bench/docker-compose.chotki.yml (project=bench-chotki)
    bench_test.go:278: [chotki] schema id: 1-1
    bench_test.go:327: [chotki] verified 1000/1000 in 42.476329s
    bench_test.go:340: [chotki] processed 1000 x 64 KiB through 
            write→sync→read in 42.476456209s (23.5 req/s, 1.47 MiB/s)

    --- PASS: TestBench (55.41s)
    --- PASS: TestBench/blurry (8.81s)
    --- PASS: TestBench/chotki (46.60s)
PASS
ok      github.com/filinvadim/blurry/bench      55.639s
```
