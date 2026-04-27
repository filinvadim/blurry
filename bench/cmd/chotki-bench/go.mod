module github.com/filinvadim/blurry/bench/cmd/chotki-bench

go 1.25

// chotki is resolved at Docker build time: the Dockerfile clones the
// upstream repo into /chotki and runs `go mod edit -replace=...=/chotki`
// before `go mod tidy && go build`. The placeholder version below is
// only there to make `go mod` happy until that point.
require github.com/drpcorg/chotki v0.0.0-00010101000000-000000000000

require github.com/cockroachdb/pebble v1.1.5
