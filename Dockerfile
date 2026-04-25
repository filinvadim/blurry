# syntax=docker/dockerfile:1.7

# ---- build ------------------------------------------------------------
FROM golang:1.25-alpine AS build

RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
COPY vendor ./vendor

COPY . .

# CGO is not needed; build a static binary.
ENV CGO_ENABLED=0 GOFLAGS=-mod=vendor

RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/blurry ./cmd/blurry

# ---- runtime ----------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/blurry /usr/local/bin/blurry

# libp2p (TCP) and HTTP API.
EXPOSE 4001 8001

VOLUME ["/data"]

ENV BLURRY_DATA_DIR=/data \
    BLURRY_LISTEN_HOST=0.0.0.0 \
    BLURRY_LISTEN_PORT=4001 \
    BLURRY_HTTP_HOST=0.0.0.0 \
    BLURRY_HTTP_PORT=8001

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/blurry"]
