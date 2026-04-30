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
ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/blurry ./cmd/blurry

# Pre-create the data dir with nonroot ownership in the build stage so
# the runtime image already has a /tmp/blurry owned by uid 65532. When
# docker (or compose) mounts a fresh named volume at this path, it
# initializes the volume from the existing image directory and inherits
# its perms — without this, the volume defaults to root:root and the
# distroless nonroot user can't write to it.
RUN install -d -o 65532 -g 65532 /var/empty/blurry-data

# ---- runtime ----------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/blurry /usr/local/bin/blurry
COPY --from=build --chown=nonroot:nonroot /var/empty/blurry-data /tmp/blurry

# libp2p (TCP) and HTTP API.
EXPOSE 4001 8001

VOLUME ["/tmp/blurry"]

ENV BLURRY_DATA_DIR=/tmp/blurry \
    BLURRY_LISTEN_HOST=0.0.0.0 \
    BLURRY_LISTEN_PORT=4001 \
    BLURRY_HTTP_HOST=0.0.0.0 \
    BLURRY_HTTP_PORT=8001

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/blurry"]
