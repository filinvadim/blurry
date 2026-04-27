# syntax=docker/dockerfile:1.7
#
# Builds the official Chotki repl binary from the upstream
# drpcorg/chotki repository — no custom Go code lives here. The repl
# binds its HTTP API to 127.0.0.1, so the entrypoint runs a small
# socat forwarder to expose it on 0.0.0.0 inside the container.
#
# CHOTKI_REF pins the upstream chotki commit. Override only when you
# explicitly want to test against a different revision.
ARG CHOTKI_REF=6a3944bc8b7f5135d4740976bad8946e59001b01

# ---- build ------------------------------------------------------------
FROM golang:1.25-alpine AS build

RUN apk add --no-cache git ca-certificates

ARG CHOTKI_REF
RUN git clone https://github.com/drpcorg/chotki.git /chotki && \
    git -C /chotki checkout "${CHOTKI_REF}"

WORKDIR /chotki
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
        -o /out/chotki ./repl

# ---- runtime ----------------------------------------------------------
FROM alpine:3.20

RUN apk add --no-cache socat ca-certificates && \
    adduser -D -u 65532 chotki && \
    mkdir -p /data && chown chotki:chotki /data

COPY --from=build /out/chotki /usr/local/bin/chotki
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER chotki:chotki
WORKDIR /data
VOLUME ["/data"]

EXPOSE 4001 8001

ENV SRC=1 \
    LISTEN_ADDR=0.0.0.0:4001 \
    HTTP_PORT=8001 \
    INTERNAL_HTTP_PORT=18001 \
    PEERS=

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
