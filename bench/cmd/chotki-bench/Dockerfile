# syntax=docker/dockerfile:1.7
#
# chotki-bench is a thin HTTP front-end on top of a Chotki replica.
# Build context is this directory (cmd/chotki-bench).
#
# Build arg CHOTKI_REF pins the upstream chotki commit (default: main).
ARG CHOTKI_REF=main

# ---- build ------------------------------------------------------------
FROM golang:1.25-alpine AS build

RUN apk add --no-cache git ca-certificates

ARG CHOTKI_REF
RUN git clone https://github.com/drpcorg/chotki.git /chotki && \
    git -C /chotki checkout "${CHOTKI_REF}"

WORKDIR /src

COPY go.mod ./
COPY main.go ./

RUN go mod edit -replace=github.com/drpcorg/chotki=/chotki && \
    go mod tidy && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
        -o /out/chotki-bench .

# ---- runtime ----------------------------------------------------------
FROM alpine:3.20

RUN adduser -D -u 65532 chotki && mkdir -p /data && chown chotki:chotki /data

COPY --from=build /out/chotki-bench /usr/local/bin/chotki-bench

USER chotki:chotki
VOLUME ["/data"]

EXPOSE 4001 8001

ENV CHOTKI_DATA_DIR=/data \
    CHOTKI_LISTEN=0.0.0.0:4001 \
    CHOTKI_HTTP_LISTEN=0.0.0.0:8001

ENTRYPOINT ["/usr/local/bin/chotki-bench"]
