#!/bin/sh
# Entrypoint for an official Chotki repl replica.
#
# Steps:
#   1. Forward 0.0.0.0:$HTTP_PORT to 127.0.0.1:$INTERNAL_HTTP_PORT so the
#      repl's `servehttp` (hardcoded to localhost) is reachable from
#      outside the container.
#   2. Build the comma-separated repl command list from env vars. Listen
#      and connect addresses are always prefixed with tcp:// because
#      chotki's parseAddr uses url.Parse, which mis-tokenises a bare
#      "host:port" as a scheme.
#   3. Exec the chotki repl with that argv. The compose service should
#      enable tty/stdin_open so the repl's readline loop blocks on stdin
#      instead of EOF-ing immediately after the initial commands.
#
# Required env vars:
#   SRC                hex source id, e.g. "1"
#   LISTEN_ADDR        sync listen address, e.g. "0.0.0.0:4001"
#   HTTP_PORT          external HTTP port (socat front)
#   INTERNAL_HTTP_PORT internal HTTP port (servehttp)
#   PEERS              comma-separated "host:port" sync peers (may be empty)

set -eu

: "${SRC:?SRC is required}"
: "${LISTEN_ADDR:?LISTEN_ADDR is required}"
: "${HTTP_PORT:?HTTP_PORT is required}"
: "${INTERNAL_HTTP_PORT:?INTERNAL_HTTP_PORT is required}"
PEERS="${PEERS:-}"

cd /data

socat -d "TCP-LISTEN:${HTTP_PORT},fork,reuseaddr" \
      "TCP:127.0.0.1:${INTERNAL_HTTP_PORT}" &

# Pick `open` if there's already a chotki dir in /data, otherwise
# `create`. The repl uses `cho<srcHex>` as the directory name.
DIR="cho${SRC}"
if [ -d "/data/${DIR}" ]; then
    INIT="open ${SRC}-0"
else
    INIT="create ${SRC}-0"
fi

# Build argv: each comma-terminated token finishes one repl command.
# The repl rejoins everything before the comma with single spaces, so
# tokens with embedded literal quotes survive intact.
set -- ${INIT}, listen "\"tcp://${LISTEN_ADDR}\","

if [ -n "${PEERS}" ]; then
    OLDIFS="${IFS}"
    IFS=','
    for p in ${PEERS}; do
        # shellcheck disable=SC2086
        set -- "$@" connect "\"tcp://${p}\","
    done
    IFS="${OLDIFS}"
fi

set -- "$@" servehttp "${INTERNAL_HTTP_PORT}"

echo "chotki repl args: $*" >&2
exec chotki "$@"
