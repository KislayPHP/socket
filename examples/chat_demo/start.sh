#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$ROOT/pids"
LOG_DIR="$ROOT/logs"
mkdir -p "$PID_DIR" "$LOG_DIR"

PHP_BIN="${PHP_BIN:-php}"
SOCKET_EXT="${SOCKET_EXT:-$ROOT/../../modules/kislayphp_socket.so}"

SOCKET_HOST="${SOCKET_HOST:-0.0.0.0}"
SOCKET_PORT="${SOCKET_PORT:-9200}"
WEB_HOST="${WEB_HOST:-127.0.0.1}"
WEB_PORT="${WEB_PORT:-9201}"

ensure_port_free() {
  local port="$1"
  if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port ${port} is already in use. Stop existing process first."
    exit 1
  fi
}

start_proc() {
  local name="$1"
  local cmd="$2"
  local pid_file="$PID_DIR/${name}.pid"
  local log_file="$LOG_DIR/${name}.log"

  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "${name} already running with PID $(cat "$pid_file")"
    return
  fi

  nohup bash -lc "$cmd" >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" > "$pid_file"
  sleep 0.3
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "Failed to start ${name}. Check ${log_file}"
    exit 1
  fi
  echo "Started ${name} (PID ${pid})"
}

ensure_port_free "$SOCKET_PORT"
ensure_port_free "$WEB_PORT"

start_proc "chat_server" \
  "${PHP_BIN} -d extension=${SOCKET_EXT} '${ROOT}/chat-server.php' ${SOCKET_PORT}"

start_proc "web" \
  "${PHP_BIN} -S ${WEB_HOST}:${WEB_PORT} -t '${ROOT}/public'"

echo
echo "Chat demo ready:"
echo "  Open http://${WEB_HOST}:${WEB_PORT} in a browser (open it twice to see room presence/chat live)"
echo "  Socket server on ${SOCKET_HOST}:${SOCKET_PORT}"
echo "  Stop with ./stop.sh"
