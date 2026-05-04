#!/usr/bin/env bash
set -u

# Script lives at <workspace>/web/scripts/restart.sh — workspace root is two levels up.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="/tmp/logs"
PID_DIR="/tmp/.pids"
SERVICES=(api collector stream web)
# Static HTTP server for web/index.html. Anything that serves the directory
# works; python3 is universally present on macOS so it's the default.
WEB_PORT=5173

mkdir -p "$LOG_DIR" "$PID_DIR"

stop_service() {
    local name="$1"
    local pid_file="$PID_DIR/$name.pid"

    if [[ -f "$pid_file" ]]; then
        local pid
        pid=$(cat "$pid_file")
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            echo "[$name] stopping pid $pid (and children)"
            pkill -TERM -P "$pid" 2>/dev/null || true
            kill -TERM "$pid" 2>/dev/null || true
            for _ in {1..10}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.5
            done
            if kill -0 "$pid" 2>/dev/null; then
                echo "[$name] forcing kill"
                pkill -KILL -P "$pid" 2>/dev/null || true
                kill -KILL "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
    fi

    if [[ "$name" == "web" ]]; then
        # The static server is a python child of nohup; the patterns below
        # cover both the bare `python3 -m http.server` invocation and the
        # `--directory` flavor we start with.
        pkill -TERM -f "python3 -m http.server $WEB_PORT" 2>/dev/null || true
        sleep 1
        pkill -KILL -f "python3 -m http.server $WEB_PORT" 2>/dev/null || true
        return
    fi

    # `go run` produces a binary in $GOCACHE (~/Library/Caches/go-build/...) and
    # execs it directly, so the process command line is the cached path —
    # neither "go run", "cmd/$name/$name", nor "bin/$name" match. Also kill
    # by listening port and by the unambiguous "/$name --config" suffix.
    pkill -TERM -f "go run ./cmd/$name" 2>/dev/null || true
    pkill -TERM -f "cmd/$name/$name " 2>/dev/null || true
    pkill -TERM -f "bin/$name" 2>/dev/null || true
    pkill -TERM -f "/$name --config" 2>/dev/null || true
    sleep 1
    pkill -KILL -f "go run ./cmd/$name" 2>/dev/null || true
    pkill -KILL -f "cmd/$name/$name " 2>/dev/null || true
    pkill -KILL -f "bin/$name" 2>/dev/null || true
    pkill -KILL -f "/$name --config" 2>/dev/null || true
}

start_service() {
    local name="$1"
    local dir="$ROOT_DIR/$name"
    local log_file="$LOG_DIR/$name.log"
    local pid_file="$PID_DIR/$name.pid"

    if [[ ! -d "$dir" ]]; then
        echo "[$name] directory $dir not found, skipping"
        return
    fi

    if [[ "$name" == "web" ]]; then
        echo "[web] starting on :$WEB_PORT (logs: $log_file)"
        (
            nohup python3 -m http.server "$WEB_PORT" --directory "$dir" \
                >"$log_file" 2>&1 &
            echo $! >"$pid_file"
        )
        return
    fi

    echo "[$name] starting (logs: $log_file)"
    (
        cd "$dir" || exit 1
        nohup go run "./cmd/$name" --config configs/config.toml \
            >"$log_file" 2>&1 &
        echo $! >"$pid_file"
    )
}

for svc in "${SERVICES[@]}"; do
    stop_service "$svc"
done

for svc in "${SERVICES[@]}"; do
    start_service "$svc"
done

echo
echo "Service status:"
for svc in "${SERVICES[@]}"; do
    pid_file="$PID_DIR/$svc.pid"
    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "  [$svc] running, pid=$(cat "$pid_file")"
    else
        echo "  [$svc] NOT running"
    fi
done

echo
echo "Open http://localhost:$WEB_PORT in your browser."
