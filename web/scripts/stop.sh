#!/usr/bin/env bash
set -u

PID_DIR="/tmp/.pids"
SERVICES=(api collector stream)

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
        else
            echo "[$name] pid file present but process not running"
        fi
        rm -f "$pid_file"
    else
        echo "[$name] no pid file"
    fi

    # `go run` execs a binary out of $GOCACHE, so the process command line is
    # the cached path — match by listening port flag and unambiguous suffixes.
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

for svc in "${SERVICES[@]}"; do
    stop_service "$svc"
done

echo
echo "Service status:"
for svc in "${SERVICES[@]}"; do
    pid_file="$PID_DIR/$svc.pid"
    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "  [$svc] STILL running, pid=$(cat "$pid_file")"
    else
        echo "  [$svc] stopped"
    fi
done
