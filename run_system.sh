#!/usr/bin/env bash

set -euo pipefail

# Single-machine smoke test launcher for the DFS project.
# For the real assignment demo across two Linux devices, use the commands in docs/DFS_Guide.md.

NUM_NODES="${1:-3}"

cleanup() {
    echo
    echo "Shutting down DFS..."
    pids="$(jobs -p || true)"
    if [[ -n "${pids}" ]]; then
        kill ${pids} 2>/dev/null || true
    fi
}

trap cleanup EXIT SIGINT SIGTERM

mkdir -p bin logs
rm -rf logs/*

echo "Cleaning up previous local DFS processes..."
pkill -f "./bin/master" || true
pkill -f "./bin/node" || true
pkill -f "./bin/client" || true
sleep 1

echo "Building components..."
go build -o bin/master ./master/master_tracker.go
go build -o bin/node ./node/data_keeper.go
go build -o bin/client ./client/client.go

echo "Starting master tracker on :50051 (dashboard :8080)..."
./bin/master > logs/master.log 2>&1 &
sleep 1

echo "Starting ${NUM_NODES} local data keeper(s)..."
for i in $(seq 1 "${NUM_NODES}"); do
    node_id="node${i}"
    port=$((7000 + i))
    echo "  -> ${node_id} on TCP ${port} / gRPC $((port + 1000))"
    ./bin/node -id "${node_id}" -port "${port}" -master localhost:50051 > "logs/${node_id}.log" 2>&1 &
done

echo "Starting client UI on :8081..."
./bin/client -master localhost:50051 -port 8081 > logs/client.log 2>&1 &

echo
echo "Local smoke-test environment is ready."
echo "  Master dashboard: http://localhost:8080"
echo "  Client UI:        http://localhost:8081"
echo "  Logs:             ./logs/"
echo
echo "This script is for one-machine verification only."
echo "Press Ctrl+C to stop every local process."

while true; do
    sleep 1
done
