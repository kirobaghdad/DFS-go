#!/bin/bash

# This script runs the DFS system and keeps all components alive.
# GUIs available at:
# Master Dashboard: http://localhost:8080
# Client GUI: http://localhost:8081

# Number of nodes from argument (default 4)
NUM_NODES=${1:-4}

# Cleanup function to kill all background processes
cleanup() {
    echo "Shutting down DFS..."
    kill $(jobs -p)
    exit
}

trap cleanup SIGINT SIGTERM

# Organize logs
mkdir -p logs
rm -rf logs/*

# Clean up any existing instances from previous runs
echo "Cleaning up existing DFS processes..."
pkill -f "./bin/master" || true
pkill -f "./bin/node" || true
pkill -f "./bin/client" || true
sleep 1

echo "Building components..."
go build -o bin/master ./master/master_tracker.go
go build -o bin/node ./node/data_keeper.go
go build -o bin/client ./client/client.go

echo "Starting Master Tracker (Port 50051, Dashboard 8080)..."
./bin/master > logs/master.log 2>&1 &
sleep 1

echo "Starting $NUM_NODES Data Keepers..."
for i in $(seq 1 $NUM_NODES); do
    NODE_ID="node$i"
    PORT=$((7000 + i))
    echo "  -> Starting $NODE_ID on port $PORT..."
    ./bin/node -id "$NODE_ID" -port "$PORT" > "logs/$NODE_ID.log" 2>&1 &
done

echo "Starting Client GUI (Port 8081)..."
./bin/client -master localhost:50051 -port 8081 > logs/client.log 2>&1 &

echo "--------------------------------------------------"
echo "SYSTEM READY ($NUM_NODES Nodes)"
echo "Master Dashboard: http://localhost:8080"
echo "Client Interface: http://localhost:8081"
echo "Logs Directory: ./logs/"
echo "--------------------------------------------------"
echo "Press Ctrl+C to stop the system."

# Keep the script running
while true; do sleep 1; done
