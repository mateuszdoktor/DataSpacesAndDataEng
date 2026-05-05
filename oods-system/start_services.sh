#!/bin/bash

pkill -f "uvicorn api:app"
pkill -f "graphql_gateway.py"

VENV_PYTHON="/home/mateusz/AGH/.venv/bin/python3"

echo "Starting Satellite A API on port 8001..."
(cd providers/satellite_A && $VENV_PYTHON -m uvicorn api:app --host 127.0.0.1 --port 8001) &
PID1=$!

echo "Starting Satellite B API on port 8002..."
(cd providers/satellite_B && $VENV_PYTHON -m uvicorn api:app --host 127.0.0.1 --port 8002) &
PID2=$!

echo "Starting Ground Station API on port 8003..."
(cd providers/ground_station && $VENV_PYTHON -m uvicorn api:app --host 127.0.0.1 --port 8003) &
PID3=$!

echo "Starting GraphQL Gateway on port 9000..."
($VENV_PYTHON gateway/graphql_gateway.py) &
PID4=$!

echo "All services started in background."
echo "PIDs: API A ($PID1), API B ($PID2), API G ($PID3), Gateway ($PID4)"
