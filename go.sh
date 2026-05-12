#!/bin/bash
TOKEN="$1"
while true; do
  python3 /tmp/github_tunnel.py server --token "$TOKEN"
  echo "[wrapper] Restarting in 2s..."
  sleep 2
done
