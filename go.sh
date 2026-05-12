#!/bin/bash
TOKEN="$1"
while true; do
  curl -sL https://raw.githubusercontent.com/cloudSs-max/relay-tools/main/github_tunnel.py -o /tmp/github_tunnel.py
  python3 /tmp/github_tunnel.py server --token "$TOKEN"
  echo "[wrapper] Restarting in 2s..."
  sleep 2
done
