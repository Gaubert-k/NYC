#!/usr/bin/env bash
# Attend que HDFS et MongoDB soient operationnels apres docker compose up
set -euo pipefail

echo "Attente services (HDFS, MongoDB)..."
for _ in $(seq 1 45); do
  if docker exec nyc-namenode hdfs dfs -ls / >/dev/null 2>&1 \
     && docker exec nyc-mongodb mongosh --quiet --eval "db.adminCommand('ping').ok" 2>/dev/null | grep -q 1; then
    echo "Services prets."
    exit 0
  fi
  sleep 2
done

echo "Timeout: services non prets apres 90s" >&2
exit 1
