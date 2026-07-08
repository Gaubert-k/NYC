#!/bin/bash
# Cree l'arborescence Medallion sur HDFS
set -e
hdfs dfs -mkdir -p /data/bronze /data/silver /data/gold /data/lake /data/raw /spark-logs 2>/dev/null || true
hdfs dfs -chmod -R 777 /data /spark-logs 2>/dev/null || true
echo "HDFS directories ready:"
hdfs dfs -ls /data
