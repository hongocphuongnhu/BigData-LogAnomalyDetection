#!/usr/bin/env sh
# Dua du lieu local len HDFS; chay lai khong xoa data lake.
set -eu
# Git Bash tren Windows khong duoc doi /lake, /data thanh duong dan host.
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
source_name="${1:-sim}"
case "$source_name" in
  sim|nasa|all) ;;
  *) echo "Cach dung: sh scripts/hdfs_put.sh [sim|nasa|all]" >&2; exit 2 ;;
esac

if [ "$source_name" != nasa ]; then
  if [ ! -d data/bronze/http_access ] || [ -z "$(find data/bronze/http_access -type f -name '*.jsonl' -print -quit)" ]; then
    echo "Chua co bronze simulator. Chay python -m generator.simulate truoc." >&2
    exit 1
  fi
fi
if [ "$source_name" != sim ] && [ ! -f data/raw/nasa/NASA_access_log_Jul95.gz ]; then
  echo "Chua co NASA_access_log_Jul95.gz. Doi Phase 02 hoac dung nguon sim." >&2
  exit 1
fi

hdfs() { docker compose exec -T namenode hdfs "$@"; }
hdfs dfs -mkdir -p /lake/bronze /lake/raw/nasa /lake/silver /lake/gold
if [ "$source_name" != nasa ]; then
  hdfs dfs -put -f /data/bronze/http_access /lake/bronze/
fi
if [ "$source_name" != sim ]; then
  hdfs dfs -put -f /data/raw/nasa/NASA_access_log_Jul95.gz /lake/raw/nasa/
fi
hdfs dfs -ls /lake
echo "HDFS upload OK ($source_name)."
