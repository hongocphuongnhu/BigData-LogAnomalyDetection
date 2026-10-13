#!/usr/bin/env sh
# Bao spark-submit len cluster standalone trong container spark-master.
#   ./scripts/submit.sh <job.py> [tham so cua job...]
# Connector Elasticsearch da co san trong image (docker/spark/Dockerfile).
set -e
cd "$(dirname "$0")/.."
[ -n "$1" ] || { echo "Cach dung: $0 <job.py> [args...]" >&2; exit 2; }

# sh -c + "$@" giu nguyen tham so co dau cach khi chuyen vao container.
exec docker compose exec -T spark-master sh -c '
  job="$0"
  exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.sql.shuffle.partitions=16 \
    --py-files /opt/spark/jobs/features_common.py \
    "/opt/spark/jobs/$job" "$@"
' "$@"
