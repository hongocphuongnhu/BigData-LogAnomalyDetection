#!/usr/bin/env sh
# Chay toan bo nhanh batch (Dinh) tu dau: du lieu -> silver -> gold -> model.
#   ./scripts/run_batch.sh            # tat ca buoc
#   ./scripts/run_batch.sh sim        # chi lam lai simulator (ingest + features) roi mo hinh
#   ./scripts/run_batch.sh models     # chi chay lai tu buoc mo hinh
# Moi buoc ghi log day du vao data/logs/<buoc>.log, man hinh chi in tom tat.
set -e
cd "$(dirname "$0")/.."
mkdir -p data/logs
FROM="${1:-all}"

step() {
  name=$1; shift
  start=$(date +%s)
  # </dev/null: docker compose exec doc stdin, neu khong se "an" mat phan con lai
  # cua script khi chay non-interactive.
  if ./scripts/submit.sh "$@" > "data/logs/$name.log" 2>&1 </dev/null; then rc=0; else rc=$?; fi
  echo "== $name: rc=$rc ($(( $(date +%s) - start ))s)"
  grep -a -v -e " INFO " -e " WARN " -e "^\s*at " -e "^[ +:]*[+-]- " -e "MapPartitionsRDD" \
    "data/logs/$name.log" | tail -n "${TAILN:-15}"
  [ "$rc" -eq 0 ] || { echo "Buoc $name loi, xem data/logs/$name.log" >&2; exit "$rc"; }
}

if [ "$FROM" = "all" ]; then
  ./scripts/download_nasa.sh
  [ -d data/bronze/http_access ] || python -m generator.simulate --minutes 1440 --rate 12 \
      --attack-repeat 30 --start 2026-01-10T00:00:00Z --seed 42
  step ingest_nasa ingest_nasa.py
  step ingest_sim ingest_http_access.py
  step features_nasa build_features.py --source nasa
  step features_sim build_features.py --source sim
elif [ "$FROM" = "sim" ]; then
  step ingest_sim ingest_http_access.py
  step features_sim build_features.py --source sim
fi
TAILN=80 step detect_sim detect_anomalies.py --source sim
TAILN=40 step detect_nasa detect_anomalies.py --source nasa
echo "Xong. Ve hinh: python analysis/eda_nasa.py && python analysis/model_report.py"
