---
phase: 1
title: "Phase 1: Nền tảng dùng chung & hợp đồng giao tiếp"
status: completed
priority: P1
effort: "1h"
owner: "Định + Claude"
dependencies: []
---

# Phase 1: Nền tảng dùng chung & hợp đồng giao tiếp — ĐÃ XONG

## Goal

Dựng xong toàn bộ hạ tầng (Spark, HDFS, ES, Kibana) và cố định giao diện giữa hai nhánh,
để từ đây Định và Như code song song mà không chặn nhau.

## Đã làm

- `docker-compose.yml`: thêm `namenode` + `datanode` (bde2020, Hadoop 3.2.1, replication 1,
  tắt permission), key mã hoá Kibana (bắt buộc cho Alerting), cổng Spark UI đổi được qua `.env`,
  cổng 7077 không publish ra host (spark-submit chạy trong container).
- `docker/spark/Dockerfile`: `apache/spark:3.5.8-java17-python3` + numpy/pandas/pyarrow
  (image gốc không có, **R1 đã xảy ra**) + jar `elasticsearch-spark-30_2.12` cùng version ES
  (`--packages` không resolve được, **R3 đã xảy ra**).
- `.env.example` (commit) / `.env` (gitignored).
- `jobs/features_common.py`: hợp đồng giao tiếp (bên dưới). `ingest_http_access.py` import schema từ đây;
  đã sửa lỗi `.add()` làm biến đổi schema dùng chung.
- `scripts/submit.ps1` và `scripts/submit.sh`: `submit <job.py> [args]` → spark-submit lên cluster, kèm `--py-files features_common.py`.
- `jobs/smoke_test.py`: kiểm tra feature, rule, KMeans + score, ghi/đọc HDFS, ghi ES. **Đã pass (`SMOKE OK`).**
- README chạy nhanh, CLAUDE.md, `.gitignore` (`data/raw`, `data/stream`; commit `data/reports`).

## Cấu hình chốt (`.env.example`)

```dotenv
SPARK_IMAGE_TAG=3.5.8-java17-python3
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=2g
ELASTIC_STACK_VERSION=8.19.14          # connector ES trong image build theo version này
ES_JAVA_OPTS=-Xms1g -Xmx1g
KIBANA_ENCRYPTION_KEY=<chuỗi ngẫu nhiên ≥ 32 ký tự>
HADOOP_IMAGE_TAG=2.0.0-hadoop3.2.1-java8
SPARK_MASTER_UI_PORT=8080   SPARK_WORKER_UI_PORT=8081   SPARK_DRIVER_UI_PORT=4040
```

## Hợp đồng giao tiếp (không đổi đơn phương)

**Cột chuẩn hoá ở silver** mà feature dựa vào:
`event_time` (timestamp UTC), `ip` (string), `method`, `path`, `status_code` (int),
`bytes_out` (int). Cột chỉ có ở simulator: `latency_ms`, `cache_status`,
`user_agent`, `is_anomaly`, `anomaly_label`.
→ **NASA ingest (Phase 02) phải xuất đúng tên cột này.**

**`jobs/features_common.py`:**

| Tên | Ý nghĩa |
|---|---|
| `HTTP_ACCESS_LOG_SCHEMA`, `TS_PATTERN` | Schema Log Contract v1 (dùng cho ingest + `readStream`) |
| `CORE_FEATURES` (11) | `n_req, burst_rate, n_distinct_paths, ratio_4xx, ratio_5xx, ratio_404, ratio_post, ratio_sus_path, ratio_static, mean_bytes, max_bytes` (có ở cả NASA và sim) |
| `EXTENDED_FEATURES` (16) | CORE + `ratio_401, ratio_cache_miss, mean_latency, p95_latency, ratio_script_ua` (chỉ sim) |
| `build_window_features(df, window="5 minutes", slide=None, extended=False)` | Gom theo `(ip, window)`. Chỉ dùng aggregate chạy được với streaming. Nếu df có nhãn thì thêm `is_anomaly` / `anomaly_label` cấp cửa sổ. Trả về `ip, window_start, window_end, <features>, [nhãn]` |
| `rules()`, `rule_score(df)` | Rule baseline → `rule_flag`, `rule_reason` (`flood, scan, many_errors, suspicious_path`) |
| `add_distance_score(df, centers, vec_col="scaled", cluster_col="cluster", out_col="score")` | Điểm = khoảng cách tới tâm cụm, tính trên JVM (không UDF), dùng được trong `foreachBatch` |
| `SILVER_SIM`, `SILVER_NASA`, `GOLD_ROOT`, `MODEL_DIR`, `MODEL_META_PATH`, `REPORTS_DIR`, `HDFS_ROOT` | Đường dẫn chuẩn |

**Artefact:**

| Artefact | Đường dẫn (trong container) | Người ghi | Người đọc |
|---|---|---|---|
| Silver simulator | `/opt/spark/data/silver/http_access` | Định | Như (ES) |
| Silver NASA | `/opt/spark/data/silver/nasa_access` | Định | Như (ES) |
| Features | `/opt/spark/data/gold/features_{sim,nasa}` | Định | Như (ES) |
| Scores / alerts | `/opt/spark/data/gold/{scores,alerts}_{sim,nasa}` | Định | Như (ES) |
| Model | `/opt/spark/data/gold/models/kmeans_core` (PipelineModel: VectorAssembler `raw` → StandardScaler `scaled` → KMeans `cluster`) | Định | Như (streaming) |
| Ngưỡng + metadata | `/opt/spark/data/reports/model_meta.json` (`features`, `window`, `k`, `threshold`, `centers`, `trained_at`) | Định | Như (streaming) |
| Metrics / hình | `data/reports/metrics.json`, `data/reports/figures/*.png` | Định | Báo cáo |

## Kết quả smoke test (30 phút sim, 4.247 dòng → 789 cửa sổ)

- Rule baseline bắt được brute_force, scan, sqli nhưng **bỏ sót 41/42 cửa sổ DDoS**. Lý do: botnet
  chia request ra nhiều IP nên mỗi IP < 20 request. Đây là điểm để model chứng minh giá trị.
- KMeans thô (k=4, chưa `log1p`) xếp các cửa sổ chỉ có 1 request (`n_req=1`) lên top → Phase 04
  cần biến đổi `log1p`, lọc hoặc gán trọng số cho cửa sổ quá ít request, và chọn k cho đúng.

## Verification (đã chạy)

- `docker compose ps`: 6 service đều lên; ES healthy; namenode/datanode healthy.
- `./scripts/submit.sh ingest_http_access.py`: 4.247 dòng vào silver.
- `./scripts/submit.sh smoke_test.py`: `SMOKE OK`; `curl localhost:9200/smoke-test/_count` → 100.
