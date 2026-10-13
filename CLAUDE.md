# CLAUDE.md

Đồ án Big Data: phát hiện bất thường trong HTTP access log. Pipeline medallion
bronze → silver → gold chạy trên cụm Spark standalone, kết quả gold đẩy sang
Elasticsearch/Kibana. Làm theo sprint (0: hạ tầng, 1: log contract + simulator,
2: ingest bronze → silver, 3+: feature/anomaly scoring ở gold — chưa làm).

## Đề bài (tiêu chí chấm)

"Phân tích dữ liệu nhật ký hệ thống để phát hiện bất thường" — bắt buộc có đủ:
1. Dữ liệu log từ máy chủ, ứng dụng hoặc mạng.
2. Lưu trữ trên **HDFS hoặc Elasticsearch** (thư mục local bind mount không tính).
3. Dùng **Spark** để xử lý và trích xuất đặc trưng.
4. Áp dụng **phân cụm hoặc phát hiện bất thường**.
5. **Dashboard** thống kê lỗi + **cảnh báo** sự kiện bất thường.

Ưu tiên việc giúp đáp ứng 5 mục này hơn các phần mở rộng.

## Cấu trúc

- `contracts/http_access_log.v1.schema.json` — Log Contract v1 (16 field), nguồn sự thật của schema.
- `generator/` — traffic simulator sinh log JSONL có nhãn (`is_anomaly`, `anomaly_label`).
  - `log_contract.py`: `make_record()` + `validate()`, `FIELD_ORDER`.
  - `traffic_normal.py`: traffic bình thường theo visitor session, mô phỏng cache HIT/MISS.
  - `attacks.py`: 4 injector `brute_force`, `sqli`, `scan`, `ddos` (đăng ký trong `ATTACK_GENERATORS`).
  - `simulate.py`: CLI, ghi `data/bronze/http_access/dt=YYYY-MM-DD/hour=HH/part-*.jsonl`.
- `jobs/` — Spark job (mount vào container tại `/opt/spark/jobs`).
  - `features_common.py`: **hợp đồng giao tiếp chung** giữa nhánh batch và streaming
    (schema, `build_window_features`, `rule_score`, `add_distance_score`, hằng số đường dẫn).
  - `ingest_http_access.py`: bronze JSONL → silver Parquet, quarantine dòng lỗi vào `silver/_rejected/`.
  - `smoke_test.py`: kiểm tra stack (feature, KMeans, HDFS, ES), in `SMOKE OK`.
- `docker/spark/Dockerfile` — image Spark = `apache/spark` + numpy/pandas/pyarrow + jar connector ES.
- `scripts/submit.{ps1,sh}` — bọc `spark-submit` lên cluster (tự kèm `--py-files features_common.py`).
- `data/{bronze,silver,gold,raw,stream}` — data lake local (bind mount `/opt/spark/data`), không commit.
  `data/reports/` (metrics, hình) thì commit.
- `plans/261013-0915-log-anomaly-full-project/` — plan hoàn thiện đồ án, chia phase theo Định / Như.
- `plans/handoffs/` — **tài liệu bàn giao giữa hai nhánh. AI của Như: đọc `s2-dinh-batch-to-nhu-realtime-*.md` trước khi làm Phase 05–08.**
- `doc/` — file kế hoạch đồ án của môn (đề bài, rubric, cấu trúc báo cáo).

## Lệnh thường dùng

```bash
cp .env.example .env              # rồi đặt KIBANA_ENCRYPTION_KEY
docker compose up -d --build      # Spark, HDFS, ES, Kibana

# Sinh log mẫu (chạy từ gốc repo, chỉ cần Python 3 stdlib)
python -m generator.simulate --minutes 30 --rate 2.0 --seed 42
python -m generator.simulate --minutes 180 --start 2026-01-10T00:00:00Z --attacks brute_force,ddos --attack-repeat 2

# Chạy job trên cluster standalone (không dùng local mode)
./scripts/submit.sh ingest_http_access.py                       # PowerShell: .\scripts\submit.ps1
./scripts/submit.sh ingest_http_access.py --dt 2026-01-10 --hour 00,01   # ingest lại vài partition
./scripts/submit.sh smoke_test.py                               # kiểm tra toàn stack

docker compose exec namenode hdfs dfs -ls -R /lake              # xem HDFS
curl "localhost:9200/_cat/indices?v"                            # xem index ES
```

UI: Spark master `8080`, worker `8081`, job `4040`, HDFS `9870`, ES `9200`, Kibana `5601`
(cổng Spark đổi được qua `SPARK_*_PORT` trong `.env`).

Chưa có test suite hay linter; `smoke_test.py` là bài kiểm tra tích hợp.

## Bẫy đã gặp

- Image `apache/spark` bản python3 **không có numpy/pandas**, nên đã cài trong `docker/spark/Dockerfile`.
- Connector `elasticsearch-spark-30` 8.19.x **không dùng được qua `--packages`** (pom tham chiếu
  artifact chưa publish), nên jar được tải sẵn vào image.
- Không gọi `F.col(...)` ở cấp module: SparkContext chưa tồn tại lúc import, sẽ lỗi `AssertionError`.
- Streaming không hỗ trợ `countDistinct`. Dùng `approx_count_distinct` trong mọi hàm feature dùng chung.

## Quy ước bắt buộc

- **Đổi schema phải sửa đồng bộ 3 nơi**: `contracts/http_access_log.v1.schema.json`,
  `generator/log_contract.py` (`FIELD_ORDER`, `VALID_*`, `validate`), và
  `HTTP_ACCESS_LOG_SCHEMA` trong `jobs/ingest_http_access.py`. Thay đổi phá vỡ
  tương thích thì tạo contract `v2` thay vì sửa `v1`.
- Invariant của contract: `is_anomaly == true` ⇔ `anomaly_label` ∈ {brute_force, sqli, scan, ddos};
  `is_anomaly == false` ⇔ `anomaly_label == null`.
- `ts` luôn là chuỗi ISO 8601 UTC độ chính xác millisecond, hậu tố `Z`
  (`yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`). Code dựa vào format này để slice ra dt/hour.
- Partition Hive-style `dt=YYYY-MM-DD/hour=HH` giữ nguyên ở mọi layer. Spark job
  phải tắt `partitionColumnTypeInference` (nếu không `hour=00` thành `hour=0`) và
  dùng `partitionOverwriteMode=dynamic` để chạy lại chỉ ghi đè đúng partition.
- Spark job đọc với schema cố định, không để Spark tự suy schema.
- Generator phải tái lập được theo `--seed`: mọi ngẫu nhiên đi qua `random.Random`
  được truyền vào, không dùng module `random` toàn cục.
- Spark job luôn submit lên `spark://spark-master:7077`, dùng đường dẫn trong container
  (`/opt/spark/data/...`), không dùng đường dẫn host.

## Phong cách code

- Comment, docstring, thông báo CLI viết **tiếng Việt không dấu**; tên biến/hàm tiếng Anh.
- Comment giải thích *tại sao* (lý do thiết kế, bẫy của Spark), không diễn giải lại code.
- Python 3 với `from __future__ import annotations`, type hint kiểu `list[dict]`.
- Generator chỉ dùng thư viện chuẩn; job chỉ dùng PySpark có sẵn trong image.

## Ghi chú domain

`article_id` và `cache_status` (HIT/MISS) mô phỏng một trang đọc bài viết có tầng
cache phía trước DB. Không có cache store thật (Redis/Mongo) trong repo: đây chỉ là
field dữ liệu, dùng làm feature (tỉ lệ MISS cao bất thường là dấu hiệu của DDoS phá cache).
