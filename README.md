# BigData-LogAnomalyDetection

Đồ án môn Nhập môn Big Data — **Phân tích dữ liệu nhật ký hệ thống để phát hiện bất thường**.

Pipeline: log HTTP (NASA thật + simulator có nhãn) → **HDFS** (data lake bronze/silver/gold)
→ **Spark** (làm sạch, trích xuất đặc trưng, KMeans) → **Elasticsearch** → **Kibana**
(dashboard thống kê lỗi + cảnh báo). Có thêm **Spark Structured Streaming** để cảnh báo real-time.

> README đang hoàn thiện dần theo plan tại [plans/261013-0915-log-anomaly-full-project/plan.md](plans/261013-0915-log-anomaly-full-project/plan.md).

## Yêu cầu

- Docker Desktop (WSL2), cấp ≥ 8GB RAM cho Docker
- Python 3.10+ trên máy host (chỉ để chạy simulator, thư viện chuẩn)

## Chạy nhanh (setup lần đầu)

```powershell
# 1. Cấu hình: copy rồi đặt KIBANA_ENCRYPTION_KEY thành chuỗi ngẫu nhiên >= 32 ký tự
Copy-Item .env.example .env

# 2. Build image Spark (có numpy/pandas + connector ES) và dựng stack
docker compose up -d --build

# 3. Sinh log mẫu + ingest bronze -> silver trên cluster Spark
python -m generator.simulate --minutes 30 --rate 2 --seed 7
.\scripts\submit.ps1 ingest_http_access.py

# 4. Kiểm tra toàn bộ: feature, rule, KMeans, HDFS, Elasticsearch -> in "SMOKE OK"
.\scripts\submit.ps1 smoke_test.py
```

Trên Git Bash / Linux / macOS, dùng `./scripts/submit.sh` thay cho `.\scripts\submit.ps1`.

## Giao diện

| Dịch vụ | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| Spark job đang chạy | http://localhost:4040 |
| HDFS NameNode UI | http://localhost:9870 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |

Nếu cổng bị project khác chiếm, đổi `SPARK_*_PORT` trong `.env`.

## Xử lý sự cố

- **`port is already allocated`**: đổi cổng trong `.env` hoặc tắt project Docker khác đang chạy.
- **Container bị kill / ES chết**: thiếu RAM. Giảm `SPARK_WORKER_MEMORY`, hoặc tăng `memory=` trong `%UserProfile%\.wslconfig`.
- **HDFS báo safe mode**: chờ khoảng 30s sau khi `namenode` khởi động.
