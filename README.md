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

## Phần serving và real-time của Như

Sau khi dựng Docker stack, chạy một lệnh để upload HDFS, ingest simulator,
export Elasticsearch và import dashboard/rule:

```powershell
.\scripts\run_all.ps1 -Serve
# Linux / Git Bash: sh scripts/run_all.sh --serve
```

Script tái sử dụng bronze sẵn có; nếu chưa có sẽ sinh mẫu. Những artifact NASA,
scores và alerts batch chưa được Định bàn giao được liệt kê rõ ở cuối lệnh.
Không lấy model kiểm thử làm model của đồ án. `--batch` dành cho script
`scripts/run_batch.ps1` / `.sh` của Định khi được bàn giao.

Chạy demo live trong hai terminal từ gốc repo:

```powershell
# Terminal 1: giữ job chạy cho đến khi kết thúc demo
.\scripts\submit.ps1 stream_detect.py

# Terminal 2: traffic thường trước, DDoS bắt đầu sau 20 giây
python -m generator.stream_simulate --attack ddos --attack-after 20 --duration 120
```

Mở [Log Overview](http://localhost:5601/app/dashboards#/view/nhu-log-overview) và
[Anomaly & Alerts](http://localhost:5601/app/dashboards#/view/nhu-anomaly-alerts).
Dashboard batch lưu time range theo dữ liệu lịch sử; dashboard live refresh 10 giây.
Native Kibana rule chạy mỗi 30 giây và ghi action vào `alert-history`.
`detector=rule_fallback` cho biết chưa dùng model chính thức. Khi nhận model và
metadata đúng hợp đồng, `auto` dùng model ở lần khởi động job tiếp theo;
`--detector model` buộc báo lỗi nếu thiếu hoặc sai artifact.

```powershell
.\scripts\kibana_import.ps1
python -B scripts/verify_realtime.py --attack ddos
python -B scripts/verify_realtime.py --attack scan
python -B scripts/verify_dashboard_import.py
.\scripts\submit.ps1 verify_serving_contract.py
```

Kết quả đã kiểm chứng trên simulator: 61.643 document ES, 0 cảnh báo trong bài thử
180 giây traffic thường; DDoS 18,33 giây, scan 12,295 giây tới alert đầu tiên.
Restart checkpoint giữ nguyên số alert. Đây là bài thử nhỏ với rule fallback,
không phải đánh giá F1/PR-AUC của model. Xem các file JSON trong `data/reports/`.

Tài liệu bàn giao: [hướng dẫn HDFS](doc/nhu-setup-hdfs.md),
[báo cáo phần Như](doc/BaoCao_PhanNhu.docx), [slide phần Như](doc/Slide_PhanNhu.pptx),
[video live dự phòng](doc/demo-live.webm), [kịch bản demo dự thảo](doc/demo-script.md).

Lưu ý vận hành: HDFS trên một DataNode cần `spark.hadoop.dfs.replication=1` ở client;
`run_on_hdfs.ps1` đã truyền cấu hình này. Không cần reset volume hoặc xóa checkpoint
để demo; dùng cặp input/checkpoint mới nếu muốn một phiên dữ liệu riêng.
