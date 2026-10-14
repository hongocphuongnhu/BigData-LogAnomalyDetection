# Thiết lập môi trường và HDFS trên máy Như

Ngày thực hiện: 13/10/2026. Source nền: `e71622f`.

## Trạng thái

Đã cập nhật `.env` theo phiên bản trong `.env.example`, giữ tùy chỉnh tài nguyên/cổng
của máy và tạo khóa Kibana. `.env` không được commit; không đưa khóa vào báo cáo.
Docker Desktop chạy Linux engine. Cả 6 service đã khởi động.

Đã chạy ingest local và smoke test toàn stack với dữ liệu simulator có sẵn:

| Kiểm tra | Kết quả |
|---|---|
| Bronze simulator | 9 file JSONL, 61.643 record |
| Silver local và silver HDFS | Mỗi bên 61.643 record |
| Dòng trùng / malformed JSON / vi phạm invariant trong lần ingest | 0 / 0 / 0 |
| So sánh nội dung local với HDFS | Không có dòng thiếu hoặc thừa |
| Timestamp null / partition giờ sai định dạng | 0 / 0 |
| Feature kiểm chứng | 13.309 cửa sổ, 16 feature |
| Smoke test | `SMOKE OK`: feature, rule, KMeans, HDFS và Elasticsearch |
| Elasticsearch `smoke-test` | 100 document |
| Kibana | API báo `available` |
| HDFS fsck cuối cùng | `HEALTHY`, 0 block thiếu replica, 0 block lỗi |
| Dung lượng bronze JSONL / silver Parquet | 31.989.953 / 5.192.880 byte (silver nhỏ hơn khoảng 6,16 lần) |

So sánh gồm 16 trường contract, `event_time`, `dt`, `hour`; bỏ qua `source_file`
và `ingested_at` vì hai lần ingest có đường dẫn/thời điểm khác nhau.
Số liệu chi tiết tại `data/reports/hdfs_verification.json`.
Kết quả fsck và dung lượng tại `data/reports/hdfs_fsck.txt`, `data/reports/hdfs_usage.txt`.

## Chạy lại từ PowerShell

Từ gốc repo, khi Docker Desktop đang chạy và `.env` đã cấu hình:

```powershell
docker compose up -d --build
.\scripts\submit.ps1 ingest_http_access.py
.\scripts\submit.ps1 smoke_test.py
.\scripts\hdfs_put.ps1 -Source sim
.\scripts\run_on_hdfs.ps1 -Source sim
```

Chỉ sinh dữ liệu nếu chưa có bronze:

```powershell
python -m generator.simulate --minutes 30 --rate 2 --seed 42
```

Generator thêm file mới mỗi lần chạy. Script upload cập nhật file cùng đường dẫn,
không xóa dữ liệu HDFS cũ; chạy lại upload cùng bộ bronze vẫn có 9 file trong
`/lake/bronze/http_access`, không tạo thêm thư mục `http_access` lồng nhau.
Trên Git Bash/Linux/macOS: `sh scripts/hdfs_put.sh sim`.

## Kiểm tra HDFS

```powershell
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs dfsadmin -safemode get
docker compose exec -T namenode hdfs dfs -ls -R /lake
docker compose exec -T namenode hdfs dfs -du -s /lake/bronze/http_access /lake/silver/http_access
docker compose exec -T namenode hdfs fsck /lake -files -blocks
```

Mở NameNode tại <http://localhost:9870>, Spark tại <http://localhost:8080>,
Kibana tại <http://localhost:5601>. HDFS có 1 DataNode, nên dùng replication=1.
`dfs.replication` là cấu hình phía client: cấu hình NameNode không tự đổi giá trị
mặc định 3 trong Spark. Vì vậy `run_on_hdfs.ps1` submit trực tiếp lên cùng cluster,
kèm `spark.hadoop.dfs.replication=1`, `features_common.py` và shuffle partitions=16.
Nếu chạy job HDFS bằng lệnh riêng, cần truyền cùng cấu hình replication này.

Output gold kiểm chứng nằm tại `/lake/gold/_verification/features_sim`.
Đây là feature kiểm tra lưu trữ; kết quả model chính thức của Định chưa có.
Smoke test cũng để lại `/lake/_smoke/features` và index ES `smoke-test`.

## Phần tiếp theo

Phần simulator của Phase 05 đã chạy được. Chưa đánh dấu toàn bộ Phase 05 hoàn thành:

- Chờ Định bàn giao NASA raw, `jobs/ingest_nasa.py` và `jobs/build_features.py`.
- Sau bàn giao, dùng `hdfs_put.ps1 -Source nasa` (hoặc `all`) và
  `run_on_hdfs.ps1 -Source nasa -BuildFeatures`; simulator cũng hỗ trợ `-BuildFeatures`.
- Chụp NameNode overview, Browse Directory `/lake` và thông tin block vào `doc/images/`.
- Bổ sung phần lý thuyết HDFS, số liệu dung lượng vào báo cáo.
- Có thể bắt đầu Phase 06: `scripts/es_setup.*` và `jobs/export_to_es.py` với silver simulator.

Không cần reset volume hoặc xóa data lake để thực hiện các bước trên.
Log kiểm tra và script đối chiếu tạm nằm trong `data/raw/setup/` (gitignored).
