---
phase: 5
title: "Phase 5: HDFS data lake"
status: in-progress
priority: P1
effort: "1.5h"
owner: "Như"
dependencies: [1]
---

# Phase 5: HDFS data lake

## Goal

Data lake bronze/silver/gold chạy được trên HDFS, đúng nguyên văn đề bài "lưu trữ trên HDFS".
Local disk chỉ dùng khi dev.

## Files to Create / Modify

- Create: `scripts/hdfs_put.ps1` và `.sh` (đưa bronze + NASA raw lên HDFS)
- Create: `scripts/run_on_hdfs.ps1` (chạy các job của Định với tham số `hdfs://namenode:9000/lake/...`)
- Create: `doc/images/hdfs_*.png` (ảnh chụp màn hình cho báo cáo)

## Tasks & Steps

1. Kiểm tra namenode/datanode (đã dựng ở Phase 01): UI `localhost:9870` → Datanodes = 1 live.
2. `hdfs_put`:
   ```
   docker compose exec namenode hdfs dfs -mkdir -p /lake/bronze /lake/raw/nasa /lake/silver /lake/gold
   docker compose exec namenode hdfs dfs -put -f /data/bronze/http_access /lake/bronze/
   docker compose exec namenode hdfs dfs -put -f /data/raw/nasa/NASA_access_log_Jul95.gz /lake/raw/nasa/
   ```
3. Chạy lại ingest sim bằng `--bronze-path hdfs://namenode:9000/lake/bronze/http_access
   --silver-path hdfs://namenode:9000/lake/silver/http_access ...`. Sau Phase 02/03 thì
   làm tương tự với NASA và build_features.
4. Thử nghiệm cho báo cáo: `hdfs fsck /lake -files -blocks` (block size 128MB, replication=1),
   so sánh dung lượng JSONL và Parquet (`hdfs dfs -du -h /lake/*`). Đây là số liệu tốt cho phần "tính chất Big Data".
5. Chụp màn hình: NameNode overview, Browse Directory `/lake`, thông tin block.
6. Viết đoạn lý thuyết HDFS (NameNode/DataNode, block, replication) và lý do chọn
   (HDFS = data lake rẻ, ghi một lần; ES = phục vụ truy vấn và dashboard).

## Verification

- `docker compose exec namenode hdfs dfs -ls -R /lake | head` thấy partition `dt=/hour=`.
- Ingest trên HDFS in ra cùng số dòng như khi chạy local.
- Nếu job đọc HDFS lỗi thì kiểm tra `fs.defaultFS` và `datanode` có trong network `lakenet`.

## Cập nhật 13/10

Đã có script upload/chạy job và ảnh minh chứng; local và HDFS khớp 61.643 dòng. Còn (D6, tuỳ chọn cho demo):
đưa `raw/nasa`, `silver/nasa_access`, `gold/*` lên HDFS. Local đã đủ để demo chạy được.
