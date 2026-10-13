---
phase: 2
title: "Phase 2: Ingest NASA log thật"
status: todo
priority: P1
effort: "1.5h"
owner: "Định"
dependencies: [1]
---

# Phase 2: Ingest NASA log thật

## Goal

Đưa bộ NASA HTTP access log tháng 7/1995 (~1,89 triệu dòng, Apache Common Log Format)
vào silver đã làm sạch, kèm thống kê chất lượng dữ liệu cho báo cáo.

## Files to Create / Modify

- Create: `scripts/download_nasa.ps1` và `.sh` → tải về `data/raw/nasa/NASA_access_log_Jul95.gz`
  từ `https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz` (đã kiểm tra tải được)
- Create: `jobs/ingest_nasa.py`
- Create: `analysis/eda_nasa.py` (chạy trên host, dùng pandas + pyarrow + matplotlib)

## Tasks & Steps

1. Tải dataset (khoảng 20MB nén). Ghi nguồn và giấy phép vào README/báo cáo.
2. `ingest_nasa.py`:
   - `spark.read.text(...gz)` rồi `repartition(16)` ngay. File gzip không chia nhỏ được,
     nên chỉ 1 task đọc được; đây là một điểm Big Data đáng nêu trong báo cáo.
   - Regex CLF: `^(\S+) \S+ \S+ \[([^\]]+)\] "(.*?)" (\d{3}) (\S+)\s*$`.
     Tách request thành `method path protocol`; request không đủ 3 phần → `malformed_request`.
   - `event_time = to_timestamp(ts, "dd/MMM/yyyy:HH:mm:ss Z")`, chuyển về UTC.
     Nếu parse lỗi thì đặt `spark.sql.legacy.timeParserPolicy=LEGACY`.
   - `bytes = "-"` → `bytes_out = 0`, `bytes_missing = true`.
   - Dòng không khớp regex → `silver/_rejected/nasa_access` với `reject_reason`.
   - Đếm dòng trùng tuyệt đối (giữ lại vì là traffic thật, chỉ báo cáo số lượng);
     ngoại lệ: `bytes_out` lớn bất thường, status hiếm.
   - Cột ra theo hợp đồng Phase 01: `event_time, ip(=host), method, path, protocol, status_code, bytes_out, bytes_missing, dt, hour`.
   - Ghi Parquet partition `dt/hour`, giữ `partitionColumnTypeInference=false` và `partitionOverwriteMode=dynamic`.
   - Tham số `--raw-path`, `--silver-path`, `--rejected-path`.
   - In và ghi `data/reports/dq_nasa.json`: tổng dòng, hợp lệ, reject theo lý do, thiếu bytes, trùng, số host khác nhau, khoảng thời gian.
3. `analysis/eda_nasa.py` đọc silver Parquet trên host và ghi ra `data/reports/figures/`:
   request theo giờ, phân bố status, top 20 host, top path 404, histogram bytes (log scale).

## Verification

- Tổng dòng đọc ≈ 1.891.715; tỉ lệ reject < 0,1%.
- `dq_nasa.json` và 5 hình tồn tại.
- Spark UI (8080) có job hoàn thành, chạy trên worker (không phải local mode); chụp màn hình cho báo cáo.
