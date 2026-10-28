---
phase: 6
title: "Phase 06: Việt hóa tiêu đề Kibana"
status: todo
priority: P2
effort: "0.5h"
dependencies: [5]
---

# Phase 06: Việt hóa tiêu đề Kibana

> Như đã đồng ý ngày 29/10. Đây là phase duy nhất sửa file của Như, nên làm xong thì gửi Như xem diff.

## Mục tiêu

Đổi tiêu đề dashboard và panel trong `scripts/kibana_setup.py` sang tiếng Việt, để iframe ở Phase 05 cũng là tiếng Việt.

## Ràng buộc

- **Giữ nguyên mọi ID** (`nhu-*`, `view-*`), vì console và `kibana/dashboards.ndjson` tham chiếu theo ID.
- **Không đổi tên rule `Nhu - New anomaly detected`**, vì `verify_realtime.py` tìm rule theo đúng tên này (rủi ro R5).
- Chỉ đổi các chuỗi `title`, `label`, `name` hiển thị. Ví dụ: "Requests" → "Số request",
  "HTTP error rate" → "Tỷ lệ lỗi HTTP", "Anomalous windows" → "Cửa sổ bất thường",
  "Score and threshold" → "Điểm bất thường và ngưỡng", "Log Overview" → "Tổng quan log",
  "Anomaly & Alerts" → "Bất thường & cảnh báo".

## Kiểm chứng

- `python -B scripts/kibana_setup.py` chạy OK, `kibana/dashboards.ndjson` được xuất lại.
- `python -B scripts/verify_dashboard_import.py` vẫn qua.
- `python -B scripts/verify_realtime.py --attack ddos` vẫn **Đạt**.
- Diff chỉ gồm chuỗi hiển thị (soát lại bằng `git diff scripts/kibana_setup.py`).
