---
phase: 6
title: "Phase 06: Việt hóa tiêu đề Kibana"
status: done
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

## Kết quả (29/10)

- 20 tiêu đề panel/dashboard/saved search + tên data view sang tiếng Việt; ID và tên rule giữ nguyên.
- Thêm `customLabel: True` cho cột metric: thiếu cờ này Lens bỏ qua nhãn và hiện "Count of records".
- `scripts/verify_dashboard_import.py` tìm lens theo tiêu đề "Ground truth vs prediction (simulator)" → đổi chuỗi tìm
  thành tiêu đề mới (file thứ hai của Như bị sửa, chỉ một chuỗi). Sau đó import vào space mới: 2 dashboard, 18 tham chiếu, OK.
- `kibana_setup.py` chạy OK, `kibana/dashboards.ndjson` xuất lại. Kiểm tra DDoS qua console **sau** khi cập nhật rule: Đạt, độ trễ 18,3 giây, 105 = 105, lịch sử 4 → 5.
- Cần gửi Như diff `scripts/kibana_setup.py` + `scripts/verify_dashboard_import.py`.
