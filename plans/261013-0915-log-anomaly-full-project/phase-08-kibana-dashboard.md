---
phase: 8
title: "Phase 8: Dashboard Kibana + alert rule"
status: in-progress
priority: P1
effort: "2h"
owner: "Như"
dependencies: [6, 7]
---

# Phase 8: Dashboard Kibana + alert rule

## Goal

Dashboard thống kê lỗi và cảnh báo bất thường (đúng nguyên văn đề bài), có alert rule
kích hoạt thật, export được thành file để chạy lại trên máy khác.

## Files to Create / Modify

- Create: `kibana/dashboards.ndjson` (export saved objects)
- Create: `scripts/kibana_import.ps1` và `.sh` (`POST /api/saved_objects/_import?overwrite=true`)
- Create: `doc/images/kibana_*.png`

## Dashboard

**1. "Log Overview"** (filter theo `source`: sim / nasa):
- KPI: tổng request, tỉ lệ lỗi 4xx/5xx, số IP khác nhau, số cảnh báo.
- Request theo thời gian, tách theo nhóm status (2xx/3xx/4xx/5xx).
- Top 10 IP theo số lỗi 4xx; top path 404; phân bố status code.
- (sim) Tỉ lệ cache MISS và latency p95 theo thời gian.

**2. "Anomaly & Alerts"**:
- Bảng cảnh báo mới nhất (ip, window, score, severity, reason, source).
- Điểm bất thường theo thời gian, có đường ngưỡng.
- Phân bố cụm (cluster) và (sim) nhãn thật so với dự đoán.
- Auto-refresh 10s cho lúc demo live.

**Alert rule:** Stack Management → Rules → **Elasticsearch query** trên `log-alerts`,
điều kiện `count > 0` trong 1 phút gần nhất, kiểm tra mỗi 30s. Action dùng connector
**Server log** + **Index** (ghi vào `alert-history`), cả hai có trong bản basic miễn phí.
Cần encryption key đã đặt ở Phase 01.

## Tasks & Steps

1. Dựng các visualization bằng Lens, ghép thành 2 dashboard.
2. Tạo connector và rule; kích hoạt thử bằng `stream_simulate.py --attack scan`.
3. Export saved objects (dashboard, data view, rule nếu export được) ra `kibana/dashboards.ndjson`.
4. Chụp màn hình cho báo cáo và slide.

## Verification

- `docker compose down -v` → `up` → `es_setup` → export → `kibana_import`: dashboard hiển thị lại đầy đủ.
- Rule chuyển trạng thái "Active" và có bản ghi trong `alert-history` khi có tấn công.

## Cập nhật 13/10 (theo đối chiếu của Như)

Đã có: 2 dashboard, 18 panel, rule và lịch sử cảnh báo, import vào space mới đã kiểm chứng. Còn thiếu (D5):
- Đồng bộ các panel với cột batch mới (`score`, `ref_cluster`, `top_feature`/`reason`, `severity`, `is_alert`).
- Panel so sánh nhãn thật với dự đoán: dùng `is_anomaly` (nhãn thật, chỉ sim) so với `is_alert` (dự đoán).
- Sau khi sửa, export lại `kibana/dashboards.ndjson`.
