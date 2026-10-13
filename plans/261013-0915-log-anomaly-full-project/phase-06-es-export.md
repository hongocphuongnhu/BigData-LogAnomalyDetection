---
phase: 6
title: "Phase 6: Export Elasticsearch"
status: in-progress
priority: P1
effort: "1.5h"
owner: "Như"
dependencies: [1]
---

# Phase 6: Export Elasticsearch

## Goal

Đẩy log đã làm sạch, feature, điểm bất thường và cảnh báo vào Elasticsearch với mapping
đúng kiểu, để Kibana dựng dashboard.

## Files to Create / Modify

- Create: `scripts/es_setup.ps1` và `.sh` (tạo index template)
- Create: `jobs/export_to_es.py`

## Index

| Index | Nguồn | `es.mapping.id` (idempotent) |
|---|---|---|
| `logs-sim` | silver/http_access | `trace_id` |
| `logs-nasa` | silver/nasa_access | hash của (ip, event_time, path) hoặc để ES tự sinh |
| `log-features` | gold/scores_{sim,nasa} (+ cột `source`) | `source-ip-window_start` |
| `log-alerts` | gold/alerts_* (batch) + streaming (Phase 07) | `source-ip-window_start` |

Mapping: `event_time`/`window_start` kiểu `date`; `ip` kiểu `keyword` (NASA là hostname, xem R6);
`path` kiểu `keyword` + `text`; `status_code` kiểu `short`; score và ratio kiểu `float`;
`anomaly_label`, `cluster`, `severity`, `source` kiểu `keyword`.

## Tasks & Steps

1. `es_setup`: `PUT _index_template/logs-template` và `alerts-template` bằng `curl`; tắt
   `refresh_interval` khi bulk rồi bật lại sau.
2. `export_to_es.py --what logs-sim|logs-nasa|features|alerts --input-path ...`:
   `df.write.format("org.elasticsearch.spark.sql")` với `es.nodes=elasticsearch`,
   `es.port=9200`, `es.nodes.wan.only=true`, `es.mapping.id=...`, `es.write.operation=index`.
   Chạy bằng `scripts/submit.ps1` (connector đã có sẵn trong image).
3. Trước S1: thử với silver sim nhỏ từ smoke test Phase 01. Sau S1/S2 thì đẩy dữ liệu đầy đủ.
4. Tạo data view trong Kibana (qua API `POST /api/data_views/data_view` trong `es_setup`)
   cho cả 4 index, trường thời gian là `event_time` / `window_start`.

## Verification

- `curl "localhost:9200/_cat/indices?v"`: `docs.count` khớp với số dòng Parquet (chạy lại không nhân đôi).
- Kibana Discover xem được dữ liệu của cả 4 data view.
- Ghi thời gian bulk 1,9 triệu dòng NASA (số liệu hiệu năng cho báo cáo). Nếu > 15 phút thì chỉ đẩy mẫu 20%.

## Cập nhật 13/10 (theo đối chiếu của Như)

Đã có: script mapping/export; `logs-sim` có 61.643 dòng. Còn thiếu (D3, D4 trong plan):
1. Trên **máy demo**, chạy `./scripts/run_batch.sh` (Git Bash, khoảng 15 phút) để có `silver/nasa_access`, `gold/scores_*`, `gold/alerts_*`.
2. Export `logs-nasa` (hiện 0 dòng), `log-features` từ `gold/scores_{sim,nasa}`, `log-alerts` từ `gold/alerts_{sim,nasa}`.
   Cột batch: `ip, window_start, window_end, <11 CORE feature>, [sim: 5 EXTENDED + is_anomaly, anomaly_label],
   source, rule_reason, rule_flag, cluster, ref_cluster, score, top_feature, threshold, is_alert, severity, reason, dt`.
   Mapping: thêm `ref_cluster` (keyword hoặc integer), `top_feature` / `reason` / `severity` (keyword), `is_alert` (boolean).
