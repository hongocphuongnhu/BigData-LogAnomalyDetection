---
phase: 7
title: "Phase 7: Streaming cảnh báo real-time"
status: todo
priority: P1
effort: "2.5h"
owner: "Như"
dependencies: [1, 4]
---

# Phase 7: Streaming cảnh báo real-time

## Goal

Spark Structured Streaming đọc log mới liên tục, tính feature, chấm điểm bằng model của
Phase 04 và ghi cảnh báo vào ES trong ≤ 60s. Đây là phần demo live.

## Files to Create / Modify

- Create: `generator/stream_simulate.py`
- Create: `jobs/stream_detect.py`

## Thiết kế

```
stream_simulate.py ──(JSONL mỗi 5s)──> data/stream/incoming/
                                           │ readStream.json(schema cố định, maxFilesPerTrigger)
                                           ▼
                              withWatermark(event_time, 2 min)
                              build_window_features(window 5 min, slide 1 min)
                                           │ foreachBatch (outputMode update, trigger 10s)
                                           ▼
             PipelineModel kmeans_core + threshold (model_meta.json)
             (fallback: rule_score nếu chưa có model)
                                           │ score > threshold
                                           ▼
                           ES log-alerts (upsert theo source-ip-window_start)
```

- `stream_simulate.py`: dùng lại `generator.traffic_normal` và `generator.attacks` với timestamp
  `now()`. Ghi file tạm rồi rename (để Spark không đọc file đang ghi dở). Tham số `--rate`,
  `--attack ddos|scan|brute_force|sqli`, `--attack-after 60` (giây).
- `stream_detect.py`: load model **một lần** ở driver; trong `foreachBatch` gọi transform rồi
  tính khoảng cách (dùng chung hàm với Phase 04, nên để hàm trong `features_common.py`).
  Ghi alert với `source="stream"`, `detected_at=current_timestamp()`.
  Checkpoint tại `/opt/spark/data/stream/_checkpoint`.

## Tasks & Steps

1. Viết `stream_simulate.py`, chạy thử trên host: file xuất hiện đều trong `data/stream/incoming/`.
2. Viết `stream_detect.py` dùng rule fallback (chưa cần model). Chạy qua `submit.ps1`.
3. Sau S2: chuyển sang model thật, kiểm tra ngưỡng.
4. Đo độ trễ từ lúc bắt đầu tấn công đến khi alert có trong ES; ghi vào báo cáo.
5. Viết đoạn lý thuyết Structured Streaming (micro-batch, watermark, window, checkpoint).

## Verification

- Chỉ chạy traffic bình thường 3 phút: ≤ 1–2 alert giả (ghi lại tỉ lệ false positive).
- `--attack ddos`: alert có `ip` thuộc botnet xuất hiện trong `log-alerts` trong ≤ 60s.
- Tắt rồi bật lại job: chạy tiếp từ checkpoint, không ghi trùng alert (nhờ upsert theo id).
