---
phase: 7
title: "Phase 7: Streaming cảnh báo real-time"
status: in-progress
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
  chấm điểm **CBLOF**, giống hệt batch (xem mục "Chấm điểm CBLOF" bên dưới).
  Ghi alert với `source="stream"`, `detected_at=current_timestamp()`.
  Checkpoint tại `/opt/spark/data/stream/_checkpoint`.

## Chấm điểm CBLOF (bắt buộc, chốt 13/10)

Streaming phải chấm điểm **giống hệt batch**, nếu không batch và stream sẽ cho kết luận trái ngược,
và lúc demo bắn DDoS sẽ không có cảnh báo. Khoảng cách tới tâm cụm được gán chỉ bắt 17% DDoS.

- `add_distance_score(model.transform(df), meta["centers"], feature_names=meta["features"],
  large_clusters=meta["large_clusters"])` — **phải truyền `large_clusters`**.
- `score = 0` khi `n_req < meta["min_req_for_scoring"]`.
- `is_alert = score > meta["threshold"]`; `severity`: ≥ 2× ngưỡng là `high`, ≥ 1,5× là `medium`, còn lại `low`; `reason = top_feature`.
- Code mẫu đầy đủ: mục 6 của `plans/handoffs/s2-dinh-batch-to-nhu-realtime-20261013-1736.md`.

## Tasks & Steps

1. Viết `stream_simulate.py`, chạy thử trên host: file xuất hiện đều trong `data/stream/incoming/`.
2. Viết `stream_detect.py` dùng rule fallback (chưa cần model). Chạy qua `submit.ps1`.
3. Merge `origin/dinh/batch`, kiểm tra `PipelineModel.load(MODEL_DIR)`, chuyển sang chấm điểm CBLOF như trên (việc D1, D2 trong plan).
4. Đo độ trễ từ lúc bắt đầu tấn công đến khi alert có trong ES; ghi vào báo cáo.
5. Viết đoạn lý thuyết Structured Streaming (micro-batch, watermark, window, checkpoint).

## Verification

- **Đối chiếu batch và stream:** đưa `gold/features_sim` qua hàm chấm điểm của streaming; số cửa sổ `is_alert` phải **bằng** số dòng của `gold/alerts_sim`.

- Chỉ chạy traffic bình thường 3 phút: ≤ 1–2 alert giả (ghi lại tỉ lệ false positive).
- `--attack ddos`: alert có `ip` thuộc botnet xuất hiện trong `log-alerts` trong ≤ 60s.
- Tắt rồi bật lại job: chạy tiếp từ checkpoint, không ghi trùng alert (nhờ upsert theo id).
