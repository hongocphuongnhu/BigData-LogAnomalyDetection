# Bàn giao phần Định — 13/10/2026

## Đã thực hiện và kiểm chứng

- Log Contract v1 (16 trường) và simulator có nhãn với 4 kiểu tấn công; tái lập được theo seed.
- Ingest NASA: 1.891.715 dòng đọc, 1.891.709 dòng vào silver, 6 dòng lỗi vào `_rejected`;
  giữ 835 dòng trùng thật, gắn cờ 19.721 dòng thiếu bytes. Báo cáo `data/reports/dq_nasa.json`.
- Ingest simulator: 1.065.283 dòng, 0 dòng trùng, 0 JSON lỗi, 0 vi phạm contract.
- Đặc trưng theo (IP × 5 phút) dùng chung với streaming qua `features_common.py`:
  238.295 cửa sổ simulator, 358.687 cửa sổ NASA.
- So sánh Rule / KMeans tâm riêng / KMeans + CBLOF / BisectingKMeans + CBLOF trên CORE và EXTENDED;
  bảng ở `data/reports/model_comparison.md`, hình ở `data/reports/figures/`.
- Model triển khai KMeans + CBLOF CORE, k = 2, ngưỡng 7,4929: P 0,720, R 0,998, F1 0,836 trên tập test.
- NASA: k = 3, 2.487 cửa sổ vượt ngưỡng, top alert và hồ sơ cụm trong `data/reports/metrics_nasa.json`.
- Báo cáo và slide phần Định: `doc/bao-cao-phan-dinh.md`, `doc/slide-phan-dinh.json`,
  bản dựng Word/PDF/PPTX bằng `scripts/build_dinh_documents.cjs`.

## Artifact đã bàn giao cho Như

- PipelineModel `data/gold/models/kmeans_core` và `data/reports/model_meta.json`
  (features đúng thứ tự CORE_FEATURES, window 5 phút, large_clusters, threshold, centers).
- `jobs/ingest_nasa.py`, `jobs/build_features.py`, `jobs/detect_anomalies.py`, `scripts/run_batch.sh`.
- Gold `features_{sim,nasa}`, `scores_{sim,nasa}`, `alerts_{sim,nasa}` trên máy Định.
- Handoff kỹ thuật: `plans/handoffs/s2-dinh-batch-to-nhu-realtime-20261013-1736.md`.

Model và hợp đồng chấm điểm đã đóng băng. Định không sửa `serving_common.py`, `stream_detect.py`,
dashboard hay script serving của Như; mọi thay đổi ở `features_common.py` phải báo trước.

## Trạng thái thực tế

- Số alert NASA khác nhau giữa hai máy: 2.487 (máy Định, `detect_anomalies.py`) và 2.423
  (máy Như, `score_demo_batch.py` fit lại cùng k = 3). Khi demo, dùng số của máy demo và nói rõ.
- Các log minh chứng của lượt demo (`data/logs/*-demo.log`, manifest `data/stream/qa-*`) chỉ có trên máy Như.
- Còn lại cho cả hai: chốt máy demo, chạy thử từ đầu đến cuối, tập nói và bấm giờ, thống nhất tỷ lệ đóng góp.
