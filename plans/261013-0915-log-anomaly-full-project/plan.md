---
title: "Hoàn thiện đồ án Log Anomaly Detection trong 1 ngày (Định + Như)"
description: "Đưa repo từ Sprint 2 lên sản phẩm nộp được: dữ liệu NASA thật, feature Spark, KMeans/BisectingKMeans so với rule baseline, HDFS + Elasticsearch, Structured Streaming cảnh báo real-time, dashboard Kibana, báo cáo và slide."
status: pending
priority: P1
effort: 24h (≈12h mỗi người, chạy song song)
branch: main
tags: [feature, infra, spark, elasticsearch, hdfs, streaming, ml]
blockedBy: []
blocks: []
created: 2026-10-13
---

# Hoàn thiện đồ án Log Anomaly Detection (deadline: 1 ngày)

## Overview

Hiện repo đã có hạ tầng (Spark + ES/Kibana), Log Contract v1, simulator có nhãn và job
ingest bronze → silver. Còn thiếu mọi thứ chiếm điểm lớn trong rubric: mô hình (2.5đ),
trực quan/ứng dụng (2.5đ), lưu trữ HDFS/ES (một phần 2.0đ), báo cáo/slide.

Plan chia làm 2 nhánh code song song, mỗi người một nhánh và một phần demo:

- **Định — Batch + Mô hình**: dữ liệu NASA thật, feature engineering, rule vs KMeans vs
  BisectingKMeans, đánh giá F1/PR-AUC, biểu đồ kết quả.
- **Như — Lưu trữ + Real-time**: HDFS, export Elasticsearch, Spark Structured Streaming
  sinh cảnh báo, dashboard Kibana + alert rule.

Hai nhánh nối với nhau qua **một hợp đồng giao tiếp** (Phase 01) để không chặn nhau.

## Đối chiếu đề bài / rubric

| Yêu cầu (docx) | Phase đáp ứng | Rubric |
|---|---|---|
| Log từ máy chủ / ứng dụng / mạng | 02 (NASA thật), 03 (simulator có nhãn) | CLO1, CLO2 |
| Lưu trữ HDFS **hoặc** ES — làm cả hai | 05 (HDFS = data lake), 06 (ES = serving) | CLO2 (2.0) |
| Spark xử lý + trích xuất đặc trưng | 02, 03, 07 | CLO2, CLO3 |
| Phân cụm / phát hiện bất thường + đánh giá, so sánh, hạn chế | 04 | CLO3 (2.5) |
| Dashboard thống kê lỗi + cảnh báo bất thường | 07, 08 | CLO3 (2.5) |
| Làm sạch: thiếu, trùng, ngoại lệ, không nhất quán | 02, 03 | CLO2 |
| Mã nguồn, hướng dẫn chạy, báo cáo đúng cấu trúc, slide, phân công | 09, 10 | CLO4, CLO5 |

## Phân công tổng quát

| | **Định** (Batch + Mô hình) | **Như** (Lưu trữ + Real-time) |
|---|---|---|
| Phase code | 01 (cùng Claude), 02, 03, 04 | 05, 06, 07, 08 |
| Phase chung | 09, 10 | 09, 10 |
| Ước lượng | ≈ 11h | ≈ 12h |
| Demo | Batch: NASA, bảng so sánh mô hình, IP bất thường thật | Live: bắn DDoS/scan → cảnh báo hiện trên Kibana |
| Báo cáo | Dữ liệu & tiền xử lý, lý thuyết thuật toán, kết quả & so sánh, hạn chế | Giới thiệu, kiến trúc & lý do chọn công nghệ, lý thuyết HDFS/ES/Streaming, dashboard, hướng dẫn chạy |

## Phases

| # | Phase | Owner | Effort | Phụ thuộc | Status |
|---|-------|-------|--------|-----------|--------|
| 01 | [Nền tảng dùng chung & hợp đồng giao tiếp](./phase-01-shared-foundation.md) | Định + Claude | 1h | – | **Done** |
| 02 | [Ingest NASA log thật](./phase-02-nasa-ingest.md) | Định | 1.5h | 01 | **Done** |
| 03 | [Dữ liệu quy mô lớn + feature engineering](./phase-03-features-at-scale.md) | Định | 1.5h | 01 | **Done** |
| 04 | [Mô hình phát hiện bất thường + đánh giá](./phase-04-anomaly-models.md) | Định | 2.5h | 03 | Pending |
| 05 | [HDFS data lake](./phase-05-hdfs-storage.md) | Như | 1.5h | 01 | Pending |
| 06 | [Export Elasticsearch](./phase-06-es-export.md) | Như | 1.5h | 01 | Pending |
| 07 | [Streaming cảnh báo real-time](./phase-07-streaming-alerts.md) | Như | 2.5h | 01 (04 để dùng model thật) | Pending |
| 08 | [Dashboard Kibana + alert rule](./phase-08-kibana-dashboard.md) | Như | 2h | 06, 07 | Pending |
| 09 | [Tích hợp, README, kịch bản demo](./phase-09-integration-demo.md) | Cả hai | 1.5h | 02–08 | Pending |
| 10 | [Báo cáo Word + slide](./phase-10-report-slides.md) | Cả hai | ≈3h/người, rải suốt ngày | – | Pending |

## Lịch trong ngày (H0 = lúc bắt đầu)

```
        H0   H1        H2.5      H4        H5.5     H6.5      H8.5     H10        H13
Định    |-01-|---02----|---03----|------04---------|--10 kết quả--|---09---|---10 + tập demo--|
Như     |-10-|---05----|---06----|--------07--------|----08--------|---09---|---10 + tập demo--|
                                 ^S1               ^S2                      ^S3
```

- **S1 (~H4)**: Định bàn giao silver/gold của simulator + `features_common.py` ổn định.
  Như export features lên ES và chạy streaming bằng rule fallback.
- **S2 (~H6)**: Định lưu model + `model_meta.json`. Như chuyển streaming sang model thật.
- **S3 (~H10)**: freeze code. Chỉ sửa bug, còn lại dồn vào báo cáo, slide, tập demo.

**Danh sách cắt nếu trễ ở H8** (theo thứ tự): BisectingKMeans → biến thể extended
features → NASA trên ES (chỉ đẩy 10% mẫu) → HDFS cho streaming (để local).
Không cắt: rule vs KMeans + đánh giá, ES, streaming alert, dashboard.

## Quy tắc làm việc chung

- **Sở hữu file tách biệt** (bảng dưới). Làm thẳng trên `main`, `git pull --rebase`
  trước khi push, commit nhỏ, mỗi người commit bằng tài khoản của mình (để chứng minh
  phân công cho tiêu chí CLO5).
- File dùng chung (`docker-compose.yml`, `jobs/features_common.py`, `.env.example`,
  `README.md`) chỉ sửa sau khi báo người kia.
- Mọi job nhận đường dẫn qua tham số `--*-path` (mặc định là local `/opt/spark/data/...`)
  để Như chạy lại trên `hdfs://namenode:9000/lake/...` mà không sửa code của Định.

| File / thư mục | Owner |
|---|---|
| `docker-compose.yml`, `.env.example`, `docker/spark/Dockerfile`, `scripts/submit.*`, `jobs/features_common.py` | Phase 01 (Định + Claude), sau đó dùng chung |
| `jobs/ingest_nasa.py`, `jobs/ingest_http_access.py`, `jobs/build_features.py`, `jobs/detect_anomalies.py`, `analysis/`, `scripts/download_nasa.*` | Định |
| `scripts/hdfs_*`, `scripts/es_setup.*`, `jobs/export_to_es.py`, `jobs/stream_detect.py`, `generator/stream_simulate.py`, `kibana/`, `scripts/kibana_import.*` | Như |
| `README.md`, `doc/demo-script.md`, `doc/` báo cáo + slide | Chung (theo mục) |

## Red-team: rủi ro chính và cách xử lý

| # | Rủi ro | Xác suất | Cách xử lý |
|---|---|---|---|
| R1 | Image `apache/spark` thiếu `numpy` → `pyspark.ml` lỗi khi import | **Đã xảy ra, đã xử lý** | `docker/spark/Dockerfile` cài numpy/pandas/pyarrow |
| R2 | WSL2 mặc định chỉ cấp ~50% RAM (≈7.6GB) cho 7 container | Cao | Worker 2–3g, ES heap 1g; tắt HDFS khi không dùng; nếu cần thì tăng `memory=` trong `%UserProfile%\.wslconfig` |
| R3 | `--packages` connector ES 8.19.x không resolve được (pom lỗi trên Maven) | **Đã xảy ra, đã xử lý** | Jar connector được tải sẵn vào image Spark lúc build |
| R4 | Dữ liệu tấn công tổng hợp quá dễ tách → F1 ≈ 1.0, giảng viên nghi ngờ | Cao | Báo cáo trung thực; so core vs extended features; thêm biến thể khó (brute force chậm) nếu còn giờ; nói rõ trong phần hạn chế |
| R5 | Streaming có watermark và window → cảnh báo trễ, demo bị chờ lâu | Trung bình | Sliding window 5 phút, slide 1 phút, trigger 10s, simulator dùng thời gian hiện tại; tập trước để biết độ trễ thực tế |
| R6 | Cột `host` của NASA là hostname, không phải IP | Chắc chắn | Map ES field `ip` kiểu `keyword` (không dùng kiểu `ip`) |
| R7 | Hai nhánh lệch giao diện (tên cột, đường dẫn model) | Trung bình | Hợp đồng cố định ở Phase 01; đổi thì phải báo người kia |
| R8 | Kibana alerting không bật được nếu thiếu encryption key | Chắc chắn nếu quên | Đặt `XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY` ngay ở Phase 01 |
| R9 | Model huấn luyện trên dữ liệu simulator áp lên NASA bị lệch phân phối | Cao | Huấn luyện riêng cho từng nguồn; chỉ đánh giá định lượng trên simulator, NASA đánh giá định tính (top IP bất thường) |

## Success Criteria

- [ ] `docker compose up -d` dựng đủ Spark, HDFS, ES, Kibana trên máy sạch theo README.
- [ ] NASA Jul95 (~1,89 triệu dòng) đã parse; có thống kê số dòng lỗi, trùng, thiếu bytes.
- [ ] Bảng so sánh rule / KMeans / BisectingKMeans có precision, recall, F1, PR-AUC và recall theo từng loại tấn công.
- [ ] Data lake chạy được trên HDFS; ES có 4 index `logs-sim`, `logs-nasa`, `log-features`, `log-alerts`.
- [ ] Demo live: bắn DDoS → cảnh báo hiện trên dashboard Kibana trong ≤ 60s, alert rule kích hoạt.
- [ ] Dashboard export ra `kibana/dashboards.ndjson`, import lại được.
- [ ] Báo cáo Word đúng cấu trúc trong docx, slide, bảng phân công; nộp lên Classroom.

## Ghi chú về NoSQL

Đề bài không yêu cầu NoSQL. `cache_status` chỉ là field giả lập (cache của reverse proxy)
và được dùng làm feature. Elasticsearch đóng vai trò kho lưu trữ document cho phần serving.

<!-- slug: log-anomaly-full-project -->
