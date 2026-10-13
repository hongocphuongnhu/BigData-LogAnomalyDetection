---
title: "Hoàn thiện đồ án Log Anomaly Detection — chỉ demo bằng code (Định + Như)"
description: "Pipeline chạy được để demo: NASA thật + simulator có nhãn, feature Spark, KMeans + chấm điểm CBLOF, HDFS + Elasticsearch, Structured Streaming cảnh báo real-time, dashboard Kibana."
status: in-progress
priority: P1
effort: "còn khoảng 3–4h (phần tích hợp + tập demo)"
branch: dinh/batch (Định), nhánh riêng của Như; gộp vào main ở mốc S3
tags: [feature, infra, spark, elasticsearch, hdfs, streaming, ml, demo]
blockedBy: []
blocks: []
created: 2026-10-13
updated: 2026-10-13
---

# Hoàn thiện đồ án Log Anomaly Detection — chỉ demo bằng code

## Phạm vi (cập nhật 13/10)

- **Chỉ làm demo bằng code.** **Không** làm Word/PPT trong plan này (Như đã có Word, slide, PDF,
  video dự phòng). Phase 10 đã huỷ.
- **Chốt mô hình: KMeans + chấm điểm CBLOF.** Model, ngưỡng và hợp đồng giữa hai nhánh **đóng băng**
  từ đây. Muốn đổi gì thì phải báo trước ảnh hưởng lên nhánh kia và được cả hai đồng ý.

## Mô hình đã chốt: KMeans + CBLOF

1. **KMeans** (Spark MLlib) gom các cửa sổ (IP × 5 phút) thành nhóm hành vi. Trên simulator, k = 2:
   cụm lớn khoảng 99% là traffic bình thường, cụm nhỏ khoảng 1% gần như toàn tấn công.
2. **CBLOF** chấm điểm bất thường: cửa sổ thuộc cụm **lớn** thì đo khoảng cách tới tâm cụm của nó; cửa sổ
   thuộc cụm **nhỏ** thì đo khoảng cách tới **tâm cụm lớn gần nhất**. Điểm vượt ngưỡng thì cảnh báo.
3. **Ngưỡng** = phân vị 99 của điểm trên tập train (không dùng nhãn). Cửa sổ có < 3 request thì điểm = 0.

Vì sao không dùng "khoảng cách tới tâm cụm được gán": tấn công tự gom thành một cụm riêng nên nằm sát
tâm cụm của chính nó và bị chấm điểm thấp. Trên tập test, cách đó chỉ đạt F1 0,22 và bắt 17% DDoS;
CBLOF đạt F1 0,84 và bắt 100% DDoS. Cách cũ chỉ giữ trong bảng so sánh để giải thích lựa chọn.
Đề bài yêu cầu "phân cụm **hoặc** phát hiện bất thường"; KMeans + CBLOF đáp ứng cả hai vế.

**Artefact (đã có trên `origin/dinh/batch`):** `data/gold/models/kmeans_core` (PipelineModel) +
`data/reports/model_meta.json` (`threshold`, `large_clusters`, `centers`, `min_req_for_scoring`).
**Batch và streaming phải chấm điểm giống hệt nhau**, bằng đoạn code ở mục 6 của
[handoff S2](../handoffs/s2-dinh-batch-to-nhu-realtime-20261013-1736.md).

## Phân công

| | **Định** (Batch + Mô hình) | **Như** (Lưu trữ + Real-time) |
|---|---|---|
| Phase | 01, 02, 03, 04 (**xong**) | 05, 06, 07, 08 (gần xong, còn đồng bộ với CBLOF) |
| Phần demo | NASA thật, bảng so sánh mô hình, top IP bất thường NASA | Live: bắn DDoS/scan → cảnh báo hiện trên Kibana |
| Chung | 09: tích hợp trên máy demo + tập demo | 09 |

## Phases

| # | Phase | Owner | Phụ thuộc | Status |
|---|-------|-------|-----------|--------|
| 01 | [Nền tảng dùng chung & hợp đồng giao tiếp](./phase-01-shared-foundation.md) | Định + Claude | – | **Done** |
| 02 | [Ingest NASA log thật](./phase-02-nasa-ingest.md) | Định | 01 | **Done** |
| 03 | [Dữ liệu quy mô lớn + feature engineering](./phase-03-features-at-scale.md) | Định | 01 | **Done** |
| 04 | [Mô hình KMeans + CBLOF + đánh giá](./phase-04-anomaly-models.md) | Định | 03 | **Done** |
| 05 | [HDFS data lake](./phase-05-hdfs-storage.md) | Như | 01 | Gần xong: còn NASA + gold |
| 06 | [Export Elasticsearch](./phase-06-es-export.md) | Như | 01, 04 | Gần xong: còn NASA, features, alerts batch |
| 07 | [Streaming cảnh báo real-time](./phase-07-streaming-alerts.md) | Như | 04 | **Cần sửa: chấm điểm CBLOF** |
| 08 | [Dashboard Kibana + alert rule](./phase-08-kibana-dashboard.md) | Như | 06, 07 | Gần xong: còn đồng bộ schema batch |
| 09 | [Tích hợp + kịch bản demo](./phase-09-integration-demo.md) | Cả hai | 05–08 | Pending |
| 10 | [~~Báo cáo Word + slide~~](./phase-10-report-slides.md) | – | – | **Cancelled** (ngoài phạm vi) |

## Còn lại để demo chạy được (theo bảng đối chiếu của Như)

| # | Việc | Vì sao cần cho demo | Phase |
|---|---|---|---|
| D1 | Như merge `origin/dinh/batch`, kiểm tra `PipelineModel.load` chạy được trên máy Như | Model mới chỉ được thử trên máy Định | 07 |
| D2 | `stream_detect.py` chấm điểm **CBLOF** (truyền `large_clusters`, `min_req`, `threshold`, `severity` từ `model_meta.json`) | Nếu không, bắn DDoS sẽ gần như không có cảnh báo | 07 |
| D3 | Chạy `./scripts/run_batch.sh` trên **máy demo** để có silver/gold NASA + sim | `data/silver`, `data/gold` không có trong git | 06 |
| D4 | Export vào ES: `logs-nasa` (đang 0 dòng), `log-features`, `log-alerts` (batch) | Dashboard thiếu phần NASA và cảnh báo batch | 06 |
| D5 | Đồng bộ dashboard Kibana với cột batch mới (`score`, `ref_cluster`, `top_feature`, `severity`, `reason`, `is_alert`) | Như ghi là "cần đồng bộ schema"; panel so sánh nhãn chưa xong | 08 |
| D6 | (Tuỳ chọn) đưa NASA + gold lên HDFS | Minh chứng lưu trữ HDFS; local đã đủ để demo | 05 |
| D7 | Chốt **máy demo**, chạy thử từ đầu tới cuối và tập 2 lần, bấm giờ | Tránh lỗi lúc thuyết trình | 09 |

**Máy demo (đề xuất):** máy Như, vì đã có sẵn ES, Kibana, dashboard và streaming. Như làm D1–D5 trên máy mình.

## Quy tắc làm việc chung

- Mỗi người làm trên **nhánh riêng** (`dinh/batch`, nhánh của Như). **Không push `main`** khi cả hai
  chưa thống nhất (mốc S3).
- **Hỏi trước mỗi lần `git push`.**
- File dùng chung (`docker-compose.yml`, `jobs/features_common.py`, `.env.example`, `README.md`,
  `CLAUDE.md`) chỉ sửa sau khi báo người kia.
- Mọi job nhận đường dẫn qua tham số `--*-path` (mặc định local `/opt/spark/data/...`; HDFS
  `hdfs://namenode:9000/lake/...`).

| File / thư mục | Owner |
|---|---|
| `docker-compose.yml`, `.env.example`, `docker/spark/Dockerfile`, `scripts/submit.*`, `jobs/features_common.py` | Dùng chung |
| `jobs/ingest_nasa.py`, `jobs/ingest_http_access.py`, `jobs/build_features.py`, `jobs/detect_anomalies.py`, `analysis/`, `scripts/run_batch.sh`, `scripts/download_nasa.*` | Định |
| `scripts/hdfs_*`, `scripts/es_setup.*`, `jobs/export_to_es.py`, `jobs/stream_detect.py`, `generator/stream_simulate.py`, `kibana/`, `scripts/kibana_import.*` | Như |

## Rủi ro còn mở

| # | Rủi ro | Cách xử lý |
|---|---|---|
| R2 | RAM Docker khoảng 7,4GB cho 6 container | Tắt project Docker khác trên máy demo; worker 2g, ES heap 1g |
| R4 | Dữ liệu tấn công giả lập dễ tách (F1 tối ưu 0,999) | Nói rõ khi bị hỏi; NASA thật là phần đánh giá định tính |
| R5 | Streaming trễ do window/watermark | Window 5 phút, slide 1 phút, trigger 10s; đo độ trễ khi tập |
| R7 | Hai nhánh lệch hợp đồng (**đã xảy ra**: streaming vẫn chấm theo tâm cụm được gán) | Hợp đồng đóng băng; streaming dùng đúng đoạn chấm điểm trong handoff (D2) |
| R10 | Checkout trên Windows làm hỏng checksum `.crc` của model | Đã có `.gitattributes` (`data/gold/models/** binary`); nếu vẫn lỗi thì `./scripts/run_batch.sh models` |

Các rủi ro đã xử lý: R1 (thiếu numpy), R3 (connector ES), R6 (NASA host là `keyword`), R8 (key Kibana), R9 (train riêng từng nguồn).

## Success Criteria (demo)

- [x] Stack Spark + HDFS + ES + Kibana dựng được bằng `docker compose up -d --build`.
- [x] NASA Jul95 đã parse và làm sạch (1.891.709 dòng); có thống kê chất lượng dữ liệu.
- [x] Bảng so sánh Rule / KMeans (tâm cụm riêng) / KMeans + CBLOF / BisectingKMeans + CBLOF.
- [ ] ES có `logs-sim`, `logs-nasa`, `log-features`, `log-alerts` với dữ liệu batch mới (D3–D4).
- [ ] Streaming chấm điểm CBLOF; bắn DDoS → cảnh báo trên Kibana trong ≤ 60s, alert rule kích hoạt (D1–D2).
- [ ] Dashboard khớp cột batch mới; import lại được từ `kibana/dashboards.ndjson` (D5).
- [ ] Chạy demo từ đầu tới cuối trên máy demo, tập 2 lần (D7).

<!-- slug: log-anomaly-full-project -->
