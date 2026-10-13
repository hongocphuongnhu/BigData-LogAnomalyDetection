---
phase: 4
title: "Phase 4: Mô hình phát hiện bất thường + đánh giá"
status: completed
priority: P1
effort: "2.5h"
owner: "Định"
dependencies: [3]
---

# Phase 4: Mô hình phát hiện bất thường + đánh giá

## Goal

So sánh 3 phương pháp trên cùng bảng feature, có số đo định lượng, rồi lưu model
cho nhánh streaming. Đây là tiêu chí 2.5 điểm.

## Files to Create / Modify

- Create: `jobs/detect_anomalies.py`
- Create: `analysis/model_report.py` (chạy trên host, vẽ hình)
- Output: `data/gold/models/kmeans_core/`, `data/reports/model_meta.json`, `data/reports/metrics.json`,
  `data/gold/{scores,alerts}_{sim,nasa}`

## Phương pháp

| Phương pháp | Cách làm | Điểm bất thường |
|---|---|---|
| Rule baseline | `features_common.rule_score`: ngưỡng trên `n_req`, `ratio_4xx`, `ratio_404`, `n_distinct_paths` | 0/1 |
| KMeans | Pipeline `log1p` → VectorAssembler → StandardScaler → KMeans; chọn k ∈ [2..8] theo silhouette | Khoảng cách Euclid tới tâm cụm được gán |
| BisectingKMeans | Cùng pipeline | Như trên |

- Tính khoảng cách **không dùng Python UDF**: `vector_to_array(scaled)` rồi cộng dồn
  `(x_i - c_i)^2` bằng biểu thức SQL, với tâm cụm đưa vào dưới dạng literal. Cách này chạy hoàn toàn trên JVM.
- Huấn luyện **không giám sát** (không đưa nhãn vào). Nhãn chỉ dùng để đánh giá.
  Chia train/test 70/30 theo thời gian (6–7 giờ cuối làm test).
- Ngưỡng: (a) phân vị 97 của điểm trên tập train (không cần nhãn, dùng trong thực tế);
  (b) ngưỡng tối ưu F1 trên tập test (để tham khảo). Báo cả hai.
- Hai biến thể feature: CORE và EXTENDED, để chứng minh feature nào đóng góp.
- NASA: fit riêng (không có nhãn). Báo top 20 IP/cửa sổ bất thường kèm lý do
  (feature lệch nhiều nhất theo z-score) để phân tích định tính.

## Tasks & Steps

1. Viết `detect_anomalies.py --source sim --features core|extended --algo kmeans|bisecting|rule`
   (hoặc chạy cả 3 phương pháp trong một lần chạy rồi ghi một file metrics).
2. Metrics ở cấp cửa sổ: precision, recall, F1, PR-AUC (`BinaryClassificationEvaluator`
   với `rawPredictionCol = score`), confusion matrix, recall theo từng `anomaly_label`, silhouette.
3. Lưu `PipelineModel` của KMeans CORE vào `gold/models/kmeans_core`. Lưu
   `model_meta.json = {features, window, k, threshold, centers, trained_at}`.
   **Mốc S2: báo Như.**
4. Ghi `gold/scores_*` (mọi cửa sổ + score + cluster) và `gold/alerts_*`
   (score > ngưỡng, `severity` theo bội số ngưỡng, `reason`).
5. `analysis/model_report.py` vẽ: bảng so sánh, đường PR, confusion matrix, recall theo
   từng loại tấn công, PCA 2D tô màu theo cụm/nhãn, đồ thị elbow/silhouette theo k.
6. Viết đoạn nhận xét và hạn chế (dữ liệu tổng hợp dễ tách, lệch phân phối với NASA,
   cửa sổ cố định bỏ sót tấn công chậm).

## Verification

- `metrics.json` có đủ 3 phương pháp × 2 bộ feature.
- `PipelineModel.load(...)` trong container chạy được (Như sẽ dùng).
- Có ít nhất 6 hình trong `data/reports/figures/`.
- Nếu F1 ≈ 1.0 thì ghi rõ lý do trong báo cáo (rủi ro R4), không giấu.

## Kết quả (đã chạy)

**Thay đổi thiết kế so với plan:** cách chấm điểm "khoảng cách tới tâm cụm của chính nó" thất bại
(F1 0,22). Silhouette chọn k = 2 và **toàn bộ tấn công tự gom thành một cụm nhỏ** (1.363 cửa sổ,
100% là tấn công), nên các cửa sổ tấn công nằm sát tâm cụm của mình. Chuyển sang **CBLOF**
(He và cộng sự, 2003): điểm của cửa sổ thuộc cụm nhỏ = khoảng cách tới tâm cụm lớn gần nhất. Ngưỡng
đổi từ p97 sang **p99** của điểm trên tập train (không dùng nhãn).

Tập test sim (30% thời gian cuối: 71.080 cửa sổ, 568 tấn công), bảng đầy đủ ở `data/reports/model_comparison.md`:

| Phương pháp | P | R | F1 | PR-AUC | Recall DDoS |
|---|---|---|---|---|---|
| Rule | 1,000 | 0,368 | 0,538 | – | 0,34 |
| KMeans, tâm cụm riêng | 0,221 | 0,211 | 0,216 | 0,157 | 0,17 |
| **KMeans + CBLOF (core)**: model cho streaming | 0,720 | 0,998 | 0,836 | 0,998 | 1,00 |
| BisectingKMeans + CBLOF (core) | 0,752 | 0,998 | 0,858 | 0,998 | 1,00 |
| KMeans + CBLOF (extended) | 0,751 | 0,998 | 0,857 | 0,998 | 1,00 |

F1 tối ưu trên test (có dùng nhãn, chỉ để tham khảo) là 0,999, nên dữ liệu giả lập tách được gần như hoàn toàn (R4).

**NASA** (k = 3, 2.487 cảnh báo): cụm 1 (1.215 cửa sổ) có tỉ lệ 404 là 53%, tức nhóm "link hỏng / dò quét".
Cảnh báo số 1 là `163.205.1.45` (dải IP của KSC), 03/07 14:40–14:55, chuỗi lỗi 5xx + POST; khớp với đỉnh 5xx
trên hình EDA. Hạn chế: feature gần như hằng số 0 (`ratio_5xx`, `ratio_post`, `ratio_sus_path`) có độ lệch chuẩn
rất nhỏ, sau StandardScaler thì điểm bị khuếch đại lên hàng trăm.

**Artefact cho Như (S2):** `data/gold/models/kmeans_core` (PipelineModel có sẵn bước log1p) và
`data/reports/model_meta.json` (`threshold`, `large_clusters`, `centers`). Streaming phải gọi
`add_distance_score(..., large_clusters=meta["large_clusters"])`.

**Hình** (theo skill dataviz, bảng màu đã qua validator): `model_comparison`, `model_recall_by_attack`,
`model_silhouette_by_k`, `model_pr_curve`, `model_score_distribution`, `model_pca_clusters`,
`model_pca_attacks`, `nasa_top_anomalies` + 6 hình EDA NASA đã vẽ lại theo cùng style.
