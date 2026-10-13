---
handoff-version: 1
generated: 2026-10-13T10:36:00Z
generator: ak:handoff@2.0.0
focus: "S2: ban giao model + hop dong tu nhanh dinh/batch (Dinh) sang AI cua Nhu lam Phase 05-08"
workspace: BigData-LogAnomalyDetection (repo GitHub hongocphuongnhu/BigData-LogAnomalyDetection)
branch: dinh/batch
head: 334df3d (+ commit them file handoff nay)
---

# HANDOFF: S2 — model + hợp đồng từ nhánh dinh/batch sang nhánh Real-time của Như

> Đọc cho: AI (Claude Code) của Như, và Như. Trước khi làm gì, đối chiếu mục **Current state**
> với repo thật (`git log --oneline -5 origin/dinh/batch`).

## Mission and current status

Focus: "S2: bàn giao model + hợp đồng từ nhánh dinh/batch (Định) sang AI của Như làm Phase 05–08".

Mục tiêu của Như (theo `plans/261013-0915-log-anomaly-full-project/plan.md`): Phase 05 HDFS, 06 export
Elasticsearch, 07 Spark Structured Streaming sinh cảnh báo, 08 dashboard Kibana + alert rule. Hạn nộp là
**khoảng 1 ngày** kể từ 13/10/2026.

Done (nhánh `dinh/batch`, Phase 01–04):
- Phase 01 (cũng có trên `main`): Spark + HDFS + ES + Kibana trong docker-compose, image Spark riêng,
  `jobs/features_common.py` (hợp đồng giao tiếp), `scripts/submit.*`, `jobs/smoke_test.py`.
- Phase 02: `jobs/ingest_nasa.py`, đưa log NASA thật (1.891.709 dòng) vào silver.
- Phase 03: `jobs/build_features.py`, tính feature cửa sổ (ip × 5 phút) vào gold; ingest simulator được siết kiểm tra.
- Phase 04: `jobs/detect_anomalies.py`, so sánh Rule / KMeans / BisectingKMeans, chấm điểm **CBLOF**.
  **Model đã train** ở `data/gold/models/kmeans_core` + `data/reports/model_meta.json` (có trong git).

Remaining (việc của Như): Phase 05, 06, 07, 08; sau đó Phase 09 (tích hợp, demo) và 10 (báo cáo) làm cùng Định.

Urgency: cao. Phần demo live (Phase 07–08) là phần gây ấn tượng nhất và đang chưa có.

## Scope and guardrails

Workspace: repo này. Chạy trên Windows + Docker Desktop (WSL2).

In scope (file Như sở hữu): `scripts/hdfs_*`, `scripts/es_setup.*`, `jobs/export_to_es.py`,
`jobs/stream_detect.py`, `generator/stream_simulate.py`, `kibana/`, `scripts/kibana_import.*`,
mục báo cáo phần Như.

Out of scope (file Định sở hữu, **không sửa**): `jobs/ingest_nasa.py`, `jobs/ingest_http_access.py`,
`jobs/build_features.py`, `jobs/detect_anomalies.py`, `analysis/`, `scripts/run_batch.sh`, `scripts/download_nasa.*`.

File dùng chung (**chỉ sửa sau khi báo Định**): `docker-compose.yml`, `jobs/features_common.py`,
`.env.example`, `README.md`, `CLAUDE.md`.

Constraints (người dùng đặt ra):
- **Không push lên `main`** khi chưa được cả hai đồng ý. Mỗi người làm trên nhánh riêng:
  Định dùng `dinh/batch`, Như dùng `nhu/realtime` (hoặc tên khác Như chọn). Chỉ gộp vào `main` ở mốc S3.
- **Hỏi người dùng trước mỗi lần `git push`**, kể cả push nhánh riêng.
- Gọi skill phù hợp khi cần (dataviz cho biểu đồ, docx/pptx cho báo cáo/slide…).
- Comment/docstring trong code viết **tiếng Việt không dấu**; tên biến/hàm tiếng Anh (xem `CLAUDE.md`).

Safety boundaries: không force-push; không `git reset --hard` trên nhánh dùng chung; không commit `.env`
(chứa `KIBANA_ENCRYPTION_KEY`).

## Current state

Branch: `dinh/batch` (đã có trên GitHub: `origin/dinh/batch`).
HEAD lúc viết: `334df3d` "Mark Spark model files as binary…", sau đó có thêm commit chứa file handoff này.
Lịch sử gần nhất:
`334df3d` .gitattributes cho model → `9f0eaf9` thêm model → `4630418` Phase 04 CBLOF → `91ee138` Phase 02–04
→ `904cfe9` revert (trên `main`) → `b2ac483` (Phase 02–03, bị revert khỏi `main`) → `e71622f` Phase 01.

**Lưu ý lịch sử `main`:** trên `main` có commit `b2ac483` rồi commit revert `904cfe9` của chính nó. Nhánh
`dinh/batch` được tạo **trên** `904cfe9`, nên khi gộp `dinh/batch` vào `main` hoặc vào nhánh của Như thì merge
sạch, không cần "revert of revert".

Working tree (máy Định): clean.
Changed files / Untracked files: không có.
Intentional local modifications: no.

Dữ liệu **không có trong git** (vì `data/` bị ignore): `data/raw`, `data/bronze`, `data/silver`, `data/gold`
(trừ `data/gold/models/kmeans_core`). Có trong git: `data/reports/` (metrics, model_meta, hình).

## Decisions and rationale

| Decision | Rationale | Alternative rejected | Reference |
|---|---|---|---|
| Chấm điểm bằng **CBLOF** | Tấn công tự gom thành một cụm nhỏ riêng, nên "khoảng cách tới tâm cụm của chính nó" rất thấp (F1 0,22). CBLOF đo tới tâm **cụm lớn** gần nhất: F1 0,84, recall DDoS 1,00 | Khoảng cách tới tâm cụm của chính nó | `phase-04-anomaly-models.md`, `data/reports/metrics_sim.json` |
| Bước `log1p` nằm **trong** PipelineModel (stage `SQLTransformer`) | Streaming chỉ cần `PipelineModel.load(...).transform(...)` là tiền xử lý khớp với lúc train | Code biến đổi riêng trong từng job | `features_common.model_input_sql` |
| Ngưỡng = phân vị 99 của điểm trên tập train | Không cần nhãn, dùng được ngoài thực tế; tỉ lệ cảnh báo giả khoảng 0,3% | p97 (gắn cờ 3% cửa sổ, precision thấp) | `model_meta.json: threshold` |
| Cửa sổ có `n_req < 3` thì điểm = 0 | 1 request 404 khiến `ratio_404` = 1 → nhiễu | Chấm điểm mọi cửa sổ | `features_common.MIN_REQ_FOR_SCORING` |
| Model streaming = KMeans + CBLOF trên **CORE features**, train từ simulator | Simulator cùng định dạng log với stream; CORE có ở mọi nguồn | Train trên NASA (lệch phân phối với stream) | `model_meta.json: trained_on` |
| Commit model vào git (`git add -f`, 62KB) + `.gitattributes` đánh dấu binary | Như dùng được ngay; tránh CRLF làm hỏng checksum `.crc` | Như tự train lại (khoảng 15 phút) | commit `9f0eaf9`, `334df3d` |
| Connector ES đã có sẵn **trong image Spark** | `--packages` của connector 8.19.x không resolve được (pom lỗi) | `--packages` / `ivy_cache` | `docker/spark/Dockerfile` |

## Work performed

- Batch pipeline đã chạy hết trên máy Định: `./scripts/run_batch.sh` (download NASA → ingest NASA/sim → features → models).
  - Simulator: `python -m generator.simulate --minutes 1440 --rate 12 --attack-repeat 30 --start 2026-01-10T00:00:00Z --seed 42`
    cho 1.065.283 dòng (brute_force 1.484, sqli 807, scan 1.230, ddos 25.461).
  - `gold/features_sim` có 238.295 cửa sổ; `gold/features_nasa` có 358.687 cửa sổ.
  - Model sim: k=2, `large_clusters=[0]`, `threshold≈7.49`. Model NASA: k=3, 2.487 cảnh báo.
- Ngoài file trong `jobs/`, hàm dùng chung đã thêm/đổi trong `jobs/features_common.py` (chỉ thêm mới, tương thích ngược):
  `LOG_FEATURES`, `MIN_REQ_FOR_SCORING`, `model_input_columns`, `model_input_sql`,
  `LARGE_CLUSTER_MIN_FRAC`, `large_clusters_from_sizes`, và `add_distance_score(..., feature_names=, large_clusters=)`.
  Hàm này thêm cột `score`, `ref_cluster`, `top_feature`.
- 0 redactions applied (handoff không chứa secret; `.env` không được nhắc giá trị).

## Verification

| Check | Command | Outcome | When |
|---|---|---|---|
| Stack + hợp đồng | `.\scripts\submit.ps1 smoke_test.py` | `SMOKE OK` (feature, KMeans, HDFS ghi/đọc, ES ghi 100 doc) | 13/10, Phase 01 |
| Ingest NASA | `submit ingest_nasa.py` | 1.891.715 dòng đọc → 1.891.709 vào silver, 6 dòng bị loại | 13/10 |
| Model sim | `submit detect_anomalies.py --source sim` | KMeans+CBLOF: P 0,720 / R 0,998 / F1 0,836 / PR-AUC 0,998 | 13/10 |
| Model NASA | `submit detect_anomalies.py --source nasa` | rc=0, 2.487 cảnh báo | 13/10 |
| Hình báo cáo | `python analysis/eda_nasa.py`, `python analysis/model_report.py` | 14 PNG trong `data/reports/figures/` | 13/10 |

Not run:
- `PipelineModel.load` của model **sau khi clone trên máy khác** → chưa thử (lý do: chỉ có máy Định). Đây là việc đầu tiên Như nên kiểm.
- `smoke_test.py` sau commit CBLOF → chưa chạy lại (hàm cũ vẫn tương thích ngược, nên rủi ro thấp).
- Chưa có job export ES, streaming hay Kibana (việc của Như).

## Open risks and blockers

- Type: risk. Owner: Như. Impact: nếu checkout trên Windows làm đổi file model (CRLF), `PipelineModel.load` báo
  `ChecksumException`. Đã có `.gitattributes` (`data/gold/models/** binary`); nếu vẫn lỗi thì xoá các file `.*.crc`
  trong thư mục model, hoặc chạy `./scripts/run_batch.sh models` để train lại.
- Type: risk. Owner: Như. Impact: Phase 06 (export ES) cần silver/gold **trên máy Như**, mà phần này không có trong git.
  Phải chạy `./scripts/run_batch.sh` (khoảng 15 phút, cần mạng để tải NASA khoảng 20MB) trước khi export dữ liệu đầy đủ.
- Type: risk. Owner: Như. Impact: streaming trễ do watermark/window (R5). Dùng window 5 phút, slide 1 phút, trigger 10s;
  đo độ trễ thật để ghi vào báo cáo.
- Type: question. Owner: Định + Như. Impact: thời điểm gộp `dinh/batch` + nhánh Như vào `main` (mốc S3) → cần thống nhất.
- Type: risk. Owner: cả hai. Impact: RAM Docker chỉ khoảng 7,4GB; tắt các project Docker khác khi chạy (trên máy Định có
  `spark-observability-demo` chiếm cổng 8080/8081/7077/4040).

## Exact next actions

1. **First safe step**: lấy nhánh về mà không đụng `main`:
   ```powershell
   git fetch origin
   git switch -c nhu/realtime origin/main      # nếu Như chưa có nhánh riêng
   git merge origin/dinh/batch                 # merge sạch (dinh/batch nằm trên commit revert của main)
   ```
   Nếu Như đã có commit trên nhánh riêng thì chỉ cần `git merge origin/dinh/batch` trên nhánh đó.
2. Dựng lại stack (image Spark đã đổi so với bản Phase 01 cũ: thêm pandas/pyarrow):
   `docker compose up -d --build`, sau đó `.\scripts\submit.ps1 smoke_test.py` phải in ra `SMOKE OK`.
3. Kiểm tra model load được (xem đoạn code ở mục 6). Nếu lỗi checksum thì xem Open risks.
4. Phase 05 (HDFS) + 06 (ES) theo `plans/.../phase-05-hdfs-storage.md`, `phase-06-es-export.md`.
   Muốn có dữ liệu đầy đủ thì chạy `./scripts/run_batch.sh` trước (Git Bash), hoặc dùng silver nhỏ của smoke test để thử.
   Đường dẫn cần export: `/opt/spark/data/silver/http_access`, `/opt/spark/data/silver/nasa_access`,
   `/opt/spark/data/gold/scores_{sim,nasa}`, `/opt/spark/data/gold/alerts_{sim,nasa}`.
   Cột của gold scores/alerts (đã đối chiếu file thật): `ip, window_start, window_end, <11 CORE feature>,
   [chỉ sim: 5 EXTENDED feature + is_anomaly, anomaly_label], source, rule_reason, rule_flag, cluster, ref_cluster,
   score, top_feature, threshold, is_alert, severity, reason, dt`.
5. Phase 07 (`jobs/stream_detect.py`): đọc `readStream.schema(HTTP_ACCESS_LOG_SCHEMA).json(...)`, parse
   `event_time = to_timestamp(ts, TS_PATTERN)`, `withWatermark("event_time", "2 minutes")`, rồi
   `build_window_features(df, window="5 minutes", slide="1 minute")`, sau đó chấm điểm trong `foreachBatch`.
6. Đoạn chấm điểm **bắt buộc dùng đúng như sau** để khớp với batch (CBLOF + ngưỡng + `MIN_REQ`):
   ```python
   import json
   from pyspark.ml import PipelineModel
   from pyspark.sql import functions as F
   from features_common import MODEL_DIR, MODEL_META_PATH, add_distance_score

   meta = json.load(open(MODEL_META_PATH, encoding="utf-8"))   # doc 1 lan o driver
   model = PipelineModel.load(MODEL_DIR)                        # load 1 lan o driver

   def score_batch(feats_df):
       out = add_distance_score(
           model.transform(feats_df), meta["centers"],
           feature_names=meta["features"], large_clusters=meta["large_clusters"],
       )
       out = out.withColumn("score", F.when(F.col("n_req") >= meta["min_req_for_scoring"], F.col("score")).otherwise(0.0))
       ratio = F.col("score") / F.lit(meta["threshold"])
       return (out.withColumn("is_alert", F.col("score") > F.lit(meta["threshold"]))
                  .withColumn("severity", F.when(ratio >= 2, "high").when(ratio >= 1.5, "medium").otherwise("low"))
                  .withColumn("reason", F.col("top_feature")))
   ```
   Ghi alert vào index `log-alerts` với `es.mapping.id` = `source-ip-window_start` và `es.write.operation=upsert`,
   để sliding window ghi lại cùng cửa sổ không bị nhân đôi. Fallback khi chưa có model: `features_common.rule_score`.
7. Phase 08: Kibana dashboard + rule "Elasticsearch query" trên `log-alerts` (key mã hoá đã có trong compose),
   rồi export ra `kibana/dashboards.ndjson`.
8. Commit trên nhánh của Như; **hỏi người dùng trước khi push**; không merge vào `main` khi chưa thống nhất (S3).

## Bẫy đã gặp (đọc trước khi debug)

- `docker compose exec` **đọc stdin**: trong script non-interactive phải thêm `</dev/null`, nếu không nó "ăn" mất
  phần còn lại của script (đã làm 2 chuỗi job chạy chồng lên nhau và ghi hỏng silver).
- Không gọi `F.col(...)` ở cấp module (trước khi có SparkContext): sẽ lỗi `AssertionError`. Xem `features_common.rules()`.
- Streaming **không hỗ trợ** `countDistinct`. `build_window_features` đã dùng `approx_count_distinct`, nên giữ nguyên.
- `F.array_position` (Spark 3.5, API Python) không nhận Column làm giá trị cần tìm → dùng `F.expr`.
- Ghi Parquet có `partitionBy` mà không `repartition` theo cột partition → hơn 10.000 file nhỏ, rất chậm trên bind mount Windows.
- `pyspark.ml.functions.vector_to_array` cần pandas trong image (đã cài).
- Console Windows dùng cp1252: script Python in tiếng Việt cần `sys.stdout.reconfigure(encoding="utf-8")`.
- Cổng bị project khác chiếm: đổi `SPARK_*_PORT` trong `.env`. Cổng 7077 không publish ra host.

## Source pointers

- `CLAUDE.md`: quy ước, lệnh, bẫy.
- `plans/261013-0915-log-anomaly-full-project/plan.md`: plan tổng, bảng sở hữu file, các mốc S1/S2/S3.
- `plans/261013-0915-log-anomaly-full-project/phase-05..08-*.md`: việc của Như.
- `plans/261013-0915-log-anomaly-full-project/phase-04-anomaly-models.md`: kết quả mô hình.
- `jobs/features_common.py`: hợp đồng giao tiếp (schema, feature, rule, CBLOF).
- `jobs/detect_anomalies.py`: hàm `score()` và cách tính `severity` (streaming phải giống).
- `data/gold/models/kmeans_core/`, `data/reports/model_meta.json`: model + ngưỡng cho streaming.
- `data/reports/metrics_sim.json`, `data/reports/model_comparison.md`: số liệu cho báo cáo.
- `docker/spark/Dockerfile`, `docker-compose.yml`, `.env.example`.
- `scripts/submit.ps1` / `scripts/submit.sh`, `scripts/run_batch.sh`.
- GitHub: https://github.com/hongocphuongnhu/BigData-LogAnomalyDetection (nhánh `dinh/batch`).
