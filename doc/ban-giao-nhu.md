# Bàn giao phần Như — 13/10/2026

## Đã thực hiện và kiểm chứng

- HDFS upload/ingest simulator; local và HDFS khớp 61.643 record; replication=1;
  fsck HEALTHY; ảnh overview, lake và block trong `doc/images/`.
- Mapping, 4 index serving và data view; bulk export idempotent, thử lại không tăng count.
- NASA hostname/duplicate và PipelineModel được kiểm thử bằng fixture riêng, không
  đưa fixture vào index/model chính thức.
- Simulator real-time atomic rename, Structured Streaming window/watermark/checkpoint;
  rule fallback và nhánh model đều chạy được trong kiểm thử.
- Live DDoS 18,33 giây, scan 12,295 giây; restart không tăng số alert; native Kibana rule
  dùng Server log + Index action đã thực thi thành công.
- Hai dashboard Lens, saved search và NDJSON; import thành công trong space mới;
  rule QA đã tắt để tránh action chạy trùng với rule demo.
- `run_all --serve` chạy toàn bộ phần simulator không cần thao tác tay.
- Báo cáo/slide phần Như, ảnh, video dự phòng và kịch bản demo dự thảo đã chuẩn bị.

## Artifact cần Định bàn giao để tích hợp cuối

- NASA raw và silver cùng `jobs/ingest_nasa.py`.
- `jobs/build_features.py`, `gold/scores_{sim,nasa}`, `gold/alerts_{sim,nasa}`.
- PipelineModel `gold/models/kmeans_core` và `reports/model_meta.json`.
- Metadata: `features` đúng thứ tự CORE_FEATURES, `window`, `threshold > 0`, `centers`.
- Metrics/bảng so sánh mô hình, hình kết quả và nội dung báo cáo thuộc phần Định.

Như không sửa `features_common.py`, generator batch hay các job batch của Định.
README/CLAUDE đã bổ sung phần serving và cách kiểm thử. Khi ghép, giữ các hướng dẫn
batch của Định và dùng `scripts/run_batch.ps1` / `.sh` làm entrypoint cho `--batch`.

## Trạng thái thực tế

Phần code và tài liệu riêng của Như đã có đủ để review/demo trên simulator.
Chưa gọi toàn bộ đồ án hoàn thành: NASA/model chính thức, ghép báo cáo nhóm, tỷ lệ
đóng góp, buổi tập nói chung, tag nộp và Classroom còn cần hai thành viên hoàn tất.
Các phase gốc vẫn giữ trạng thái ban đầu; bảng này ghi rõ tiến độ có bằng chứng
để tránh đánh dấu hoàn thành phần phụ thuộc chưa được bàn giao.
