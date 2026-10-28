---
phase: 3
title: "Phase 03: Mô phỏng tấn công live"
status: todo
priority: P1
effort: "2h"
dependencies: [1, 2]
---

# Phase 03: Mô phỏng tấn công live

> Chế độ deep: trước khi làm, kiểm chứng R1 và R3 bằng tay. Submit `stream_detect.py` với
> `--conf spark.cores.max=2`, xem Spark master có hiện 2/4 core không. Kill client `docker compose exec`
> rồi xem driver trong container có còn sống không (`docker compose exec spark-master ps aux`).
> Cập nhật phase theo kết quả.

## Mục tiêu

Bấm **"Bật bộ phát hiện"** để chạy stream; chọn kịch bản rồi bấm **"Chạy"** để phát traffic.
Cảnh báo hiện ngay ở vùng Cảnh báo (Phase 02).

## File

- Tạo `console/runner.py`: quản lý tiến trình (stream + simulator), có khóa "một việc một lúc".
- Sửa `console/server.py`: thêm `POST /api/live/start`, `POST /api/live/stop`,
  `POST /api/live/scenario {"attack": ...}` và `GET /api/live/status`.
- Sửa `console/static/*`: vùng Mô phỏng.
- **Không sửa** `jobs/stream_detect.py` và `generator/stream_simulate.py`, chỉ gọi lệnh có sẵn.

## Thiết kế

- Thư mục riêng `data/stream/console/{incoming,checkpoint}` để không lẫn với lượt `qa-*` của Như.
- Lệnh stream (dựa trên lệnh trong `verify_realtime.py`, thêm `cores.max`):
  `docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
  --conf spark.cores.max=2 --conf spark.sql.shuffle.partitions=4
  --py-files /opt/spark/jobs/features_common.py /opt/spark/jobs/stream_detect.py
  --input-path /opt/spark/data/stream/console/incoming --checkpoint-path /opt/spark/data/stream/console/checkpoint
  --detector model --duration 600`.
  Không dùng `--conf spark.app.name`, vì `builder.appName("stream_detect")` trong job sẽ đè lên.
  **Marker để dọn = đường dẫn checkpoint** `stream/console/checkpoint` (chỉ console dùng).
- Output stream và simulator ghi ra file log trong `data/stream/console/` (không dùng `subprocess.PIPE`,
  vì pipe không ai đọc sẽ đầy và chặn tiến trình). Chạy với `cwd=ROOT`, simulator chạy bằng `sys.executable`.
- `--duration 600` làm chốt chặn: nếu console chết, driver tự tắt sau 10 phút. UI hiện thời gian còn lại
  và nút "Gia hạn" (dừng rồi bật lại; checkpoint giữ tiến độ nên không đọc lại file cũ).
- **Dọn driver mồ côi** (rủi ro R3): lúc server khởi động và trước mỗi lần bật, chạy
  `docker compose exec -T spark-master pgrep -f stream/console/checkpoint`. Nếu còn driver thì `pkill` rồi
  mới bật, để không bao giờ có hai driver ghi chung một checkpoint.
- **Sẵn sàng** (rủi ro R8) khi log có `STREAM READY` **và** Spark master `/json/` báo app RUNNING với `cores > 0`.
  Chưa có core sau 60 giây thì báo lỗi "Cụm Spark đang bận, xem thẻ Hạ tầng".
- Kịch bản được phép (whitelist): `none` (Bình thường), `scan`, `sqli`, `brute_force`, `ddos`.
  Lệnh: `python -B -m generator.stream_simulate --attack X --duration 90 --attack-after 20 --rate 2
  --seed <ngẫu nhiên> --out-dir data/stream/console/incoming --manifest data/stream/console/run-<id>.json`.
- `status` đọc manifest: số bản ghi đã phát, `first_attack_at`, `attack_ips`. Tìm alert `source=stream`
  của `attack_ips`; **độ trễ phát hiện** = giờ host lúc console *thấy* alert đầu tiên trừ `first_attack_at`,
  giống cách `verify_realtime.py` làm. `detected_at` chỉ để hiển thị, vì nó theo đồng hồ container (rủi ro R9).
  Chạy lại cùng kịch bản thì IP có thể trùng lượt trước, nên chỉ tính alert xuất hiện sau khi lượt này bắt đầu
  (so theo tập `doc_id` đã thấy trước đó).
- Mỗi kịch bản có mô tả "câu chuyện" theo chủ đề báo, đúng với simulator. Ví dụ DDoS: "Vài chục IP mở các bài viết
  ngẫu nhiên để phá cache, làm database quá tải" (botnet sinh 20–60 IP, xem `generator/attacks.py`).
- Dừng: kill tiến trình trên host, sau đó `docker compose exec -T spark-master pkill -f stream/console/checkpoint`.
  Ctrl+C cũng dọn như vậy. Đóng cửa sổ thì không dọn được, nên lần khởi động sau sẽ dọn (xem mục mồ côi ở trên).
- Tiến trình trên Windows: `creationflags=CREATE_NO_WINDOW`, không dùng `shell=True`.

## Tiêu chí nghiệm thu

- Bật bộ phát hiện → Spark master hiện app `RUNNING` với 2 core; thẻ Hạ tầng cập nhật theo.
- Chạy DDoS → alert của `attack_ips` hiện trên trang trong ≤ 60 giây sau `first_attack_at`; trang hiện độ trễ.
- Chạy "Bình thường" 90 giây → không quá 2 cảnh báo (cùng ngưỡng `verify_realtime.py` dùng).
- Bấm Dừng → trong ≤ 15 giây không còn app console trên Spark master và không còn driver trong container.
- Bấm "Chạy" khi đang có kịch bản chạy → bị từ chối kèm thông báo tiếng Việt, không chạy chồng.
- `POST /api/live/scenario {"attack": "rm -rf"}` → HTTP 400.
- Đóng cửa sổ terminal của server khi stream đang chạy → mở lại server thì driver cũ bị dọn, bật lại chạy bình thường.
- Bấm "Bật" hai lần liên tiếp → chỉ có **một** driver dùng `stream/console/checkpoint` (`pgrep -c` = 1).
