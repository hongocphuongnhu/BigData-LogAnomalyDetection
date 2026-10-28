---
phase: 4
title: "Phase 04: Kiểm tra tự động Đạt / Không đạt"
status: todo
priority: P1
effort: "1.5h"
dependencies: [1, 3]
---

# Phase 04: Kiểm tra tự động Đạt / Không đạt

> Chế độ deep: trước khi làm, chạy tay `python scripts/verify_realtime.py --attack ddos` một lần,
> bấm giờ và lưu lại stdout mẫu (dòng nào in khi nào), rồi cập nhật phase.

## Mục tiêu

Bấm **"Kiểm tra"** cho một kịch bản: trang chạy bài kiểm tra end-to-end có sẵn của Như và hiện
**Đạt / Không đạt** cùng các con số chứng minh, không cần đọc terminal.

## File

- Sửa `console/runner.py`: thêm việc "check" dùng chung khóa với live.
- Sửa `console/server.py`: thêm `POST /api/check {"attack": ...}` và `GET /api/check/status`.
- Sửa `console/static/*`: vùng Kiểm tra tự động.
- **Không sửa** `scripts/verify_realtime.py`. Gọi nó như lệnh, vì nó là một `main()` liền khối dùng `assert`
  nên không import từng hàm ra được.

## Thiết kế

- Điều kiện chạy: không có live đang chạy **và** Spark master `/json/` báo `activeapps` rỗng (rủi ro R1, R2),
  vì stream của `verify_realtime.py` không đặt `cores.max` và cần cả 4 core. Không thỏa thì nút bị khóa,
  kèm dòng "Hãy dừng mô phỏng trước, bài kiểm tra tự chạy bộ phát hiện riêng".
- **Dọn sau kiểm tra:** nếu `verify_realtime.py` lỗi giữa chừng (assert/RuntimeError), nó thoát nhưng driver
  `qa-*` trong container vẫn chạy tới hết `--duration` và giữ cả 4 core. Sau mỗi lượt (Đạt, Không đạt, hoặc Hủy)
  chạy `docker compose exec -T spark-master pkill -f /opt/spark/data/stream/qa-`, rồi **chỉ nhả khóa khi
  `activeapps` rỗng**. Có nút "Hủy" làm đúng việc này.
- Lệnh: `python -B scripts/verify_realtime.py --attack X --detector model`; stdout/stderr ghi ra
  `data/stream/console/check-<id>.log`.
- Dịch tiến trình sang các bước tiếng Việt, suy từ thời gian và dòng log:
  1. "Chờ hệ thống cảnh báo yên lặng" (tới 110 giây đầu);
  2. "Khởi động bộ phát hiện" (đến khi có `STREAM READY` trong log `data/raw/setup/qa-*.log`);
  3. "Phát traffic tấn công";
  4. "Đã thấy cảnh báo đầu tiên" (dòng `First ... alert observed`);
  5. "Khởi động lại để kiểm tra không ghi trùng";
  6. Kết quả.
- Mã thoát 0 → **Đạt**; parse JSON cuối stdout (lấy từ dòng cuối cùng bắt đầu bằng `{` tới hết, vì lượt tấn công
  in JSON nhiều dòng `indent=2`, còn lượt `none` in một dòng) và hiện bằng tiếng Việt: độ trễ phát hiện (`latency_seconds`),
  số alert trước/sau khởi động lại (bằng nhau nghĩa là không trùng), số lần rule Kibana ghi lịch sử.
  Kịch bản `none` hiện số cảnh báo giả (`false_positive_windows`).
- Mã thoát ≠ 0 → **Không đạt**, hiện câu lỗi `AssertionError` / `RuntimeError` và 20 dòng log cuối.
- Lưu lịch sử các lượt kiểm tra vào `data/stream/console/checks.jsonl` (thư mục data không commit),
  để trang hiện bảng "các lần chạy gần đây".
- Hiện thời gian ước tính "khoảng 4–5 phút" và đồng hồ đếm.

## Tiêu chí nghiệm thu

- Kiểm tra DDoS → **Đạt**, độ trễ ≤ 60 giây, số alert trước = sau.
- Kiểm tra "Bình thường" → **Đạt** với ≤ 2 cảnh báo giả.
- Cố tình tắt Kibana trong lúc kiểm tra → **Không đạt** kèm lý do đọc được, server không treo, khóa được nhả.
- Đang live → không bấm được Kiểm tra.
- Bấm Hủy giữa chừng → trong ≤ 15 giây Spark master không còn app `qa-*`, bật live lại có core ngay.
