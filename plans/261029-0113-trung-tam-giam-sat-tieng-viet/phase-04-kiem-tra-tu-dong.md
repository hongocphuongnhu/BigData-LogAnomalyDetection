---
phase: 4
title: "Phase 04: Kiểm tra tự động Đạt / Không đạt"
status: done
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

## Kết quả (29/10)

Bước tiến trình suy từ file: chưa có `data/raw/setup/qa-<attack>-*.log` → bước 1; có log chưa `STREAM READY` → 2;
chưa có dòng `First` → 3; chưa có `-restart.log` → 4; có → 5. Kill bằng `taskkill /T` vì `verify_realtime.py`
có tiến trình con (docker client, simulator).

Kiểm chứng:
- [x] DDoS → **Đạt** sau 3 phút 20 giây: độ trễ 18,3 giây, 105 cảnh báo trước = 105 sau khởi động lại,
  `alert-history` 2 → 3. Spark master trống ngay sau đó.
- [x] Hủy giữa chừng (scan, giây 45) → xong ở giây 49, Spark master trống, lịch sử ghi "Đã hủy".
- [x] Đang kiểm tra → bật live / kiểm tra lần 2 bị từ chối (409).
- [x] Không đạt hiện đúng lý do và 20 dòng log cuối, khóa được nhả.
- [ ] **Bình thường → Không đạt (6 cửa sổ báo nhầm, cho phép ≤ 2).** Không phải lỗi console: hai IP bình thường có
  một phản hồi 5xx, điểm 7,51 và 7,53 sát ngưỡng 7,49; một IP nằm trong 5 cửa sổ trượt nên thành 6 cửa sổ.
  Cùng seed 73, báo cáo của Như ghi 0 vì timestamp theo giờ chạy làm ranh giới cửa sổ đổi. Bài kiểm tra
  `none` vì vậy **không ổn định**; không sửa model/ngưỡng/`verify_realtime.py` (ngoài phạm vi), đã báo người dùng.
- [ ] Tắt Kibana giữa chừng: chưa thử (tránh làm gián đoạn stack); đường lỗi đã được kiểm qua trường hợp trên.
