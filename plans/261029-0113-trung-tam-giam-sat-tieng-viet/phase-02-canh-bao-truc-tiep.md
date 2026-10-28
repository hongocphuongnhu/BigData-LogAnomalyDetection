---
phase: 2
title: "Phase 02: Cảnh báo trực tiếp + từ điển tiếng Việt"
status: todo
priority: P1
effort: "1.5h"
dependencies: [1]
---

# Phase 02: Cảnh báo trực tiếp + từ điển tiếng Việt

> Chế độ deep: trước khi làm, scout lại một document thật trong `log-alerts` (`_search?size=3`)
> để chốt tên trường, rồi cập nhật phase này.

## Mục tiêu

Hiện danh sách cảnh báo mới nhất, mỗi dòng là **một câu tiếng Việt** giải thích vì sao IP bị nghi,
thay vì các cột `top_feature`, `score`, `ratio_404`.

## File

- Tạo `console/explain.py`: bảng dịch thuần Python, không phụ thuộc server, test được riêng.
- Sửa `console/server.py`: thêm `GET /api/alerts?source=stream|sim|nasa&limit=50`.
- Sửa `console/static/*`: vùng Cảnh báo.

## Thiết kế

- Query `log-alerts`, lọc theo `source`, sắp xếp `detected_at desc`. Gọi `urllib` trực tiếp với timeout 3 giây,
  **không** dùng `stack_api.request`: hàm đó timeout 60 giây × 3 lần retry cho `_search`, nên khi ES treo,
  mỗi lần poll 3 giây sẽ giam một thread tới vài phút và thread dồn đống.
- `explain.py` có ba bảng dịch:
  - `FEATURE_VI`: 11 feature CORE → tên dễ hiểu và mẫu câu. Ví dụ `n_req` → "Gửi {n_req:.0f} request trong 5 phút";
    `ratio_404` → "{ratio_404:.0%} request trả lỗi 404 (trang không tồn tại)";
    `ratio_sus_path` → "Đường dẫn chứa mẫu tấn công (SQL, file nhạy cảm)".
  - `SEVERITY_VI`: high/medium/low → Cao/Trung bình/Thấp, kèm giải thích "điểm gấp ≥ 2 lần ngưỡng" (theo `add_alert_fields`).
  - `GROUND_TRUTH_VI`: brute_force → "Dò mật khẩu", sqli → "Chèn SQL", scan → "Dò quét lỗ hổng",
    ddos → "DDoS phá cache", normal → "Bình thường".
- **Không ghi loại tấn công như thể model đoán ra** (rủi ro R4). Mỗi dòng hiện:
  `IP · thời gian · mức độ · lý do (từ top_feature)`, và một nhãn nhỏ riêng
  "Nhãn mô phỏng: DDoS phá cache (model không dùng nhãn này)" nếu có `ground_truth`.
- Hiện "Điểm 15,2 / ngưỡng 7,49" kèm thanh ngang trực quan.
- Dòng mới xuất hiện được tô sáng vài giây. Polling mỗi 3 giây.
- Thêm mục "Thuật ngữ" gập/mở được: cửa sổ 5 phút, điểm bất thường, ngưỡng p99, cụm KMeans, CBLOF,
  giải thích bằng 1–2 câu đời thường.

## Tiêu chí nghiệm thu

- `python -c "from console.explain import explain; ..."` với document mẫu cho mỗi `top_feature` trả câu có dấu, không lỗi format.
- Feature lạ hoặc thiếu trường → câu dự phòng "Hành vi lệch xa bình thường", không ném lỗi.
- Với alert NASA và sim đã có sẵn trong ES (`source=nasa`, `sim`), trang hiện được danh sách khi chưa chạy stream.
- Giao diện không có chữ nào ghi model "phát hiện DDoS/SQLi".
