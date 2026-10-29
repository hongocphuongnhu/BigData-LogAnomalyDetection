---
phase: 5
title: "Phase 05: Nhúng dashboard Kibana"
status: done
priority: P2
effort: "0.5h"
dependencies: [1]
---

# Phase 05: Nhúng dashboard Kibana

> Chế độ deep: trước khi làm, `curl -I localhost:5601/app/dashboards` để xem header
> `X-Frame-Options` / `Content-Security-Policy frame-ancestors` (rủi ro R6).

## Mục tiêu

Vùng Dashboard nhúng hai dashboard có sẵn của Như, ẩn menu tiếng Anh của Kibana.

## File

- Sửa `console/static/*`: vùng Dashboard có 2 tab.
- **Không sửa** Kibana hay `kibana_setup.py`.

## Thiết kế

- Tab "Cảnh báo bất thường" → `http://localhost:5601/app/dashboards#/view/nhu-anomaly-alerts?embed=true&_g=(refreshInterval:(pause:!f,value:10000),time:(from:now-30m,to:now))`.
- Tab "Tổng quan log" → `.../view/nhu-log-overview?embed=true` (giữ khoảng thời gian dashboard tự lưu).
- Mỗi tab có nút "Mở trong Kibana ↗" (URL không có `embed=true`).
- Kibana không chạy → hiện khung báo "Kibana chưa sẵn sàng" thay cho iframe trắng (dựa vào probe Kibana ở Phase 01).
- Tiêu đề biểu đồ được Việt hóa ở Phase 06; trước khi xong Phase 06, chấp nhận iframe còn tiêu đề tiếng Anh.

## Tiêu chí nghiệm thu

- Mở bằng `browser-automation`: iframe hiện biểu đồ, không có thanh menu Kibana, không có lỗi framing trong console.
- Nếu Kibana chặn framing: thay bằng 2 nút mở tab mới và ghi lý do vào mục Ghi chú scout.

## Kết quả (29/10)

Scout R6: Kibana không gửi `X-Frame-Options`, CSP không có `frame-ancestors` → nhúng được. Iframe chỉ tạo lại khi đổi tab
hoặc Kibana vừa sống lại (không nạp lại mỗi lần poll). Kibana chưa sẵn sàng → khung báo đỏ thay cho iframe trắng.
Lỗi console trong lần kiểm tra đều từ bên trong Kibana (user profile 404 vì không bật security, CSP nội bộ), không từ trang.
Kibana hiện banner "Help us improve" và toast "Your data is not secure" cho trình duyệt mới; tắt một lần là nhớ
(ghi trong `doc/demo-script.md`).
