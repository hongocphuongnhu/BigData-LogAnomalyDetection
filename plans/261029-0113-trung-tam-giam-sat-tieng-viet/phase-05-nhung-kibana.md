---
phase: 5
title: "Phase 05: Nhúng dashboard Kibana"
status: todo
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
