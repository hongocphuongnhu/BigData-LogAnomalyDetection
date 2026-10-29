---
phase: 7
title: "Phase 07: Tích hợp, README, CLAUDE.md"
status: done
priority: P2
effort: "1h"
dependencies: [1, 2, 3, 4, 5]
---

# Phase 07: Tích hợp, README, CLAUDE.md

## Mục tiêu

Chạy trọn một vòng demo từ trang và ghi lại cách dùng, để cả hai người (và AI của hai người) dùng được.

## Việc cần làm

- Chạy end-to-end trên stack sạch: `docker compose up -d` → `python -m console.server` → Hạ tầng xanh
  → bật bộ phát hiện → chạy lần lượt 5 kịch bản → dừng → kiểm tra tự động DDoS và Bình thường.
- README: thêm mục "Trung tâm giám sát" (lệnh chạy, cổng 8765, 5 vùng, thứ tự bấm khi demo, xử lý sự cố R1–R3).
- CLAUDE.md: thêm `console/` vào mục Cấu trúc, ghi ngoại lệ "UI có dấu", thêm bẫy R1 (cores.max) và R5 (tên rule).
  Nhân tiện sửa dòng "3+: … chưa làm" đã cũ.
- `.gitignore`: chắc chắn `data/stream/console/` không bị commit (thư mục `data/stream` đã ignore, cần kiểm tra lại).
- Báo Như: gửi tóm tắt những gì console đọc/gọi từ phần của Như.
- **Kịch bản bảo vệ trên trang**: bổ sung vào `doc/demo-script.md` một đoạn "Demo bằng Trung tâm giám sát":
  mỗi vùng bấm gì, nói câu gì, mất bao lâu; tập ít nhất 2 lần và bấm giờ.
  Chạy sẵn một lượt Kiểm tra tự động **trước** giờ bảo vệ (mất 4–5 phút), để lúc trình bày chỉ cần mở bảng lịch sử.

## Tiêu chí nghiệm thu

- Một người chưa đọc code làm theo README chạy được demo trong ≤ 10 phút (stack đã có dữ liệu).
- `git diff main --stat` chỉ gồm `console/`, `README.md`, `CLAUDE.md`, `doc/demo-script.md`, `plans/`,
  `scripts/kibana_setup.py` và `kibana/dashboards.ndjson` (Phase 06).

## Kết quả (29/10)

- README: mục "Trung tâm giám sát" (lệnh, 5 vùng, thứ tự bấm, xử lý sự cố R1–R3).
- CLAUDE.md: sửa dòng sprint cũ, thêm `console/`, ngoại lệ chữ có dấu, bẫy cores.max / driver mồ côi / tên rule.
- `doc/demo-script.md`: đoạn "Phương án: demo bằng Trung tâm giám sát" (chuẩn bị, 7 bước bấm/nói/thời gian).
- `data/stream/console/` bị ignore bởi `data/stream/*` (đã kiểm `git check-ignore`).
- Chưa làm: tập demo 2 lần có bấm giờ cùng Như (việc của người).
