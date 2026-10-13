---
phase: 9
title: "Phase 9: Tích hợp, README, kịch bản demo"
status: todo
priority: P1
effort: "1.5h"
owner: "Định + Như"
dependencies: [2, 3, 4, 5, 6, 7, 8]
---

# Phase 9: Tích hợp, README, kịch bản demo

## Goal

Chạy toàn bộ pipeline từ đầu trên môi trường sạch bằng một script, có README đủ để
giảng viên tự chạy lại, và có kịch bản demo đã tập.

## Files to Create / Modify

- Create: `scripts/run_all.ps1` và `.sh`. Phần Định (`--batch`): download → ingest → features → models.
  Phần Như (`--serve`): hdfs_put → es_setup → export → kibana_import.
- Modify: `README.md` (Định viết phần batch, Như viết phần hạ tầng/serving/streaming)
- Create: `doc/demo-script.md`

## README gồm

Giới thiệu, kiến trúc (sơ đồ), yêu cầu (Docker Desktop, RAM ≥ 12GB, Python 3.10+),
`.env`, chạy nhanh `run_all`, từng bước chi tiết, cổng UI, xử lý sự cố (RAM WSL,
cổng bị trùng, HDFS safe mode), nguồn dữ liệu.

## Kịch bản demo (~8 phút)

| Phút | Người | Nội dung |
|---|---|---|
| 0–1 | Như | Kiến trúc: HDFS → Spark → ES → Kibana; mở Spark UI, HDFS UI |
| 1–4 | Định | NASA: dữ liệu thật, thống kê làm sạch; feature; bảng so sánh mô hình; top IP bất thường NASA |
| 4–7 | Như | Live: chạy `stream_detect` → `stream_simulate --attack ddos` → alert hiện trên dashboard, rule kích hoạt |
| 7–8 | Cả hai | Hạn chế + hướng phát triển |

Chuẩn bị phương án B: video quay màn hình phần live, phòng khi máy chậm lúc thuyết trình.

## Tasks & Steps

1. `docker compose down -v`, xoá `data/silver|gold`, chạy `run_all` từ đầu, sửa mọi chỗ lỗi.
2. Hoàn thiện README; cập nhật `CLAUDE.md`.
3. Tập demo 2 lần, bấm giờ; quay video phương án B.
4. Commit, gắn tag `v1.0-submit`.

## Verification

- `run_all` chạy hết không cần can thiệp tay (trừ thao tác tạo rule Kibana nếu không import được).
- Demo vừa ≤ 8 phút.
