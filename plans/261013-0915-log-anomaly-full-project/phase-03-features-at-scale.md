---
phase: 3
title: "Phase 3: Dữ liệu quy mô lớn + feature engineering"
status: todo
priority: P1
effort: "1.5h"
owner: "Định"
dependencies: [1]
---

# Phase 3: Dữ liệu quy mô lớn + feature engineering

## Goal

Có dữ liệu simulator có nhãn ở quy mô ~1 triệu dòng và bảng feature theo cửa sổ
`(ip, 5 phút)` cho cả simulator lẫn NASA ở tầng gold.

## Files to Create / Modify

- Modify: `jobs/ingest_http_access.py` (sửa các lỗi đã ghi nhận: kiểm tra enum/khoảng giá trị,
  lưu nguyên dòng khi reject bằng `to_json(struct(*))`, xoá `_rejected` cũ khi chạy lại)
- Create: `jobs/build_features.py`

## Tasks & Steps

1. Sinh dữ liệu 24h:
   `python -m generator.simulate --minutes 1440 --rate 12 --attack-repeat 8 --start 2026-01-10T00:00:00Z --seed 42`
   (khoảng 1 triệu dòng normal + vài nghìn đến chục nghìn dòng tấn công). Ghi lại thời gian chạy.
   Nếu quá chậm (> 10 phút) thì giảm `--rate`.
2. Sửa ingest (≈ 30 phút). Đây là nội dung cho mục "làm sạch: thiếu / không nhất quán" trong báo cáo.
   Chạy ingest qua `scripts/submit.ps1`.
3. `build_features.py --source sim|nasa --window "5 minutes" --silver-path ... --gold-path ...`:
   - Gọi `features_common.build_window_features` (extended=True với sim).
   - Ghi `gold/features_{source}` partition theo `dt`.
   - In: số cửa sổ, số cửa sổ bất thường theo nhãn (sim), phân vị p50/p95/p99 của từng feature.
4. **Mốc S1:** báo Như rằng silver + features của sim đã sẵn sàng.

## Verification

- Silver sim: số dòng khớp output của generator, trừ đi số dòng reject.
- `gold/features_sim`: mỗi loại tấn công có ≥ 1 cửa sổ được gán nhãn.
- `gold/features_nasa`: số cửa sổ > 0, không có giá trị null trong CORE_FEATURES.
