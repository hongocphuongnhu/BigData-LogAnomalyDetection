---
phase: 3
title: "Phase 3: Dữ liệu quy mô lớn + feature engineering"
status: completed
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

## Kết quả (đã chạy)

- Simulator: `--attack-repeat 30` (thay vì 8, để tập test có đủ mẫu mỗi loại tấn công) cho 1.065.283 dòng
  (brute_force 1.484, sqli 807, scan 1.230, ddos 25.461). Sinh mất khoảng 40s, ingest khoảng 1 phút.
- Ingest đã được siết: kiểm tra null của field bắt buộc, enum, khoảng giá trị, invariant; lưu nguyên dòng
  bị loại dạng JSON; quarantine ghi đè tĩnh để không còn dòng lỗi cũ.
- `gold/features_nasa`: 358.687 cửa sổ, 81.982 host (khoảng 90s).
- Khác biệt phân phối NASA so với sim: `ratio_static` p50 là 0,6 so với 0; `mean_bytes` p95 là 145KB so với 15KB.
  Vì vậy model được huấn luyện riêng cho từng nguồn (R9).
