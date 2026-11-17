---
title: Console phase 02 canh bao
date: 2026-10-28
summary: "Nap du lieu that, xong Phase 02 (commit 4a5311c); sua cau giai thich sai chieu nho du lieu that"
---

# Console phase 02 canh bao

## What happened
- `run_all.py --batch --serve --hdfs` exit 0: ES co 1,89M log NASA, 1,07M log sim, 597k cua so, 4.538 canh bao. O C con 6 GB.
- Phase 02: `console/explain.py`, `console/alerts.py`, muc 3 tren trang.
- Bug ve nghia phat hien nho du lieu that: alert DDoS co top_feature=max_bytes=483 B (thap bat thuong),
  cau "phan hoi lon bat thuong" sai. Sua: so voi trung vi cua cua so khong canh bao (percentiles agg, cache 5 phut).
- Top theo diem cua sim toan SQLi -> them chip loc theo nhan mo phong; lo sim co 706/2.069 bao nham (nguong p99).

## Decision
- Khong commit kibana/dashboards.ndjson, kibana/rule-id.json do kibana_setup ghi lai theo may nay (file cua Nhu, ID rieng tung may).

## Next steps
- Phase 03 mo phong live: kiem chung R1 (cores.max=2) va R3 (pkill theo checkpoint) bang tay truoc.

> Historical work record — not durable authority. Prefer docs/specs/ADRs for current decisions.
