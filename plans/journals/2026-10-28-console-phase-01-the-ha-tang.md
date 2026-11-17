---
title: Console phase 01 the Ha tang
date: 2026-10-28
summary: "Xong Phase 01: server stdlib + the Ha tang tieng Viet, commit 08c93e3 tren feat/console"
---

# Console phase 01 the Ha tang

## What happened
- Lam Phase 01 cua plan 261029-0113: `console/server.py`, `console/probes.py`, `console/static/*`.
- Scout API that: ten truong khop plan. Kibana khong chan iframe (R6 go). Spark/HDFS SAMEORIGIN dung nhu du kien.
- Bat ngo 1: `.env` may nay dat Spark UI 18080/14040; cong 8080/4040 la cum Spark cua project khac
  (`spark-observability-demo`). Code doc cong tu `.env` nen dung cum.
- Bat ngo 2: poll khi moi dich vu chet mat 14,6s. Nguyen nhan: Windows ~2s moi dia chi cho cong dong,
  `localhost` thu ca ::1 lan 127.0.0.1. Probe goi 127.0.0.1 -> 2,5s.
- May nay chua co du lieu serving (ES chi smoke-test, HDFS chi _smoke, silver trong).

## Verification
- 0 loi console, khong tran ngang o 1366x768 sang/toi va 390px; tat ES -> the do sau 1,9s.
- Bo chan Host/Origin/X-Console/Content-Type/traversal/Content-Length deu tra dung ma loi.

## Next steps
- Nap du lieu (`scripts/run_all.py --serve`) truoc Phase 02 de co canh bao that trong ES.
- Phase 02: canh bao truc tiep + explain.py.

> Historical work record — not durable authority. Prefer docs/specs/ADRs for current decisions.
