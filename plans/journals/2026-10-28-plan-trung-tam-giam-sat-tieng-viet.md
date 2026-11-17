---
title: Plan trung tam giam sat tieng Viet
date: 2026-10-28
summary: Lap plan 7 phase cho trang dieu khien demo tieng Viet; red-team 11 phat hien deu ap dung
---

# Plan trung tam giam sat tieng Viet

## What happened
- Nguoi dung thay pipeline toan tieng Anh, kho hieu va kho test lai -> muon mot trang tieng Viet.
- Tao plan `plans/261029-0113-trung-tam-giam-sat-tieng-viet/` (--deep, 7 phase): server stdlib trong `console/`,
  the Ha tang (Spark/HDFS/ES/Kibana qua API), canh bao co giai thich, mo phong live, kiem tra tu dong goi
  `scripts/verify_realtime.py`, nhung Kibana `?embed=true`, Viet hoa tieu de Kibana.
- Scout phat hien: worker 4 core khong co `spark.cores.max` (app thu hai treo WAITING); verify_realtime tu chay
  stream rieng va cho 110s yen lang; rule Kibana duoc tim theo ten `Nhu - New anomaly detected`.

## Red-team (11 phat hien, ap dung het)
- CSRF / DNS rebinding toi localhost co the chay tien trinh -> bat buoc JSON + header `X-Console`, kiem Host/Origin.
- Kill `docker compose exec` khong kill driver trong container -> marker = duong dan checkpoint, pkill + pgrep don mo coi.
- `STREAM READY` in ra truoc khi co core -> san sang = RUNNING va cores > 0.
- `detected_at` theo dong ho container -> do tre tinh bang dong ho host nhu verify_realtime.
- Sau verify loi, driver `qa-*` giu 4 core -> pkill sau moi luot, chi nha khoa khi `activeapps` rong.

## Decision
- Nguoi xem chinh: hoi dong bao ve -> uu tien trinh chieu (chu to, thu tu ke chuyen).
- Nhu dong y Phase 06; giu nguyen moi ID va ten rule.
- Code tren nhanh `feat/console` tach tu main.
- Model khong giam sat nen UI khong duoc ghi "phat hien DDoS"; nhan mo phong hien rieng.

## Next steps
- `/ak:cook` Phase 01 (scout curl API truoc khi code).

> Historical work record — not durable authority. Prefer docs/specs/ADRs for current decisions.
