---
title: "Console phase 03-07: live, kiem tra tu dong, Kibana tieng Viet"
date: 2026-10-29
summary: Hoan tat plan trung tam giam sat; phat hien bai kiem tra none khong on dinh
---

# Console phase 03-07: live, kiem tra tu dong, Kibana tieng Viet

## What happened
- console/runner.py: khoa mot viec mot luc cho stream console (cores.max=2), simulator va verify_realtime.py.
  Scout xac nhan R1/R3: kill client docker exec khong kill driver; pkill theo checkpoint don trong <1s.
- Live DDoS: alert sau 11,1s; dung trong 3s; kill cung server -> lan sau tu don driver mo coi.
- Kiem tra tu dong DDoS: Dat (18,3s, 105=105, alert-history tang), ca truoc va sau khi doi tieu de Kibana.
- Phase 06: 20 tieu de Kibana sang tieng Viet + customLabel (Lens bo label neu thieu co nay).
  verify_dashboard_import.py tim lens theo tieu de tieng Anh -> phai doi chuoi tim theo.

## Decision
- Bai kiem tra none KHONG on dinh: 6 cua so bao nham (cho phep 2). Hai IP binh thuong co mot phan hoi 5xx,
  diem 7,51/7,53 sat nguong 7,49; cua so truot 1 phut nhan 1 IP thanh 5 cua so. Cung seed 73 nhung ranh gioi
  cua so doi theo gio chay. Khong sua model/nguong/verify_realtime (ngoai pham vi, cua Dinh/Nhu).
- Bo nut "Gia han": Bat lai sau khi tu tat lam dung viec do.

## Next steps
- Gui Nhu diff kibana_setup.py + verify_dashboard_import.py va van de none.
- Tap demo 2 lan bang trang, tat banner Kibana tren trinh duyet demo.

> Historical work record — not durable authority. Prefer docs/specs/ADRs for current decisions.
