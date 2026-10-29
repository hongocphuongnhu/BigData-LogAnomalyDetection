---
title: "Trung tâm giám sát tiếng Việt — trang điều khiển demo cho báo điện tử bị tấn công"
description: "Một trang web tiếng Việt (server Python stdlib + HTML) gom hạ tầng Spark/HDFS/ES, mô phỏng tấn công, cảnh báo trực tiếp có giải thích, kiểm tra tự động Đạt/Không đạt và dashboard Kibana nhúng."
status: done
priority: P2
effort: "~9h"
branch: "feat/console (tách từ main)"
tags: [frontend, demo, ui, streaming, elasticsearch, kibana]
blockedBy: []
blocks: []
created: 2026-10-29
---

# Trung tâm giám sát tiếng Việt

## Vì sao làm

Pipeline đã chạy đủ 5 tiêu chí đề bài, nhưng mọi thứ nhìn thấy đều là tiếng Anh và thuật ngữ
(Spark UI, HDFS UI, Kibana, `ratio_404`, `top_feature`). Muốn test "tấn công → phát hiện → cảnh báo"
phải gõ lệnh và đọc JSON. Trang này kể lại đúng câu chuyện đó bằng tiếng Việt, bấm nút là chạy lại được.

Chủ đề: **trang báo điện tử bị tấn công**, khớp với simulator sẵn có (đọc bài viết + tầng cache).
Chủ đề chỉ nằm ở lớp hiển thị (tên trang, mô tả kịch bản), **không đổi dữ liệu, contract hay model**.

## Phạm vi và ranh giới

- **Code mới nằm riêng trong `console/`**, chỉ dùng thư viện chuẩn Python (giống quy ước generator).
- **Chỉ đọc output của Như**: ES index `log-alerts` / `log-features`, dashboard Kibana, gọi
  `generator.stream_simulate` và `scripts/verify_realtime.py` như một lệnh, import `scripts/stack_api.py`.
  **Không sửa file nào của Như**, trừ Phase 06 (Việt hóa tiêu đề Kibana, Như đã đồng ý ngày 29/10).
- **Không đổi** `contracts/`, `features_common.py`, model, ngưỡng.
- Chữ trên giao diện **có dấu**. Đây là ngoại lệ được chốt so với quy ước "CLI không dấu";
  comment/docstring trong code vẫn không dấu.
- Server chỉ bind `127.0.0.1`. Nó có quyền chạy tiến trình nên chỉ nhận danh sách kịch bản cố định.

## Ngoài phạm vi

Đăng nhập / phân quyền, WebSocket (dùng polling), đoán loại tấn công (model không giám sát nên không làm được,
xem rủi ro R4), đổi traffic simulator sang nội dung tiếng Việt, deploy ra ngoài máy.

## Kiến trúc

```
Trình duyệt ──HTTP──► console/server.py (127.0.0.1:8765, stdlib http.server)
                         │
   /api/infra ───────────┼──► Spark master :8080/json/, Spark app :4040/api/v1/applications
                         ├──► HDFS :9870 JMX + WebHDFS GETCONTENTSUMMARY /lake/{bronze,silver,gold}
                         ├──► ES :9200 _cluster/health, _cat/indices ; Kibana :5601/api/status
   /api/alerts ──────────┼──► ES log-alerts (source=stream) → console/explain.py → câu tiếng Việt
   /api/live/* ──────────┼──► docker compose exec spark-master spark-submit stream_detect.py (cores.max=2)
                         │    python -m generator.stream_simulate --out-dir data/stream/console/incoming
   /api/check/* ─────────┴──► python scripts/verify_realtime.py --attack X  (stdout → nhật ký tiếng Việt)
   iframe ──────────────────► Kibana /app/dashboards#/view/nhu-anomaly-alerts?embed=true
```

Server làm proxy cho mọi API nên trình duyệt không vướng CORS hay `X-Frame-Options` của Spark/HDFS.

## Các phase

| # | Phase | Ưu tiên | Effort | Phụ thuộc | Trạng thái |
|---|---|---|---|---|---|
| 01 | [Khung server + thẻ Hạ tầng](phase-01-server-va-the-ha-tang.md) | P1 | 2h | — | done |
| 02 | [Cảnh báo trực tiếp + từ điển tiếng Việt](phase-02-canh-bao-truc-tiep.md) | P1 | 1.5h | 01 | done |
| 03 | [Mô phỏng tấn công live](phase-03-mo-phong-live.md) | P1 | 2h | 01, 02 | done |
| 04 | [Kiểm tra tự động Đạt / Không đạt](phase-04-kiem-tra-tu-dong.md) | P1 | 1.5h | 01, 03 | done |
| 05 | [Nhúng dashboard Kibana](phase-05-nhung-kibana.md) | P2 | 0.5h | 01 | done |
| 06 | [Việt hóa tiêu đề Kibana](phase-06-viet-hoa-kibana.md) | P2 | 0.5h | 05 (Như đã đồng ý 29/10) | done |
| 07 | [Tích hợp, README, CLAUDE.md](phase-07-tich-hop-tai-lieu.md) | P2 | 1h | 01–05 | done |

Chế độ `--deep`: Phase 01 viết chi tiết. Phase 02+ có sẵn mục tiêu, file và tiêu chí nghiệm thu,
nhưng **trước khi làm mỗi phase phải scout lại** (đọc file liên quan, curl API thật) và cập nhật phase đó.

## Rủi ro chính

| # | Rủi ro | Cách xử lý |
|---|---|---|
| R1 | Worker chỉ có 4 core, không có `spark.cores.max`: app Spark đầu tiên chiếm hết core, app thứ hai treo ở WAITING. Stream của `verify_realtime.py` xin cả 4 core | Stream của console submit với `spark.cores.max=2`; server khóa chỉ cho chạy **một** việc (live hoặc kiểm tra) tại một thời điểm; chỉ nhả khóa khi Spark master không còn app nào (`activeapps` rỗng) |
| R2 | `verify_realtime.py` chờ tới 110 giây không có alert mới rồi mới bắt đầu, sau đó tự chạy stream riêng; tổng 4–5 phút | Chỉ chạy khi đã dừng live; UI hiện tiến trình và thời gian ước tính, không treo trang |
| R3 | Kill tiến trình `docker compose exec` trên host **không** kill driver Spark trong container (red-team đã xác nhận). Đóng cửa sổ terminal thì `atexit` không chạy, để lại driver mồ côi giữ core và checkpoint | Marker là đường dẫn checkpoint; nút Dừng chạy `pkill -f` trong container; lúc server khởi động và trước mỗi lần bật đều `pgrep` dọn mồ côi; `--duration 600` làm chốt chặn cuối |
| R7 | Trang web lạ mở trên cùng trình duyệt có thể POST "simple request" tới `127.0.0.1:8765` để chạy tiến trình (CSRF / DNS rebinding). Bind localhost không chặn được | POST bắt buộc `Content-Type: application/json` + header `X-Console: 1` (ép preflight); từ chối `Origin`/`Host` khác `127.0.0.1:8765`/`localhost:8765`; không trả header CORS |
| R8 | `STREAM READY` in ra ngay sau `query.start()`, kể cả khi app đang WAITING chưa có core | "Sẵn sàng" = có `STREAM READY` **và** Spark master báo app RUNNING với `cores > 0` |
| R9 | `detected_at` theo đồng hồ container, `first_attack_at` theo đồng hồ host; máy ngủ dậy thì WSL2 lệch giờ | Độ trễ tính bằng giờ host lúc console *thấy* alert đầu tiên trừ `first_attack_at`, giống `verify_realtime.py`; lọc alert theo `attack_ips`, không lọc chặt theo `detected_at >= started_at` |
| R4 | Model không giám sát, **không biết** loại tấn công. Ghi "Phát hiện DDoS" là sai | Câu giải thích dựa trên feature lệch nhiều nhất ("lượng request tăng vọt"); nhãn thật của simulator hiện riêng với chú thích "nhãn mô phỏng, model không dùng" |
| R5 | Đổi tên rule Kibana làm hỏng `verify_realtime.py` (tìm theo tên `Nhu - New anomaly detected`) | Phase 06 **không đổi tên rule**, chỉ đổi tiêu đề dashboard/panel; giữ nguyên mọi ID |
| R6 | Kibana chặn iframe | Kibana 8.x mặc định cho phép (`server.securityResponseHeaders.disableEmbedding=false`); Phase 05 kiểm chứng, nếu bị chặn thì thay bằng nút mở tab mới |

## Nhật ký red-team (29/10)

Một reviewer độc lập đối chiếu plan với code, ra 11 phát hiện, **áp dụng cả 11**:
CSRF/DNS rebinding → R7 + Phase 01 · driver `qa-*` mồ côi giữ 4 core sau kiểm tra lỗi → Phase 04 ·
driver console mồ côi / hai driver chung checkpoint → R3 + Phase 03 · `STREAM READY` chưa có nghĩa là có core → R8 ·
lệch đồng hồ host/container → R9 · pipe đầy làm treo tiến trình → Phase 03 · `stack_api` timeout dài → Phase 02 ·
JSON nhiều dòng → Phase 04 · path traversal static → Phase 01 · "hàng trăm IP" sai (thực tế 20–60) → Phase 03 ·
`--conf spark.app.name` bị job đè → bỏ, dùng checkpoint làm marker.
Đã xác nhận đúng: R1, R2, R5, chuỗi log, khóa JSON báo cáo, trường alert, ID dashboard, `data/stream/` đã ignore.

## Nhật ký validate (29/10)

| Câu hỏi | Trả lời | Ảnh hưởng |
|---|---|---|
| Trang dùng cho ai? | **Chủ yếu để bảo vệ** | Phase 01: đầu tư thiết kế (chữ to đọc được trên máy chiếu, kể chuyện theo thứ tự 5 vùng). Phase 07: tập demo bằng trang, có kịch bản bấm |
| Như đồng ý chưa? | **Đồng ý cả Phase 06** | Phase 06 mở khóa; ràng buộc giữ ID và tên rule vẫn giữ nguyên |
| Nhánh? | **`feat/console` tách từ `main`** | Tài liệu phần Định chưa commit vẫn nằm trong working tree, không lẫn vào nhánh này |

## Tiêu chí hoàn thành toàn plan

- `python -m console.server` mở http://127.0.0.1:8765, toàn bộ chữ tiếng Việt.
- Có đủ 5 thẻ: Hạ tầng, Mô phỏng, Cảnh báo, Kiểm tra tự động, Dashboard.
- Tắt một container (ví dụ ES) thì thẻ tương ứng báo đỏ "Không kết nối được", trang không sập.
- Kịch bản DDoS live: cảnh báo hiện trên trang trong ≤ 60 giây sau khi tấn công bắt đầu.
- Kiểm tra tự động DDoS cho kết quả **Đạt** kèm độ trễ phát hiện.
- `git diff main -- jobs/ scripts/ generator/ contracts/` chỉ có chuỗi hiển thị trong `scripts/kibana_setup.py` (Phase 06).
