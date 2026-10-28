---
phase: 1
title: "Phase 01: Khung server + thẻ Hạ tầng"
status: done
priority: P1
effort: "2h"
dependencies: []
---

# Phase 01: Khung server + thẻ Hạ tầng

## Mục tiêu

Dựng server và trang rỗng, kèm thẻ **Hạ tầng** đọc số liệu thật từ Spark, HDFS, ES, Kibana bằng tiếng Việt.
Phase này không chạy tiến trình nào, chỉ đọc, nên an toàn để làm trước và kiểm chứng toàn bộ đường kết nối.

## File

- Tạo `console/__init__.py` (rỗng).
- Tạo `console/server.py`: `ThreadingHTTPServer` bind `127.0.0.1`, cổng mặc định `8765` (`--port`).
  Phục vụ `console/static/*` và các route `/api/*` trả JSON (`Content-Type: application/json; charset=utf-8`).
- Tạo `console/probes.py`: mỗi dịch vụ một hàm `probe_*()`, trả `{"ok": bool, ...}`, **không bao giờ ném lỗi**.
- Tạo `console/static/index.html`, `console/static/app.js`, `console/static/style.css`.
- **Không sửa** file nào ngoài `console/`.

## Thiết kế

### Đọc cấu hình

Import `env_config` và `request` từ `scripts/stack_api.py`: thêm `ROOT / "scripts"` vào `sys.path`,
giống cách các script serving đang import nhau. Cổng lấy từ `.env`: `SPARK_MASTER_UI_PORT` (8080),
`SPARK_DRIVER_UI_PORT` (4040); HDFS 9870, ES 9200, Kibana 5601 cố định theo `docker-compose.yml`.

Spark và HDFS gọi bằng `urllib` với timeout 3 giây. `stack_api.request` có timeout 60 giây và retry,
quá chậm cho thẻ trạng thái, nên chỉ dùng nó cho ES/Kibana ở các phase sau.

### `GET /api/infra`

Gọi song song 5 probe (`ThreadPoolExecutor`), trả về:

```json
{
  "spark":  {"ok": true, "workers": 1, "cores": 4, "cores_used": 2, "memory_mb": 2048,
             "apps": [{"name": "stream_detect", "cores": 2, "state": "RUNNING", "duration_s": 120}]},
  "hdfs":   {"ok": true, "live_nodes": 1, "capacity_used": 1700000000, "blocks": 512,
             "lake": {"bronze": 1200000000, "silver": 300000000, "gold": 80000000}},
  "es":     {"ok": true, "status": "green", "indices": {"logs-sim": 1065283, "log-alerts": 2600}},
  "kibana": {"ok": true, "level": "available"},
  "spark_app": {"ok": false, "error": "Không có tác vụ Spark nào đang chạy"}
}
```

| Probe | Endpoint | Trường đọc (scout lại bằng curl trước khi code) |
|---|---|---|
| Spark master | `:8080/json/` | `aliveworkers`, `cores`, `coresused`, `memory`, `activeapps[].{name,cores,state,duration}` |
| Spark app | `:4040/api/v1/applications` | Tên app; không có app thì cổng đóng, đây là trạng thái bình thường |
| HDFS | `:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState` | `NumLiveDataNodes`, `CapacityUsed`, `BlocksTotal` |
| HDFS lake | `:9870/webhdfs/v1/lake/{tier}?op=GETCONTENTSUMMARY` | `ContentSummary.length`; 404 thì báo "chưa có" |
| ES | `:9200/_cluster/health`, `/_cat/indices?format=json` | `status`, `docs.count` theo index |
| Kibana | `:5601/api/status` | `status.overall.level` |

### Bảo mật (dựng ngay từ phase này, các phase sau dùng lại)

Server sẽ có quyền chạy tiến trình (Phase 03/04), nên bộ chặn phải có từ khung đầu tiên (rủi ro R7):

- Từ chối mọi request có `Host` khác `127.0.0.1:8765` / `localhost:8765` (chống DNS rebinding).
- POST: bắt buộc `Content-Type: application/json` **và** header `X-Console: 1`. Nếu có `Origin` thì phải là
  `http://127.0.0.1:8765` hoặc `http://localhost:8765`. Không bao giờ trả header `Access-Control-Allow-*`.
- Static: phục vụ theo whitelist cố định (`index.html`, `app.js`, `style.css`), không ghép đường dẫn từ URL.

### Giao diện

- Tiêu đề trang: **"Trung tâm giám sát — Báo điện tử"**, có dòng phụ giải thích một câu hệ thống làm gì.
- Bố cục 5 vùng theo thứ tự: Hạ tầng, Mô phỏng, Cảnh báo, Kiểm tra tự động, Dashboard.
  Phase này chỉ làm vùng Hạ tầng; 4 vùng còn lại để khung "Sẽ có ở bước sau".
- Mỗi dịch vụ một thẻ: chấm trạng thái (xanh/đỏ), 2–3 con số định dạng tiếng Việt
  (`1.200.000 dòng`, `1,2 GB`), một câu "Dùng để làm gì" và nút **"Mở giao diện gốc ↗"** (tab mới).
- Tự làm mới mỗi 5 giây; lỗi thì thẻ đổi đỏ, ghi "Không kết nối được" kèm gợi ý `docker compose up -d`.
- Khi tạo trang, nạp skill `ak:frontend-design` để giao diện không bị kiểu "SaaS chung chung".
- **Người xem chính là hội đồng bảo vệ** (đã chốt khi validate), nên ưu tiên trình chiếu:
  chữ thân ≥ 18px, con số chính ≥ 32px, tương phản cao, đọc rõ trên máy chiếu 1366×768.
  Thứ tự 5 vùng chính là thứ tự kể chuyện: hạ tầng thật → gây tấn công → hệ thống phát hiện → kiểm chứng → dashboard.
- Có dark mode, màn hẹp vẫn đọc được.

## Các bước

1. Bật stack, curl từng endpoint trong bảng trên và ghi lại tên trường thật vào mục "Ghi chú scout".
2. Viết `probes.py` và test từng probe bằng `python -c`.
3. Viết `server.py` cho static file và route `/api/infra`.
4. Viết HTML/JS/CSS cho thẻ Hạ tầng.
5. Chạy kiểm chứng (bên dưới).

## Kiểm chứng

- `python -m console.server` → `curl -s 127.0.0.1:8765/api/infra` trả JSON hợp lệ, cả 5 khóa có mặt.
- Mở trang bằng skill `browser-automation`: không có lỗi console, có ảnh chụp, chữ tiếng Việt hiển thị đúng dấu.
- `docker compose stop elasticsearch` → thẻ ES đỏ trong ≤ 10 giây, các thẻ khác vẫn xanh,
  server không in traceback. Sau đó `docker compose start elasticsearch`.
- `curl` từ IP khác `127.0.0.1` không kết nối được (server không bind `0.0.0.0`).
- `curl -H "Host: evil.example" 127.0.0.1:8765/api/infra` → 403.
- `curl 127.0.0.1:8765/../console/server.py` và `/static/../server.py` → 404.
- Route POST thử (`/api/ping`): thiếu `X-Console` → 403; `Origin: http://evil.example` → 403;
  `Content-Type: text/plain` → 415; đúng cả ba → 200.
- `git status` chỉ có file mới trong `console/`.

## Ghi chú scout (29/10)

- Tên trường thật khớp bảng trên: Spark `/json/` (`aliveworkers`, `cores`, `coresused`, `memory`, `activeapps`),
  HDFS JMX `FSNamesystemState` (`NumLiveDataNodes`, `CapacityUsed`, `BlocksTotal`, `FilesTotal`),
  WebHDFS `ContentSummary.length/fileCount` và 404 khi thư mục chưa có, Kibana `status.overall.level`.
- **Cổng Spark lấy từ `.env`, không ghi cứng**: máy này đặt `SPARK_MASTER_UI_PORT=18080`, `SPARK_DRIVER_UI_PORT=14040`.
  Cổng 8080/4040 đang là cụm Spark của project khác (`spark-observability-demo`). Cụm của dự án: 1 worker, 4 lõi, 2 GB.
- Windows mất ~2 giây cho mỗi lần kết nối tới cổng đóng, `localhost` thử cả `::1` lẫn `127.0.0.1` → probe gọi `127.0.0.1`
  (lượt poll khi mọi dịch vụ chết: 14,6 s → 2,5 s). Link cho trình duyệt vẫn dùng `localhost`.
- `X-Frame-Options: SAMEORIGIN` ở Spark và HDFS xác nhận đúng. Kibana **không** có `X-Frame-Options`, CSP không có
  `frame-ancestors` → nhúng iframe được (R6 đã gỡ, Phase 05 chỉ cần kiểm chứng lại bằng trình duyệt).
- **Máy này chưa có dữ liệu serving**: ES chỉ có index `smoke-test`, HDFS chỉ có `/lake/_smoke`, `data/silver/` trống
  (gold có sẵn). Phase 02 cần nạp dữ liệu trước (`python scripts/run_all.py --serve`, hoặc `--batch --serve` nếu thiếu silver).
- ES báo `yellow` do `smoke-test` có replica; thẻ hiển thị "Chạy, thiếu bản sao", đúng nghĩa.

## Kết quả kiểm chứng (29/10)

- [x] `/api/infra` trả đủ 5 khóa + `links`, cả 5 `ok: true` khi stack chạy.
- [x] Trình duyệt 1366×768 sáng/tối và 390px: 0 lỗi console, 0 request hỏng, không tràn ngang, chữ thân 18px, số 32px.
- [x] Tắt ES → thẻ ES đỏ sau 1,9 s, 4 thẻ còn lại xanh, log server không có traceback.
- [x] Bind `127.0.0.1`: gọi từ IP LAN không kết nối được.
- [x] `Host: evil.example` → 403; traversal `/../console/server.py`, `/static/../server.py` → 404.
- [x] `/api/ping`: thiếu `X-Console` 403, `text/plain` 415, Origin lạ 403, đúng cả ba 200, OPTIONS 403, không có header CORS.
- [x] `git status`: chỉ thêm `console/` (ngoài các file chưa commit có sẵn).
