# Kịch bản demo dự thảo để Như và Định cùng thống nhất

Mục tiêu tổng thời gian: khoảng 8 phút. Đây là bản chuẩn bị để thảo luận; chưa thay
cho việc hai thành viên cùng tập nói và bấm giờ. Phần NASA/model chính thức chờ Định.

## Chuẩn bị trước buổi thuyết trình

1. Docker Desktop chạy, đủ 6 service; HDFS có 1 DataNode live, ES healthy.
2. Chạy `scripts/run_all.ps1 -Serve`; mở sẵn Spark UI, HDFS UI và hai dashboard.
3. Kiểm tra `detector` đang là `rule_fallback` hay `kmeans_core`, nói đúng chế độ thực tế.
4. Chuẩn bị hai terminal, không chạy nhiều Spark job cùng lúc nếu worker chỉ có 2 core.
5. Mở được `doc/demo-live.webm` và slide/PDF dự phòng. Chọn time range live gần hiện tại.
6. Khi nhận model: kiểm tra CORE_FEATURES đúng thứ tự, window/threshold/centers, load
   bằng `--detector model`; chạy lại test DDoS/scan trước khi đổi nội dung kết luận.

## Phút 0–1: Như giới thiệu bài toán và kiến trúc

“Mỗi request HTTP tạo một dòng log. Nhóm dùng HDFS để lưu dữ liệu, Spark để làm sạch
và trích đặc trưng, Elasticsearch để truy vấn, Kibana để xem lỗi và cảnh báo.”

Mở NameNode `/lake`, chỉ bronze/silver/gold. Mở Spark Master để cho thấy job chạy trên
worker của cluster standalone. Nêu cấu hình một DataNode phục vụ demo, replication=1.

## Phút 1–4: Định trình bày dữ liệu và mô hình

Định dùng số liệu của mình: nguồn NASA, làm sạch, feature, so sánh rule/KMeans/
BisectingKMeans, precision/recall/F1/PR-AUC và hạn chế. Không dùng fixture kiểm thử
hoặc số liệu simulator live để thay thế kết quả batch chính thức.

## Phút 4–7: Như demo real-time

Terminal 1:

```powershell
.\scripts\submit.ps1 stream_detect.py
```

Đợi log `STREAM READY`. Terminal 2:

```powershell
python -m generator.stream_simulate --attack ddos --attack-after 20 --duration 120
```

Mở dashboard Anomaly & Alerts. Giải thích ba trạng thái theo thứ tự:

- Trước tấn công: đang có traffic thường, không quyết định bất thường bằng nhãn thật.
- Khi alert xuất hiện: chỉ `ip`, `window_start`, `score`, `threshold`, `reason`, `detector`.
- Khi rule chạy: xem `alert-history`/Rules, chứng minh action thực thi thành công.

“Trong bài thử đã đo, DDoS mất 18,33 giây và scan mất 12,295 giây tới alert đầu tiên.
Đây là số đo quan sát trên máy thử, không cam kết mọi lần chạy đều đúng con số này.”

Chỉ rõ một IP có nhiều cửa sổ trượt. Số alert window không phải số cuộc tấn công.
Nếu đang fallback, nói rõ score là 0/1 và model chính thức chưa được dùng trong phiên đó.
Nếu máy chậm, chuyển ngay sang video thật, giải thích đây là bản ghi từ cùng pipeline.

## Phương án: demo bằng Trung tâm giám sát

Thay cho hai terminal ở phút 4–7, có thể demo toàn bộ trên một trang tiếng Việt
(`python -m console.server`, mở http://127.0.0.1:8765). Thời gian đo thật ngày 29/10.

**Trước giờ bảo vệ (khoảng 10 phút):**
- Mở trang, cả 5 thẻ Hạ tầng phải xanh.
- Ở mục 4, chạy Kiểm tra tự động "DDoS phá cache" (khoảng 3–4 phút), rồi "Bình thường" (khoảng 4–5 phút).
  Lúc trình bày chỉ cần mở bảng "Các lần kiểm tra gần đây".
- Cuộn xuống mục 5 một lần và tắt hai thông báo của Kibana: bấm *Dismiss* ở banner "Help us improve",
  và tích *Don't show again* ở hộp "Your data is not secure". Trình duyệt nhớ lựa chọn này.
- Bấm "Bật bộ phát hiện" ở mục 2 khoảng 1 phút trước khi đến lượt demo (mất khoảng 25 giây).

| Bước | Bấm gì | Nói gì | Thời gian |
|---|---|---|---|
| 1 | Mục 1 Hạ tầng | "Năm dịch vụ chạy thật trong Docker: Spark 1 worker 4 lõi, HDFS chia tầng bronze/silver/gold, ES, Kibana." Bấm "Xem thư mục /lake" nếu được hỏi | 30 giây |
| 2 | Mục 2: chọn "DDoS phá cache", bấm Chạy | Đọc mô tả kịch bản. "20 giây đầu chỉ có bạn đọc bình thường, sau đó vài chục IP cùng phá cache" | 20 giây |
| 3 | Mục 3, tab Thời gian thực | Khoảng 10–20 giây sau khi tấn công bắt đầu, cảnh báo nhấp sáng. Đọc một câu giải thích, nhấn mạnh "model chỉ biết lệch ở đâu, nhãn mô phỏng chỉ để đối chiếu" | 1 phút |
| 4 | Quay lại mục 2 | Đọc ô "độ trễ phát hiện" và "IP tấn công bị cảnh báo x/y" | 20 giây |
| 5 | Mục 5 Dashboard | Kibana tự làm mới, số "Cửa sổ bất thường" tăng; luật cảnh báo kích hoạt | 30 giây |
| 6 | Mục 4: bảng lịch sử | "Bài kiểm tra tự động đã chạy trước: Đạt, có kiểm tra khởi động lại không ghi trùng" | 20 giây |
| 7 | Mục 2: Dừng | Trả lõi CPU cho cụm | 5 giây |

Nếu bộ phát hiện không lên trong 1 phút, xem thẻ "Tác vụ Spark" (có job khác giữ lõi CPU),
hoặc chuyển sang video `doc/demo-live.webm`.

## Phút 7–8: Cả hai kết luận

Như kết luận phần lưu trữ, serving và cảnh báo. Định kết luận chất lượng mô hình theo
metrics thật. Nêu hạn chế: dữ liệu tổng hợp, một node, ảnh hưởng window/ngưỡng và lệch
phân phối giữa simulator với NASA. Hướng phát triển: thêm worker/DataNode và nguồn Kafka.

## Câu hỏi cần cùng chuẩn bị

- Vì sao dùng cả HDFS và ES? HDFS lưu dữ liệu xử lý; ES phục vụ tìm kiếm/dashboard.
- Vì sao dùng window trượt? Cập nhật tín hiệu liên tục, đổi lại các cửa sổ chồng nhau.
- Watermark có phải độ trễ 2 phút? Không; đó là chính sách xử lý event-time/trạng thái.
- Làm sao tránh alert trùng khi retry? Checkpoint theo dõi input, sink dùng ID ổn định.
- F1 cao có chứng minh tốt ngoài thực tế? Cần xem dữ liệu/đánh giá; simulator dễ tách
  không thay cho kiểm chứng trên dữ liệu và tình huống mới.

## Bàn giao cuối trước khi nộp

- Ghép Word/slide phần Như với phần Định; thống nhất phân công và phần trăm đóng góp.
- Tập ít nhất hai lượt cùng nhau, bấm giờ thật và kiểm tra phương án video dự phòng.
- Chỉ gắn tag bản nộp sau khi NASA/model và báo cáo nhóm đã được rà soát chung.
- Người dùng tự xác nhận/nộp Classroom sau buổi rà soát; hiện chưa nộp bài.
