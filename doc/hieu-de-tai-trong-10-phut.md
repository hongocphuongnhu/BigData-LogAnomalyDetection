# Hiểu đề tài trong 10 phút

Tài liệu ôn nhanh để trình bày và trả lời câu hỏi. Số liệu lấy từ [bao-cao-nhom.md](bao-cao-nhom.md);
nếu giảng viên hỏi sâu, mở đúng mục trong báo cáo đó.

## 1. Đề tài nói gì (nói trong 30 giây)

> "Nhóm em làm hệ thống **phát hiện tấn công từ log truy cập web**. Mỗi lần có người mở một trang,
> máy chủ ghi một dòng log. Nhóm lưu hàng triệu dòng log trên **HDFS**, dùng **Spark** tính đặc trưng
> hành vi của từng IP trong mỗi khung 5 phút, dùng **phân cụm KMeans** để tìm IP hành xử khác xa người
> dùng bình thường, rồi đẩy kết quả lên **Elasticsearch/Kibana** để vẽ dashboard và bắn cảnh báo.
> Ngoài chạy theo lô, hệ thống còn phát hiện **thời gian thực** bằng Spark Streaming, khoảng 13–18 giây
> sau khi tấn công bắt đầu là có cảnh báo."

Chủ đề minh họa: một **trang báo điện tử** (đọc bài viết, tìm kiếm, đăng nhập quản trị) bị tấn công.

## 2. Một dòng log trông thế nào

Mỗi request là một dòng, gồm: thời gian, IP, phương thức (GET/POST), đường dẫn, mã trạng thái
(200 thành công, 404 không tìm thấy, 5xx máy chủ lỗi), dung lượng trả về, cache HIT/MISS...

Hai nguồn dữ liệu:

| Nguồn | Là gì | Dùng để |
|---|---|---|
| Log NASA (7/1995) | Log thật của máy chủ web NASA, khoảng 1,9 triệu dòng, **không có nhãn** | Chứng minh pipeline chạy được trên dữ liệu thật |
| Log mô phỏng | Do nhóm tự sinh, khoảng 1,07 triệu dòng, **có nhãn** tấn công | Đo độ chính xác (precision, recall, F1) và demo thời gian thực |

## 3. Bốn kiểu tấn công được mô phỏng

| Tên | Hiểu đơn giản | Dấu hiệu trong log |
|---|---|---|
| Dò quét lỗ hổng (scan) | Một IP gõ cửa hàng loạt đường dẫn nhạy cảm `/.env`, `/wp-admin` | Rất nhiều 404, nhiều đường dẫn khác nhau |
| Chèn SQL (sqli) | Nhét câu lệnh SQL vào ô tìm kiếm để đọc trộm database | Đường dẫn chứa mẫu `' OR '1'='1`, `UNION SELECT` |
| Dò mật khẩu (brute force) | Thử đăng nhập trang quản trị hàng chục lần | Nhiều POST vào trang login, nhiều lỗi 401/403 |
| DDoS phá cache | Vài chục IP cùng mở bài viết hiếm để né cache, làm sập DB | Cache MISS cao, máy chủ trả 5xx, tốc độ request cao |

## 4. Đường đi của dữ liệu (5 bước = 5 tiêu chí chấm)

```
Log HTTP ──► HDFS (bronze → silver → gold) ──► Spark: làm sạch + đặc trưng + KMeans ──► Elasticsearch ──► Kibana: dashboard + cảnh báo
```

1. **Dữ liệu log**: NASA thật + mô phỏng.
2. **Lưu trữ HDFS**, chia tầng như "kho" nhiều lớp:
   - *Bronze*: log gốc, giữ nguyên để có thể chạy lại.
   - *Silver*: log đã làm sạch, đúng kiểu dữ liệu (Parquet). Dòng lỗi bị tách riêng chứ không xóa.
   - *Gold*: đặc trưng, điểm bất thường, cảnh báo.
3. **Spark** gom log theo **(IP, khung 5 phút)** và tính 11 đặc trưng, ví dụ: số request, tốc độ request,
   số đường dẫn khác nhau, tỷ lệ 404, tỷ lệ 5xx, tỷ lệ POST, tỷ lệ đường dẫn chứa mẫu tấn công,
   dung lượng trả về trung bình/lớn nhất.
4. **Phân cụm KMeans + chấm điểm CBLOF** (xem mục 5).
5. **Elasticsearch** lưu để tìm kiếm nhanh, **Kibana** vẽ 2 dashboard và chạy **luật cảnh báo** mỗi 30 giây.

## 5. Mô hình hoạt động thế nào (giải thích không cần công thức)

- KMeans chia các "hành vi IP trong 5 phút" thành vài nhóm (cụm) giống nhau. Với log mô phỏng, chọn
  **k = 2** (theo chỉ số Silhouette): một **cụm lớn** (121.608 cửa sổ) là hành vi phổ biến, một **cụm nhỏ** (796 cửa sổ).
- Ý tưởng **CBLOF**: cụm lớn = bình thường. Điểm bất thường của một cửa sổ = khoảng cách từ nó
  tới tâm **cụm lớn** gần nhất. Càng xa càng đáng ngờ.
- **Ngưỡng** = phân vị 99 của điểm trên tập huấn luyện (θ = 7,49). Vượt ngưỡng thì cảnh báo.
  Đặt ngưỡng kiểu này **không cần nhãn**, nên áp được cho log thật.
- **Không giám sát**: mô hình không biết đó là kiểu tấn công gì, chỉ biết "lệch ở đâu". Nhãn mô phỏng
  chỉ dùng để chấm điểm mô hình.

Vì sao cần CBLOF mà không đo tới tâm cụm của chính nó? Vì các cửa sổ tấn công tự gom thành cụm nhỏ,
so với tâm của chính cụm nhỏ đó thì trông lại "bình thường". Đổi sang CBLOF thì F1 tăng từ **0,216 lên 0,836**.

## 6. Con số nên nhớ

| Chỉ số | Giá trị | Ý nghĩa dễ hiểu |
|---|---|---|
| Recall | 0,998 | Bắt được 567/568 cửa sổ tấn công |
| Precision | 0,720 | Trong 100 cảnh báo, khoảng 72 cái đúng, 28 cái báo nhầm |
| F1 | 0,836 | Điểm cân bằng giữa hai chỉ số trên |
| PR-AUC | 0,998 | Mô hình xếp hạng tấn công lên trên rất tốt |
| So với luật viết tay | F1 0,538, chỉ bắt 1/3 DDoS | Mô hình tốt hơn hẳn luật cố định |
| Độ trễ thời gian thực | khoảng 13–18 giây | Từ lúc tấn công tới lúc có cảnh báo |
| Khởi động lại job | Không ghi trùng cảnh báo | Nhờ checkpoint và ID cảnh báo cố định |

Vì sao không dùng accuracy? Chỉ khoảng 0,6% cửa sổ là tấn công. Đoán "bình thường" hết cũng
được 99,4% accuracy, nên accuracy vô nghĩa.

## 7. Phần của Như (nói khi giảng viên hỏi "em làm phần nào")

- Dựng **data lake trên HDFS** (các tầng bronze/silver/gold).
- Đưa dữ liệu sang **Elasticsearch** (5 index: log NASA, log mô phỏng, đặc trưng, cảnh báo, lịch sử cảnh báo).
- **Phát hiện thời gian thực** bằng Spark Structured Streaming và bộ phát log liên tục.
- **Dashboard Kibana** và **luật cảnh báo** (chạy mỗi 30 giây, ghi lịch sử).
- Trang demo **Trung tâm giám sát** và hướng dẫn vận hành.

Phần của Định: bộ sinh log có nhãn, làm sạch log, trích đặc trưng, huấn luyện và đánh giá KMeans/CBLOF.

## 8. Chạy demo

1. Mở Docker Desktop, bấm Start project. Chờ khoảng 1 phút.
2. Trong VS Code mở Terminal (Ctrl + `), gõ `python -m console.server`, mở http://127.0.0.1:8765.
   Chỉ mở **một** terminal chạy lệnh này. Nếu lỡ mở hai cái, nút "Bật bộ phát hiện" sẽ báo lỗi.
3. Làm theo khung "Cách xem demo trong 4 bước" ở đầu trang.
4. Nếu máy chậm hoặc lỗi, chiếu video dự phòng [demo-live.webm](demo-live.webm).

## 9. Câu hỏi hay gặp và cách trả lời

**Vì sao dùng cả HDFS lẫn Elasticsearch?**
HDFS là kho lưu trữ lớn, rẻ, để Spark xử lý theo lô. Elasticsearch đánh chỉ mục để tìm kiếm và vẽ
dashboard nhanh. Mỗi cái làm tốt một việc.

**Vì sao chia bronze/silver/gold?**
Giữ log gốc để làm lại được khi code sai; mỗi tầng sạch hơn tầng trước; dòng log lỗi được tách ra
`_rejected` để kiểm tra chứ không mất.

**Vì sao gom theo khung 5 phút?**
Một request riêng lẻ không nói lên gì. Phải nhìn cả chuỗi hành vi của một IP mới thấy bất thường,
ví dụ 40 request trong 5 phút, phần lớn là 404.

**Streaming dùng cửa sổ trượt nghĩa là gì?**
Khung 5 phút, cứ mỗi 1 phút trượt một lần, nên cảnh báo cập nhật liên tục. Đổi lại một IP có thể
xuất hiện ở nhiều cửa sổ chồng nhau: 105 cảnh báo không có nghĩa là 105 cuộc tấn công.

**Watermark 2 phút có phải độ trễ 2 phút?**
Không. Watermark là thời gian chờ log đến muộn trước khi chốt một cửa sổ. Độ trễ phát hiện thực tế khoảng 13–18 giây.

**Khởi động lại job thì có bị cảnh báo trùng không?**
Không. Spark lưu checkpoint để biết đã đọc tới đâu, và mỗi cảnh báo có ID cố định (IP + thời gian),
ghi lại thì đè lên chứ không thêm mới. Bài kiểm tra tự động đã xác nhận điều này.

**Mô hình có biết đó là DDoS hay SQL injection không?**
Không, đây là học không giám sát. Mô hình chỉ biết cửa sổ lệch ở đặc trưng nào (ví dụ tỷ lệ 404 cao),
trang demo dịch đặc trưng đó thành câu tiếng Việt để người xem hiểu.

**Precision 0,72 có thấp không?**
Khoảng 28% cảnh báo là báo nhầm, đây là hạn chế đã ghi trong báo cáo. Bù lại recall gần tuyệt đối,
không bỏ sót tấn công. Nâng ngưỡng sẽ ít báo nhầm hơn nhưng có thể bỏ sót. Bài thử 3 phút traffic bình
thường với mô hình có 6 cửa sổ báo nhầm, vượt mức cho phép 2: nhóm ghi rõ là chưa đạt.

**F1 cao như vậy có đúng với thực tế không?**
Chưa chắc. Log mô phỏng do nhóm tự viết nên tấn công có dấu hiệu rõ. Log NASA thật không có nhãn
nên chưa đo được. Đây là hạn chế, hướng phát triển là thử trên log thật có nhãn.

**Vì sao chọn KMeans mà không phải BisectingKMeans dù F1 thấp hơn 0,02?**
Recall bằng nhau, chênh lệch nhỏ trên một bộ dữ liệu thì chưa đủ để đổi mô hình đã tích hợp xong
với streaming, và KMeans phẳng nên dễ giải thích hơn.

**Hệ thống có mở rộng được không?**
Đang chạy 1 worker Spark, 1 DataNode, 1 node Elasticsearch để demo trên laptop. Kiến trúc cho phép thêm
worker/DataNode; hướng phát triển là thêm Kafka làm nguồn log thời gian thực.

## 10. Từ khóa

| Từ | Nghĩa |
|---|---|
| Log / access log | Nhật ký truy cập của máy chủ web |
| HDFS | Hệ thống file phân tán của Hadoop, chia file thành block lưu trên nhiều máy |
| Spark | Bộ máy xử lý dữ liệu lớn phân tán; Structured Streaming là phần xử lý luồng liên tục |
| Đặc trưng (feature) | Con số mô tả hành vi, ví dụ tỷ lệ 404 trong 5 phút |
| KMeans | Thuật toán chia dữ liệu thành k cụm, mỗi cụm có một tâm |
| CBLOF | Cách chấm điểm bất thường dựa trên khoảng cách tới cụm lớn |
| Ngưỡng p99 | Giá trị mà 99% cửa sổ huấn luyện có điểm thấp hơn |
| Precision / Recall | Tỷ lệ cảnh báo đúng / tỷ lệ tấn công bắt được |
| Checkpoint | Nơi Spark lưu tiến độ để chạy tiếp sau khi khởi động lại |
| Elasticsearch / Kibana | Cơ sở dữ liệu tìm kiếm / công cụ vẽ dashboard trên nó |
