# Kịch bản demo bảo vệ đồ án (8 phút)

Demo toàn bộ trên trang **Trung tâm giám sát** (http://127.0.0.1:8765), một máy nối máy chiếu.
Không gõ lệnh Spark trong lúc trình bày. Như dẫn mở đầu và phần thời gian thực, Định trình bày dữ liệu và mô hình.

## 0. Số liệu dùng khi nói

Chỉ nói những số dưới đây. Số đo trực tiếp trên lớp thì **đọc đúng số đang hiện**, không đọc số cũ.

| Nội dung | Số | Nguồn, ngày đo |
|---|---|---|
| Log NASA (7/1995) | 1.891.715 dòng đầu vào, 1.891.709 dòng hợp lệ | `bao-cao-nhom.md` 5.1, 3.8.1 |
| Dashboard NASA | tỷ lệ lỗi 0,58%, 81.919 IP | `bao-cao-nhom.md` 3.8.1, chụp 29/10/2026 |
| NASA bị đánh dấu | 2.469 cửa sổ (lần chạy 29/10) | `bao-cao-nhom.md` 4.3 |
| Log mô phỏng | 1.065.283 dòng, có nhãn | `bao-cao-nhom.md` 5.1 |
| Mô hình | KMeans k = 2, ngưỡng θ = 7,4929 (p99 tập train) | `bao-cao-nhom.md` 4.2 |
| Chất lượng (tập test mô phỏng) | Precision 0,720 · Recall 0,998 · F1 0,836 · PR-AUC 0,998 | `bao-cao-nhom.md` 4.2 (báo cáo không ghi ngày đo) |
| Ma trận nhầm lẫn | 567 TP, 221 FP, 1 FN → bắt 567/568, báo nhầm 221/788 | `bao-cao-nhom.md` 4.2 |
| So với luật cố định | F1 0,538, chỉ bắt 33,7% cửa sổ DDoS | `bao-cao-nhom.md` 4.2 |
| CBLOF so với tâm cụm riêng | F1 0,216 → 0,836 | `bao-cao-nhom.md` 4.2 |
| Streaming DDoS (model) | cảnh báo đầu sau 18,317 / 18,318 giây; 105 cửa sổ, sau restart vẫn 105 | `bao-cao-nhom.md` 4.4, ngày 29/10/2026 |
| Streaming traffic bình thường (model) | **6 cửa sổ báo nhầm / 180 giây**, mức cho phép 2 → **Không đạt** | `bao-cao-nhom.md` 4.4, ngày 29/10/2026 |
| Chạy thử DDoS trên trang demo | 36/36 IP tấn công bị cảnh báo, cảnh báo đầu sau ~13 giây; Kibana 185 cửa sổ, 37 IP, luật kích hoạt 1 lần | Như chạy thử ngày 18/11/2026, chưa ghi vào báo cáo |

Khi nói độ trễ, dùng **"khoảng 13 đến 18 giây"** (hai lần đo khác nhau ở trên).

## 1. Chuẩn bị trước giờ bảo vệ (làm trước ~15 phút)

| # | Việc | Kiểm tra đạt khi |
|---|---|---|
| 1 | Mở Docker Desktop, bấm Start project. Chờ ~1 phút | Các container đều chạy |
| 2 | VS Code, mở **một** terminal (Ctrl + \`), gõ `python -m console.server` | Terminal không báo lỗi. **Không** mở terminal thứ hai chạy lệnh này |
| 3 | Mở http://127.0.0.1:8765, bấm thử từng mục trên thanh điều hướng | Phần **Hạ tầng**: 5 thẻ (Cụm Spark, Tác vụ Spark, HDFS, Elasticsearch, Kibana) đều xanh |
| 4 | Phần **Kibana**, bấm tab "Log & lỗi" | Dashboard hiện số, không có popup |
| 5 | Phần **Cảnh báo**, bấm lần lượt tab Thời gian thực / Lô mô phỏng / NASA | Tab Lô mô phỏng và NASA có danh sách cảnh báo |
| 6 | Thẻ **Tác vụ Spark** không có job nào đang chạy | Worker có 2 lõi; bộ phát hiện cần cả 2 lõi |
| 7 | Mở sẵn file `doc/demo-live.webm` (trình phát video, tạm dừng ở đầu) | Phát được |
| 8 | Phóng to trình duyệt (Ctrl + +) cho lớp đọc rõ, để trang ở phần đầu | |

**Không** bấm "Chạy kiểm tra" ở phần Kiểm tra: nút này chưa được chạy thử, và `verify_realtime.py` cần 4 lõi trong khi worker hiện có 2.

Nên tập trước ít nhất 1 lượt đầy đủ với kịch bản DDoS (mục 2 dưới đây), bấm giờ.

## 2. Kịch bản từng bước

Tổng 8:00. Cột "Bấm" ghi đúng chữ trên trang.

### Phần Như: mở đầu và hạ tầng (0:00–1:30)

| Giờ | Bấm | Màn hình hiện | Nói |
|---|---|---|---|
| 0:00 | (không bấm) | Phần đầu trang, dải PIPELINE: HTTP log → HDFS → Spark · KMeans → Elasticsearch → Kibana | "Em chào thầy. Nhóm em làm hệ thống phát hiện tấn công từ log truy cập web. Mỗi lần có người mở trang, máy chủ ghi một dòng log; dải này là đường đi của log: lưu trên HDFS, Spark tính đặc trưng, KMeans chấm điểm bất thường, rồi đẩy sang Elasticsearch và Kibana để xem và cảnh báo." |
| 0:40 | **Hạ tầng** | 5 thẻ xanh: Cụm Spark, Tác vụ Spark, HDFS, Elasticsearch, Kibana | "Các dịch vụ này chạy thật trong Docker trên máy em. HDFS chia ba tầng: bronze là log gốc, silver là log đã làm sạch, gold là đặc trưng và cảnh báo." |
| 1:00 | **Mô phỏng** → **Bật bộ phát hiện** | Ô Spark Streaming hiện "Đang khởi động … / ~35s" | "Em bật trước bộ phát hiện thời gian thực, mất khoảng nửa phút để Spark khởi động. Trong lúc chờ, bạn Định trình bày dữ liệu và mô hình." |

Bật ở đây để đến phút 4:30 đã "Sẵn sàng". Bộ phát hiện tự tắt sau 10 phút, đủ cho 8 phút demo.
Từ lúc này **không chạy thêm job Spark nào**.

### Phần Định: dữ liệu và mô hình (1:30–4:30)

| Giờ | Bấm | Màn hình hiện | Nói |
|---|---|---|---|
| 1:30 | **Kibana** → tab **Log & lỗi** | Dashboard tổng quan log NASA: số request, tỷ lệ lỗi, mã trạng thái, đường dẫn 404 | "Đây là log thật của máy chủ web NASA tháng 7/1995, gần 1,9 triệu dòng. Spark làm sạch, dòng lỗi được tách riêng chứ không xóa. Dashboard thống kê lỗi: tỷ lệ lỗi khoảng 0,58%, các đường dẫn hay bị 404." |
| 2:15 | **Cảnh báo** → tab **NASA** | Danh sách cảnh báo trên log NASA, mỗi cái có IP, câu giải thích tiếng Việt, thanh điểm so với ngưỡng | "Spark gom log theo từng IP trong khung 5 phút, tính 11 đặc trưng như số request, tỷ lệ 404. NASA không có nhãn, nên những cửa sổ này chỉ là đầu mối để điều tra, chưa phải xác nhận tấn công." |
| 3:00 | Tab **Lô mô phỏng** → bấm một nhãn (vd DDoS) | Cảnh báo lọc theo nhãn mô phỏng | "Để đo được độ chính xác, nhóm tự sinh hơn 1 triệu dòng log có nhãn với 4 kiểu tấn công. Mô hình không nhìn nhãn; nhãn chỉ dùng để chấm điểm." |
| 3:30 | (chiếu slide "So sánh có đối chứng" trong `Slide_PhanDinh.pptx`, hoặc chỉ nói) | Bảng so sánh phương pháp | "KMeans chia hành vi thành cụm lớn là bình thường và cụm nhỏ. Điểm bất thường là khoảng cách tới cụm lớn; vượt ngưỡng thì cảnh báo. Trên tập test, mô hình bắt 567/568 cửa sổ tấn công, F1 0,836, còn luật cố định chỉ được 0,538. Điểm yếu là precision 0,72: cứ 100 cảnh báo có khoảng 28 cái báo nhầm." |
| 4:15 | (trả máy cho Như) | | "Tiếp theo bạn Như cho chạy thử tấn công thời gian thực." |

### Phần Như: thời gian thực (4:30–7:00)

| Giờ | Bấm | Màn hình hiện | Nói |
|---|---|---|---|
| 4:30 | **Mô phỏng** | Ô Spark Streaming hiện **"Sẵn sàng"** · 2 CPU · còn mm:ss | "Bộ phát hiện đã sẵn sàng, đang dùng mô hình của Định." |
| 4:35 | Chọn **DDoS phá cache** → **Chạy kịch bản** | Ô kết quả "Kịch bản: …", thanh tiến độ 90 giây, số "dòng log đã phát" tăng | "Kịch bản dài 90 giây: 20 giây đầu chỉ có người đọc báo bình thường, sau đó vài chục IP cùng mở bài viết hiếm để né cache, làm quá tải database. Đây là log giả lập, không tấn công trang thật." |
| 5:00 | (đợi, chỉ vào thanh tiến độ) | Từ giây 20 có "IP tấn công", "request tấn công" | "Tấn công bắt đầu. Spark đọc log mới theo từng đợt nhỏ, tính đặc trưng và chấm điểm ngay." |
| 5:15 | **Cảnh báo** → tab **Thời gian thực** | Cảnh báo mới hiện lên đầu danh sách, có câu giải thích và thanh điểm vượt ngưỡng | "Cảnh báo đã xuất hiện, khoảng 13 đến 18 giây sau khi tấn công bắt đầu. Mỗi cảnh báo ghi IP, khung 5 phút, điểm so với ngưỡng và lý do bằng tiếng Việt, ví dụ tốc độ request cao bất thường." |
| 5:50 | **Mô phỏng** (cuộn xuống ô kết quả) | "IP tấn công bị cảnh báo x/y", "độ trễ phát hiện … giây", "cửa sổ 5 phút bị cảnh báo" | Đọc **đúng số đang hiện**: "Hệ thống cảnh báo x trên y IP tấn công, độ trễ … giây. Số cửa sổ lớn hơn số IP vì cửa sổ trượt mỗi phút, một IP nằm ở nhiều cửa sổ." |
| 6:20 | **Kibana** → tab **Bất thường · 30 phút** | Số cửa sổ bất thường, số IP bị cảnh báo, số lần luật cảnh báo kích hoạt; tự làm mới 10 giây | "Cùng kết quả trên Kibana. Luật cảnh báo chạy 30 giây một lần, thấy cảnh báo mới thì kích hoạt và ghi lịch sử. Đây là phần dashboard và cảnh báo của đề bài." |

Nếu số "Luật kích hoạt" còn 0: chờ thêm 30 giây (luật chạy theo chu kỳ), nói "luật chạy mỗi 30 giây, đang chờ lượt tiếp theo".

### Kết luận (7:00–8:00)

| Giờ | Ai | Bấm | Nói |
|---|---|---|---|
| 7:00 | Như | **Mô phỏng** → **Dừng** | "Em dừng bộ phát hiện để trả CPU cho cụm. Tóm lại hệ thống đủ năm phần đề bài: log máy chủ web, lưu trên HDFS và Elasticsearch, Spark trích đặc trưng, KMeans phát hiện bất thường, dashboard và luật cảnh báo trên Kibana." |
| 7:30 | Định | (không bấm) | "Hạn chế: log mô phỏng do nhóm tự viết nên dễ tách, precision mới 0,72, và chạy thử traffic bình thường trên streaming còn báo nhầm 6 cửa sổ trong 3 phút. Hướng tiếp theo là thử trên log thật có nhãn và chỉnh ngưỡng cho cửa sổ trượt. Nhóm em xin cảm ơn thầy." |

## 3. Dự phòng khi lỗi

| Tình huống | Làm gì | Nói |
|---|---|---|
| Thẻ ở **Hạ tầng** đỏ | Docker Desktop → Restart project, chờ 1 phút, tải lại trang. Trong lúc đó Định trình bày trước bằng slide | "Dịch vụ đang khởi động lại, nhóm trình bày phần mô hình trước." |
| "Bật bộ phát hiện" báo lỗi ngay | Kiểm tra có **hai** terminal đang chạy `console.server` không; tắt bớt (Ctrl + C), chỉ giữ một, tải lại trang | |
| "Đang khởi động" quá 1 phút | Xem thẻ **Tác vụ Spark**: có job khác giữ lõi CPU thì chờ hoặc tắt. Quá 2 phút → chuyển video | |
| Đã "Chạy kịch bản" 40 giây mà chưa có cảnh báo | Chờ đến hết 90 giây. Vẫn không có → chuyển video | |
| Máy chậm, trang treo | Mở `doc/demo-live.webm` | "Đây là bản ghi màn hình từ cùng hệ thống, nhóm chiếu để tiết kiệm thời gian." |
| Kibana nhúng không hiện | Bấm "Mở trong Kibana" (mở tab riêng) | |

Lưu ý video: file `demo-live.webm` ghi ngày 13/10/2026, cùng ngày các lần chạy thử dùng **bộ phát hiện luật** (`rule_fallback`, `bao-cao-nhom.md` 4.4), trước khi có mô hình KMeans ở streaming. **Xem lại video trước** để nói đúng; nếu video dùng luật thì nói "bản ghi lúc hệ thống còn dùng luật cố định, pipeline giống hệt".

Không trình diễn kịch bản **Bình thường** trên lớp: đã biết có báo nhầm (mục 0). Chỉ nói nếu bị hỏi.

## 4. Câu hỏi hay gặp

| Câu hỏi | Trả lời ngắn |
|---|---|
| Vì sao dùng cả HDFS lẫn Elasticsearch? | HDFS là kho lớn, rẻ, để Spark xử lý theo lô. Elasticsearch đánh chỉ mục để tìm kiếm và vẽ dashboard nhanh. |
| Vì sao chia bronze/silver/gold? | Giữ log gốc để chạy lại khi code sai; mỗi tầng sạch hơn; dòng lỗi tách ra `_rejected` chứ không mất. |
| Vì sao gom theo khung 5 phút? | Một request riêng lẻ không nói lên gì; phải nhìn chuỗi hành vi của một IP, ví dụ 40 request trong 5 phút mà phần lớn là 404. |
| Mô hình có biết đó là DDoS hay SQL injection? | Không. Học không giám sát, chỉ biết cửa sổ lệch ở đặc trưng nào. Trang demo dịch đặc trưng đó thành câu tiếng Việt. |
| Vì sao không dùng accuracy? | Tập test chỉ có 568/71.080 cửa sổ là tấn công (~0,8%, tính từ ma trận nhầm lẫn mục 4.2); đoán "bình thường" hết cũng được ~99,2%. |
| **Precision 0,72 có thấp không?** | Có, khoảng 28% cảnh báo là báo nhầm (221/788 cửa sổ). Bù lại recall 0,998, gần như không bỏ sót. Nâng ngưỡng thì ít báo nhầm hơn nhưng dễ bỏ sót. Đây là hạn chế đã ghi trong báo cáo. |
| **Traffic bình thường có bị báo nhầm không?** | Có. Bài thử 3 phút traffic bình thường ngày 29/10 có 6 cửa sổ báo nhầm, vượt mức cho phép 2, nhóm ghi là Không đạt. Nhóm chưa phân tích từng cửa sổ; nghi ngờ do ngưỡng đặt trên cửa sổ không chồng, còn streaming dùng cửa sổ trượt. Hướng sửa: ngưỡng riêng cho streaming hoặc gộp cửa sổ liền nhau của cùng IP. |
| F1 cao vậy có đúng thực tế? | Chưa chắc. Log mô phỏng do nhóm viết nên tấn công rõ; NASA thật không có nhãn nên chưa đo được. |
| Vì sao chọn KMeans dù BisectingKMeans F1 cao hơn 0,02? | Recall bằng nhau, chênh nhỏ trên một bộ dữ liệu chưa đủ để đổi mô hình đã tích hợp với streaming; KMeans phẳng nên dễ giải thích. |
| Vì sao cần CBLOF? | Tấn công tự gom thành cụm nhỏ, so với tâm của chính cụm đó lại trông bình thường. Đo tới cụm lớn thì F1 tăng từ 0,216 lên 0,836. |
| Watermark 2 phút có phải trễ 2 phút? | Không. Đó là thời gian chờ log đến muộn trước khi chốt cửa sổ; độ trễ phát hiện khoảng 13–18 giây. |
| Khởi động lại job có ghi trùng cảnh báo? | Không. Spark lưu checkpoint biết đã đọc tới đâu; mỗi cảnh báo có ID cố định (IP + thời gian) nên ghi lại thì đè lên. Lần thử 29/10: 105 cảnh báo trước và sau restart. |
| Vì sao 185 cửa sổ mà chỉ 36 IP tấn công? | Cửa sổ 5 phút trượt mỗi phút, một IP nằm ở nhiều cửa sổ. Số cửa sổ không phải số cuộc tấn công. |
| Hệ thống mở rộng được không? | Đang chạy 1 worker Spark (2 lõi), 1 DataNode, 1 node Elasticsearch để demo trên laptop, chưa chứng minh mở rộng. Kiến trúc cho thêm worker/DataNode; hướng tiếp là Kafka làm nguồn log. |
| Em làm phần nào? (Như) | HDFS, đưa dữ liệu sang Elasticsearch, Spark Streaming phát hiện thời gian thực, dashboard Kibana và luật cảnh báo, trang demo. |
| Em làm phần nào? (Định) | Bộ sinh log có nhãn, làm sạch dữ liệu, trích đặc trưng, huấn luyện và đánh giá KMeans/CBLOF. |

**Cần kiểm tra trước khi bảo vệ:** lần chạy 18/11 Kibana báo **37 IP** trong khi có 36 IP tấn công. IP dư có thể là một IP bình thường bị báo nhầm, chưa xác minh. Nếu bị hỏi mà chưa kiểm tra: "Có thể là một IP bình thường bị báo nhầm, đúng với hạn chế precision nhóm đã nêu."
