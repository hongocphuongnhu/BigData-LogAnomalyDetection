# Phân tích HTTP access log để phát hiện bất thường

Phần thực hiện của Định: dữ liệu NASA và simulator, làm sạch, trích xuất đặc trưng bằng Spark, mô hình phát hiện bất thường và đánh giá.

Nhóm: Định và Như. Ngày kiểm chứng: 13/10/2026.

Tài liệu này cung cấp phần nội dung của Định để ghép vào báo cáo nhóm. Phần HDFS, Elasticsearch, Structured Streaming, dashboard Kibana và rule cảnh báo thuộc phần Như; tài liệu này chỉ nhắc tới ở mức giao diện giữa hai phần.

## 1. Giới thiệu đề tài

Mỗi request HTTP tới máy chủ web để lại một dòng access log: thời điểm, địa chỉ client, phương thức, đường dẫn, mã trạng thái và dung lượng phản hồi. Một dòng log riêng lẻ hầu như không nói lên điều gì. Hành vi bất thường như dò đường dẫn [16], thử mật khẩu [14], chèn SQL [17] hay flood làm quá tải dịch vụ [15] chỉ lộ ra khi nhìn nhiều request của cùng một nguồn trong một khoảng thời gian. Khảo sát của He và cộng sự về phân tích log tự động [9] cho thấy log thường là văn bản bán cấu trúc, phải được phân tích cú pháp thành dạng có cấu trúc trước khi dùng để phát hiện bất thường. Phần việc dưới đây cũng đi theo hướng đó: đọc log thành bảng có cột rõ ràng rồi mới tính đặc trưng.

Phần việc của Định là biến log thô thành dữ liệu có thể phân tích: đọc và làm sạch log, tóm tắt hành vi của từng nguồn theo cửa sổ thời gian thành vector đặc trưng, rồi dùng phân cụm để chấm điểm mức độ khác thường của mỗi cửa sổ. Đầu ra là bảng điểm, danh sách cảnh báo, bảng so sánh phương pháp và một model đóng gói để phần streaming của Như dùng lại.

Đồ án dùng hai nguồn dữ liệu bổ trợ nhau. Log NASA tháng 7/1995 [11] là log thật, có nhiễu và lỗi định dạng thật nhưng không có nhãn tấn công. Simulator của nhóm sinh log có nhãn, trộn traffic bình thường với bốn kiểu tấn công, dùng để đo precision/recall một cách định lượng.

Mọi job chạy trên cụm Spark 3.5.8 [19] chế độ standalone, dựng bằng Docker Compose, gồm một master và một worker. Đây là môi trường học tập và demo, không phải thực nghiệm hiệu năng trên nhiều máy.

## 2. Phân công và giao diện giữa hai thành viên

- Định phụ trách log contract, simulator, ingest NASA và simulator, đặc trưng theo cửa sổ, huấn luyện và so sánh mô hình, đánh giá precision/recall/F1/PR-AUC, hình kết quả và model bàn giao.
- Như phụ trách HDFS data lake, export Elasticsearch, streaming simulator và streaming detector, dashboard, native Kibana alert rule và hướng dẫn vận hành.
- Hợp đồng chung là jobs/features_common.py: schema, hàm build_window_features, danh sách và thứ tự 11 CORE_FEATURES, hàm chấm điểm add_distance_score. Batch và streaming gọi cùng các hàm này nên đặc trưng tính ra ở hai bên có cùng định nghĩa.
- Model bàn giao gồm PipelineModel tại data/gold/models/kmeans_core và data/reports/model_meta.json chứa features, window, k, large_clusters, threshold, centers và mốc dữ liệu huấn luyện. Sau khi bàn giao, model được giữ nguyên; mọi thay đổi về đặc trưng, cách chấm điểm hay ngưỡng phải báo trước cho Như vì ảnh hưởng trực tiếp tới streaming.
- Tỷ lệ đóng góp, lịch làm việc nhóm và báo cáo cuối cùng cần thống nhất dựa trên công việc và commit thật, không tự gán phần trăm.

## 3. Cơ sở lý thuyết cho phần dữ liệu và mô hình

### 3.1 Access log và Common Log Format

Log NASA theo Apache Common Log Format [1]: host, hai trường định danh thường để trống, thời điểm có múi giờ, dòng request trong ngoặc kép, mã trạng thái và số byte trả về. Ví dụ: 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245.

Mã trạng thái 2xx là thành công, 3xx là chuyển hướng hoặc dùng bản cache, 4xx là lỗi phía yêu cầu như 404 không tìm thấy, 5xx là lỗi phía máy chủ [8]. Một request 404 đơn lẻ là chuyện bình thường; tỷ lệ 404 cao kèm nhiều đường dẫn khác nhau trong thời gian ngắn mới là dấu hiệu dò quét.

Log simulator theo Log Contract v1 của nhóm gồm 16 trường, ghi ở dạng JSONL. Ngoài các trường giống CLF còn có latency, trạng thái cache HIT/MISS, user-agent, session và hai trường nhãn is_anomaly, anomaly_label. Nhãn chỉ dùng để đánh giá, không đưa vào mô hình.

### 3.2 Đặc trưng theo cửa sổ thời gian

Đơn vị phân tích là cặp (nguồn, cửa sổ 5 phút): nguồn là IP với simulator, là IP hoặc hostname với NASA. Mỗi cặp được tóm tắt thành một vector đặc trưng. Cách gom này biến hàng triệu dòng log thành vài trăm nghìn bản tóm tắt hành vi và cho phép so sánh các nguồn với nhau.

11 CORE_FEATURES tính được trên cả NASA và simulator: n_req (số request), burst_rate (số request chia cho khoảng thời gian thực tế giữa request đầu và cuối cộng 1 giây), n_distinct_paths (số đường dẫn khác nhau), ratio_4xx, ratio_5xx, ratio_404, ratio_post, ratio_sus_path (tỷ lệ đường dẫn chứa payload SQLi hoặc trỏ tới vùng nhạy cảm), ratio_static (tỷ lệ tải css/js/ảnh), mean_bytes, max_bytes.

Tốc độ request được tính theo công thức (3.1):

burst_rate = n_req / (t_cuối − t_đầu + 1)     (3.1)

Trong đó n_req là số request của nguồn trong cửa sổ, t_đầu và t_cuối là thời điểm (tính bằng giây) của request đầu và request cuối trong cửa sổ. Cộng thêm 1 giây để mẫu số không bằng 0 khi cửa sổ chỉ có một request.

Simulator có thêm 5 đặc trưng EXTENDED: ratio_401, ratio_cache_miss, mean_latency, p95_latency, ratio_script_ua. NASA không có latency, cache hay user-agent nên model triển khai chỉ dùng CORE để áp dụng được cho cả hai nguồn.

burst_rate dùng khoảng thời gian thực tế thay vì chia cứng cho 300 giây, vì một flood dồn trong 10 giây và một người đọc rải đều 5 phút có thể có cùng n_req nhưng tốc độ khác hẳn nhau. n_distinct_paths dùng approx_count_distinct vì Structured Streaming không hỗ trợ phép distinct trên dữ liệu luồng [4]; batch dùng cùng hàm để hai bên cho cùng giá trị.

### 3.3 Chuẩn hóa và KMeans

Các đặc trưng có thang đo rất khác nhau: tỷ lệ nằm trong khoảng 0 đến 1, còn số byte có thể lên hàng triệu. Nếu đưa thẳng vào KMeans, khoảng cách Euclid gần như chỉ phụ thuộc vào byte. Pipeline vì vậy gồm bốn bước, đều là thành phần có sẵn của Spark MLlib [3]: SQLTransformer áp log1p lên các đặc trưng đếm, tốc độ và dung lượng; VectorAssembler ghép đặc trưng theo đúng thứ tự; StandardScaler trừ trung bình rồi chia cho độ lệch chuẩn của từng chiều, hai thống kê này lấy từ tập train; cuối cùng là KMeans. Hai bước biến đổi được viết thành công thức (3.2) và (3.3):

x' = ln(1 + x)     (3.2)

z_j = (x'_j − μ_j) / σ_j     (3.3)

Trong đó x là giá trị gốc của một đặc trưng đếm, tốc độ hoặc dung lượng (các đặc trưng tỷ lệ giữ nguyên, x' = x); x'_j là giá trị chiều j sau bước (3.2); μ_j và σ_j là trung bình và độ lệch chuẩn của chiều j trên tập train; z_j là giá trị đưa vào KMeans. Phép ln(1 + x) nén các giá trị rất lớn và vẫn xác định khi x = 0.

KMeans [13] chia các điểm thành k cụm sao cho tổng bình phương khoảng cách từ mỗi điểm tới tâm cụm của nó nhỏ nhất:

J = Σ ‖z_i − m_c(i)‖², lấy tổng theo i = 1..n     (3.4)

Trong đó n là số cửa sổ train, z_i là vector đã chuẩn hóa của cửa sổ i, c(i) là cụm được gán cho cửa sổ i, m_k là tâm cụm k và ‖·‖ là khoảng cách Euclid. Spark MLlib khởi tạo tâm cụm bằng kmeans||, một biến thể chạy song song của k-means++ [2], [5].

Số cụm k được chọn trong khoảng 2 đến 10 theo hệ số silhouette [18] trên một mẫu tối đa 50.000 cửa sổ của tập train, tính bằng ClusteringEvaluator của Spark [2]:

s(i) = (b(i) − a(i)) / max(a(i), b(i))     (3.5)

Trong đó a(i) là khoảng cách trung bình từ điểm i tới các điểm cùng cụm, b(i) là khoảng cách trung bình nhỏ nhất từ i tới các điểm của một cụm khác. s(i) nằm trong đoạn [−1, 1]; hệ số silhouette của cả tập là trung bình của s(i). Silhouette đo mức điểm gần cụm của mình hơn cụm khác; nó đánh giá cấu trúc cụm chứ không trực tiếp đo khả năng phát hiện tấn công.

Toàn bộ tiền xử lý nằm trong cùng một PipelineModel. Streaming nạp lại model này và dùng đúng mean/std đã học, không fit lại scaler theo từng micro-batch.

### 3.4 Chấm điểm theo CBLOF

Các phương pháp phát hiện bất thường dựa trên phân cụm thường giả định dữ liệu bình thường thuộc các cụm lớn và dày, còn dữ liệu bất thường thuộc cụm nhỏ hoặc nằm xa tâm cụm [6]. Cách đơn giản nhất là lấy khoảng cách từ mỗi điểm tới tâm cụm của chính nó làm điểm bất thường. Cách này hỏng khi các cửa sổ tấn công giống nhau và tự tạo thành một cụm nhỏ: mỗi điểm tấn công nằm sát tâm cụm của nó nên nhận điểm thấp.

Ý tưởng CBLOF (Cluster-Based Local Outlier Factor) của He và cộng sự [10] là phân biệt cụm lớn và cụm nhỏ. Cụm lớn đại diện cho hành vi phổ biến. Điểm thuộc cụm lớn được so với tâm cụm của nó; điểm thuộc cụm nhỏ được so với tâm cụm lớn gần nhất. Như vậy cả một nhóm nhỏ khác xa hành vi phổ biến sẽ nhận điểm cao. Điểm của nhóm được tính theo (3.6) và (3.7):

score(i) = ‖z_i − m_r(i)‖     (3.6)

r(i) = c(i) nếu c(i) thuộc L; ngược lại r(i) là cụm k thuộc L có ‖z_i − m_k‖ nhỏ nhất     (3.7)

Trong đó L là tập cụm lớn, r(i) là cụm tham chiếu của cửa sổ i (cột ref_cluster), các ký hiệu còn lại như ở (3.4).

Trong code, cụm lớn là cụm chiếm ít nhất 5% số cửa sổ train, luôn giữ ít nhất một cụm. Công thức gốc trong [10] nhân khoảng cách với kích thước cụm; code chỉ giữ phần khoảng cách như (3.6). Cửa sổ có dưới 3 request được gán điểm 0, vì với một hai request các tỷ lệ dễ nhảy lên 100% và gây báo động giả. Cùng với điểm, code ghi lại cluster, ref_cluster (cụm dùng làm tham chiếu) và top_feature là chiều có bình phương độ lệch lớn nhất, giúp giải thích vì sao cửa sổ bị đánh dấu.

### 3.5 Ngưỡng và độ đo đánh giá

Ngưỡng cảnh báo θ là phân vị 99 của điểm trên tập train, chỉ tính các cửa sổ có ít nhất 3 request. Một cửa sổ bị cảnh báo khi score(i) > θ. Cách đặt ngưỡng này không cần nhãn nên áp dụng được cho dữ liệu thật. Tập train không bị lọc bỏ tấn công bằng nhãn.

Với simulator, nhãn của một cửa sổ là bất thường nếu cửa sổ chứa ít nhất một request tấn công. Các độ đo được tính theo (3.8) đến (3.11):

Precision = TP / (TP + FP)     (3.8)

Recall = TP / (TP + FN)     (3.9)

F1 = 2 × Precision × Recall / (Precision + Recall)     (3.10)

FPR = FP / (FP + TN)     (3.11)

Trong đó TP là số cửa sổ có tấn công và bị cảnh báo, FP là số cửa sổ bình thường bị cảnh báo, FN là số cửa sổ có tấn công nhưng không bị cảnh báo, TN là số cửa sổ bình thường không bị cảnh báo. Precision cho biết trong các cảnh báo có bao nhiêu phần đúng, Recall cho biết bắt được bao nhiêu phần tấn công, F1 là trung bình điều hòa của hai đại lượng, FPR là tỷ lệ cửa sổ bình thường bị báo nhầm.

PR-AUC là diện tích dưới đường precision–recall khi thay đổi ngưỡng. Với dữ liệu lệch lớp mạnh, đường precision–recall cho nhiều thông tin hơn đường ROC [7]. Ở đây FPR rất nhỏ vẫn có thể đi kèm precision chỉ ở mức trung bình, nên cần đọc các độ đo cùng nhau.

## 4. Kiến trúc và cài đặt

### 4.1 Nguồn dữ liệu

- NASA: access log máy chủ NASA Kennedy Space Center tháng 7/1995, tải bằng scripts/download_nasa.ps1 hoặc .sh vào data/raw/nasa.
- Simulator: generator/simulate.py sinh traffic bình thường theo phiên người đọc bài viết, có mô phỏng cache HIT/MISS, rồi chèn bốn injector tấn công brute_force, sqli, scan, ddos. Mọi ngẫu nhiên đi qua random.Random nhận seed, nên cùng tham số luôn sinh lại đúng cùng dữ liệu.
- Bộ simulator dùng cho đánh giá: python -m generator.simulate --minutes 1440 --rate 12 --attack-repeat 30 --start 2026-01-10T00:00:00Z --seed 42, tức 24 giờ traffic với mỗi kiểu tấn công lặp 30 lần để tập test có đủ mẫu.
- Log được ghi theo partition Hive-style dt=YYYY-MM-DD/hour=HH ở mọi tầng bronze, silver, gold.

### 4.2 Ingest và làm sạch

Job ingest_nasa.py đọc log nén bằng regex CLF, chuẩn hóa timestamp về UTC rồi ghi silver Parquet có cùng tên cột chuẩn với simulator: event_time, ip, method, path, status_code, bytes_out. Các quyết định làm sạch:

- Dòng không khớp CLF hoặc request không bắt đầu bằng HTTP method bị đưa vào silver/_rejected kèm lý do, không xóa im lặng.
- Request kiểu HTTP/0.9 thiếu protocol được giữ, protocol để null.
- Request có dấu cách hoặc rác trong đường dẫn được giữ và gắn cờ request_inconsistent.
- bytes bằng "-" (thường gặp ở 304/302 không có body) được thay bằng 0 và gắn cờ bytes_missing để biết đây là giá trị thay thế.
- Dòng trùng tuyệt đối được giữ: hai request thật của cùng host trong cùng giây là chuyện bình thường, xóa đi sẽ làm sai số request và burst_rate. Số dòng trùng chỉ được đếm để báo cáo.

Job ingest_http_access.py đọc JSONL simulator bằng schema cố định, khử trùng theo trace_id, kiểm tra contract và đưa dòng lỗi vào silver/_rejected. Cả hai job tắt partitionColumnTypeInference để hour=00 không bị đọc thành 0, và dùng partitionOverwriteMode=dynamic để chạy lại chỉ ghi đè đúng partition.

### 4.3 Trích xuất đặc trưng

Job build_features.py đọc silver, gọi build_window_features trong features_common.py và ghi gold/features_{source} theo partition ngày. Simulator tính đủ 16 đặc trưng (CORE và EXTENDED) kèm nhãn tổng hợp của cửa sổ; NASA chỉ tính 11 CORE. Job in ra số cửa sổ, số IP và phân vị p50/p95/p99 của từng đặc trưng; các phân vị này dùng để đặt ngưỡng cho rule baseline.

Batch dùng cửa sổ 5 phút không chồng. Streaming của Như dùng cửa sổ 5 phút trượt 1 phút với cùng hàm, nên số cửa sổ của hai luồng khác nhau nhưng định nghĩa đặc trưng là một.

### 4.4 Huấn luyện, chấm điểm và so sánh

Job detect_anomalies.py so sánh các phương pháp trên cùng bảng đặc trưng:

- rule: ngưỡng viết tay gồm flood (burst_rate ≥ 2 và n_req ≥ 20), scan (ratio_404 ≥ 0,5 và n_distinct_paths ≥ 10), many_errors (ratio_4xx ≥ 0,6 và n_req ≥ 10), suspicious_path (ratio_sus_path ≥ 0,3). Đây là baseline kiểu chuyên gia.
- kmeans_own: KMeans với điểm là khoảng cách tới tâm cụm của chính điểm đó.
- kmeans: KMeans với điểm CBLOF.
- bisecting: BisectingKMeans (phân cụm chia đôi phân cấp) [2] với điểm CBLOF.
- Mỗi phương pháp phân cụm chạy trên cả CORE và EXTENDED để xem đặc trưng chỉ có ở simulator đóng góp bao nhiêu.

Với simulator, dữ liệu được chia theo thời gian: 70% đầu khoảng thời gian để train, 30% cuối để test, tránh để mô hình nhìn thấy tương lai. Với NASA không có nhãn, model fit trên toàn bộ dữ liệu và xuất danh sách cửa sổ có điểm cao nhất kèm hồ sơ từng cụm.

Kết quả ghi ra gold/scores_{source}, gold/alerts_{source} và data/reports/metrics_{source}.json. Script analysis vẽ các hình so sánh vào data/reports/figures.

### 4.5 Bàn giao model cho streaming

Model triển khai là KMeans + CBLOF trên 11 CORE_FEATURES, train trên simulator. Lý do dùng simulator là generator live của Như sinh log cùng dạng và cùng cơ chế traffic với nguồn này. detect_anomalies.py --source sim lưu PipelineModel vào data/gold/models/kmeans_core và metadata vào data/reports/model_meta.json.

Khi nạp, phía streaming kiểm tra metadata khớp hợp đồng: danh sách và thứ tự features đúng CORE_FEATURES, window là 5 phút, algo là kmeans, scoring là cblof, threshold dương, min_req_for_scoring bằng 3, centers trong metadata khớp tâm cụm trong model. Sai một điều kiện là job dừng với thông báo rõ thay vì chấm điểm sai âm thầm. File model Spark được đánh dấu binary trong .gitattributes để checkout không làm hỏng checksum .crc.

## 5. Kết quả thực nghiệm

Môi trường: Spark 3.5.8 standalone, một master và một worker trong Docker. Số liệu dưới đây lấy từ output thật lưu trong data/reports và data/logs, ghi nhận ngày 13/10/2026.

### 5.1 Chất lượng dữ liệu NASA

- Đọc 1.891.715 dòng, ghi 1.891.709 dòng vào silver. Loại 6 dòng: 5 request sai định dạng và 1 dòng bị cắt cụt.
- Giữ 835 dòng trùng tuyệt đối; 19.721 dòng thiếu số byte được gắn cờ bytes_missing; 3.732 request HTTP/0.9 thiếu protocol; 974 request có đường dẫn không nhất quán.
- Khoảng 79.433 host khác nhau (ước lượng), từ 01/07/1995 04:00 đến 28/07/1995 17:32 UTC.
- Phân bố status: 200 chiếm 1.701.534 dòng, 304 có 132.627, 302 có 46.573, 404 có 10.845, 403 có 54, 500 có 62, 501 có 14. Phương thức gần như toàn GET (1.887.646), HEAD 3.952, POST 111.

### 5.2 Dữ liệu và đặc trưng simulator

- 1.065.283 dòng được ingest vào silver; 0 dòng trùng, 0 JSON lỗi, 0 vi phạm contract.
- 238.295 cửa sổ đặc trưng từ 109.035 IP. Theo nhãn: 236.901 cửa sổ bình thường, 1.297 ddos, 34 brute_force, 31 scan, 32 sqli. Dữ liệu mất cân bằng mạnh, khoảng 0,6% cửa sổ có tấn công.
- Tập train có 167.215 cửa sổ trước mốc 2026-01-10 16:48 UTC; tập test có 71.080 cửa sổ sau mốc.
- NASA cho 358.687 cửa sổ CORE. Đây là số cặp (host, 5 phút), không phải số host hay số request.

### 5.3 So sánh phương pháp trên tập test simulator

Các con số ở cấp cửa sổ, với ngưỡng lấy từ train không dùng nhãn:

- Rule baseline: precision 1,000, recall 0,368, F1 0,538. Không báo nhầm nhưng bỏ sót 2/3 cửa sổ DDoS (recall ddos 0,337) vì mỗi IP DDoS chỉ gửi ít request, không đạt ngưỡng flood.
- KMeans, khoảng cách tới tâm cụm riêng: precision 0,221, recall 0,211, F1 0,216, PR-AUC 0,157. Recall ddos chỉ 0,172.
- KMeans + CBLOF, CORE (model triển khai): precision 0,720, recall 0,998, F1 0,836, PR-AUC 0,998, FPR 0,003.
- BisectingKMeans + CBLOF, CORE: precision 0,752, recall 0,998, F1 0,858.
- KMeans + CBLOF, EXTENDED: precision 0,751, recall 0,998, F1 0,857.
- BisectingKMeans + CBLOF, EXTENDED: precision 0,736, recall 0,998, F1 0,848.

Ma trận nhầm lẫn của model triển khai: 567 TP, 221 FP, 1 FN, 70.291 TN. Nói cách khác, model tìm được 567 trong 568 cửa sổ có tấn công, nhưng trong 788 cửa sổ đã báo có 221 cửa sổ báo nhầm. Recall theo loại: brute_force, ddos, scan đều 1,0; sqli 0,9.

Phát hiện quan trọng nhất là bước từ khoảng cách tới tâm riêng sang CBLOF: F1 tăng từ 0,216 lên 0,836. Hình chiếu PCA cho thấy lý do: các cửa sổ tấn công tự gom thành một cụm nhỏ tách xa cụm lớn, nên khi so với tâm của chính nó chúng trông bình thường.

BisectingKMeans CORE có F1 cao hơn KMeans CORE khoảng 0,02. Nhóm vẫn chốt KMeans CORE làm model triển khai và đóng băng hợp đồng với streaming: hai phương pháp có cùng recall, chênh lệch F1 nhỏ trên một bộ simulator chưa đủ để đổi model đã tích hợp, và KMeans phân cụm phẳng nên dễ giải thích hơn. Đây là lựa chọn triển khai, không phải khẳng định KMeans tốt nhất. EXTENDED chỉ nhỉnh hơn CORE một chút, cho thấy 11 đặc trưng chung đã mang phần lớn tín hiệu.

Silhouette chọn k = 2 (0,3656, cao nhất trong khoảng 2 đến 10). Cụm 0 là cụm lớn với 121.608 cửa sổ train, cụm 1 là cụm nhỏ với 796 cửa sổ. Ngưỡng triển khai là 7,4929.

Ngưỡng tối ưu tìm trên chính tập test cho F1 khoảng 0,999, nhưng ngưỡng đó dùng nhãn test nên chỉ là số tham khảo, không được trình bày như kết quả triển khai.

### 5.4 Kết quả trên log thật NASA

- Silhouette chọn k = 3 cho KMeans CORE. Cụm 0 (199.002 cửa sổ) và cụm 2 (39.667 cửa sổ) là cụm lớn; cụm 1 chỉ có 1.215 cửa sổ với tỷ lệ 404 trung bình 0,532, tức một nhóm nhỏ chủ yếu nhận lỗi không tìm thấy.
- Ngưỡng p99 là 6,2966; 2.487 trong 358.687 cửa sổ vượt ngưỡng, khoảng 0,69%.
- Cửa sổ điểm cao nhất: host 163.205.1.45 lúc 14:50 ngày 03/07, 35 request, đặc trưng nổi bật là ratio_5xx; host này còn xuất hiện ở các cửa sổ liền kề 14:40, 14:45, 14:55. Tiếp theo là các cửa sổ có ratio_sus_path cao (piweba4y.prodigy.com, sun1.who.ch) và các cửa sổ POST hiếm gặp trên một site gần như chỉ có GET.
- Trên máy demo của Như, score_demo_batch.py fit lại KMeans NASA với cùng k = 3 và được 2.423 cửa sổ cảnh báo. Khi trình bày trên máy đó dùng 2.423 và nói rõ là đã fit lại; không trộn hai con số và không dùng chung ngưỡng NASA với ngưỡng simulator.

NASA không có nhãn tấn công nên không có precision/recall cho nguồn này. Các cửa sổ bị đánh dấu là đầu mối để người vận hành điều tra, không phải xác nhận tấn công.

## 6. Hạn chế và hướng phát triển

Simulator do nhóm tự viết nên tấn công có dấu hiệu rõ, dễ tách hơn traffic thật; kết quả F1 0,836 chỉ có ý nghĩa trên bộ dữ liệu này. NASA không có nhãn nên chưa đánh giá định lượng được trên log thật. Precision 0,72 nghĩa là khoảng 28% cảnh báo là báo nhầm; nâng ngưỡng sẽ giảm báo nhầm nhưng có thể bỏ sót.

Gom theo IP bỏ qua trường hợp nhiều người dùng chung một IP qua NAT/proxy, và DDoS phân tán với rất ít request trên mỗi IP có thể rơi dưới mức 3 request bị gán điểm 0. Thay bytes thiếu bằng 0 có thể làm lệch mean_bytes và max_bytes trên NASA. Trên NASA, các đặc trưng gần như luôn bằng 0 như ratio_5xx, ratio_post, ratio_sus_path có độ lệch chuẩn rất nhỏ; sau StandardScaler, một cửa sổ hiếm hoi khác 0 bị khuếch đại thành điểm hàng trăm, nên thứ hạng top alert NASA thiên về các sự kiện hiếm hơn là sự kiện nghiêm trọng. Cụm lớn được giả định là hành vi bình thường; nếu tấn công đủ phổ biến để thành cụm lớn thì giả định này sai.

Cấu hình một worker chưa chứng minh khả năng mở rộng. Hướng phát triển gồm: đánh giá trên log web hiện đại có nhãn, phân tích cụ thể các false positive, thêm đặc trưng tổng hợp toàn hệ thống để bắt DDoS phân tán, so sánh với Isolation Forest [12] hoặc autoencoder, và benchmark cùng job trên nhiều kích thước dữ liệu và số worker.

## 7. Phụ lục vận hành

- Dựng môi trường: cp .env.example .env, đặt KIBANA_ENCRYPTION_KEY, rồi docker compose up -d --build.
- Tải NASA: powershell -File scripts/download_nasa.ps1 hoặc sh scripts/download_nasa.sh.
- Sinh simulator: python -m generator.simulate --minutes 1440 --rate 12 --attack-repeat 30 --start 2026-01-10T00:00:00Z --seed 42.
- Ingest: scripts/submit.ps1 ingest_nasa.py và scripts/submit.ps1 ingest_http_access.py.
- Đặc trưng: scripts/submit.ps1 build_features.py --source sim, rồi --source nasa.
- Mô hình: scripts/submit.ps1 detect_anomalies.py --source sim (train, đánh giá, lưu model streaming), rồi --source nasa.
- Toàn bộ chuỗi batch: sh scripts/run_batch.sh. Chuẩn bị đủ batch và serving cho demo: scripts/run_all.ps1 -Batch -Serve -Hdfs.
- Kết quả nằm ở data/reports: dq_nasa.json, metrics_sim.json, metrics_nasa.json, model_comparison.md, model_meta.json và thư mục figures.
- Job chỉ submit lên spark://spark-master:7077 và dùng đường dẫn trong container /opt/spark/data/...; muốn đọc từ HDFS thì truyền hdfs://namenode:9000/lake/... qua tham số đường dẫn.

## 8. Tài liệu tham khảo

Tiếng Anh

1. Apache Software Foundation, Apache HTTP Server Version 2.4 Documentation: Log Files, https://httpd.apache.org/docs/2.4/logs.html, truy cập ngày 29/10/2026.
2. Apache Software Foundation, Spark 3.5.8 Documentation: Clustering, https://spark.apache.org/docs/3.5.8/ml-clustering.html, truy cập ngày 29/10/2026.
3. Apache Software Foundation, Spark 3.5.8 Documentation: Extracting, transforming and selecting features, https://spark.apache.org/docs/3.5.8/ml-features.html, truy cập ngày 29/10/2026.
4. Apache Software Foundation, Spark 3.5.8 Documentation: Structured Streaming Programming Guide, https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html, truy cập ngày 29/10/2026.
5. Bahmani B., Moseley B., Vattani A., Kumar R., Vassilvitskii S. (2012), "Scalable k-means++", Proceedings of the VLDB Endowment 5 (7), pp. 622-633. DOI: 10.14778/2180912.2180915.
6. Chandola V., Banerjee A., Kumar V. (2009), "Anomaly detection: A survey", ACM Computing Surveys 41 (3), pp. 1-58. DOI: 10.1145/1541880.1541882.
7. Davis J., Goadrich M. (2006), "The relationship between Precision-Recall and ROC curves", Proceedings of the 23rd International Conference on Machine Learning (ICML '06), pp. 233-240. DOI: 10.1145/1143844.1143874.
8. Fielding R., Nottingham M., Reschke J. (2022), HTTP Semantics, RFC 9110, Internet Engineering Task Force, https://www.rfc-editor.org/rfc/rfc9110.html.
9. He S., He P., Chen Z., Yang T., Su Y., Lyu M.R. (2021), "A survey on automated log analysis for reliability engineering", ACM Computing Surveys 54 (6), pp. 1-37. DOI: 10.1145/3460345.
10. He Z., Xu X., Deng S. (2003), "Discovering cluster-based local outliers", Pattern Recognition Letters 24 (9-10), pp. 1641-1650. DOI: 10.1016/S0167-8655(03)00003-5.
11. Internet Traffic Archive, NASA-HTTP: Two months of HTTP requests to the NASA Kennedy Space Center WWW server, https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html, truy cập ngày 29/10/2026.
12. Liu F.T., Ting K.M., Zhou Z.-H. (2008), "Isolation Forest", 2008 Eighth IEEE International Conference on Data Mining, pp. 413-422. DOI: 10.1109/ICDM.2008.17.
13. Lloyd S. (1982), "Least squares quantization in PCM", IEEE Transactions on Information Theory 28 (2), pp. 129-137. DOI: 10.1109/TIT.1982.1056489.
14. OWASP Foundation, Brute Force Attack, https://owasp.org/www-community/attacks/Brute_force_attack, truy cập ngày 29/10/2026.
15. OWASP Foundation, Denial of Service, https://owasp.org/www-community/attacks/Denial_of_Service, truy cập ngày 29/10/2026.
16. OWASP Foundation, Forced Browsing, https://owasp.org/www-community/attacks/Forced_browsing, truy cập ngày 29/10/2026.
17. OWASP Foundation, SQL Injection, https://owasp.org/www-community/attacks/SQL_Injection, truy cập ngày 29/10/2026.
18. Rousseeuw P.J. (1987), "Silhouettes: A graphical aid to the interpretation and validation of cluster analysis", Journal of Computational and Applied Mathematics 20, pp. 53-65. DOI: 10.1016/0377-0427(87)90125-7.
19. Zaharia M., Xin R.S., Wendell P., Das T., Armbrust M., Dave A., Meng X., Rosen J., Venkataraman S., Franklin M.J., Ghodsi A., Gonzalez J., Shenker S., Stoica I. (2016), "Apache Spark: A unified engine for big data processing", Communications of the ACM 59 (11), pp. 56-65. DOI: 10.1145/2934664.
