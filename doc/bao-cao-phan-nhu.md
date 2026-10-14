# Phân tích HTTP access log để phát hiện bất thường

Phần thực hiện của Như: HDFS, Elasticsearch, Structured Streaming, Kibana và cảnh báo.

Nhóm: Định và Như. Ngày kiểm chứng: 13/10/2026.

Tài liệu này cung cấp phần nội dung của Như để ghép vào báo cáo nhóm. Kết quả dữ liệu NASA, mô hình chính thức, bảng so sánh thuật toán và lịch làm việc nhóm thuộc phần Định; không thay thế bằng số liệu giả hoặc model kiểm thử.

## 1. Giới thiệu đề tài

HTTP access log ghi lại thời điểm, địa chỉ client, phương thức, đường dẫn, mã trạng thái và kích thước phản hồi của từng request. Khi lưu lượng tăng, việc kiểm tra từng dòng bằng tay khó phát hiện nhanh các hành vi như dò đường dẫn, thử mật khẩu hoặc flood làm quá tải dịch vụ.

Mục tiêu của đồ án là xây dựng quy trình lưu trữ, xử lý và khai thác log bằng công nghệ Big Data. Đầu vào gồm log thật NASA và log simulator có nhãn. Đầu ra gồm dữ liệu đã làm sạch, đặc trưng theo cửa sổ thời gian, điểm bất thường, dashboard thống kê lỗi và cảnh báo có thể truy vết.

Phạm vi hiện thực được kiểm chứng của Như là simulator trên cụm Spark standalone và HDFS một DataNode, Elasticsearch một node, Kibana 8.19.14. Các dịch vụ chạy bằng Docker Compose. Hệ thống phục vụ học tập và demo local; chưa phải kiến trúc vận hành có tính sẵn sàng cao.

Giá trị của sản phẩm nằm ở việc kết nối dữ liệu thô với thông tin có thể hành động: client nào bất thường, trong cửa sổ nào, do tín hiệu gì và rule cảnh báo có thực thi hay không.

## 2. Phân công và giao diện giữa hai thành viên

- Định phụ trách NASA ingest, feature batch, mô hình phát hiện bất thường, đánh giá precision/recall/F1/PR-AUC và nhận xét kết quả.
- Như phụ trách HDFS data lake, export Elasticsearch, streaming simulator, streaming detector, dashboard, native Kibana alert rule và hướng dẫn vận hành.
- Hai bên dùng chung schema và các hàm trong jobs/features_common.py; thứ tự 11 CORE_FEATURES phải khớp giữa train và inference.
- Model bàn giao gồm PipelineModel tại data/gold/models/kmeans_core và data/reports/model_meta.json chứa features, window, threshold, centers và thông tin huấn luyện.
- Tỷ lệ đóng góp, lịch làm việc nhóm và báo cáo cuối cùng cần thống nhất dựa trên công việc/commit thật, không tự gán phần trăm.

## 3. Cơ sở lý thuyết cho phần lưu trữ và real-time

### 3.1 HDFS

HDFS chia file thành các block và phân phối block trên DataNode. NameNode quản lý namespace, thông tin file và vị trí block; DataNode lưu và phục vụ dữ liệu. Replication giúp có nhiều bản sao khi cụm có nhiều DataNode. Trong môi trường đồ án chỉ có một DataNode, replication được đặt bằng 1 và block size là 128 MB.

Data lake được tổ chức thành bronze, silver, gold. Bronze giữ dữ liệu JSONL đầu vào; silver lưu Parquet sau làm sạch; gold lưu feature, score và alert. Partition theo dt/hour giúp lọc dữ liệu theo thời gian. Parquet có cấu trúc theo cột và nén, phù hợp với truy vấn phân tích chỉ đọc một phần thuộc tính.

HDFS được chọn làm kho dữ liệu xử lý theo lô. Elasticsearch được dùng cho truy vấn phục vụ dashboard. Hai hệ thống có vai trò khác nhau, nên dữ liệu được xử lý bằng Spark rồi đẩy phần cần khai thác sang ES.

### 3.2 Elasticsearch và Kibana

Elasticsearch tổ chức dữ liệu thành document trong index và hỗ trợ tìm kiếm, lọc, tổng hợp. Mapping xác định kiểu trường để việc tìm kiếm và aggregation đúng nghĩa. Field ip được map keyword vì dữ liệu NASA có thể chứa hostname; date được dùng cho event_time/window_start/detected_at; score và ratio là số thực.

Kibana dùng data view để truy vấn index và xây dựng dashboard. Lens hỗ trợ các KPI, biểu đồ thời gian, phân bố status, top client và top path. Saved search hiển thị danh sách cảnh báo cụ thể. File dashboards.ndjson chứa saved objects cùng các tham chiếu cần thiết để import trên môi trường khác.

### 3.3 Structured Streaming

Structured Streaming xử lý dữ liệu mới bằng các micro-batch và dùng cùng biểu thức DataFrame với batch. Job đọc JSON bằng schema cố định, parse event_time UTC, đặt watermark 2 phút và gom theo (ip, cửa sổ 5 phút) với bước trượt 1 phút.

Update mode phát ra các cửa sổ thay đổi ở mỗi micro-batch. foreachBatch chấm điểm và ghi dữ liệu sang ES. Trigger mặc định là 10 giây; thời gian batch thực tế còn phụ thuộc tài nguyên và số trạng thái đang giữ. Watermark hỗ trợ giới hạn trạng thái và xác định dữ liệu quá muộn; không phải là cam kết độ trễ cảnh báo.

Checkpoint lưu tiến độ đọc file và trạng thái aggregation. Simulator ghi file tạm rồi atomic rename để Spark không đọc file đang ghi dở. Sink sử dụng document ID ổn định để retry cùng cửa sổ ghi đè document, thay vì thêm bản sao.

## 4. Kiến trúc và cài đặt

### 4.1 Luồng batch và lưu trữ

Simulator JSONL → HDFS /lake/bronze/http_access → Spark ingest → HDFS /lake/silver/http_access → Spark export → ES logs-sim → Kibana Log Overview.

Job ingest đọc schema cố định, khử trùng trace_id, parse timestamp và ghi silver Parquet. Đường dẫn nhận qua tham số để dùng local hoặc HDFS. Silver trên HDFS được đối chiếu với local trên 16 trường contract cùng event_time, dt, hour; bỏ qua source_file và ingested_at vì khác lần ingest.

Cấu hình dfs.replication phải truyền ở Spark client. Chỉ đặt replication=1 trong NameNode không thay đổi mặc định 3 của Hadoop client bên Spark. Script run_on_hdfs.ps1 truyền spark.hadoop.dfs.replication=1 để tránh trạng thái thiếu replica trên cụm một DataNode.

### 4.2 Các index serving và tính idempotent

- logs-sim: log simulator đã làm sạch; document ID là trace_id.
- logs-nasa: log NASA; dùng hash nội dung cộng số thứ tự trong nhóm dòng giống nhau để giữ multiplicity của dữ liệu thật khi export lặp lại.
- log-features: feature/score theo cửa sổ, có source và detector; ID là hash của source, ip, window_start.
- log-alerts: các cửa sổ vượt ngưỡng; có score, threshold, severity, reason, detected_at và detector.
- alert-history: bằng chứng native Kibana rule đã thực thi Index action.

Export dùng connector elasticsearch-spark-30, bulk 2.000 document, tạm tắt refresh_interval rồi khôi phục cấu hình trong finally. Các cột Vector của ML được loại khỏi document serving. Field error_flag/cache_miss_flag dạng số giúp tính tỷ lệ lỗi và cache MISS bằng average trong Lens.

### 4.3 Chấm điểm streaming

Job hỗ trợ auto, rule và model. Auto chỉ dùng model khi đủ artifact; metadata phải khớp tên/thứ tự CORE_FEATURES, window và ngưỡng hợp lệ. Pipeline được load một lần tại driver và transform trong foreachBatch. Điểm model là khoảng cách Euclid tới tâm cụm được gán, sử dụng hàm chung add_distance_score.

Trong các bài kiểm tra hiện tại, model chính thức chưa được bàn giao nên detector được ghi rõ là rule_fallback. Score rule là 0/1, threshold là 0,5; đây không phải điểm KMeans. Nhãn is_anomaly/anomaly_label chỉ phục vụ đối chiếu, không dùng làm tín hiệu quyết định.

Ngoài rule chung flood, scan, many_errors và suspicious_path, fallback streaming bổ sung cache_busting_flood: n_req và n_distinct_paths ít nhất 5, tỷ lệ MISS ít nhất 95%, tỷ lệ 5xx ít nhất 20% và mean_latency ít nhất 800 ms. Tín hiệu này nhằm xử lý flood phân tán qua nhiều IP mà mỗi IP chưa đạt ngưỡng flood chung.

### 4.4 Dashboard và native alert rule

Log Overview hiển thị số request, tỷ lệ lỗi, số client, request theo thời gian, phân bố status, top client lỗi 4xx, top path 404, tỷ lệ cache MISS và latency p95. Time range mặc định lấy từ khoảng thời gian dữ liệu batch, tránh tình trạng log lịch sử không hiện khi dùng Last 15 minutes.

Anomaly & Alerts hiển thị số cửa sổ bất thường, client bị ảnh hưởng, số lần rule kích hoạt, bảng cảnh báo mới nhất, đường score/threshold, nhãn thật so với dự đoán và các lý do cảnh báo. Auto-refresh là 10 giây. Panel cluster dành cho model chính thức; để trống khi đang dùng fallback.

Native Elasticsearch query rule chạy mỗi 30 giây, kiểm tra log-alerts theo detected_at trong một phút gần nhất, điều kiện count > 0. Rule dùng Server log action và Index action vào alert-history. Action chạy khi trạng thái chuyển sang nhóm cảnh báo, tránh ghi lặp lại mỗi lần lịch chạy nếu cùng trạng thái còn active.

## 5. Kết quả thực nghiệm

Môi trường kiểm chứng: Spark 3.5.8 trên cluster standalone, Elasticsearch/Kibana 8.19.14, Hadoop 3.2.1; một Spark worker và một HDFS DataNode. Số liệu dưới đây lấy từ output thật lưu trong data/reports.

- 61.643 record simulator được ingest vào silver; 0 dòng trùng, 0 malformed JSON, 0 contract violation trong dữ liệu thử hiện có.
- Silver local và HDFS khớp nội dung hoàn toàn. Không có event_time null hoặc partition hour sai hai chữ số.
- Bronze JSONL 31.989.953 byte; silver Parquet 5.192.880 byte, nhỏ hơn khoảng 6,16 lần. Đây là so sánh kích thước của cùng dữ liệu thử, không phải benchmark tổng quát.
- Feature kiểm chứng gồm 13.309 cửa sổ và 16 thuộc tính. Smoke test chạy thành công feature, rule, KMeans, HDFS và ES.
- Export logs-sim hai lần đều có 61.643 document. Lần đầu khoảng 12,636 giây; lần cập nhật khoảng 12,491 giây. Thời gian bao gồm đọc Parquet, tạo document và ghi/refresh ES.
- 180 giây traffic bình thường, 352 record được publish: 0 cảnh báo giả trong bài thử này.
- DDoS: cảnh báo đầu tiên quan sát được sau 18,330 giây; 105 document cửa sổ bất thường, giữ nguyên 105 sau restart checkpoint; native rule ghi lịch sử thành công.
- Scan: cảnh báo đầu tiên quan sát được sau 12,295 giây; 5 document cửa sổ bất thường, giữ nguyên 5 sau restart; native rule tiếp tục thực thi thành công.
- Import saved objects trong Kibana space mới thành công: 2 dashboard, 18 tham chiếu panel. Không xóa volume hiện có để thực hiện kiểm thử này.

Độ trễ được đo từ timestamp request tấn công đầu tiên tới thời điểm chương trình polling thấy alert trong Elasticsearch. Đây là độ trễ quan sát end-to-end, không dùng timestamp đầu micro-batch làm thời điểm hoàn tất ghi.

Một IP có thể nằm trong nhiều cửa sổ trượt. Vì vậy 105 document cảnh báo không có nghĩa là 105 cuộc tấn công độc lập. Các số liệu trên thuộc rule_fallback và bộ simulator thử; không được trình bày thành precision/F1 của model chính thức hoặc kết quả trên NASA.

## 6. Hạn chế và hướng phát triển

Hệ thống local dùng một worker/DataNode/ES node nên chưa chứng minh tính sẵn sàng cao hoặc khả năng chịu lỗi khi mất node. Dữ liệu simulator có đặc trưng tấn công rõ và phạm vi kiểm thử còn nhỏ. Kết quả 0 cảnh báo giả trong 3 phút không thể suy rộng thành tỷ lệ false positive của hệ thống thực tế.

Cần tích hợp dữ liệu NASA và model chính thức của Định, đánh giá trên cùng feature contract, điều chỉnh ngưỡng bằng số liệu thật và kiểm tra lại trên phân phối dữ liệu khác. Các fixture NASA/model chỉ kiểm tra tương thích giao diện, không thay cho thực nghiệm chính thức.

Hướng phát triển gồm thêm DataNode/worker, Kafka làm nguồn log, quản lý schema/version, đánh giá độ trễ ở quy mô lớn, hiệu chỉnh cảnh báo theo tài nguyên dịch vụ, bảo mật kết nối và lưu trữ dữ liệu phục vụ vận hành thực tế.

## 7. Phụ lục vận hành

- Dựng môi trường: cấu hình .env từ .env.example, đặt khóa Kibana, rồi docker compose up -d --build.
- Chạy phần serving: powershell -File scripts/run_all.ps1 -Serve. Linux/Git Bash dùng sh scripts/run_all.sh --serve.
- Chạy streaming: scripts/submit.ps1 stream_detect.py. Để buộc kiểm tra model, thêm --detector model; thiếu artifact sẽ báo lỗi rõ.
- Sinh traffic demo: python -m generator.stream_simulate --attack ddos --attack-after 20 --duration 120.
- Import dashboard: scripts/kibana_import.ps1; connector/rule được tái tạo qua API, không đưa secret vào NDJSON.
- Kiểm thử live: python scripts/verify_realtime.py --attack ddos hoặc --attack scan; chương trình dùng thư mục/checkpoint riêng mỗi lần.
- Kiểm tra HDFS: hdfs dfsadmin -report, hdfs fsck /lake -files -blocks và hdfs dfs -du -s trên các tầng.
- Mở Spark 8080, HDFS 9870, Elasticsearch 9200, Kibana 5601. Cổng Spark có thể đổi trong .env.

Không cần xóa volume hoặc checkpoint đang dùng để demo. Nếu muốn phiên demo mới, chọn --input-path và --checkpoint-path mới đồng thời cho job, và --out-dir tương ứng cho simulator.

## 8. Tài liệu tham khảo

- Apache Spark 3.5.8, Structured Streaming Programming Guide: https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html
- Apache Hadoop 3.2.1, HDFS Architecture: https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
- Elastic 8.19, Elasticsearch query rule: https://www.elastic.co/guide/en/kibana/8.19/rule-type-es-query.html
- Elastic, Set up Kibana alerting: https://www.elastic.co/docs/explore-analyze/alerting/alerts/alerting-setup
- Elastic, Kibana Lens: https://www.elastic.co/guide/en/kibana/8.19/lens.html
- Kế hoạch môn học trong doc/Ke hoach Do an mon hoc Nhap mon Big data_SV.docx; source và các phase trong repository của nhóm.
