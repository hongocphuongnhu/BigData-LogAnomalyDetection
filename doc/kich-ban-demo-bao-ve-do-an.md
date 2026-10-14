# Kịch bản bảo vệ và demo đồ án: Phát hiện bất thường trong HTTP access log

**Người trình bày:** Định và Như.  
**Bản soạn:** 14/10/2026. Số liệu kiểm chứng được ghi nhận ngày 13/10/2026.  
**Phạm vi:** nội dung nói, thao tác demo và kiến thức phản biện; không phải báo cáo kết quả mới.  
**Mốc source đối chiếu:** `main` tại `60e143c`, cùng code serving/streaming local trên `nhu/realtime`.

**Dùng bản này để tập buổi bảo vệ hiện tại.** [demo-script.md](demo-script.md) là bản nháp cũ, còn số liệu fallback và nội dung chờ model bàn giao.

> **Thông điệp xuyên suốt:** “Từ rất nhiều dòng nhật ký rời rạc, nhóm tạo ra bản tóm tắt hành vi, tìm những hành vi khác thường và đưa cảnh báo có thông tin để người vận hành kiểm tra.”

## 1. Cách sử dụng tài liệu

- **Phần 2–4:** chốt thông điệp, số liệu và chuẩn bị máy trước buổi bảo vệ.
- **Phần 5:** kịch bản chính khoảng **15 phút**, gồm lời nói có thể tập trực tiếp.
- **Phần 6–7:** kiến thức đào sâu và câu hỏi phản biện. Không đọc toàn bộ phần này trên lớp.
- **Phần 8:** bản rút gọn **8 phút** nếu thầy giới hạn thời gian.
- **Phần 9–11:** xử lý sự cố, tập demo và nguồn đối chiếu.

Mỗi cảnh có bốn phần: **mục đích → thao tác → lời dẫn dễ hiểu → ghi chú kỹ thuật**. Phần lời dẫn dành cho người nghe; phần kỹ thuật dành cho hai người trình bày nắm bản chất.

**Nguyên tắc nói:** nêu một ví dụ quen thuộc trước, chỉ một bằng chứng trên màn hình sau, rồi mới gọi tên thuật ngữ. Không cần giả giọng trẻ em; dùng câu ngắn, giải thích từ mới và giữ sự chính xác.

**Thời lượng 15 phút là đề xuất tập luyện, chưa phải thời lượng được giảng viên xác nhận.** Khi có giới hạn chính thức, dùng bản rút gọn hoặc điều chỉnh sau khi bấm giờ.

## 2. Câu chuyện để mọi người đều hiểu

### 2.1. Ví dụ “cuốn sổ của người trực thư viện”

Hình dung một thư viện ghi lại mọi lần có người đến hỏi mượn sách. Mỗi dòng sổ ghi thời gian, người đến, cuốn sách được hỏi và kết quả có tìm được sách hay không.

- Một người hỏi vài cuốn sách là việc thường gặp.
- Một nhóm liên tục hỏi hàng trăm cuốn trong thời gian ngắn có thể khiến người trực quá tải.
- Một người liên tục hỏi những cuốn không tồn tại là một hành vi đáng xem xét.
- Một ngày có sự kiện lớn cũng có thể rất đông người, dù không có ai phá rối.

**Tương ứng trong đề tài:** website là thư viện, request là một lần hỏi, đường dẫn là thứ được yêu cầu, HTTP status là kết quả, log là cuốn sổ ghi chép.

> “Máy tính của nhóm đóng vai người giúp đọc cuốn sổ. Nó không kết luận ai là người xấu. Nó chỉ khoanh những đoạn có hành vi khác thường để người quản lý kiểm tra.”

**Giới hạn của phép so sánh:** IP/hostname chỉ là định danh nguồn trong log, không chắc tương ứng một người. Nhiều người có thể dùng chung một IP; một hoạt động cũng có thể đến từ nhiều IP.

### 2.2. Bốn câu cần làm cho người nghe trả lời được

1. **Dữ liệu là gì?** Nhật ký các request HTTP.
2. **Hệ thống làm gì?** Làm sạch, tóm tắt hành vi, chấm điểm khác thường và ghi cảnh báo.
3. **Vì sao cần Spark?** Để biểu diễn và thực hiện các bước xử lý dữ liệu bằng DataFrame, aggregation, mô hình và streaming trên môi trường Spark.
4. **Kết quả giúp ai?** Người vận hành website có thêm thông tin để khoanh vùng và điều tra.

## 3. Sổ số liệu: nói đúng con số và đúng ý nghĩa

Đây là **ảnh chụp kết quả của lần kiểm chứng trước**, không phải cam kết rằng mọi lần chạy đều có cùng số cảnh báo hay cùng độ trễ.

| Nội dung | Số liệu được dùng | Ý nghĩa và nguồn |
|---|---:|---|
| NASA đọc đầu vào | 1.891.715 dòng | Báo cáo chất lượng dữ liệu [dq_nasa.json](../data/reports/dq_nasa.json) |
| NASA hợp lệ | 1.891.709 dòng | Loại 6 dòng không parse được; không gọi đây là “gần 1,9 triệu tấn công” |
| Dòng trùng NASA được giữ | 835 dòng trùng | Log trùng có thể là những request thực sự giống nhau; không tự động xóa |
| NASA thiếu dung lượng phản hồi | 19.721 dòng | Có cờ `bytes_missing`; giá trị số được thay bằng 0 trong ingest |
| Simulator batch | 1.065.283 dòng | Kết quả đối chiếu local/HDFS/ES trong [verify_demo_data-demo.log](../data/logs/verify_demo_data-demo.log) |
| Features simulator batch | 238.295 cửa sổ | Đơn vị phân tích là IP × cửa sổ thời gian |
| Features NASA batch | 358.687 cửa sổ | Không phải số IP và không phải số request |
| Alerts simulator batch | 2.069 cửa sổ | Tính trên bộ batch, gồm cả các khoảng dùng train và test |
| Alerts NASA trên máy Như | 2.423 cửa sổ | KMeans k=3 được fit lại ở máy demo; xem [log chấm NASA](../data/logs/score_demo_batch-nasa.log) |
| Model streaming | KMeans k=2 + điểm CBLOF | Model train từ simulator, giữ nguyên artifact bàn giao |
| Ngưỡng streaming | 7,492854… | Lấy từ [model_meta.json](../data/reports/model_meta.json); trên màn hình có thể làm tròn 7,493 |
| Normal live trước đây | 180 giây; 346 log; 254 cửa sổ; 0 cửa sổ cảnh báo | Một lần thử có giới hạn, không chứng minh hệ thống luôn không báo nhầm |
| DDoS live trước đây | 18,771 giây đến khi quan sát thấy alert trong ES | 105 alert document trước và sau restart |
| Scan live trước đây | 18,398 giây đến khi quan sát thấy alert trong ES | 5 alert document trước và sau restart |

**Nguồn và giới hạn của số liệu live:** các kết quả CBLOF cuối được ghi nhận ở stdout của phiên kiểm thử ngày 13/10. Các manifest và log vận hành còn ở máy local, ví dụ [DDoS](../data/stream/qa-ddos-57e5a0d5/manifest.json), [scan](../data/stream/qa-scan-4f18a673/manifest.json), [normal](../data/stream/qa-none-1cdf9fbe/manifest.json). Manifest mô tả traffic, không tự chứng minh thời điểm quan sát alert. Trước khi bảo vệ nên chạy lại [verify_realtime.py](../scripts/verify_realtime.py) và lưu stdout của lần tập nếu cần minh chứng độc lập. Các đường dẫn dữ liệu runtime có thể không có trên máy khác.

**Không lấy số liệu từ nhầm phiên:** `data/reports/realtime_ddos.json` và `realtime_scan.json` cũ thuộc `rule_fallback`, không phải nguồn cho hai độ trễ CBLOF ở trên. Bản NASA cũ của Định có 2.487 cửa sổ cảnh báo; khi trình bày máy Như dùng 2.423 và nói rõ đã fit lại. Không ghép ngưỡng NASA với ngưỡng streaming.

### 3.1. Kết quả mô hình được phép trình bày

Nguồn: [metrics_sim.json](../data/reports/metrics_sim.json), [bảng so sánh](../data/reports/model_comparison.md). Các giá trị sau được làm tròn, trên **tập test simulator ở cấp cửa sổ**:

| Phương pháp, CORE features | Precision | Recall | F1 |
|---|---:|---:|---:|
| Rule baseline | 1,000 | 0,368 | 0,538 |
| KMeans, khoảng cách tới tâm cụm được gán | 0,221 | 0,211 | 0,216 |
| KMeans + CBLOF, model được chọn cho streaming | 0,720 | 0,998 | 0,836 |
| BisectingKMeans + CBLOF | 0,752 | 0,998 | 0,858 |

**Không nói “KMeans là phương pháp có F1 cao nhất”.** BisectingKMeans CORE cao hơn trong bảng này. Nhóm đã chốt KMeans CORE làm artifact triển khai; khi được hỏi phải thừa nhận kết quả so sánh và giải thích lựa chọn triển khai, không đổi tên phương pháp hay che kết quả.

**Không dùng F1 ≈ 0,999 làm kết quả triển khai.** `best_f1_on_test` là phép tìm ngưỡng tối ưu có sử dụng nhãn test để tham khảo. Ngưỡng triển khai được chọn từ điểm train không dùng nhãn, cho F1 khoảng 0,836.

## 4. Chuẩn bị máy và lệnh trước buổi bảo vệ

### 4.1. Kiểm tra trước giờ, không tính vào thời gian trình bày

- Hai bạn thống nhất phiên bản source và số liệu; tài liệu cũ có thể còn ghi model chưa bàn giao hoặc fallback.
- Docker Desktop đang chạy; đủ sáu service. Mở UI để xác nhận worker đã đăng ký và HDFS sẵn sàng.
- Dữ liệu batch, model và các index đã chuẩn bị. Dành thời gian riêng cho download, train và bulk export.
- Kiểm tra các cổng UI thực tế theo `.env`; hướng dẫn dưới dùng các cổng mặc định.
- Dùng một ứng dụng Spark tại một thời điểm trên máy demo hiện tại để tránh tranh chấp tài nguyên.
- Trước lượt live, ngừng các generator cũ; dành khoảng 90 giây không phát sinh alert mới để rule có thời gian phục hồi. Rule đang active từ lượt trước có thể không tạo action chuyển trạng thái mới ngay.
- Tập riêng cảnh batch HDFS → HDFS và bấm giờ. Cảnh này dùng job đã có nhưng chưa có số đo thời gian được ghi nhận cho đúng câu lệnh dưới; không hứa sẽ xong trong một số giây cố định.

```powershell
Set-Location C:\BigData-LogAnomalyDetection
docker compose up -d
docker compose ps
docker compose exec -T namenode hdfs dfs -ls /lake/silver
docker compose exec -T namenode hdfs dfs -ls /lake/gold
```

Nếu máy chưa có bộ dữ liệu demo đầy đủ, chuẩn bị trước giờ bằng lệnh của dự án:

```powershell
.\scripts\run_all.ps1 -Batch -Serve -Hdfs
```

Lệnh này xử lý dữ liệu và nạp serving, có thể tốn nhiều phút. Không dùng nó thay cho cảnh demo batch ngắn có chủ đích.

### 4.2. Các tab cần mở

| Tab | Địa chỉ mặc định | Sử dụng lúc nào |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | Xem `/lake`, input và output batch |
| Spark Master | http://localhost:8080 | Xem worker và running application |
| Spark Application | http://localhost:4040 | Xem Jobs, Stages, SQL, Executors khi app đang chạy |
| NASA overview | http://localhost:5601/app/dashboards#/view/nhu-log-overview | Thống kê dữ liệu thật |
| Live anomalies | http://localhost:5601/app/dashboards#/view/nhu-anomaly-alerts | Cảnh báo, score, cluster, lý do |
| Kibana Rules | Stack Management → Rules | Rule `Nhu - New anomaly detected` |
| Kibana Discover | Data view `alert-history` | Xem thời điểm rule ghi lịch sử |

Ở dashboard NASA, giữ khoảng thời gian lịch sử năm 1995 đã lưu. Ở dashboard live, chọn Last 30 minutes và refresh 10 giây. `alert-history` dùng thời điểm ghi lịch sử hiện tại.

**4040 chỉ hoạt động khi driver đang chạy.** Khi app kết thúc, UI có thể đóng. Dự án hiện không cấu hình Spark History Server; không hứa có thể mở lại toàn bộ Jobs/Stages của app đã dừng.

### 4.3. Terminal A: khai báo helper và tên phiên mới một lần

Helper này chỉ là lệnh thao tác lúc demo, không yêu cầu thêm file code. Nó truyền replication=1 khi ghi HDFS, phù hợp máy có một DataNode.

```powershell
Set-Location C:\BigData-LogAnomalyDetection
$demoRun = "defense-" + (Get-Date -Format "yyyyMMdd-HHmmss")
Write-Host "TEN PHIEN DEMO: $demoRun"

function Invoke-DemoSpark {
    param([string]$Job, [string[]]$JobArgs)
    & docker compose exec -T spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --conf spark.sql.shuffle.partitions=16 `
        --conf spark.hadoop.dfs.replication=1 `
        --py-files /opt/spark/jobs/features_common.py `
        "/opt/spark/jobs/$Job" @JobArgs
    if ($LASTEXITCODE -ne 0) { throw "Spark application that bai: $Job" }
}
```

Giữ terminal này xuyên suốt. Chép **đúng tên phiên vừa in** sang terminal B ở phần live; không tạo hai tên khác nhau ở hai terminal.

## 5. Kịch bản chính khoảng 15 phút

| Thời gian dự kiến | Cảnh | Người chính |
|---|---|---|
| 00:00–01:00 | Bài toán và ví dụ thư viện | Như |
| 01:00–02:00 | Kiến trúc và vai trò công nghệ | Như |
| 02:00–03:00 | Dữ liệu thật, làm sạch và chất lượng | Định |
| 03:00–05:00 | Submit batch Spark, quan sát UI, kiểm tra output | Định thao tác; Như hỗ trợ UI |
| 05:00–07:00 | Features, KMeans và CBLOF | Định |
| 07:00–08:00 | Đánh giá và so sánh baseline | Định |
| 08:00–11:00 | Streaming DDoS, Spark UI, Kibana và native rule | Như |
| 11:00–12:00 | Restart checkpoint và kiểm tra số cảnh báo | Như |
| 12:00–13:00 | Đối chiếu dữ liệu và độ tin cậy | Như |
| 13:00–15:00 | Giới hạn, giá trị và kết luận | Cả hai |

Đây là phân bổ để tập, không phải kết quả bấm giờ. Có thể nói phần features trong lúc batch chạy, nhưng không khởi chạy streaming khi batch chưa kết thúc.

### Cảnh 1 — Người nghe hiểu bài toán trước khi thấy công nghệ

**Mục đích:** cả lớp hiểu nhóm đang giải quyết việc gì và giúp ai.

**Như nói:**

> “Mỗi khi chúng ta bấm vào một trang web, máy chủ có thể ghi lại một dòng nhật ký: lúc nào có yêu cầu, yêu cầu trang nào và kết quả ra sao. Tên của cuốn nhật ký đó là log.
>
> Hãy hình dung thư viện ghi lại mọi lần có người đến hỏi mượn sách. Nếu có hàng triệu dòng, người quản lý không thể đọc từng dòng để biết chuyện gì đang xảy ra.
>
> Nhóm em xây dựng một hệ thống giúp đọc cuốn sổ đó: tóm tắt hành vi, tìm những đoạn khác thường và đưa cảnh báo để người vận hành kiểm tra.
>
> Hôm nay nhóm sẽ cho thấy dữ liệu thật đã được xử lý, chạy một ứng dụng Spark ngay trên lớp, sau đó tạo một đợt log DDoS giả lập để quan sát cảnh báo mới.”

**Chuyển cảnh:** “Trước khi chạy, nhóm giới thiệu ngắn nơi cất dữ liệu và nơi thực hiện công việc.”

### Cảnh 2 — Kiến trúc bằng ví dụ kho sách, bàn làm việc và bảng thông báo

**Thao tác:** Như mở HDFS rồi Spark Master, chỉ các thư mục và worker.

**Như nói:**

> “HDFS giống kho lưu các cuốn sổ dữ liệu. Spark giống nhóm người cùng chia nhau xử lý các phần việc. Elasticsearch giúp tìm kết quả nhanh, còn Kibana là bảng thông báo để người dùng nhìn thấy kết quả.
>
> Dữ liệu được tổ chức thành ba lớp: bronze là đầu vào; silver là dữ liệu đã làm sạch; gold là đặc trưng, điểm số và cảnh báo.
>
> Máy demo dùng các container trên cùng một máy: một Spark worker và một HDFS DataNode. Nhóm dùng môi trường này để thực hiện và quan sát cơ chế xử lý; khả năng mở rộng ra nhiều máy cần thực nghiệm riêng.”

**Hai luồng phải phân biệt đúng:**

```text
Batch đã chuẩn bị:
NASA + simulator → Spark ingest/làm sạch → Parquet silver
                                    → features → mô hình/điểm → ES → Kibana
Các bảng batch được đưa lên HDFS và đã đối chiếu số dòng.

Cảnh batch thực hiện trên lớp:
HDFS silver NASA → Spark build_features → thư mục gold demo riêng trên HDFS

Live trên lớp:
Generator JSONL → Spark Structured Streaming → ES features/alerts → Kibana/rule
```

**Ghi chú kỹ thuật:** pipeline chuẩn bị hiện có các bước xử lý trên bind mount local rồi đưa bảng lên HDFS. Luồng live đọc file JSONL, không đi qua Kafka và không mặc định lưu mọi event live vào HDFS. Chỉ nói đúng đường đi của từng cảnh.

### Cảnh 3 — Dữ liệu thật và quyết định làm sạch

**Thao tác:** Định mở NASA overview, chỉ tổng request, status distribution và top 404 paths.

**Định nói:**

> “Nguồn dữ liệu thật là access log NASA tháng 7 năm 1995. Job đọc 1.891.715 dòng và giữ lại 1.891.709 dòng hợp lệ.
>
> Làm sạch không chỉ là xóa tất cả những dòng nhìn lạ. Ví dụ, có dòng thiếu số byte trả về: nhóm giữ một cờ để biết giá trị đó bị thiếu. Có những dòng trùng nhau: nhóm giữ chúng vì hai request thực sự có thể giống nhau.
>
> Trên dashboard, tổng request cho thấy lượng dữ liệu đã xử lý. Biểu đồ status cho thấy kết quả phục vụ; bảng đường dẫn 404 giúp tìm những yêu cầu thường không tìm thấy tài nguyên.
>
> NASA không có nhãn xác nhận tấn công. Vì vậy, nhóm dùng nguồn này để phân tích dữ liệu thật và xem các hành vi nổi bật, không tuyên bố mọi dòng cảnh báo là một cuộc tấn công.”

**Nếu hỏi HTTP status:** 2xx thường biểu thị xử lý thành công; 4xx là lỗi phía yêu cầu; 5xx là lỗi phía máy chủ. Một request 404 đơn lẻ không đủ kết luận tấn công.

**Chuyển cảnh:** “Bây giờ nhóm sẽ chạy bước biến nhật ký thành bảng tóm tắt hành vi bằng Spark.”

### Cảnh 4 — Chạy batch Spark thật từ HDFS, không chỉ mở dashboard

**Mục đích:** chứng minh input, quá trình tính toán và output bằng một lần chạy mới.

**Định thao tác ở terminal A:**

```powershell
$batchOutput = "hdfs://namenode:9000/lake/gold/demo/$demoRun/features_nasa"
Invoke-DemoSpark -Job "build_features.py" -JobArgs @(
    "--source", "nasa",
    "--silver-path", "hdfs://namenode:9000/lake/silver/nasa_access",
    "--gold-path", $batchOutput
)
```

**Như hỗ trợ:** mở Spark Master 8080, chọn application `build_features_nasa`; mở 4040 khi driver sẵn sàng. Nếu nhìn thấy app vừa chạy thì chỉ trực tiếp, không dùng ảnh của app khác để nói là hiện tại.

**Định nói lúc submit:**

> “Lệnh này gửi một ứng dụng Spark lên cluster. Dữ liệu đầu vào là silver NASA trong HDFS; kết quả được ghi vào một thư mục demo mới để giữ nguyên kết quả chính đã chuẩn bị.
>
> Spark đang gom các dòng có cùng nguồn truy cập và cùng khoảng 5 phút, rồi tính những con số mô tả hành vi của nhóm dòng đó.”

**Lời dẫn khi chỉ UI:**

> “Ở Jobs, chúng ta thấy các công việc tính toán. Mỗi job được chia thành stages; trong mỗi stage có các tasks xử lý từng phần dữ liệu. Executor trên worker thực hiện các task đó.
>
> Khi gom nhóm theo IP và thời gian, những dòng thuộc cùng nhóm cần được đưa về nơi có thể tổng hợp chúng. Bước trao đổi dữ liệu này gọi là shuffle. Nó giải thích vì sao chỉ tăng số dòng chưa đủ để dự đoán thời gian chạy.”

**Nếu mở SQL:** chỉ các bước đọc Parquet, aggregation và trao đổi dữ liệu nếu UI thể hiện. Không hứa tên node hoặc số stage cố định vì Spark có thể thay đổi kế hoạch thực thi.

**Sau khi lệnh hoàn thành:** đọc số cửa sổ thật trong console. Với dữ liệu đã đối chiếu, kỳ vọng 358.687 cửa sổ; nếu khác thì ghi nhận và kiểm tra input thay vì đọc thuộc con số.

```powershell
docker compose exec -T namenode hdfs dfs -ls "/lake/gold/demo/$demoRun/features_nasa"
```

**Định kết cảnh:**

> “Đây là kết quả mới vừa được tạo từ dữ liệu trong HDFS. Một dòng kết quả tóm tắt một nguồn truy cập trong một khoảng thời gian, thay vì trình bày từng request rời rạc.”

**Giới hạn cần nói đúng:** cảnh này tạo features, chưa train lại model hay export thư mục demo sang ES. Dashboard NASA đang dùng bảng chính đã chuẩn bị trước; cảnh streaming tiếp theo mới chứng minh dashboard nhận kết quả mới trực tiếp.

### Cảnh 5 — Vì sao phải tạo features trước khi học mô hình?

**Định nói:**

> “Nếu chỉ nhìn từng lần hỏi mượn sách, rất khó biết một người đang hỏi bình thường hay hỏi dồn dập. Ta cần nhìn cách họ hành động trong một khoảng thời gian.
>
> Với website cũng vậy. Nhóm tạo bản tóm tắt gồm: có bao nhiêu request, request đến nhanh đến mức nào, hỏi bao nhiêu đường dẫn khác nhau, tỷ lệ lỗi và dung lượng phản hồi.
>
> Những con số tóm tắt này được gọi là đặc trưng, hay features.”

**Ví dụ để giải thích — hoàn toàn minh họa, không phải số đo của NASA:**

| Hành vi trong một khoảng quan sát | Người dùng A | Nguồn B |
|---|---:|---:|
| Số request | 8 | 300 |
| Số đường dẫn khác nhau | 4 | 250 |
| Tỷ lệ request trả về 404 | 0% | 80% |

> “B nhìn khác A ở nhiều mặt. Nhưng B vẫn có thể là một hoạt động hợp lệ. Features giúp mô tả khác biệt; bước tiếp theo là chấm điểm và đưa thông tin cho người vận hành.”

**Ghi chú cho thầy:** model CORE dùng 11 features. `burst_rate` được tính từ số request chia cho khoảng giữa request đầu/cuối cộng 1 giây; không đơn thuần lấy số request chia 300 giây. Số đường dẫn dùng `approx_count_distinct`, là giá trị xấp xỉ. Định nghĩa chi tiết nằm trong [features_common.py](../jobs/features_common.py).

### Cảnh 6 — KMeans và CBLOF: giải thích được vì sao cách cũ bỏ sót

**Định nói:**

> “Hãy tưởng tượng mỗi bản tóm tắt hành vi là một điểm. Những điểm có nhiều đặc điểm giống nhau được gom lại gần nhau.
>
> KMeans tìm các nhóm như vậy. Với model streaming của nhóm, số nhóm là 2. Tâm nhóm là một điểm đại diện cho các điểm trong nhóm.
>
> Nhưng có một vấn đề: những hành vi đáng ngờ có thể giống nhau và tự tạo thành một nhóm nhỏ. Nếu chỉ hỏi ‘điểm này có gần tâm nhóm của chính nó không?’, ta có thể bỏ qua cả nhóm nhỏ đó.
>
> Vì vậy nhóm dùng cách chấm điểm dựa trên CBLOF. Điểm thuộc nhóm lớn được so với tâm nhóm của nó; điểm thuộc nhóm nhỏ được so với tâm nhóm lớn gần nhất. Cách này giúp nhận ra một nhóm nhỏ khác xa các hành vi phổ biến.”

**Định nói tiếp về ngưỡng:**

> “Score là mức độ khác biệt, không phải phần trăm chắc chắn bị tấn công. Nhóm lấy một mốc từ điểm số trên dữ liệu train: score vượt mốc thì tạo cảnh báo.
>
> Với model streaming đã bàn giao, mốc này khoảng 7,493. Các cửa sổ chỉ có một hoặc hai request được cho score bằng 0, vì quá ít dữ liệu có thể làm tỷ lệ lỗi trông rất lớn.”

**Ghi chú kỹ thuật:** code dùng biến thể khoảng cách dựa trên ý tưởng CBLOF, không nhân score với kích thước cụm. Không đọc công thức của thư viện CBLOF khác rồi khẳng định code này giống hệt. Cụm lớn được xác định từ tỷ lệ kích thước tối thiểu 5%, có bảo đảm giữ ít nhất một cụm; metadata streaming hiện có `large_clusters=[0]`.

### Cảnh 7 — Đánh giá có đối chứng, không chỉ khoe một số F1

**Thao tác:** mở bảng [model_comparison.md](../data/reports/model_comparison.md) hoặc hình [model_comparison.png](../data/reports/figures/model_comparison.png) đã có.

**Định nói:**

> “Để biết phương pháp có hữu ích không, nhóm so với các cách khác trên cùng bài toán.
>
> Luật viết tay báo khá chắc trong bài thử này nhưng bỏ sót nhiều cửa sổ tấn công. KMeans dùng khoảng cách tới tâm cụm riêng có F1 khoảng 0,216. Khi đổi cách chấm điểm sang CBLOF, F1 lên khoảng 0,836.
>
> Điều quan trọng ở đây là nhóm tìm ra một nguyên nhân bỏ sót và kiểm tra một cách cải thiện, thay vì chỉ chạy một thuật toán rồi chấp nhận kết quả.”

**Giải thích precision và recall:**

> “Precision trả lời: trong những cửa sổ máy đã báo, bao nhiêu cửa sổ thực sự có log tấn công theo nhãn của simulator? Recall trả lời: trong những cửa sổ có log tấn công, máy đã tìm được bao nhiêu?
>
> Model được chọn đạt precision khoảng 72% và recall khoảng 99,8% trên tập test này. Nó tìm được phần lớn cửa sổ tấn công, nhưng vẫn có báo nhầm. F1 khoảng 0,836 tóm tắt sự cân bằng giữa hai mặt.”

**Ghi chú kỹ thuật:** train/test chia theo 70%/30% khoảng thời gian, không khẳng định 70%/30% số dòng. Nhãn được dùng để tính các độ đo và đối chiếu, không nằm trong 11 feature đầu vào. Tập train không được lọc bỏ tấn công bằng nhãn; ngưỡng lấy p99 điểm trên train đủ số request.

**Chuyển lời:** “Các kết quả vừa rồi là xử lý dữ liệu đã có. Sau đây Như sẽ cho thấy model đó được áp dụng với log mới.”

### Cảnh 8 — Live: tạo log mới, xem Spark xử lý và cảnh báo trên Kibana

**Mục đích:** chứng minh luồng hoạt động, không chỉ chiếu kết quả lưu sẵn.

**Terminal A, sau khi batch kết thúc:**

```powershell
$streamArgs = @(
    "--detector", "model",
    "--input-path", "/opt/spark/data/stream/$demoRun/incoming",
    "--checkpoint-path", "/opt/spark/data/stream/$demoRun/checkpoint"
)
Invoke-DemoSpark -Job "stream_detect.py" -JobArgs ($streamArgs + @("--duration", "125"))
```

Chờ `STREAM READY: detector=kmeans_cblof`.

**Như nói:**

> “Ứng dụng streaming đã sẵn sàng. Nó dùng model và cách tiền xử lý đã học ở phần batch. Model không được train lại theo từng nhóm log mới.
>
> Generator sau đây tạo các dòng log mô phỏng và ghi vào file. Nó không gửi một cuộc tấn công mạng tới NASA hay một website bên ngoài.”

**Terminal B, dán đúng tên phiên từ terminal A:**

```powershell
Set-Location C:\BigData-LogAnomalyDetection
$demoRun = "DAN_DUNG_TEN_PHIEN_TU_TERMINAL_A_VAO_DAY"
$liveSince = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
Write-Host "MOC LOC DEMO: $liveSince"
Write-Host ('source: stream and detector: kmeans_cblof and detected_at >= "' + $liveSince + '"')

python -B -m generator.stream_simulate `
    --attack ddos --attack-after 20 --duration 75 --rate 2 --seed 73 `
    --out-dir "data/stream/$demoRun/incoming" `
    --manifest "data/stream/$demoRun/manifest.json"
```

Như chép chuỗi KQL vừa in vào thanh tìm kiếm của dashboard Anomaly & Alerts. Đây là filter cho panel đọc `log-features`/`log-alerts`; panel `alert-history` không có các trường này nên có thể trống khi dùng filter toàn dashboard. Xem lịch sử rule ở Discover riêng trong bước tiếp theo.

**Như nói trước tấn công:**

> “Trong 20 giây đầu, generator phát sinh traffic bình thường. Sau đó nó chèn một đợt DDoS giả lập: nhiều nguồn yêu cầu dồn dập những tài nguyên khác nhau. Trong log mô phỏng còn có dấu hiệu lỗi khi tải tăng.
>
> Nhóm dùng bộ lọc thời gian vừa in để quan sát kết quả của lượt này, tránh đọc nhầm cảnh báo cũ.”

**Thao tác trong lúc chờ:** mở 4040, chỉ micro-batch/job mới; sau đó quay về Kibana. Không nói mỗi 10 giây chắc chắn có một batch hoàn thành: nếu xử lý lâu hoặc không có dữ liệu, nhịp thực tế có thể khác.

**Như giải thích cửa sổ:**

> “Spark nhìn hành vi trong cửa sổ 5 phút và cập nhật mỗi lần cửa sổ trượt 1 phút. Dù cửa sổ chưa kết thúc, kết quả đang có vẫn được cập nhật, nên không cần chờ 5 phút mới cảnh báo.
>
> Máy đang xử lý theo các đợt nhỏ. Dashboard cũng tự làm mới định kỳ, vì vậy có một khoảng trễ từ lúc log phát sinh đến lúc chúng ta nhìn thấy dòng mới.”

**Khi có alert, chỉ một dòng thực tế:**

> “Đây là nguồn truy cập và khoảng thời gian bị đánh dấu. Score lớn hơn threshold nên `is_alert` là true.
>
> Cluster là nhóm được gán. Ref_cluster là nhóm được dùng làm tham chiếu để tính điểm. Nếu hai số khác nhau, ta đang thấy cách chấm điểm cho một cửa sổ thuộc cụm nhỏ.
>
> Top_feature cho biết đặc trưng có độ lệch bình phương lớn nhất so với tâm tham chiếu sau tiền xử lý. Ví dụ, nếu ở đây là burst_rate, tốc độ request là điểm nổi bật trong phép so sánh này.”

Đọc đúng giá trị đang thấy; không mặc định mọi alert đều có cùng reason hay severity. `top_feature` giải thích độ lệch của mô hình, không chứng minh nguyên nhân gốc của sự cố.

**Thao tác rule:** chuyển Rules và Discover data view `alert-history`, đặt Last 15 minutes, tìm bản ghi có thời gian trong lượt demo. Bỏ KQL có `detector`/`detected_at` khi chuyển sang data view lịch sử.

**Như nói:**

> “Rule này kiểm tra mỗi 30 giây xem có alert mới trong một phút gần nhất không. Khi chuyển sang trạng thái cảnh báo, action ghi vào lịch sử và server log.
>
> Đây là bằng chứng phản ứng tự động sau bước phát hiện. Chỉ bật rule chưa đủ; nhóm kiểm tra cả bản ghi lịch sử mới và kết quả thực thi.”

**Ba thời gian khác nhau cần phân biệt:** log đầu tiên của attack → quan sát alert trong ES; ES → màn hình tự refresh; điều kiện rule → lần kiểm tra/action. Số 18,771 giây là phép đo thứ nhất ở lần thử trước, không phải cam kết thời gian tới email hay thời gian network của website.

### Cảnh 9 — Restart để chứng minh có xử lý vận hành

**Điều kiện:** generator đã kết thúc 75 giây; ứng dụng streaming ban đầu đã dừng sau thời lượng 125 giây và không báo lỗi. Không đếm khi vẫn còn dữ liệu mới đang được ghi/xử lý.

**Terminal B, khai báo hàm đếm sau khi generator kết thúc:**

```powershell
function Get-DemoAlertCount {
    param([string]$Since)
    $body = @{
        query = @{ bool = @{ filter = @(
            @{ term = @{ source = "stream" } },
            @{ term = @{ detector = "kmeans_cblof" } },
            @{ range = @{ detected_at = @{ gte = $Since } } }
        ) } }
    } | ConvertTo-Json -Depth 10
    (Invoke-RestMethod -Method Post `
        -Uri "http://localhost:9200/log-alerts/_count" `
        -ContentType "application/json" -Body $body).count
}
$before = Get-DemoAlertCount -Since $liveSince
Write-Host "TRUOC RESTART: $before"
```

Lệnh API này dùng cấu hình ES local hiện tại không bật authentication. Nếu cấu hình đã thay đổi thì dùng client của dự án có đọc `.env`, không in thông tin xác thực lên màn chiếu.

**Terminal A, dùng lại chính input và checkpoint:**

```powershell
Invoke-DemoSpark -Job "stream_detect.py" -JobArgs ($streamArgs + @("--duration", "25"))
```

**Như nói lúc restart:**

> “Nhóm khởi động lại ứng dụng với cùng checkpoint, giống mở lại cuốn sổ ở chỗ đã đánh dấu. Checkpoint lưu thông tin tiến độ và trạng thái tổng hợp.
>
> Khi ghi Elasticsearch, hệ thống còn dùng ID ổn định theo nguồn dữ liệu, IP và thời điểm bắt đầu cửa sổ. Ghi lại cùng cửa sổ sẽ cập nhật document có cùng ID.”

**Sau khi restart kết thúc, terminal B:**

```powershell
$after = Get-DemoAlertCount -Since $liveSince
Write-Host "SAU RESTART: $after"
Write-Host "KHONG TANG TRUNG: $($before -eq $after)"
```

**Chỉ kết luận đạt khi:** before > 0, after = before, job restart kết thúc thành công. Nếu cả hai đều 0 thì chưa chứng minh được phục hồi alert. Lần kiểm chứng trước có DDoS 105 → 105 và scan 5 → 5; không hứa lượt hiện tại phải ra đúng hai số đó.

**Không gọi đây là bằng chứng tuyệt đối exactly-once cho mọi sự cố.** Bài thử chứng minh trường hợp restart đã thực hiện; checkpoint và ID ổn định là cơ chế giảm ghi trùng. Sink `foreachBatch` cần thiết kế idempotent và kiểm thử thêm các kiểu lỗi khác.

### Cảnh 10 — Chất lượng hệ thống, giới hạn và kết luận

**Như nói:**

> “Nhóm kiểm tra không chỉ mô hình mà cả đường đi của dữ liệu. Trong lần đối chiếu, log, scores và alerts của cả NASA và simulator có cùng số dòng giữa local, HDFS và Elasticsearch.
>
> Nhóm cũng kiểm tra model load trên máy demo, điểm batch và streaming cho cùng bảng features, khả năng import dashboard sang space mới và restart checkpoint.
>
> Lần thử traffic bình thường 3 phút ghi nhận 0 cửa sổ cảnh báo giả. DDoS và scan được quan sát thấy alert trong khoảng 18 giây. Đây là kết quả của các bài thử cụ thể, cần đánh giá dài hơn trước khi dùng ngoài thực tế.”

**Định nói:**

> “Giới hạn thứ nhất là simulator do nhóm kiểm soát nên có thể dễ phân biệt hơn traffic thật. Thứ hai, NASA không có nhãn tấn công nên không thể tính precision/recall có ý nghĩa cho NASA chỉ từ những điểm bị đánh dấu.
>
> Cấu hình hiện tại chạy trên một máy với một worker và một DataNode. Nhóm chưa có thực nghiệm nhiều máy hay benchmark đủ rộng để kết luận khả năng mở rộng.”

**Như kết lại:**

> “Giá trị của hệ thống là biến nhật ký rời rạc thành thông tin có thể kiểm tra: nguồn nào, thời gian nào, mức độ khác biệt ra sao và đặc trưng nào nổi bật.
>
> Nhóm đã xây dựng và kết nối các bước từ xử lý dữ liệu đến mô hình, dashboard và cảnh báo tự động. Bước tiếp theo hợp lý là đánh giá trên nguồn log mới, kéo dài bài thử traffic bình thường và đo hiệu năng khi tăng dữ liệu hoặc tài nguyên.
>
> Nhóm em xin kết thúc phần demo và sẵn sàng trao đổi về dữ liệu, thuật toán và cách hệ thống vận hành.”

## 6. Kiến thức đào sâu: hiểu để giải thích bằng lời của mình

### 6.1. Log, request, cửa sổ và alert là bốn thứ khác nhau

| Khái niệm | Cách nói dễ hiểu | Bản chất kỹ thuật |
|---|---|---|
| Request | Một lần hỏi website lấy một thứ gì đó | Một yêu cầu HTTP |
| Log record | Một dòng ghi lại chuyện vừa xảy ra | Có timestamp, nguồn, path, status và các thuộc tính khác |
| Feature window | Bản tóm tắt một nguồn trong một khoảng thời gian | Một dòng `(ip, window_start, window_end, features...)` |
| Alert document | Một bản tóm tắt vượt ngưỡng và được ghi cảnh báo | Khóa document ổn định theo source/IP/window_start |

Vì cửa sổ trượt có chồng nhau, một đợt tấn công có thể làm nhiều cửa sổ của cùng IP vượt ngưỡng. Do đó, 105 alert document không có nghĩa có 105 cuộc tấn công hay 105 kẻ tấn công.

### 6.2. Batch và streaming giống, khác nhau ở đâu?

**Ví dụ:** batch là đọc cả cuốn sổ đã viết; streaming là đọc tiếp các trang mới khi người trực đang viết.

- Batch có một tập dữ liệu đầu vào hữu hạn để xử lý.
- Streaming đọc file JSONL mới và cập nhật trạng thái tổng hợp theo micro-batch.
- Hai bên dùng chung cách tạo features, cùng preprocessing/model/metadata khi chấm bằng model streaming.
- **Batch mặc định là cửa sổ 5 phút không chồng; live là cửa sổ 5 phút, trượt 1 phút.** Số cửa sổ không giống nhau.
- Kiểm chứng “batch/stream parity” đã so hai hàm chấm điểm trên cùng bảng features. Nó không khẳng định hai luồng luôn sinh ra đúng cùng tập cửa sổ hay cùng số alert.
- Cửa sổ live còn đang mở có ít dữ liệu hơn cửa sổ batch đầy đủ. Đây là một nguồn khác biệt phân phối cần đánh giá, không được bỏ qua chỉ vì dùng cùng model.

### 6.3. Driver, worker, executor, job, stage, task

**Cách nói dễ hiểu:** có một người tổ chức công việc, nơi bố trí người làm, những người trực tiếp làm và từng phần việc nhỏ được giao.

| Thành phần | Bản chất |
|---|---|
| Application | Chương trình Spark được submit, có driver và executor |
| Driver | Tạo SparkContext/SparkSession, lập kế hoạch và điều phối |
| Master standalone | Cluster manager cấp tài nguyên cho application |
| Worker | Tiến trình/nút có thể chạy executor |
| Executor | Tiến trình thực thi task, quản lý dữ liệu cache/shuffle của app |
| Job | Công việc tính toán được kích hoạt bởi action, ví dụ ghi Parquet hoặc `count()` |
| Stage | Một tập task của job có thể thực hiện trong cùng đoạn kế hoạch; shuffle thường tạo ranh giới stage |
| Task | Một đơn vị thực thi trên partition trong một stage |

Không gọi Spark Master là nơi xử lý toàn bộ các dòng; không đồng nhất một file Python với đúng một Spark job. Một application có thể tạo nhiều jobs.

Một worker có thể chạy nhiều tasks theo tài nguyên được cấp. Điều đó thể hiện cơ chế chia việc trên máy demo, chưa phải thực nghiệm nhiều worker trên nhiều máy vật lý.

### 6.4. Vì sao groupBy có shuffle? Vì sao không phải càng nhiều partition càng nhanh?

Các dòng cùng IP/cửa sổ có thể nằm ở nhiều partition. Spark cần trao đổi kết quả để gom đúng nhóm; bước đó tốn CPU, bộ nhớ, mạng và có thể I/O đĩa. Aggregation có thể có giai đoạn tổng hợp cục bộ trước khi trao đổi dữ liệu; kế hoạch thực tế xem ở SQL UI.

Partition quá ít làm ít phần việc chạy đồng thời; quá nhiều gây overhead và nhiều file nhỏ. Dự án có `repartition` theo cột partition trước khi ghi để hạn chế tạo quá nhiều file. Chưa có benchmark đủ rộng để nói 16 là số partition tối ưu.

Với file NASA gzip, bước đọc file nén không tự chia đều ra nhiều task chỉ vì có nhiều executor. Job ingest đọc rồi repartition cho các bước phía sau; không khẳng định repartition làm bản thân việc giải nén một file gzip trở thành đọc song song tùy ý.

### 6.5. Vì sao cần log1p và StandardScaler?

**Ví dụ:** nếu một phép so sánh dùng cả số quyển sách và dung lượng tính bằng byte, con số byte có thể lớn đến mức che các thông tin khác.

Pipeline model gồm:

1. `SQLTransformer`: `log1p` trên các feature số lượng/tốc độ/dung lượng được chỉ định; ratio không qua log1p.
2. `VectorAssembler`: ghép các feature đúng thứ tự.
3. `StandardScaler`: đưa các chiều về thang chuẩn hóa với mean/std từ train.
4. KMeans: gán cụm trên vector đã biến đổi.

`log1p(x) = log(1+x)` giúp giảm độ chênh giữa các giá trị rất lớn và thông thường. StandardScaler giúp so sánh các chiều ở thang đo phù hợp hơn. Preprocessing nằm trong PipelineModel để batch và streaming tái sử dụng đúng cùng phép biến đổi; không fit scaler mới trong mỗi micro-batch.

### 6.6. Công thức chấm điểm đang dùng

Gọi `z` là vector sau preprocessing; `c_j` là tâm cụm j; `L` là tập cụm lớn; `a` là cụm được KMeans gán.

```text
Nếu n_req < 3:
    score = 0
Nếu n_req >= 3 và a thuộc L:
    ref_cluster = a
    score = khoảng cách Euclid(z, c_a)
Nếu n_req >= 3 và a không thuộc L:
    ref_cluster = cụm lớn gần z nhất
    score = khoảng cách Euclid(z, c_ref)

is_alert = (score > threshold)
```

Code vẫn tính các cột tham chiếu trước khi áp dụng gate score=0. Công thức trên giải thích quyết định chấm điểm, không yêu cầu các cột tham chiếu phải trống khi `n_req < 3`.

Khoảng cách Euclid là căn bậc hai của tổng bình phương độ lệch từng chiều. `top_feature` là chiều có bình phương độ lệch lớn nhất tới tâm tham chiếu. Mức độ alert theo tỷ lệ `score / threshold`: từ 2 trở lên là high; từ 1,5 tới dưới 2 là medium; các alert còn lại là low. Low cũng là alert đã vượt ngưỡng, không đồng nghĩa bình thường.

**Cụm lớn không được bảo đảm toàn bình thường.** Đó là quy ước tham chiếu dựa trên mức phổ biến. Một hành vi xấu rất phổ biến hoặc một hành vi tốt hiếm gặp có thể làm giả định này kém phù hợp.

### 6.7. Chọn k, ngưỡng và nhãn như thế nào?

- Batch so sánh k từ 2 đến 10 bằng silhouette trên mẫu train, rồi fit lại pipeline cho k được chọn.
- Silhouette đánh giá cấu trúc cụm theo không gian đặc trưng; không trực tiếp đo chất lượng phát hiện tấn công.
- Model triển khai streaming đã được bàn giao với k=2; không chọn lại k trên sân khấu.
- Ngưỡng là p99 điểm trên train đủ số request. Train có thể chứa log tấn công vì cách học không dùng nhãn để loại chúng.
- p99 không có nghĩa chính xác 1% request hợp lệ sẽ bị báo nhầm trên mọi dữ liệu mới.
- Nhãn `is_anomaly`/`anomaly_label` được tổng hợp để đánh giá cửa sổ simulator. Một cửa sổ có ít nhất một event tấn công được gắn nhãn bất thường trong đánh giá.
- Regex đường dẫn đáng ngờ là kiến thức domain được thiết kế thành feature. “Không dùng nhãn để train” không có nghĩa hệ thống không chứa bất kỳ quy tắc thiết kế nào của con người.

### 6.8. Đọc bảng đánh giá như thế nào cho đúng?

Đây là số liệu thật của KMeans + CBLOF CORE trên tập test trong `metrics_sim.json`:

| Nhãn thật ở cấp cửa sổ | Máy báo bất thường | Máy không báo |
|---|---:|---:|
| Có event tấn công | 567 — TP | 1 — FN |
| Không có event tấn công | 221 — FP | 70.291 — TN |

```text
Precision = TP / (TP + FP) = 567 / 788 ≈ 0,7195
Recall    = TP / (TP + FN) = 567 / 568 ≈ 0,9982
F1        = 2 × Precision × Recall / (Precision + Recall) ≈ 0,8363
FPR       = FP / (FP + TN) = 221 / 70.512 ≈ 0,00313
```

**Ví dụ giải thích cho cả lớp:** “Máy tìm được 567 trong 568 cửa sổ tấn công, nhưng trong 788 cửa sổ đã báo có 221 cửa sổ báo nhầm.”

FPR chỉ khoảng 0,313% trên các cửa sổ bình thường, nhưng precision vẫn khoảng 72% vì cửa sổ bình thường đông hơn rất nhiều. Đây là lý do cần xem nhiều độ đo cùng nhau.

PR-AUC tóm tắt đường precision–recall khi thay đổi ngưỡng. PR-AUC cao không có nghĩa precision tại ngưỡng đang dùng luôn gần 100%. Số F1 tối ưu trên test chỉ để tham khảo; dùng nhãn test để tối ưu rồi gọi đó là kết quả triển khai sẽ làm đánh giá thiếu công bằng.

### 6.9. Event time, watermark và độ trễ

- **Event time:** thời điểm nằm trong log, dùng để gom cửa sổ.
- **Processing time:** thời điểm hệ thống xử lý event.
- **Watermark 2 phút:** cơ chế theo dõi tiến độ event time và quản lý trạng thái/log đến muộn. Nó không bắt mọi request chờ 2 phút mới được chấm điểm.
- Không mô tả watermark bằng câu “mọi event đến sau 2 phút theo đồng hồ hiện tại đều bị bỏ”. Hành vi còn phụ thuộc event time đã quan sát, cửa sổ và trạng thái của phép tổng hợp.
- Trigger 10 giây là cấu hình nhịp xử lý, không phải cam kết độ trễ 10 giây.
- Lần thử trước đo lúc bắt đầu attack trong manifest tới lúc script quan sát được alert phù hợp trong ES. Thời gian nhìn thấy trên Kibana còn phụ thuộc refresh; action rule có nhịp 30 giây riêng.

### 6.10. Checkpoint, ID ổn định và giới hạn phục hồi

Checkpoint lưu tiến độ source và trạng thái tổng hợp của truy vấn. File source có checkpoint để theo dõi file đã đọc. Generator ghi file tạm rồi rename giúp tránh để Spark đọc một file đang viết dở.

Khi sink ghi lại cùng khóa `source + ip + window_start`, document ES có cùng ID được ghi đè/cập nhật. Code hiện dùng `es.write.operation=index` với ID ổn định; không nói nó đang dùng cấu hình `upsert` nếu chưa đối chiếu source.

Một cặp input/checkpoint mới tạo phiên đọc mới, nhưng ID ES vẫn theo cửa sổ, không theo tên phiên. Hai lượt chạy gần nhau với cùng IP/cửa sổ có thể cập nhật cùng document. Bộ lọc thời gian và điều kiện “không còn dữ liệu mới khi đếm” rất quan trọng.

Bài thử restart vừa nêu chưa mô phỏng mọi trường hợp mất máy, hỏng checkpoint, mất DataNode, lỗi mạng giữa chừng hoặc đổi schema không tương thích. Không nói hệ thống chịu mọi lỗi hay không bao giờ mất dữ liệu.

### 6.11. Vì sao NASA và simulator dùng model riêng?

NASA là website cũ với đường dẫn, dung lượng và nhịp truy cập khác simulator. Model streaming train trên simulator vì generator live cùng dạng log và cơ chế traffic với nguồn này. NASA được fit KMeans riêng để phân tích khác biệt trong chính dữ liệu NASA.

Ngưỡng simulator streaming khoảng 7,493. Bản NASA local hiện có k=3, threshold khoảng 7,993; đó là một model/không gian chuẩn hóa khác. Không so trực tiếp hai score như thể cùng thang hoặc dùng một ngưỡng cho mọi nguồn.

Khi fit lại, kết quả có thể thay đổi theo môi trường, partition và quá trình tối ưu. Với NASA local, nhóm đã cố định k theo bản so sánh bàn giao nhưng kết quả vẫn khác bản cũ. Không quy toàn bộ chênh lệch cho một nguyên nhân cụ thể khi chưa có thực nghiệm cô lập; ghi nhận đúng phiên và artifact dùng để chấm điểm.

### 6.12. “Big Data” và quy mô 10 tuần: nói bằng công việc và bằng chứng

Khối lượng gần 3 triệu dòng là dữ liệu thực nghiệm có ý nghĩa cho môn nhập môn. Nó không tự chứng minh một máy thông thường không xử lý nổi, cũng không chứng minh hệ thống có throughput cao hơn một giải pháp khác.

Chiều sâu có thể chỉ ra trong đồ án:

| Năng lực | Bằng chứng có thể chỉ |
|---|---|
| Hiểu dữ liệu và chất lượng | Parser NASA, xử lý thiếu/trùng, quarantine và thống kê DQ |
| Thiết kế luồng xử lý | Bronze/silver/gold, partition, HDFS và Parquet |
| Xử lý bằng Spark | Submit, DataFrame aggregation, Jobs/Stages/SQL UI |
| Tìm và kiểm tra cải thiện | Rule → KMeans own-center → KMeans CBLOF, so sánh độ đo |
| Tích hợp batch/live | PipelineModel, metadata, hàm dùng chung, kiểm chứng cùng input features |
| Vận hành và phục hồi | Checkpoint, ID ổn định, restart test, import dashboard |
| Đánh giá giới hạn | Simulator dễ tách, NASA thiếu nhãn, một máy, chưa có benchmark mở rộng |

**Lời nói nếu thầy hỏi “10 tuần các em làm được gì?”:**

> “Phần công việc của nhóm gồm tìm hiểu và làm sạch nguồn log, xây dựng bảng đặc trưng bằng Spark, thực nghiệm baseline và các cách chấm điểm, rồi tích hợp kết quả vào lưu trữ, streaming, dashboard và rule. Nhóm có các kiểm chứng cho đường đi dữ liệu và restart. Hôm nay nhóm đang trình bày những bằng chứng đó; phần mở rộng nhiều máy và benchmark quy mô lớn chưa được thực hiện.”

Lịch làm việc và đóng góp theo tuần phải dựa trên quá trình thực tế, Git và minh chứng của nhóm. Không tạo một lịch 10 tuần hư cấu để hợp lý hóa thời gian môn học. Không suy từ số dòng code hay số container ra số tuần làm việc.

## 7. Bộ câu hỏi phản biện để hai người luyện với nhau

Đầu tiên trả lời một câu dễ hiểu; nếu thầy hỏi tiếp mới mở phần kỹ thuật và source.

### 7.1. Dữ liệu và bài toán

**1. Nhóm phát hiện tấn công hay phát hiện bất thường?**

> “Mô hình chấm mức khác thường của hành vi. Simulator có các kịch bản tấn công để kiểm tra khả năng phát hiện; với dữ liệu thật chưa có nhãn, cảnh báo là đầu mối điều tra, chưa phải xác nhận tấn công.”

**2. Tại sao dùng NASA cũ từ năm 1995?**

> “Đó là nguồn log thật có thể tái lập, phù hợp minh họa ingest, làm sạch và phân tích. Nhóm không coi nó đại diện đầy đủ cho ứng dụng web hiện đại.”

**3. Tại sao không xóa hết log trùng?**

> “Hai request có thể thực sự giống nhau trong cùng một giây. Xóa tự động có thể làm sai số lượng request và mức độ dồn dập. Nhóm giữ các dòng trùng thật của NASA và thiết kế ID export có số thứ tự để giữ đủ số dòng.”

**4. Thay bytes thiếu bằng 0 có làm sai không?**

> “Có thể gây thiên lệch cho các feature dung lượng. Code giữ cờ bytes_missing để biết đây là giá trị thay thế. Hiện CORE vẫn dùng mean/max bytes, nên đây là một giới hạn cần đánh giá thêm, không khẳng định 0 là giá trị thật.”

**5. IP có phải một người không?**

> “Không chắc. IP/hostname là đơn vị gom nhóm của bài toán. NAT, proxy hoặc nhiều IP cho một hoạt động là các giới hạn của cách gom theo nguồn.”

**6. Có thực sự bắn DDoS vào máy chủ không?**

> “Không. Generator tạo log mô phỏng hành vi DDoS vào file. Demo chứng minh pipeline phát hiện từ log; chưa phải thí nghiệm làm quá tải một máy chủ web thật.”

### 7.2. Spark và kiến trúc

**7. Sao không dùng pandas? Spark có cần thiết cho vài triệu dòng không?**

> “Vài triệu dòng có thể vẫn xử lý bằng công cụ trên một máy. Nhóm dùng Spark để thực hiện luồng DataFrame, mô hình và streaming phù hợp mục tiêu môn học. Muốn kết luận Spark nhanh hơn phải có benchmark cùng điều kiện; nhóm chưa có phép so sánh đó.”

**8. HDFS với Elasticsearch khác nhau thế nào?**

> “HDFS lưu các bảng/file phục vụ xử lý lại. Elasticsearch phục vụ tìm kiếm và tổng hợp cho dashboard. Hai thành phần có vai trò khác nhau, không phải hai tên của cùng một kho.”

**9. Docker có chứng minh xử lý phân tán không?**

> “Docker giúp dựng các thành phần và môi trường Spark standalone. Máy demo có một worker nên chưa chứng minh tăng tốc nhờ nhiều máy; nhóm chỉ ra cơ chế task/partition thực thi trên cấu hình hiện tại.”

**10. Có replication hay chịu lỗi HDFS không?**

> “Máy hiện có một DataNode và replication=1. Dữ liệu có lưu trên HDFS, nhưng chưa có bản sao dự phòng trên nhiều DataNode và chưa kiểm thử mất node.”

**11. Đâu là Spark job trong demo?**

> “Ứng dụng build_features được submit ngay ở phần batch; các actions đọc/tổng hợp/ghi tạo các jobs, stages và tasks quan sát được trên UI. Phần live cũng là Spark application, thực hiện nhiều micro-batch.”

**12. Output batch mới có lên Kibana ngay không?**

> “Trong cảnh batch, nhóm ghi features vào thư mục demo riêng. Thư mục đó chưa tự động chấm điểm/export. Dashboard NASA dùng kết quả chính đã chuẩn bị; cảnh live chứng minh ES/Kibana nhận kết quả mới.”

### 7.3. Mô hình và đánh giá

**13. Hai cụm có nghĩa bình thường và tấn công không?**

> “KMeans không được dạy hai nhãn đó. Nó tạo hai nhóm theo độ giống nhau của vector. Việc xem thành phần nhãn và đánh giá được làm sau; số hiệu cụm không tự mang nghĩa tốt/xấu.”

**14. Tại sao đổi từ own-center sang CBLOF?**

> “Một nhóm tấn công có thể nằm gần tâm của chính nó. So với cụm lớn giúp nhận ra nhóm nhỏ khác biệt; bảng đối chứng cho thấy F1 CORE tăng từ khoảng 0,216 lên 0,836 trong bộ thử này.”

**15. Vì sao không chọn BisectingKMeans khi F1 cao hơn?**

> “Trong bảng này BisectingKMeans CORE đạt F1 khoảng 0,858, cao hơn KMeans CORE khoảng 0,836. Nhóm đã chốt KMeans CORE cho artifact và tích hợp streaming. Đây là lựa chọn triển khai của phiên hiện tại; nhóm không có bằng chứng nói KMeans tốt nhất về mọi mặt hay nhanh hơn trong benchmark.”

**16. Các em có dùng nhãn tấn công để train không?**

> “Các stage của pipeline chỉ nhận các feature hành vi. Nhãn được giữ để tính độ đo/đối chiếu, không nằm trong feature vector. Nhóm cũng không dùng nhãn để lọc train thành chỉ traffic bình thường.”

**17. Ngưỡng p99 có phải tùy ý không?**

> “Đó là một lựa chọn vận hành để đánh dấu phần điểm cao trên train mà không cần nhãn. Nó cần được đánh giá trên dữ liệu mới và theo chi phí báo nhầm/bỏ sót. Nhóm không khẳng định p99 tối ưu cho mọi môi trường.”

**18. Sao không dùng ngưỡng tối ưu test để đạt F1 0,999?**

> “Ngưỡng đó được tìm bằng cách nhìn nhãn test nên chỉ là phân tích tham khảo. Nhóm triển khai ngưỡng từ train và báo kết quả tại ngưỡng đó để tránh dùng đáp án test làm quyết định triển khai.”

**19. F1 0,836 có nghĩa đúng 83,6% request không?**

> “Không. Đây là F1 ở cấp cửa sổ, kết hợp precision và recall. Nó không phải accuracy của từng request và không phải xác suất đúng của từng cảnh báo.”

**20. Precision 72% có quá thấp không?**

> “Nó cho thấy vẫn có báo nhầm đáng kể trong số cảnh báo. Đổi ngưỡng có thể giảm báo nhầm nhưng tăng bỏ sót. Cần lựa chọn theo nhu cầu vận hành; hiện nhóm trình bày rõ cả hai mặt thay vì chỉ báo recall cao.”

**21. Tại sao 0 false positive trong live nhưng batch có 221 FP?**

> “Đó là hai bài thử khác nhau. 0 là kết quả của một đoạn normal live 3 phút; 221 là số cửa sổ báo nhầm trong tập test batch lớn hơn. Một đoạn ngắn không phủ hết hành vi bình thường.”

**22. NASA có F1 bao nhiêu?**

> “Nhóm không có nhãn đúng/sai cho tấn công NASA nên không báo F1 phát hiện tấn công cho nguồn này. Nhóm chỉ phân tích cấu trúc dữ liệu và các cửa sổ có điểm nổi bật.”

### 7.4. Streaming và vận hành

**23. Window 5 phút, watermark 2 phút mà alert khoảng 18 giây là sao?**

> “Window đang mở được cập nhật bằng update mode theo micro-batch. Watermark quản lý event-time/state, không bắt chờ 2 phút. Độ trễ thực tế còn phụ thuộc thời điểm log được publish, xử lý, ghi ES và quan sát.”

**24. Sao không dùng Kafka?**

> “Nguồn live hiện tại là file JSONL được publish nguyên tử, phù hợp dựng và kiểm thử trong phạm vi này. Chưa có broker, consumer offsets hay thí nghiệm Kafka; nếu tích hợp nguồn log vận hành liên tục thì đó là một hướng mở rộng cần thiết kế riêng.”

**25. Rule đã enabled có nghĩa demo alert thành công chưa?**

> “Chưa. Nhóm kiểm tra alert document mới, kết quả thực thi rule và bản ghi alert-history trong đúng lượt demo. Dashboard cũng có thể hiển thị alert cũ nếu không lọc thời gian.”

**26. Một sự kiện có thể tạo nhiều alert không?**

> “Có. Cửa sổ 5 phút trượt 1 phút chồng nhau và các IP khác nhau được tổng hợp riêng. Số alert document đo số cửa sổ bị đánh dấu, không đo số cuộc tấn công độc lập.”

**27. Restart không trùng thì đã exactly-once hoàn toàn chưa?**

> “Chưa kết luận tuyệt đối. Nhóm có checkpoint và ID ổn định, đồng thời kiểm thử restart đã nêu. Cần thêm các tình huống lỗi và kiểm tra nhất quán sink để đánh giá đầy đủ hơn.”

**28. Hệ thống có tự chặn IP không?**

> “Không. Hiện hệ thống tạo cảnh báo và lịch sử để người vận hành kiểm tra. Chưa có firewall, cơ chế block hay vòng quyết định tự động.”

**29. Top_feature có xác định nguyên nhân của cuộc tấn công không?**

> “Nó chỉ ra chiều tạo độ lệch lớn nhất trong phép tính khoảng cách đã chuẩn hóa. Nó giúp diễn giải score, chưa phải bằng chứng nhân quả hay phân loại chính xác loại tấn công.”

**30. DDoS ít request trên mỗi IP có bị bỏ sót không?**

> “Có khả năng. Hệ thống hiện tổng hợp theo IP, và cửa sổ dưới 3 request bị gate về score 0. Các hành vi phối hợp nhiều IP với lưu lượng thấp từng IP cần thêm tín hiệu tổng thể hoặc cách tổng hợp khác.”

**31. Những gì chưa thực nghiệm?**

> “Chưa có benchmark nhiều máy, throughput tối đa, bài thử normal dài ngày, nguồn log web server đang vận hành, và thử nghiệm mất DataNode/hỏng checkpoint. Các phép kiểm chứng đã có không thay thế những thực nghiệm đó.”

## 8. Bản rút gọn 8 phút

Giữ phần thể hiện Spark thực thi thật; rút ngắn lời giải thích và để phần sâu cho phản biện.

| Thời gian | Nội dung | Câu cần nói |
|---|---|---|
| 00:00–00:45 | Như: bài toán + ví dụ thư viện | “Nhóm biến log thành bản tóm tắt hành vi và cảnh báo để kiểm tra.” |
| 00:45–01:30 | Định: NASA và làm sạch | “Có 1.891.709 dòng hợp lệ; giữ trùng thật và đánh dấu dữ liệu thiếu.” |
| 01:30–03:15 | Định: submit batch, xem Spark UI/output | “Đây là một lượt tính features mới từ HDFS, ghi thư mục demo riêng.” |
| 03:15–04:00 | Định: CBLOF và bảng so sánh | “Own-center có thể bỏ qua một nhóm nhỏ; CBLOF so với cụm lớn. F1 test từ 0,216 lên 0,836.” |
| 04:00–07:15 | Như: streaming + attack + rule | “Đây là log mới, score vượt threshold và action ghi lịch sử.” |
| 07:15–08:00 | Cả hai: kiểm chứng và giới hạn | “Đã kiểm thử restart; NASA thiếu nhãn; cấu hình một máy; chưa kết luận mở rộng.” |

Trong bản này, restart trực tiếp chỉ thực hiện nếu còn thời gian; nếu không thì trình bày rõ đó là kết quả kiểm chứng trước. Không vừa nói “restart ngay bây giờ” vừa chỉ số liệu của lần cũ.

Nếu cảnh batch mất hơn thời lượng sau khi tập, chọn một partition ngày NASA làm input demo bằng đường dẫn `.../nasa_access/dt=YYYY-MM-DD` đã xác nhận tồn tại. Nói rõ đây là phần dữ liệu dùng để minh họa; số cửa sổ sẽ khác toàn bộ 358.687. Giữ kết quả toàn bộ làm minh chứng xử lý trước, không đổi tên tập con thành toàn bộ NASA.

## 9. Phương án khi demo gặp trục trặc

| Tình huống | Việc kiểm tra/thao tác | Lời nói với người nghe |
|---|---|---|
| 4040 chưa mở | Xem app đã khởi tạo driver chưa; refresh khi app chạy. Nếu app đã dừng, dùng console output và Master UI để đối chiếu | “Application UI chỉ tồn tại trong lúc driver chạy; nhóm chuyển sang kết quả thực thi vừa ghi.” |
| Batch chạy lâu | Xem tasks có tiến triển; trong lúc chờ trình bày features/mô hình. Không khởi chạy thêm app tranh cores | “Job đang tổng hợp và ghi dữ liệu; nhóm giải thích phép tính trong lúc theo dõi tiến độ.” |
| Alert chưa thấy | Xác nhận `kmeans_cblof`, generator đã qua mốc attack, input/checkpoint cùng tên; xem log lỗi, time range và KQL | “Nhóm đang phân biệt chưa có alert với alert đã có nhưng chưa nằm trong bộ lọc hiển thị.” |
| NASA dashboard trống | Chọn lại khoảng thời gian năm 1995 | “Nguồn này là dữ liệu lịch sử, cần xem đúng khoảng thời gian của event.” |
| Alert-history panel trống | Không dùng filter `detector`/`detected_at` của index alerts cho history; mở Discover history theo `@timestamp` | “Lịch sử rule dùng các trường thời gian và schema riêng.” |
| Rule chưa tạo history mới | Xem `last_run`, điều kiện last 1 minute; rule có thể đang active từ lượt trước và chỉ action khi đổi trạng thái | “Rule kiểm tra định kỳ; nhóm sẽ kiểm tra thời điểm và kết quả thực thi, không suy từ nút enabled.” |
| Count trước/sau khác | Xác nhận generator đã dừng và batch cuối đã ghi xong; kiểm tra lỗi restart và đúng filter | “Kết quả này chưa đáp ứng điều kiện so sánh; nhóm chưa kết luận phục hồi đạt.” |
| Không tạo được alert trong thời gian cho phép | Chỉ kết quả/log của lần đã kiểm chứng và ghi rõ là bằng chứng trước; nhận diện phần live hôm nay chưa đạt | “Lượt trực tiếp chưa hoàn tất. Đây là kết quả kiểm chứng trước của cùng luồng; nhóm không coi đó là thành công của lượt hiện tại.” |

Không reset volume, xóa checkpoint đang dùng hoặc đổi ngưỡng trên sân khấu để ép xuất hiện alert. Đó là thay đổi điều kiện thí nghiệm và có thể làm mất dữ liệu chuẩn bị.

## 10. Tập trình bày và bằng chứng cho quy mô 10 tuần

### 10.1. Hai lượt tập tối thiểu

**Lượt 1 — kiểm tra thao tác:**

- Bấm giờ batch đúng input/output HDFS dự kiến dùng; kiểm tra UI mở được khi app còn chạy.
- Xác nhận đủ đường đi live và rule có action mới; dùng tên phiên mới.
- Đếm trước/sau restart khi không còn dữ liệu mới.
- Ghi nhận số đo thực tế. Chỉ sửa lời dẫn nếu kết quả trên máy khác bản snapshot.

**Lượt 2 — tập nói như trước lớp:**

- Như và Định đổi vai hỏi các câu trong phần 7.
- Người thao tác nói trước sẽ bấm gì và muốn chứng minh điều gì.
- Người còn lại giúp mở UI, tránh cả hai cùng nói hoặc cùng tìm tab.
- Bỏ bớt chi tiết nếu quá giờ; giữ ba bằng chứng chính: Spark chạy, alert mới, phản ứng/kiểm chứng.
- Nói thử phần mở đầu cho một người không học CNTT; nếu họ chưa nói lại được “đầu vào → xử lý → đầu ra”, sửa cách giải thích.

### 10.2. Cách nói để người nghe theo kịp

| Thay vì chỉ đọc thuật ngữ | Hãy nói |
|---|---|
| “Aggregate features theo sliding window” | “Gom hành vi trong một khoảng thời gian và cập nhật bản tóm tắt khi có dữ liệu mới.” |
| “Score lớn nên anomaly” | “Bản tóm tắt này khác xa nhóm tham chiếu hơn mốc đang dùng, nên được đánh dấu để kiểm tra.” |
| “Hệ thống distributed” | “Ứng dụng chia dữ liệu thành các phần việc. Máy demo có một worker; nhóm đang chỉ cách các task thực hiện.” |
| “Pipeline có fault tolerance” | “Nhóm đã thử khởi động lại với checkpoint và kiểm tra số document không tăng trùng trong bài thử đó.” |
| “Model chính xác 99,8%” | “Recall ở cấp cửa sổ trên tập test simulator khoảng 99,8%; vẫn có báo nhầm.” |

Khi chỉ một dòng alert, dừng khoảng vài giây để người nghe thấy con số. Giải thích lần lượt **nguồn → thời gian → score/ngưỡng → đặc trưng nổi bật**. Không đọc tất cả các cột.

### 10.3. Những phần còn có thể làm sâu hơn nhưng chưa được coi là đã hoàn thành

- Benchmark cùng một job trên các lượng dữ liệu khác nhau, có ghi cấu hình và thời gian.
- So sánh tài nguyên trong điều kiện kiểm soát, chẳng hạn số core; nhiều container trên cùng máy vẫn chưa thay thế benchmark nhiều máy.
- Thử traffic bình thường lâu hơn, nhiều seed và nhiều kiểu hành vi hợp lệ.
- Phân tích cụ thể các false positive và false negative, không chỉ báo bảng tổng hợp.
- Thu thập log từ một web server thật và đánh giá lệch phân phối.

Nếu được yêu cầu đánh giá hiệu năng, có thể đo thời gian bằng stopwatch quanh cùng câu lệnh batch ở terminal A. Phải ghi rõ thời gian đó bao gồm đọc, aggregation, ghi output và các thống kê cuối job. Không gọi đó là throughput tối đa của streaming hay suy ra khả năng phục vụ HTTP thật. Hiện tài liệu này không điền kết quả benchmark chưa đo.

### 10.4. Checklist cuối cùng

- [ ] Cả hai giải thích được log, feature, cluster, score, threshold bằng ví dụ riêng.
- [ ] Nắm khác biệt anomaly/tấn công, request/window, train/test và batch/live.
- [ ] Chốt số liệu NASA của đúng máy demo, tránh trộn 2.423 với 2.487.
- [ ] Bảng F1 dùng ngưỡng triển khai, không dùng ngưỡng tối ưu nhìn test.
- [ ] App batch thực sự đọc HDFS và ghi thư mục demo mới.
- [ ] Nói đúng giới hạn một worker, một DataNode, replication=1.
- [ ] Live buộc `--detector model`, quan sát `kmeans_cblof`.
- [ ] Rule có bằng chứng action mới; không chỉ nhìn enabled.
- [ ] Có điều kiện rõ ràng trước khi so count restart.
- [ ] Phân biệt kết quả hôm nay với số liệu lần kiểm chứng trước.
- [ ] Đã tập và bấm giờ theo thời lượng giảng viên cho phép.
- [ ] Lịch làm việc/đóng góp phản ánh công việc thật của nhóm.

## 11. Nguồn để mở khi thầy yêu cầu xem code hoặc minh chứng

| Nội dung | Nguồn chịu trách nhiệm |
|---|---|
| Yêu cầu môn và rubric | [Kế hoạch đồ án môn học](<Ke hoach Do an mon hoc Nhap mon Big data_SV.docx>) |
| Hợp đồng log | [Schema JSON](../contracts/http_access_log.v1.schema.json), [generator contract](../generator/log_contract.py) |
| Ingest NASA và chính sách làm sạch | [ingest_nasa.py](../jobs/ingest_nasa.py), [dq_nasa.json](../data/reports/dq_nasa.json) |
| Features/window/CBLOF | [features_common.py](../jobs/features_common.py) |
| Job batch demo | [build_features.py](../jobs/build_features.py) |
| Train và đánh giá | [detect_anomalies.py](../jobs/detect_anomalies.py), [metrics_sim.json](../data/reports/metrics_sim.json) |
| So sánh mô hình | [model_comparison.md](../data/reports/model_comparison.md) |
| Artifact streaming đóng băng | [model_meta.json](../data/reports/model_meta.json), [PipelineModel](../data/gold/models/kmeans_core) |
| Chấm điểm batch cho demo | [score_demo_batch.py](../jobs/score_demo_batch.py) |
| Streaming | [stream_detect.py](../jobs/stream_detect.py), [stream_simulate.py](../generator/stream_simulate.py) |
| Ghi ES và ID ổn định | [serving_common.py](../jobs/serving_common.py), [export_to_es.py](../jobs/export_to_es.py) |
| Mapping/time field | [es_setup.py](../scripts/es_setup.py) |
| Dashboard và native rule | [kibana_setup.py](../scripts/kibana_setup.py), [dashboards.ndjson](../kibana/dashboards.ndjson) |
| Orchestration và copy HDFS | [run_all.py](../scripts/run_all.py) |
| Đối chiếu sáu bảng | [verify_demo_data.py](../jobs/verify_demo_data.py), [log kết quả](../data/logs/verify_demo_data-demo.log) |
| Kiểm chứng score/model | [verify_cblof.py](../jobs/verify_cblof.py), [log kết quả](../data/logs/verify_cblof-demo.log) |
| Kiểm chứng live/restart | [verify_realtime.py](../scripts/verify_realtime.py) |
| Kiểm chứng import dashboard | [verify_dashboard_import.py](../scripts/verify_dashboard_import.py) |

Tài liệu kỹ thuật chính thức để đối chiếu khái niệm:

- [Spark 3.5.8 — Cluster overview: application, driver, executor, job, stage, task](https://spark.apache.org/docs/3.5.8/cluster-overview.html).
- [Spark 3.5.8 — Structured Streaming: window, watermark, trigger, foreachBatch](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html).

**Câu nhắc trước khi lên lớp:** “Nói điều mình hiểu, chỉ bằng chứng mình có, phân biệt điều đã đo với điều dự định làm.”

