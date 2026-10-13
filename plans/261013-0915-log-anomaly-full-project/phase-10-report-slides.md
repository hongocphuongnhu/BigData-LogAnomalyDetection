---
phase: 10
title: "Phase 10: Báo cáo Word + slide"
status: todo
priority: P1
effort: "≈3h mỗi người (rải suốt ngày)"
owner: "Định + Như"
dependencies: []
---

# Phase 10: Báo cáo Word + slide

## Goal

Báo cáo Word và slide đúng cấu trúc trong file kế hoạch của môn, mỗi phần có chủ rõ ràng,
nộp lên Classroom kèm source code.

## Files to Create / Modify

- Create: `doc/BaoCao_LogAnomalyDetection.docx`
- Create: `doc/Slide_LogAnomalyDetection.pptx`
- Nguồn hình: `data/reports/figures/`, `doc/images/`

## Phân công mục báo cáo (theo đúng cấu trúc yêu cầu)

| Mục | Người viết | Bắt đầu được từ |
|---|---|---|
| Trang bìa, mục lục | Như | H0 |
| Lịch làm việc nhóm hàng tuần (dựa trên lịch sử commit thật) | Định | H0 |
| Công việc từng thành viên + % đóng góp | Cả hai thống nhất | H10 |
| 1. Giới thiệu đề tài: bài toán, bối cảnh, mục tiêu, phạm vi, input/output | Như | H0 |
| 2.1 Lý thuyết: Big Data, Spark (RDD/DataFrame, cluster standalone), MLlib | Định | H0 |
| 2.2 Lý thuyết: KMeans, BisectingKMeans, silhouette, precision/recall/F1/PR-AUC | Định | H0 |
| 2.3 Lý thuyết: HDFS, Elasticsearch, Structured Streaming, Kibana | Như | H0 |
| 3.1 Kiến trúc hệ thống + luồng dữ liệu + lý do chọn công nghệ | Như | H1 |
| 3.2 Dữ liệu: NASA (nguồn, quy mô, định dạng) + simulator (Log Contract) | Định | H2.5 |
| 3.3 Khảo sát & tiền xử lý (thiếu, trùng, ngoại lệ, không nhất quán) + EDA | Định | H2.5 |
| 3.4 Trích xuất đặc trưng | Định | H4 |
| 3.5 Mô hình, tham số, kết quả, so sánh | Định | H6 |
| 3.6 Lưu trữ HDFS + ES (số liệu dung lượng, thời gian) | Như | H4 |
| 3.7 Streaming + dashboard + cảnh báo (ảnh chụp) | Như | H8 |
| 4. Kết luận, hạn chế, hướng phát triển | Cả hai | H10 |
| Tài liệu tham khảo, phụ lục (hướng dẫn chạy) | Như | H10 |

## Slide (~15 trang)

Định: dữ liệu, tiền xử lý, feature, mô hình, kết quả (6–7 trang).
Như: bài toán, kiến trúc, HDFS/ES, streaming, dashboard, demo (6–7 trang).
Chung: bìa, phân công, kết luận.

## Lưu ý

- Hình và bảng lấy từ output thật (`metrics.json`, ảnh chụp). Không tự bịa số liệu.
- Ghi nguồn cho dataset NASA, tài liệu tham khảo và mọi đoạn code tham khảo. File kế hoạch
  của môn ghi rõ: copy bài thì 0 điểm.
- Có thể dùng skill `docx` / `pptx` để Claude dựng file từ nội dung đã viết.

## Verification

- Báo cáo có đủ các mục theo đúng thứ tự trong file kế hoạch của môn; đã soát chính tả.
- Đã nộp lên Classroom: source code (zip hoặc link repo), file Word, file ppt.
