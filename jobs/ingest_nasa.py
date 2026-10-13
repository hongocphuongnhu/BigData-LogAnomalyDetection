"""Phase 02: ingest NASA HTTP access log (thang 7/1995, log THAT) -> silver.

Nguon: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html (tai bang
scripts/download_nasa.*). Dinh dang Apache Common Log Format:

    199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245

Lam sach (so lieu dua vao data/reports/dq_nasa.json cho bao cao):
  - dong khong khop CLF (bi cat cut)          -> reject "malformed_line"
  - request khong bat dau bang HTTP method    -> reject "malformed_request"
  - request kieu HTTP/0.9 (thieu protocol)    -> giu, protocol = null
  - path co dau cach / dinh text rac           -> giu, request_inconsistent = true
  - bytes = "-" (304/302 khong co body)        -> bytes_out = 0, bytes_missing = true
  - dong trung tuyet doi                        -> GIU (traffic that, cung host
    cung giay la binh thuong), chi dem de bao cao

Xuat dung cot chuan hoa cua hop dong features_common (event_time, ip, method,
path, status_code, bytes_out) + protocol, bytes_missing, request_inconsistent,
partition dt/hour theo UTC giong silver simulator.

    ./scripts/submit.ps1 ingest_nasa.py
    ./scripts/submit.ps1 ingest_nasa.py --raw-path hdfs://namenode:9000/lake/raw/nasa \
        --silver-path hdfs://namenode:9000/lake/silver/nasa_access
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from features_common import DATA_ROOT, REPORTS_DIR, SILVER_NASA

DEFAULT_RAW_PATH = f"{DATA_ROOT}/raw/nasa"
DEFAULT_REJECTED_PATH = f"{DATA_ROOT}/silver/_rejected/nasa_access"

CLF_RE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(.*)" (\d{3}) (\S+)\s*$'
METHOD_RE = r"^(GET|HEAD|POST|PUT|DELETE|OPTIONS)$"
PROTOCOL_RE = r"^HTTP/\d\.\d$"
CLF_TS_PATTERN = "dd/MMM/yyyy:HH:mm:ss Z"
LARGE_RESPONSE_BYTES = 10 * 1024 * 1024  # nguong "ngoai le" de bao cao


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw-path", default=DEFAULT_RAW_PATH, help="File/thu muc log NASA (.gz doc truc tiep).")
    p.add_argument("--silver-path", default=SILVER_NASA)
    p.add_argument("--rejected-path", default=DEFAULT_REJECTED_PATH)
    p.add_argument("--partitions", type=int, default=16, help="So partition sau khi doc file gzip.")
    p.add_argument("--dq-report", default=f"{REPORTS_DIR}/dq_nasa.json")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("nasa_ingest").getOrCreate()
    # dt/hour va event_time deu tinh theo UTC, dong nhat voi silver simulator.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    # gzip KHONG chia nho duoc (non-splittable) -> ca file chi duoc 1 task doc.
    # Repartition ngay sau khi doc de cac buoc parse/ghi chay song song tren cluster.
    raw = spark.read.text(args.raw_path).withColumnRenamed("value", "line").repartition(args.partitions)

    parsed = raw.select(
        "line",
        F.regexp_extract("line", CLF_RE, 1).alias("ip"),
        F.regexp_extract("line", CLF_RE, 2).alias("raw_ts"),
        F.regexp_extract("line", CLF_RE, 3).alias("request"),
        F.regexp_extract("line", CLF_RE, 4).alias("status_raw"),
        F.regexp_extract("line", CLF_RE, 5).alias("bytes_raw"),
    ).withColumn("parts", F.split(F.trim("request"), r"\s+"))

    n_parts = F.size("parts")
    last_part = F.element_at("parts", -1)
    has_protocol = (n_parts >= 2) & last_part.rlike(PROTOCOL_RE)
    # Path = moi thu giua method va protocol, giu ca dau cach (vi du query
    # "wais.pl?Space Acceleration ...") thay vi cat mat phan sau dau cach.
    path_expr = F.when(
        has_protocol,
        F.expr("array_join(slice(parts, 2, size(parts) - 2), ' ')"),
    ).otherwise(F.expr("array_join(slice(parts, 2, size(parts) - 1), ' ')"))

    classified = (
        parsed.withColumn(
            "reject_reason",
            F.when(F.col("ip") == "", F.lit("malformed_line"))
            .when(~F.element_at("parts", 1).rlike(METHOD_RE) | (n_parts < 2), F.lit("malformed_request"))
            .when(F.to_timestamp("raw_ts", CLF_TS_PATTERN).isNull(), F.lit("bad_timestamp")),
        )
        .withColumn("method", F.element_at("parts", 1))
        .withColumn("path", path_expr)
        .withColumn("protocol", F.when(has_protocol, last_part))
        .withColumn("request_inconsistent", n_parts > F.when(has_protocol, 3).otherwise(2))
    ).cache()

    rejected = classified.filter(F.col("reject_reason").isNotNull()).select("line", "reject_reason")
    good = (
        classified.filter(F.col("reject_reason").isNull())
        .withColumn("event_time", F.to_timestamp("raw_ts", CLF_TS_PATTERN))
        .withColumn("status_code", F.col("status_raw").cast("int"))
        .withColumn("bytes_missing", F.col("bytes_raw") == "-")
        .withColumn("bytes_out", F.when(F.col("bytes_missing"), 0).otherwise(F.col("bytes_raw").cast("int")))
        .withColumn("dt", F.date_format("event_time", "yyyy-MM-dd"))
        .withColumn("hour", F.date_format("event_time", "HH"))
        .select(
            "event_time", "ip", "method", "path", "protocol", "status_code", "bytes_out",
            "bytes_missing", "request_inconsistent", "dt", "hour",
        )
    )

    # repartition theo dung cot partition truoc khi ghi: neu khong, moi task
    # trong 16 task ghi vao ~670 thu muc dt/hour -> >10.000 file Parquet nho
    # (small files problem), ghi/commit cham va lam doc ve sau cham theo.
    good.repartition("dt", "hour").write.mode("overwrite").partitionBy("dt", "hour").parquet(args.silver_path)
    rejected.coalesce(1).write.mode("overwrite").parquet(args.rejected_path)

    # --- Thong ke chat luong du lieu (doc lai silver da ghi, khong tinh lai pipeline) ---
    silver = spark.read.parquet(args.silver_path)
    total = classified.count()
    reject_counts = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}
    agg = silver.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("bytes_missing").cast("int")).alias("bytes_missing"),
        F.sum(F.col("request_inconsistent").cast("int")).alias("request_inconsistent"),
        F.sum(F.col("protocol").isNull().cast("int")).alias("http09_no_protocol"),
        F.sum((F.col("bytes_out") > LARGE_RESPONSE_BYTES).cast("int")).alias("large_responses"),
        F.max("bytes_out").alias("max_bytes"),
        F.approx_count_distinct("ip").alias("distinct_hosts"),
        F.min("event_time").alias("first_event"),
        F.max("event_time").alias("last_event"),
    ).first()
    duplicates = total - raw.distinct().count()
    status_dist = {str(r["status_code"]): r["count"] for r in silver.groupBy("status_code").count().collect()}
    method_dist = {r["method"]: r["count"] for r in silver.groupBy("method").count().collect()}

    report = {
        "source": args.raw_path,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_lines": total,
        "silver_rows": agg["rows"],
        "rejected": reject_counts,
        "exact_duplicate_lines_kept": duplicates,
        "bytes_missing": agg["bytes_missing"],
        "http09_no_protocol": agg["http09_no_protocol"],
        "request_inconsistent": agg["request_inconsistent"],
        "large_responses_over_10MB": agg["large_responses"],
        "max_bytes": agg["max_bytes"],
        "distinct_hosts_approx": agg["distinct_hosts"],
        "first_event_utc": agg["first_event"].isoformat(),
        "last_event_utc": agg["last_event"].isoformat(),
        "status_distribution": dict(sorted(status_dist.items())),
        "method_distribution": method_dist,
    }
    if not args.dq_report.startswith("hdfs://"):
        os.makedirs(os.path.dirname(args.dq_report), exist_ok=True)
        with open(args.dq_report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

    print(json.dumps(report, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
