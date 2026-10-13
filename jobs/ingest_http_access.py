"""Sprint 2: ingest bronze -> silver cho http_access_log (Log Contract v1).

Doc JSONL o bronze/http_access/dt=YYYY-MM-DD/hour=HH/*.jsonl bang schema
CO DINH (khong suy schema tu du lieu), tach rieng cac dong khong hop le
(sai kieu/thieu field HOAC vi pham invariant is_anomaly<->anomaly_label)
vao khu quarantine, khu trung theo trace_id, parse `ts` thanh timestamp
thuc de phuc vu windowed processing sau nay, roi ghi Parquet vao silver,
partition theo dt/hour (giong bronze) de query/prune hieu qua.

Chay tren cluster standalone qua script bao spark-submit (tu kem
--py-files features_common.py):

    ./scripts/submit.ps1 ingest_http_access.py        # Windows PowerShell
    ./scripts/submit.sh ingest_http_access.py         # Git Bash / Linux / macOS

Chi ingest lai mot vai partition cu the (idempotent - ghi de dung
partition do, khong dung den cac partition khac):

    ./scripts/submit.ps1 ingest_http_access.py --dt 2026-01-10 --hour 00,01
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    struct,
    to_json,
    to_timestamp,
)
from pyspark.sql.types import StringType, StructField, StructType

from features_common import HTTP_ACCESS_LOG_SCHEMA, TS_PATTERN

CORRUPT_COL = "_corrupt_record"

# Gia tri hop le theo contracts/http_access_log.v1.schema.json (enum).
VALID_METHODS = ["GET", "POST", "PUT", "DELETE", "HEAD"]
VALID_EVENT_TYPES = ["view_article", "list_articles", "search", "login", "logout", "static_asset", "other"]
VALID_CACHE_STATUSES = ["HIT", "MISS"]
VALID_ANOMALY_LABELS = ["brute_force", "sqli", "scan", "ddos"]

DEFAULT_BRONZE_PATH = "/opt/spark/data/bronze/http_access"
DEFAULT_SILVER_PATH = "/opt/spark/data/silver/http_access"
DEFAULT_REJECTED_PATH = "/opt/spark/data/silver/_rejected/http_access"


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bronze-path", default=DEFAULT_BRONZE_PATH, help="Thu muc goc bronze (chua dt=/hour=/*.jsonl).")
    p.add_argument("--silver-path", default=DEFAULT_SILVER_PATH, help="Thu muc goc silver de ghi Parquet.")
    p.add_argument("--rejected-path", default=DEFAULT_REJECTED_PATH, help="Thu muc quarantine cho dong khong hop le.")
    p.add_argument("--dt", default=None, help="Chi ingest cac ngay nay (phan cach boi dau phay, vi du 2026-01-10,2026-01-11). Bo qua = tat ca.")
    p.add_argument("--hour", default=None, help="Chi ingest cac gio nay (phan cach boi dau phay, vi du 00,01). Bo qua = tat ca.")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    spark = SparkSession.builder.appName("http_access_ingest").getOrCreate()
    # Ghi de dung partition dt/hour duoc chon, khong dong tat ca cac
    # partition khac -> ingest lai mot khung gio cu the la an toan (idempotent).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Khong cho Spark tu suy kieu partition column: neu khong, "hour=00" bi
    # doc thanh so nguyen 0 va ghi lai thanh "hour=0", lam silver lech dinh
    # dang partition so voi bronze (mat so 0 dung dau).
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    # Tao StructType moi thay vi .add(): .add() sua truc tiep schema dung chung
    # cua features_common (stream_detect.py cung import schema nay).
    schema_with_corrupt = StructType(
        HTTP_ACCESS_LOG_SCHEMA.fields + [StructField(CORRUPT_COL, StringType(), nullable=True)]
    )

    # Doc thang thu muc goc de Spark tu phat hien partition dt=/hour= tu ten
    # thu muc (Hive-style partition discovery) - vua co filter pruning, vua
    # khong can code song song logic parse duong dan.
    raw_df = (
        spark.read.schema(schema_with_corrupt)
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .json(args.bronze_path)
    )

    if args.dt:
        wanted_dt = [d.strip() for d in args.dt.split(",") if d.strip()]
        raw_df = raw_df.filter(col("dt").isin(wanted_dt))
    if args.hour:
        wanted_hour = [h.strip() for h in args.hour.split(",") if h.strip()]
        raw_df = raw_df.filter(col("hour").isin(wanted_hour))

    # cache() bat buoc truoc khi filter chi tren _corrupt_record: Spark tu
    # Spark 2.3 cam query JSON/CSV thô ma CHI tham chieu cot noi bo nay khi
    # chua materialize DataFrame (xem AnalysisException lien quan).
    raw_df = raw_df.withColumn("source_file", input_file_name()).cache()

    malformed_df = raw_df.filter(col(CORRUPT_COL).isNotNull()).withColumn(
        "reject_reason", lit("malformed_json")
    )

    parsed_df = raw_df.filter(col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)

    # Schema JSON chi bat duoc sai KIEU. nullable=False cua StructType khong
    # duoc reader JSON enforce, nen field bat buoc bi thieu (null), gia tri
    # ngoai enum, so am, va invariant is_anomaly <-> anomaly_label phai kiem
    # tra tuong minh o day.
    contract_cols = [f.name for f in HTTP_ACCESS_LOG_SCHEMA.fields]
    required_cols = [f.name for f in HTTP_ACCESS_LOG_SCHEMA.fields if not f.nullable]
    required_ok = None
    for c in required_cols:
        required_ok = col(c).isNotNull() if required_ok is None else required_ok & col(c).isNotNull()
    contract_ok = (
        required_ok
        & col("method").isin(VALID_METHODS)
        & col("event_type").isin(VALID_EVENT_TYPES)
        & col("cache_status").isin(VALID_CACHE_STATUSES)
        & col("status_code").between(100, 599)
        & (col("latency_ms") >= 0)
        & (col("bytes_out") >= 0)
        & (
            (col("is_anomaly") & col("anomaly_label").isin(VALID_ANOMALY_LABELS))
            | (~col("is_anomaly") & col("anomaly_label").isNull())
        )
    )
    # Bieu thuc co null (thieu field) cho ket qua null chu khong phai False,
    # phai coi la vi pham chu khong de lot qua ca hai nhanh filter.
    contract_ok = contract_ok.isNotNull() & contract_ok
    contract_violation_df = parsed_df.filter(~contract_ok).withColumn(
        "reject_reason", lit("contract_violation")
    )
    good_df = parsed_df.filter(contract_ok)

    # Khu trung theo trace_id - ingest lai (rerun/retry) khong duoc tao ban ghi trung lap.
    before_dedup = good_df.count()
    good_df = good_df.dropDuplicates(["trace_id"])
    after_dedup = good_df.count()

    good_df = good_df.withColumn("event_time", to_timestamp(col("ts"), TS_PATTERN)).withColumn(
        "ingested_at", current_timestamp()
    )

    # repartition theo cot partition: moi thu muc dt/hour mot file, tranh
    # moi task ghi vao moi thu muc (small files problem).
    good_df.repartition("dt", "hour").write.mode("overwrite").partitionBy("dt", "hour").parquet(
        args.silver_path
    )

    rejected_df = malformed_df.select(
        CORRUPT_COL, "reject_reason", "source_file", "dt", "hour"
    ).unionByName(
        # Luu NGUYEN dong (dang JSON) de con debug/sua va ingest lai duoc.
        contract_violation_df.select(
            to_json(struct(*contract_cols)).alias(CORRUPT_COL), "reject_reason", "source_file", "dt", "hour"
        )
    )
    malformed_count = malformed_df.count()
    contract_violation_count = contract_violation_df.count()
    # Luon ghi (ke ca rong) va dung overwrite STATIC: quarantine phan anh dung
    # lan chay gan nhat, khong con sot dong loi cu cua lan truoc da duoc sua.
    rejected_df.write.mode("overwrite").option("partitionOverwriteMode", "static").partitionBy(
        "dt", "hour"
    ).parquet(args.rejected_path)

    total_read = before_dedup + malformed_count + contract_violation_count
    duplicate_count = before_dedup - after_dedup
    normal_count = good_df.filter(~col("is_anomaly")).count()
    anomaly_counts = {
        row["anomaly_label"]: row["count"]
        for row in good_df.filter(col("is_anomaly")).groupBy("anomaly_label").count().collect()
    }
    cache_hits = good_df.filter(col("cache_status") == "HIT").count()

    print(f"Da doc {total_read} dong tho tu {args.bronze_path}")
    print(f"  ghi vao silver: {after_dedup} dong -> {args.silver_path}")
    print(f"  trung lap (bo qua): {duplicate_count}")
    print(f"  reject - malformed_json: {malformed_count}")
    print(f"  reject - contract_violation: {contract_violation_count}")
    print(f"  normal      : {normal_count}")
    for label in ("brute_force", "sqli", "scan", "ddos"):
        if anomaly_counts.get(label):
            print(f"  {label:12s}: {anomaly_counts[label]}")
    if after_dedup:
        print(f"  cache HIT ratio: {cache_hits}/{after_dedup} ({100 * cache_hits / after_dedup:.1f}%)")

    spark.stop()


if __name__ == "__main__":
    main()
