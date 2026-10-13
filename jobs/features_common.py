"""Hop dong giao tiep dung chung giua nhanh Batch (Dinh) va Real-time (Nhu).

Moi job (ingest, build_features, detect_anomalies, stream_detect) import tu
day de batch va streaming tinh feature GIONG HET nhau - neu lech, model train
o batch se cham diem sai o streaming. Doi ten cot/feature o day phai bao ca
hai nguoi.

Cot chuan hoa o silver ma feature dua vao:
    event_time (timestamp UTC), ip, method, path, status_code, bytes_out
Cot chi co o simulator (dung cho EXTENDED_FEATURES va nhan danh gia):
    latency_ms, cache_status, user_agent, is_anomaly, anomaly_label

Spark submit kem --py-files /opt/spark/jobs/features_common.py
(scripts/submit.* da lam san).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Schema + duong dan
# ---------------------------------------------------------------------------

# Phai khop voi contracts/http_access_log.v1.schema.json - moi thay doi field
# o contract can duoc cap nhat o day.
HTTP_ACCESS_LOG_SCHEMA = StructType(
    [
        StructField("ts", StringType(), nullable=False),
        StructField("trace_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("method", StringType(), nullable=False),
        StructField("path", StringType(), nullable=False),
        StructField("article_id", IntegerType(), nullable=True),
        StructField("status_code", IntegerType(), nullable=False),
        StructField("latency_ms", IntegerType(), nullable=False),
        StructField("cache_status", StringType(), nullable=False),
        StructField("bytes_out", IntegerType(), nullable=False),
        StructField("ip", StringType(), nullable=False),
        StructField("user_agent", StringType(), nullable=False),
        StructField("is_anomaly", BooleanType(), nullable=False),
        StructField("anomaly_label", StringType(), nullable=True),
    ]
)
TS_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

DATA_ROOT = "/opt/spark/data"
HDFS_ROOT = "hdfs://namenode:9000/lake"

SILVER_SIM = f"{DATA_ROOT}/silver/http_access"
SILVER_NASA = f"{DATA_ROOT}/silver/nasa_access"
GOLD_ROOT = f"{DATA_ROOT}/gold"
MODEL_DIR = f"{GOLD_ROOT}/models/kmeans_core"
REPORTS_DIR = f"{DATA_ROOT}/reports"
MODEL_META_PATH = f"{REPORTS_DIR}/model_meta.json"

DEFAULT_WINDOW = "5 minutes"

# ---------------------------------------------------------------------------
# Feature theo cua so (ip, window)
# ---------------------------------------------------------------------------

# Co o ca NASA (log that) va simulator -> model CORE ap dung duoc cho ca hai.
CORE_FEATURES = [
    "n_req",             # so request trong cua so
    "burst_rate",        # n_req / (khoang thoi gian thuc te co request + 1s): bat flood/DDoS
    "n_distinct_paths",  # nhieu path khac nhau trong thoi gian ngan: bat scan
    "ratio_4xx",
    "ratio_5xx",
    "ratio_404",         # scan sinh hang loat 404
    "ratio_post",        # brute force login la POST lien tuc
    "ratio_sus_path",    # path chua payload SQLi / duong dan nhay cam
    "ratio_static",      # nguoi that tai kem css/js/anh, bot thi khong
    "mean_bytes",
    "max_bytes",
]

# Chi simulator co (latency, cache, user-agent) - dung de so sanh dong gop feature.
EXTENDED_ONLY_FEATURES = [
    "ratio_401",
    "ratio_cache_miss",
    "mean_latency",
    "p95_latency",
    "ratio_script_ua",
]
EXTENDED_FEATURES = CORE_FEATURES + EXTENDED_ONLY_FEATURES

STATIC_PATH_RE = r"(?i)\.(css|js|ico|gif|jpe?g|png|svg|woff2?)(\?.*)?$"
SUSPICIOUS_PATH_RE = (
    r"(?i)(union\s+select|select\s|sleep\(|drop\s+table|'|%27|--|\.\./|"
    r"/\.env|/\.git|/\.aws|/\.ssh|/\.htpasswd|wp-login|wp-admin|phpmyadmin|"
    r"xmlrpc|shell\.php|config\.php|backup\.sql|/etc/passwd)"
)
SCRIPT_UA_RE = r"(?i)(curl|python-requests|sqlmap|nikto|go-http-client|wget|scrapy|masscan|zgrab)"


def _ratio(cond: Column) -> Column:
    return F.avg(F.when(cond, 1.0).otherwise(0.0))


def build_window_features(
    df: DataFrame,
    window: str = DEFAULT_WINDOW,
    slide: str | None = None,
    extended: bool = False,
) -> DataFrame:
    """Gom log theo (ip, cua so thoi gian) va tinh feature.

    Chi dung ham aggregate ho tro ca streaming (approx_count_distinct thay vi
    countDistinct, percentile_approx) de stream_detect.py goi lai duoc y nguyen.

    Ket qua: ip, window_start, window_end, <features>, va neu df co cot nhan
    (simulator) thi them is_anomaly / anomaly_label cap cua so: cua so bat
    thuong neu co it nhat mot request bat thuong.
    """
    win = F.window("event_time", window, slide) if slide else F.window("event_time", window)

    aggs = [
        F.count(F.lit(1)).alias("n_req"),
        F.min("event_time").alias("_first"),
        F.max("event_time").alias("_last"),
        F.approx_count_distinct("path").alias("n_distinct_paths"),
        _ratio(F.col("status_code").between(400, 499)).alias("ratio_4xx"),
        _ratio(F.col("status_code") >= 500).alias("ratio_5xx"),
        _ratio(F.col("status_code") == 404).alias("ratio_404"),
        _ratio(F.col("method") == "POST").alias("ratio_post"),
        _ratio(F.col("path").rlike(SUSPICIOUS_PATH_RE)).alias("ratio_sus_path"),
        _ratio(F.col("path").rlike(STATIC_PATH_RE)).alias("ratio_static"),
        F.avg("bytes_out").alias("mean_bytes"),
        F.max("bytes_out").cast("double").alias("max_bytes"),
    ]
    if extended:
        aggs += [
            _ratio(F.col("status_code") == 401).alias("ratio_401"),
            _ratio(F.col("cache_status") == "MISS").alias("ratio_cache_miss"),
            F.avg("latency_ms").alias("mean_latency"),
            F.percentile_approx("latency_ms", 0.95).cast("double").alias("p95_latency"),
            _ratio(F.col("user_agent").rlike(SCRIPT_UA_RE)).alias("ratio_script_ua"),
        ]

    has_labels = "is_anomaly" in df.columns
    if has_labels:
        aggs += [
            F.max(F.col("is_anomaly").cast("int")).alias("_is_anomaly"),
            F.first("anomaly_label", ignorenulls=True).alias("anomaly_label"),
        ]

    out = (
        df.groupBy(F.col("ip"), win.alias("w"))
        .agg(*aggs)
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .withColumn(
            "burst_rate",
            F.col("n_req")
            / (F.col("_last").cast("double") - F.col("_first").cast("double") + F.lit(1.0)),
        )
        .withColumn("n_req", F.col("n_req").cast("double"))
        .withColumn("n_distinct_paths", F.col("n_distinct_paths").cast("double"))
        .drop("w", "_first", "_last")
    )
    if has_labels:
        out = out.withColumn("is_anomaly", F.col("_is_anomaly") == 1).drop("_is_anomaly")

    feature_cols = EXTENDED_FEATURES if extended else CORE_FEATURES
    label_cols = ["is_anomaly", "anomaly_label"] if has_labels else []
    return out.select("ip", "window_start", "window_end", *feature_cols, *label_cols)


# ---------------------------------------------------------------------------
# Rule baseline + diem bat thuong
# ---------------------------------------------------------------------------

def rules() -> dict[str, Column]:
    """Nguong cho cua so 5 phut - baseline "chuyen gia viet tay" de so voi model.

    La ham (khong phai hang so module) vi F.col() can SparkContext da khoi tao,
    goi luc import module se loi.
    """
    return {
        "flood": (F.col("burst_rate") >= 2.0) & (F.col("n_req") >= 20),
        "scan": (F.col("ratio_404") >= 0.5) & (F.col("n_distinct_paths") >= 10),
        "many_errors": (F.col("ratio_4xx") >= 0.6) & (F.col("n_req") >= 10),
        "suspicious_path": F.col("ratio_sus_path") >= 0.3,
    }


def rule_score(df: DataFrame) -> DataFrame:
    """Them rule_flag (bool) va rule_reason (cac luat bi vi pham, cach nhau dau phay)."""
    reasons = [F.when(cond, F.lit(name)) for name, cond in rules().items()]
    return df.withColumn("rule_reason", F.concat_ws(",", *reasons)).withColumn(
        "rule_flag", F.col("rule_reason") != ""
    )


def add_distance_score(
    df: DataFrame,
    centers: list[list[float]],
    vec_col: str = "scaled",
    cluster_col: str = "cluster",
    out_col: str = "score",
) -> DataFrame:
    """Diem bat thuong = khoang cach Euclid tu diem toi tam cum duoc gan.

    Tinh bang bieu thuc SQL tren mang (vector_to_array + zip_with + aggregate)
    thay vi Python UDF -> chay hoan toan tren JVM, khong ton chi phi
    serialize sang Python, dung duoc ca trong foreachBatch cua streaming.
    """
    from pyspark.ml.functions import vector_to_array

    centers_lit = F.array(*[F.array(*[F.lit(float(x)) for x in c]) for c in centers])
    center = F.element_at(centers_lit, F.col(cluster_col) + 1)
    sq = F.zip_with(vector_to_array(F.col(vec_col)), center, lambda a, b: (a - b) * (a - b))
    return df.withColumn(out_col, F.sqrt(F.aggregate(sq, F.lit(0.0), lambda acc, x: acc + x)))
