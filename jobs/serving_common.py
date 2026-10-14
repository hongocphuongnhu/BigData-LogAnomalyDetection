"""Ghi ES idempotent va cham diem, dung chung batch export/streaming."""
from __future__ import annotations

import base64
import json
import os
import math
from pathlib import Path
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import ArrayType, MapType, StructType
from pyspark.ml.linalg import VectorUDT
from pyspark.ml import PipelineModel

from features_common import CORE_FEATURES, add_distance_score, model_input_columns, rule_score


def load_scoring_model(model_path: str, meta_path: str, window: str):
    meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
    if meta.get("features") != CORE_FEATURES or meta.get("window") != window:
        raise ValueError("Model features/window khong khop hop dong.")
    if meta.get("algo") != "kmeans" or meta.get("scoring") != "cblof":
        raise ValueError("Demo yeu cau model KMeans + CBLOF.")
    threshold = float(meta["threshold"])
    if not math.isfinite(threshold) or threshold <= 0 or meta.get("min_req_for_scoring") != 3:
        raise ValueError("Model threshold/min_req_for_scoring khong hop le.")
    model = PipelineModel.load(model_path)
    centers = [center.tolist() for center in model.stages[-1].clusterCenters()]
    expected = meta["centers"]
    large = meta["large_clusters"]
    if (not large or any(type(c) is not int or c < 0 or c >= len(centers) for c in large)
            or len(centers) != len(expected)
            or any(len(a) != len(CORE_FEATURES) or len(a) != len(b)
                   or any(not math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-9) for x, y in zip(a, b))
                   for a, b in zip(centers, expected))):
        raise ValueError("Model va metadata centers/large_clusters khong khop.")
    return model, meta


def score_model(df: DataFrame, model, meta: dict) -> DataFrame:
    out = add_distance_score(model.transform(rule_score(df)), meta["centers"],
                             feature_names=meta["features"], large_clusters=meta["large_clusters"])
    out = out.withColumn("score", F.when(F.col("n_req") >= meta["min_req_for_scoring"], F.col("score")).otherwise(0.0))
    return add_alert_fields(out, float(meta["threshold"]), "kmeans_cblof").drop(*model_input_columns(meta["features"]))


def es_request(method: str, path: str, body=None):
    headers = {"Content-Type": "application/json"}
    password = os.environ.get("ELASTIC_PASSWORD")
    if password:
        token = base64.b64encode(f"elastic:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    req = Request(os.environ.get("ES_URL", "http://elasticsearch:9200") + path,
                  data=json.dumps(body).encode() if body is not None else None,
                  headers=headers, method=method)
    with urlopen(req, timeout=60) as response:
        return json.load(response)


def prepare_documents(df: DataFrame, kind: str, source: str) -> DataFrame:
    # Vector ML khong phai field serving; tranh connector serialize VectorUDT.
    scalar_cols = [field.name for field in df.schema.fields if not isinstance(field.dataType, (ArrayType, MapType, StructType, VectorUDT))]
    out = df.select(*scalar_cols).withColumn("source", F.lit(source))
    if kind.startswith("logs-"):
        out = out.withColumn("error_flag", F.when(F.col("status_code") >= 400, 1.0).otherwise(0.0))
        if "cache_status" in out.columns:
            out = out.withColumn("cache_miss_flag", F.when(F.col("cache_status") == "MISS", 1.0).otherwise(0.0))
    if kind == "logs-sim":
        out = out.withColumn("doc_id", F.col("trace_id"))
    elif kind == "logs-nasa":
        # NASA giu dong trung that. So thu tu trong nhom dong giong nhau tao ID
        # on dinh theo tap du lieu, khong lam mat multiplicity nhu hash don thuan.
        identity = [name for name in sorted(scalar_cols) if name not in ("ingested_at", "source_file")]
        out = out.withColumn("_hash", F.sha2(F.to_json(F.struct(*identity)), 256))
        out = out.withColumn("_ordinal", F.row_number().over(Window.partitionBy("_hash").orderBy(F.lit(1))))
        out = out.withColumn("doc_id", F.concat_ws("-", "_hash", "_ordinal")).drop("_hash", "_ordinal")
    else:
        if "anomaly_label" in out.columns:
            out = out.withColumn("ground_truth", F.coalesce("anomaly_label", F.lit("normal")))
        out = out.withColumn("doc_id", F.sha2(F.concat_ws("|", F.col("source"), F.col("ip"), F.col("window_start").cast("string")), 256))
        if "cluster" in out.columns:
            out = out.withColumn("cluster", F.col("cluster").cast("string"))
        if "ref_cluster" in out.columns:
            out = out.withColumn("ref_cluster", F.col("ref_cluster").cast("string"))
        if "is_alert" in out.columns:
            out = out.withColumn("predicted_anomaly", F.col("is_alert"))
        if "score" in out.columns and "detector" not in out.columns:
            out = out.withColumn("detector", F.lit("kmeans_cblof"))
        if kind == "alerts" and "detected_at" not in out.columns:
            out = out.withColumn("detected_at", F.current_timestamp())
    return out


def write_es(df: DataFrame, index: str) -> None:
    writer = (df.write.format("org.elasticsearch.spark.sql")
              .option("es.nodes", os.environ.get("ES_HOST", "elasticsearch"))
              .option("es.port", "9200").option("es.nodes.wan.only", "true")
              .option("es.mapping.id", "doc_id").option("es.write.operation", "index")
              .option("es.batch.size.entries", "2000").option("es.batch.write.refresh", "false"))
    if os.environ.get("ELASTIC_PASSWORD"):
        writer = writer.option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", os.environ["ELASTIC_PASSWORD"])
    writer.mode("append").save(index)


def add_alert_fields(df: DataFrame, threshold: float, detector: str) -> DataFrame:
    ratio = F.col("score") / F.lit(float(threshold))
    reason = F.col("top_feature") if "top_feature" in df.columns else F.col("rule_reason")
    return (df.withColumn("threshold", F.lit(float(threshold)))
            .withColumn("is_alert", F.col("score") > F.col("threshold"))
            .withColumn("predicted_anomaly", F.col("is_alert"))
            .withColumn("severity", F.when(F.col("is_alert"), F.when(ratio >= 2, "high").when(ratio >= 1.5, "medium").otherwise("low")))
            .withColumn("detector", F.lit(detector))
            .withColumn("reason", F.when(F.col("is_alert"), reason)))
