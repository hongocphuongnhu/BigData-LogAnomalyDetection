"""Doi chieu model that: batch/streaming va cong thuc CBLOF doc lap."""
from __future__ import annotations

import json
import math

from pyspark.sql import SparkSession, functions as F

from features_common import DEFAULT_WINDOW, MODEL_DIR, MODEL_META_PATH
from detect_anomalies import score as batch_score
from serving_common import load_scoring_model, score_model


def main() -> None:
    spark = SparkSession.builder.appName("verify_official_cblof").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    model, meta = load_scoring_model(MODEL_DIR, MODEL_META_PATH, DEFAULT_WINDOW)
    features = spark.read.parquet("/opt/spark/data/gold/features_sim")
    # Co ca tan cong va binh thuong, kem cua so it request de kiem tra gate.
    sample = features.filter("is_anomaly").limit(40).unionByName(features.filter("not is_anomaly").limit(40))
    sample = sample.unionByName(sample.limit(3).withColumn("n_req", F.lit(2)))
    actual = score_model(sample, model, meta).cache()
    expected = batch_score(model, sample, meta["features"], meta["large_clusters"])
    a = sorted((r.ip, str(r.window_start), r.n_req, r.score) for r in actual.collect())
    b = sorted((r.ip, str(r.window_start), r.n_req, r.score) for r in expected.collect())
    assert len(a) == len(b) and all(x[:3] == y[:3] and math.isclose(x[3], y[3], abs_tol=1e-9) for x, y in zip(a, b))
    reassigned = sparse = 0
    for row in actual.collect():
        vec = list(row.scaled)
        distances = [math.dist(vec, center) for center in meta["centers"]]
        ref = row.cluster if row.cluster in meta["large_clusters"] else min(meta["large_clusters"], key=lambda c: distances[c])
        expected_score = distances[ref] if row.n_req >= meta["min_req_for_scoring"] else 0.0
        assert row.ref_cluster == ref and math.isclose(row.score, expected_score, abs_tol=1e-8)
        assert row.is_alert == (row.score > meta["threshold"]) == row.predicted_anomaly
        if row.is_alert:
            ratio = row.score / meta["threshold"]
            assert row.severity == ("high" if ratio >= 2 else "medium" if ratio >= 1.5 else "low")
            assert row.reason == row.top_feature
        reassigned += row.cluster != ref
        sparse += row.n_req < 3
    assert reassigned > 0 and sparse >= 3
    print(json.dumps({"official_model_load": "passed", "batch_stream_parity": "passed", "independent_cblof": "passed",
                      "sample_windows": len(a), "small_cluster_windows": reassigned, "sparse_windows": sparse}))
    actual.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
