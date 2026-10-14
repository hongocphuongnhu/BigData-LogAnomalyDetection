"""Chuan bi gold cho demo; giu nguyen model/nguong simulator da ban giao."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from features_common import (CORE_FEATURES, DEFAULT_WINDOW, EXTENDED_FEATURES, MODEL_DIR,
                             MODEL_META_PATH, MIN_REQ_FOR_SCORING, large_clusters_from_sizes,
                             model_input_columns)
from serving_common import load_scoring_model, score_model


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["sim", "nasa"], required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"score_demo_{args.source}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    features = spark.read.parquet(f"/opt/spark/data/gold/features_{args.source}").cache()
    count = features.count()
    if args.source == "sim":
        model, meta = load_scoring_model(MODEL_DIR, MODEL_META_PATH, DEFAULT_WINDOW)
    else:
        # NASA co phan phoi rieng; tai su dung cach fit KMeans/CORE cua nhanh batch.
        from detect_anomalies import build_pipeline, parse_args, score
        train = features.filter(F.col("n_req") >= MIN_REQ_FOR_SCORING).cache()
        config = parse_args(["--source", "nasa"])
        # Giu k da duoc chon o batch; lay mau silhouette lai co the doi theo partition.
        report = json.loads(Path("/opt/spark/data/reports/metrics_nasa.json").read_text(encoding="utf-8"))
        selected = next(r for r in report["results"] if r["method"] == "kmeans" and r["features"] == "core")
        k = selected["k"]
        model = build_pipeline("kmeans", CORE_FEATURES, k, config.seed).fit(train)
        sizes = {r["cluster"]: r["count"] for r in model.transform(train).groupBy("cluster").count().collect()}
        large = large_clusters_from_sizes(sizes)
        threshold = score(model, train, CORE_FEATURES, large).approxQuantile("score", [0.99], 0.0005)[0]
        meta = {"algo": "kmeans", "features": CORE_FEATURES, "window": DEFAULT_WINDOW,
                "k": k, "scoring": "cblof", "large_clusters": large, "threshold": threshold,
                "min_req_for_scoring": MIN_REQ_FOR_SCORING,
                "centers": [c.tolist() for c in model.stages[-1].clusterCenters()]}
        # Artefact NASA rieng, khong ghi de model streaming dong bang trong Git.
        model.write().overwrite().save("/opt/spark/data/raw/setup/nasa_core")
        Path("/opt/spark/data/raw/setup/nasa_model_meta.json").write_text(json.dumps(meta), encoding="utf-8")
        train.unpersist()
    scored = (score_model(features, model, meta).withColumn("source", F.lit(args.source))
              .withColumn("dt", F.date_format("window_start", "yyyy-MM-dd"))
              .drop("raw", "scaled", *model_input_columns(EXTENDED_FEATURES)))
    root = "/opt/spark/data/gold"
    scored.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(f"{root}/scores_{args.source}")
    saved = spark.read.parquet(f"{root}/scores_{args.source}")
    alerts = saved.filter("is_alert")
    alerts.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(f"{root}/alerts_{args.source}")
    print(json.dumps({"source": args.source, "windows": count, "alerts": alerts.count(),
                      "scoring": meta["scoring"], "threshold": meta["threshold"], "k": meta["k"]}))
    features.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
