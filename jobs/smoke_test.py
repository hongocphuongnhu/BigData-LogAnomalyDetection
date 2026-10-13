"""Kiem tra nhanh ha tang + hop dong features_common sau khi clone/dung lai stack.

Can co san silver simulator (sinh log + chay ingest_http_access.py truoc).

    ./scripts/submit.ps1 smoke_test.py                 # day du: feature, KMeans, HDFS, ES
    ./scripts/submit.ps1 smoke_test.py --skip-es       # bo qua buoc ghi ES

In "SMOKE OK" o cuoi neu moi buoc deu chay duoc.
"""
from __future__ import annotations

import argparse

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from features_common import (
    CORE_FEATURES,
    EXTENDED_FEATURES,
    HDFS_ROOT,
    SILVER_SIM,
    add_distance_score,
    build_window_features,
    rule_score,
)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver-path", default=SILVER_SIM)
    p.add_argument("--skip-hdfs", action="store_true")
    p.add_argument("--skip-es", action="store_true")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("smoke_test").getOrCreate()

    silver = spark.read.parquet(args.silver_path)
    print(f"[1] silver: {silver.count()} dong")

    feats = rule_score(build_window_features(silver, extended=True)).cache()
    n_win = feats.count()
    print(f"[2] features: {n_win} cua so, {len(EXTENDED_FEATURES)} feature")
    feats.groupBy("is_anomaly", "anomaly_label", "rule_flag").count().orderBy("is_anomaly", "anomaly_label").show()

    pipe = Pipeline(
        stages=[
            VectorAssembler(inputCols=CORE_FEATURES, outputCol="raw"),
            StandardScaler(inputCol="raw", outputCol="scaled", withMean=True, withStd=True),
            KMeans(featuresCol="scaled", predictionCol="cluster", k=4, seed=1),
        ]
    )
    model = pipe.fit(feats)
    centers = [c.tolist() for c in model.stages[-1].clusterCenters()]
    scored = add_distance_score(model.transform(feats), centers)
    print("[3] KMeans + score: top 10 cua so bat thuong nhat")
    scored.orderBy(F.desc("score")).select(
        "ip", "window_start", "n_req", "ratio_404", "score", "cluster", "anomaly_label", "rule_reason"
    ).show(10, truncate=False)

    if not args.skip_hdfs:
        hdfs_path = f"{HDFS_ROOT}/_smoke/features"
        feats.drop("rule_reason").write.mode("overwrite").parquet(hdfs_path)
        print(f"[4] HDFS: ghi + doc lai {spark.read.parquet(hdfs_path).count()} dong tai {hdfs_path}")

    if not args.skip_es:
        (
            scored.select("ip", "window_start", "n_req", "score", "cluster")
            .limit(100)
            .write.format("org.elasticsearch.spark.sql")
            .option("es.nodes", "elasticsearch")
            .option("es.port", "9200")
            .option("es.nodes.wan.only", "true")
            .mode("overwrite")
            .save("smoke-test")
        )
        print("[5] ES: da ghi 100 doc vao index smoke-test")

    print("SMOKE OK")
    spark.stop()


if __name__ == "__main__":
    main()
