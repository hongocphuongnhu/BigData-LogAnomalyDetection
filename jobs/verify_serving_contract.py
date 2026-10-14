"""Kiem thu hop dong NASA duplicate, ES upsert va PipelineModel; fixture tach rieng."""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F

from features_common import CORE_FEATURES, add_distance_score
from serving_common import es_request, prepare_documents, write_es


def main() -> None:
    spark = SparkSession.builder.appName("verify_serving_contract").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    index = "qa-nasa-" + uuid.uuid4().hex[:8]
    mappings = es_request("GET", "/logs-nasa/_mapping")["logs-nasa"]["mappings"]
    es_request("PUT", f"/{index}", {"settings": {"number_of_replicas": 0}, "mappings": mappings})
    try:
        row = ("host.example.org", datetime(1995, 7, 1), "GET", "/index.html", 200, 120)
        fixture = spark.createDataFrame([row, row, ("other.example.org", datetime(1995, 7, 1), "GET", "/missing", 404, 0)],
                                        "ip string,event_time timestamp,method string,path string,status_code int,bytes_out int")
        for _ in range(2):
            write_es(prepare_documents(fixture, "logs-nasa", "fixture"), index)
        es_request("POST", f"/{index}/_refresh")
        nasa_count = es_request("GET", f"/{index}/_count")["count"]
        assert nasa_count == 3
        feats = spark.read.parquet("hdfs://namenode:9000/lake/gold/_verification/features_sim").limit(300).cache()
        model = Pipeline(stages=[VectorAssembler(inputCols=CORE_FEATURES, outputCol="raw"),
                                 StandardScaler(inputCol="raw", outputCol="scaled", withMean=True, withStd=True),
                                 KMeans(featuresCol="scaled", predictionCol="cluster", k=2, seed=7)]).fit(feats)
        model_path = "/opt/spark/data/raw/qa-model/kmeans_core"
        model.write().overwrite().save(model_path)
        loaded = PipelineModel.load(model_path)
        centers = [center.tolist() for center in loaded.stages[-1].clusterCenters()]
        scored = add_distance_score(loaded.transform(feats), centers)
        assert scored.filter(F.col("score").isNull() | F.isnan("score")).count() == 0
        docs = prepare_documents(scored, "features", "qa-model")
        assert "raw" not in docs.columns and "scaled" not in docs.columns
        assert docs.select("doc_id").distinct().count() == docs.count()
        meta = {"features": CORE_FEATURES, "window": "5 minutes", "k": 2, "threshold": 10000.0, "centers": centers,
                "trained_at": "TEST FIXTURE - KHONG PHAI MODEL DO AN"}
        Path("/opt/spark/data/raw/qa-model/model_meta.json").write_text(json.dumps(meta))
        report = {"nasa_fixture_rows": 3, "nasa_documents_after_two_exports": nasa_count,
                  "hostname_keyword_accepted": True, "true_duplicate_preserved": True,
                  "pipeline_model_reload": True, "finite_distance_scores": True, "vector_columns_removed": True,
                  "note": "Model nay chi la fixture kiem thu hop dong; khong thay the model Phase 04 cua Dinh."}
        Path("/opt/spark/data/reports/serving_contract_verification.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        es_request("DELETE", f"/{index}")
        spark.stop()


if __name__ == "__main__":
    main()
