"""Doi chieu so dong local/HDFS/Elasticsearch cua bo demo day du."""
from __future__ import annotations

import json

from pyspark.sql import SparkSession
from serving_common import es_request


def main() -> None:
    spark = SparkSession.builder.appName("verify_demo_data").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    checks = []
    for source, silver, log_index, expected in (("sim", "demo_sim", "logs-sim", 1065283),
                                               ("nasa", "nasa_access", "logs-nasa", 1891709)):
        datasets = [(f"silver/{silver}", log_index, expected)]
        datasets.extend((f"gold/{kind}_{source}", index, None) for kind, index in (("scores", "log-features"), ("alerts", "log-alerts")))
        for path, index, expected_count in datasets:
            local = spark.read.parquet(f"/opt/spark/data/{path}").count()
            hdfs = spark.read.parquet(f"hdfs://namenode:9000/lake/{path}").count()
            es = es_request("POST", f"/{index}/_count", {"query": {"term": {"source": source}}})["count"]
            assert local == hdfs == es, (source, path, local, hdfs, es)
            if expected_count is not None:
                assert local == expected_count, (source, local, expected_count)
            checks.append({"source": source, "path": path, "local": local, "hdfs": hdfs, "es": es})
    print(json.dumps({"full_demo_data_parity": "passed", "checks": checks}))
    spark.stop()


if __name__ == "__main__":
    main()
