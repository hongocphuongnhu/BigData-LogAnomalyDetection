"""Export silver/gold Parquet sang ES, ID on dinh de chay lai khong nhan doi."""
from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from serving_common import es_request, prepare_documents, write_es


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--what", choices=["logs-sim", "logs-nasa", "features", "alerts"], required=True)
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--source", choices=["sim", "nasa", "stream"])
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--replace-source", action="store_true", help="Loai document cu cung source sau khi bulk thanh cong")
    args = parser.parse_args()
    source = args.source or ("nasa" if args.what == "logs-nasa" else "sim")
    index = {"features": "log-features", "alerts": "log-alerts"}.get(args.what, args.what)
    spark = SparkSession.builder.appName(f"export_{args.what}_{source}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    start = time.monotonic()
    docs = prepare_documents(spark.read.parquet(args.input_path), args.what, source)
    export_id = uuid.uuid4().hex
    if args.replace_source:
        docs = docs.withColumn("serving_export_id", F.lit(export_id))
    docs = docs.cache()
    count = docs.count()
    previous = es_request("GET", f"/{index}/_settings")[index]["settings"]["index"].get("refresh_interval", "1s")
    try:
        es_request("PUT", f"/{index}/_settings", {"index.refresh_interval": "-1"})
        write_es(docs, index)
    finally:
        es_request("PUT", f"/{index}/_settings", {"index.refresh_interval": previous})
    es_request("POST", f"/{index}/_refresh")
    if args.replace_source:
        es_request("POST", f"/{index}/_delete_by_query?refresh=true", {"query": {"bool": {
            "filter": [{"term": {"source": source}}], "must_not": [{"term": {"serving_export_id": export_id}}]}}})
    result = {"index": index, "source": source, "input_path": args.input_path, "input_rows": count,
              "source_documents": es_request("POST", f"/{index}/_count", {"query": {"term": {"source": source}}})["count"],
              "seconds": round(time.monotonic() - start, 3)}
    print(json.dumps(result))
    assert result["source_documents"] == count, result
    if args.report_path:
        path = Path(args.report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        metrics = json.loads(path.read_text()) if path.exists() else {}
        metrics[f"{index}:{source}"] = result
        path.write_text(json.dumps(metrics, indent=2) + "\n")
    docs.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
