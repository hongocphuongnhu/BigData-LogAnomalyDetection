"""Structured Streaming: watermark/window -> rule hoac model -> ES upsert."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from features_common import (HTTP_ACCESS_LOG_SCHEMA, MODEL_DIR,
                             MODEL_META_PATH, TS_PATTERN,
                             build_window_features, rule_score)
from serving_common import add_alert_fields, es_request, prepare_documents, write_es, load_scoring_model, score_model


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", default="/opt/spark/data/stream/incoming")
    parser.add_argument("--checkpoint-path", default="/opt/spark/data/stream/_checkpoint")
    parser.add_argument("--model-path", default=MODEL_DIR)
    parser.add_argument("--model-meta-path", default=MODEL_META_PATH)
    parser.add_argument("--detector", choices=["auto", "rule", "model"], default="auto")
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--slide", default="1 minute")
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--max-files-per-trigger", type=int, default=100)
    parser.add_argument("--features-index", default="log-features")
    parser.add_argument("--alerts-index", default="log-alerts")
    parser.add_argument("--duration", type=float, default=0)
    args = parser.parse_args()
    Path(args.input_path).mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("stream_detect").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    model = None
    meta = {}
    available = Path(args.model_meta_path).is_file() and Path(args.model_path).exists()
    if args.detector == "model" and not available:
        raise FileNotFoundError("Chua co model/meta Phase 04. Dung --detector rule hoac doi ban giao.")
    if args.detector != "rule" and available:
        model, meta = load_scoring_model(args.model_path, args.model_meta_path, args.window)
        threshold = float(meta["threshold"])
        detector = "kmeans_cblof"
    else:
        threshold = 0.5
        detector = "rule_fallback"
        print("RULE FALLBACK: chua dung model Phase 04; ket qua la rule, khong phai KMeans.", flush=True)
    raw = (spark.readStream.schema(HTTP_ACCESS_LOG_SCHEMA)
           .option("maxFilesPerTrigger", args.max_files_per_trigger).json(args.input_path))
    events = raw.withColumn("event_time", F.to_timestamp("ts", TS_PATTERN)).filter(
        F.col("event_time").isNotNull() & F.col("ip").isNotNull() & F.col("status_code").between(100, 599))
    # Chi mot stateful aggregation de update mode duoc Spark ho tro.
    # File source + checkpoint theo doi file da doc; ES ID on dinh xu ly retry sink.
    events = events.withWatermark("event_time", "2 minutes")
    features = build_window_features(events, window=args.window, slide=args.slide, extended=True)

    def consume(batch, batch_id: int) -> None:
        if batch.isEmpty():
            return
        if model is not None:
            scored = score_model(batch, model, meta)
        else:
            scored = rule_score(batch)
            # Bo sung tin hieu DDoS phan tan theo IP; KHONG dung nhan ground truth.
            cache_flood = ((F.col("n_req") >= 5) & (F.col("n_distinct_paths") >= 5)
                           & (F.col("ratio_cache_miss") >= 0.95) & (F.col("ratio_5xx") >= 0.2)
                           & (F.col("mean_latency") >= 800))
            scored = (scored.withColumn("rule_reason", F.concat_ws(",", F.col("rule_reason"), F.when(cache_flood, "cache_busting_flood")))
                      .withColumn("rule_flag", F.col("rule_flag") | cache_flood)
                      .withColumn("score", F.col("rule_flag").cast("double")))
            scored = add_alert_fields(scored, threshold, detector)
        scored = scored.withColumn("detected_at", F.current_timestamp()).cache()
        try:
            write_es(prepare_documents(scored, "features", "stream"), args.features_index)
            alerts = scored.filter("is_alert")
            alert_count = alerts.count()
            if alert_count:
                write_es(prepare_documents(alerts, "alerts", "stream"), args.alerts_index)
            es_request("POST", f"/{args.features_index},{args.alerts_index}/_refresh")
            print(json.dumps({"batch_id": batch_id, "alerts": alert_count, "detector": detector, "processed_at": time.time()}), flush=True)
        finally:
            scored.unpersist()

    query = (features.writeStream.outputMode("update").option("checkpointLocation", args.checkpoint_path)
             .trigger(processingTime=args.trigger).foreachBatch(consume).start())
    print(f"STREAM READY: detector={detector}, checkpoint={args.checkpoint_path}", flush=True)
    try:
        if args.duration > 0:
            query.awaitTermination(args.duration)
            query.stop()
        else:
            query.awaitTermination()
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
