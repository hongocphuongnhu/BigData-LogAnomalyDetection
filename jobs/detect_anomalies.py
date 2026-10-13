"""Phase 04: phat hien bat thuong tren gold/features_* va danh gia.

So sanh 3 phuong phap tren CUNG bang feature cua so (ip, 5 phut):
  - rule     : nguong viet tay (features_common.rule_score) - baseline
  - kmeans   : KMeans + diem CBLOF (khoang cach toi tam cum LON gan nhat)
  - bisecting: BisectingKMeans (phan cum chia doi phan cap), cung cach cham diem
  (them bien the kmeans_own: khoang cach toi tam cum cua chinh no, de chung
  minh vi sao can CBLOF khi tan cong tu gom thanh cum nho)

Model hoc KHONG giam sat (khong dung nhan). Voi simulator, nhan chi dung de
danh gia tren tap test (30% thoi gian cuoi). k chon theo silhouette tren
tap train; nguong canh bao = phan vi --threshold-pct cua diem tren train
(khong can nhan -> dung duoc ngoai thuc te).

    ./scripts/submit.ps1 detect_anomalies.py --source sim     # train + danh gia + luu model cho streaming
    ./scripts/submit.ps1 detect_anomalies.py --source nasa    # fit rieng tren log that, xuat top bat thuong

Output:
  gold/scores_{source}, gold/alerts_{source}            (Parquet, partition dt)
  reports/metrics_{source}.json                          (bang so sanh, chi so cum)
  gold/models/kmeans_core + reports/model_meta.json      (chi --source sim: model cho stream_detect.py)
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import BisectingKMeans, KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator
from pyspark.ml.feature import SQLTransformer, StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from features_common import (
    CORE_FEATURES,
    DEFAULT_WINDOW,
    EXTENDED_FEATURES,
    GOLD_ROOT,
    MIN_REQ_FOR_SCORING,
    large_clusters_from_sizes,
    MODEL_DIR,
    MODEL_META_PATH,
    REPORTS_DIR,
    add_distance_score,
    model_input_columns,
    model_input_sql,
    rule_score,
)

ALGOS = {"kmeans": KMeans, "bisecting": BisectingKMeans}
LABELS = ["brute_force", "sqli", "scan", "ddos"]


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", choices=["sim", "nasa"], required=True)
    p.add_argument("--features-path", default=None, help="Mac dinh gold/features_{source}.")
    p.add_argument("--gold-root", default=GOLD_ROOT)
    p.add_argument("--k-min", type=int, default=2)
    p.add_argument("--k-max", type=int, default=10)
    p.add_argument("--threshold-pct", type=float, default=0.99, help="Phan vi diem tren train lam nguong canh bao.")
    p.add_argument("--train-frac", type=float, default=0.7, help="(sim) ty le thoi gian dau dung de train.")
    p.add_argument("--silhouette-sample", type=int, default=50000)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args(argv)


def build_pipeline(algo: str, feature_cols: list[str], k: int, seed: int) -> Pipeline:
    return Pipeline(
        stages=[
            SQLTransformer(statement=model_input_sql(feature_cols)),
            VectorAssembler(inputCols=model_input_columns(feature_cols), outputCol="raw"),
            StandardScaler(inputCol="raw", outputCol="scaled", withMean=True, withStd=True),
            ALGOS[algo](featuresCol="scaled", predictionCol="cluster", k=k, seed=seed),
        ]
    )


def score(model: PipelineModel, df: DataFrame, feature_cols: list[str], large: list[int] | None) -> DataFrame:
    centers = [c.tolist() for c in model.stages[-1].clusterCenters()]
    out = add_distance_score(model.transform(df), centers, feature_names=feature_cols, large_clusters=large)
    # Cua so qua it request: khong du bang chung -> diem 0 (xem MIN_REQ_FOR_SCORING).
    return out.withColumn(
        "score", F.when(F.col("n_req") >= MIN_REQ_FOR_SCORING, F.col("score")).otherwise(F.lit(0.0))
    )


def choose_k(train: DataFrame, algo: str, feature_cols: list[str], args) -> tuple[int, dict]:
    """Chon k co silhouette cao nhat tren mau cua tap train (train CHUA qua pipeline)."""
    sample = train
    n = train.count()
    if n > args.silhouette_sample:
        sample = train.sample(fraction=args.silhouette_sample / n, seed=args.seed)
    sample = sample.cache()
    evaluator = ClusteringEvaluator(featuresCol="scaled", predictionCol="cluster", metricName="silhouette")
    scores = {}
    for k in range(args.k_min, args.k_max + 1):
        model = build_pipeline(algo, feature_cols, k, args.seed).fit(sample)
        scores[k] = evaluator.evaluate(model.transform(sample))
        print(f"    {algo} k={k}: silhouette={scores[k]:.4f}")
    sample.unpersist()
    return max(scores, key=scores.get), scores


def binary_metrics(df: DataFrame, pred_col: str, score_col: str | None) -> dict:
    """Precision/recall/F1 (+ PR-AUC neu co diem lien tuc) o cap cua so; recall theo tung loai tan cong."""
    row = df.agg(
        F.sum((F.col(pred_col) & F.col("is_anomaly")).cast("int")).alias("tp"),
        F.sum((F.col(pred_col) & ~F.col("is_anomaly")).cast("int")).alias("fp"),
        F.sum((~F.col(pred_col) & F.col("is_anomaly")).cast("int")).alias("fn"),
        F.sum((~F.col(pred_col) & ~F.col("is_anomaly")).cast("int")).alias("tn"),
    ).first()
    tp, fp, fn, tn = (row[c] or 0 for c in ("tp", "fp", "fn", "tn"))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    per_label = {
        r["anomaly_label"]: round(r["recall"], 4)
        for r in df.filter(F.col("is_anomaly"))
        .groupBy("anomaly_label")
        .agg(F.avg(F.col(pred_col).cast("double")).alias("recall"))
        .collect()
    }
    out = {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4),
        "fpr": round(fp / (fp + tn), 5) if fp + tn else 0.0,
        "recall_by_label": dict(sorted(per_label.items())),
    }
    if score_col:
        evaluator = BinaryClassificationEvaluator(rawPredictionCol=score_col, labelCol="label")
        labeled = df.withColumn("label", F.col("is_anomaly").cast("double"))
        out["pr_auc"] = round(evaluator.evaluate(labeled, {evaluator.metricName: "areaUnderPR"}), 4)
        out["roc_auc"] = round(evaluator.evaluate(labeled, {evaluator.metricName: "areaUnderROC"}), 4)
    return out


def best_f1_threshold(df: DataFrame, score_col: str) -> dict:
    """Nguong cho F1 tot nhat tren test (CO dung nhan) - chi de tham khao, khong dung khi trien khai.

    Tap test o cap cua so chi vai chuc-tram nghin dong -> keo (score, nhan) ve
    driver, sap xep giam dan roi quet mot lan: chinh xac va nhanh hon nhieu so
    voi chay mot Spark job cho moi nguong ung vien.
    """
    rows = sorted(
        ((r[0], bool(r[1])) for r in df.select(score_col, "is_anomaly").collect()),
        key=lambda x: -x[0],
    )
    total_pos = sum(1 for _, y in rows if y)
    best = {"threshold": None, "precision": 0.0, "recall": 0.0, "f1": 0.0}
    tp = fp = 0
    for i, (s, y) in enumerate(rows):
        tp += y
        fp += not y
        # chi danh gia tai ranh gioi giua hai muc diem khac nhau
        if i + 1 < len(rows) and rows[i + 1][0] == s:
            continue
        precision, recall = tp / (tp + fp), tp / total_pos if total_pos else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        if f1 > best["f1"]:
            best = {"threshold": round(s, 4), "precision": round(precision, 4),
                    "recall": round(recall, 4), "f1": round(f1, 4)}
    return best


def severity(score_col: str, threshold: float):
    ratio = F.col(score_col) / F.lit(threshold)
    return F.when(ratio >= 2, "high").when(ratio >= 1.5, "medium").otherwise("low")


def write_json(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
    print(f"  -> {path}")


def main(argv=None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName(f"detect_anomalies_{args.source}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    feats = spark.read.parquet(args.features_path or f"{args.gold_root}/features_{args.source}")
    feats = rule_score(feats).cache()
    labeled = "is_anomaly" in feats.columns
    print(f"Doc {feats.count()} cua so ({args.source}), co nhan: {labeled}")

    # Chia train/test theo THOI GIAN (khong random) de tranh ro ri giua cac
    # cua so lien tiep cua cung mot chien dich tan cong.
    if labeled:
        bounds = feats.agg(F.min("window_start").alias("lo"), F.max("window_start").alias("hi")).first()
        cutoff = bounds["lo"] + (bounds["hi"] - bounds["lo"]) * args.train_frac
        train = feats.filter(F.col("window_start") < F.lit(cutoff)).cache()
        test = feats.filter(F.col("window_start") >= F.lit(cutoff)).cache()
        print(f"  train: {train.count()} cua so (< {cutoff}), test: {test.count()} cua so")
    else:
        train, test, cutoff = feats, None, None

    # sim: so sanh CORE vs EXTENDED; nasa chi co CORE.
    variants = {"core": CORE_FEATURES}
    if args.source == "sim":
        variants["extended"] = EXTENDED_FEATURES

    results = []
    if labeled:
        results.append({"method": "rule", "features": "core", **binary_metrics(test, "rule_flag", None)})

    eligible_train = train.filter(F.col("n_req") >= MIN_REQ_FOR_SCORING)
    final_scored = None
    final_meta = None
    for vname, feature_cols in variants.items():
        for algo in ALGOS:
            print(f"[{algo}/{vname}] chon k ...")
            k, sil = choose_k(eligible_train, algo, feature_cols, args)
            model = build_pipeline(algo, feature_cols, k, args.seed).fit(eligible_train)
            sizes = {r["cluster"]: r["count"] for r in model.transform(eligible_train).groupBy("cluster").count().collect()}
            large = large_clusters_from_sizes(sizes)

            # kmeans core: them bien the "own" (khong CBLOF) de so sanh trong bao cao.
            scorings = [("cblof", large)] + ([("own", None)] if algo == "kmeans" and vname == "core" else [])
            for scoring, large_arg in scorings:
                train_scored = score(model, eligible_train, feature_cols, large_arg)
                threshold = train_scored.approxQuantile("score", [args.threshold_pct], 0.0005)[0]
                entry = {
                    "method": algo if scoring == "cblof" else f"{algo}_own", "features": vname, "k": k,
                    "scoring": scoring, "large_clusters": large_arg,
                    "silhouette_by_k": {str(kk): round(v, 4) for kk, v in sil.items()},
                    "threshold": round(threshold, 4), "threshold_pct": args.threshold_pct,
                    "cluster_sizes": {str(c): n for c, n in sorted(sizes.items())},
                }
                if labeled:
                    test_scored = score(model, test, feature_cols, large_arg).withColumn("pred", F.col("score") > threshold)
                    entry.update(binary_metrics(test_scored, "pred", "score"))
                    entry["best_f1_on_test"] = best_f1_threshold(test_scored, "score")
                print(f"  {json.dumps({k2: entry[k2] for k2 in entry if k2 not in ('silhouette_by_k', 'cluster_sizes')}, default=str)}")
                results.append(entry)

            # Model dua vao streaming + scores/alerts: KMeans + CBLOF tren CORE (ap dung duoc cho moi nguon).
            if algo == "kmeans" and vname == "core":
                train_thr = score(model, eligible_train, feature_cols, large).approxQuantile(
                    "score", [args.threshold_pct], 0.0005
                )[0]
                final_scored = score(model, feats, feature_cols, large)
                final_meta = {"model": model, "k": k, "threshold": train_thr, "features": feature_cols, "large": large}

    # --- scores + alerts (KMeans/CORE) ---
    thr = final_meta["threshold"]
    scored = (
        final_scored.withColumn("threshold", F.lit(thr))
        .withColumn("is_alert", F.col("score") > F.lit(thr))
        .withColumn("severity", F.when(F.col("is_alert"), severity("score", thr)))
        .withColumn("reason", F.when(F.col("is_alert"), F.col("top_feature")))
        .withColumn("source", F.lit(args.source))
        .withColumn("dt", F.date_format("window_start", "yyyy-MM-dd"))
        .drop("raw", "scaled", *model_input_columns(EXTENDED_FEATURES if args.source == "sim" else CORE_FEATURES))
    )
    scored_path = f"{args.gold_root}/scores_{args.source}"
    alerts_path = f"{args.gold_root}/alerts_{args.source}"
    scored.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(scored_path)
    saved = spark.read.parquet(scored_path)
    saved.filter("is_alert").repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(alerts_path)
    n_alerts = saved.filter("is_alert").count()
    print(f"Ghi {scored_path}; {n_alerts} canh bao -> {alerts_path}")

    top = (
        saved.filter("is_alert")
        .orderBy(F.desc("score"))
        .select("ip", "window_start", "n_req", "ratio_4xx", "ratio_404", "n_distinct_paths", "score",
                "severity", "reason", "rule_reason", *(["anomaly_label"] if labeled else []))
        .limit(20)
    )
    top.show(20, truncate=False)

    # Thanh phan cac cum: cum nao tuong ung hanh vi nao (muc "phan cum hanh vi" trong bao cao).
    profile_cols = [F.count(F.lit(1)).alias("windows")] + [F.round(F.avg(c), 3).alias(c) for c in final_meta["features"]]
    if labeled:
        profile_cols.append(F.round(F.avg(F.col("is_anomaly").cast("double")), 4).alias("anomaly_rate"))
    cluster_profile = [r.asDict() for r in saved.filter(F.col("n_req") >= MIN_REQ_FOR_SCORING)
                       .groupBy("cluster").agg(*profile_cols).orderBy("cluster").collect()]

    report = {
        "source": args.source,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window": DEFAULT_WINDOW,
        "min_req_for_scoring": MIN_REQ_FOR_SCORING,
        "train_test_cutoff": cutoff,
        "results": results,
        "alerts": n_alerts,
        "cluster_profile_kmeans_core": cluster_profile,
        "top_alerts": [r.asDict() for r in top.collect()],
    }
    write_json(f"{REPORTS_DIR}/metrics_{args.source}.json", report)

    # --- model cho streaming (chi train tu simulator: cung dinh dang log voi stream) ---
    if args.source == "sim":
        model = final_meta["model"]
        model.write().overwrite().save(MODEL_DIR)
        write_json(MODEL_META_PATH, {
            "model_dir": MODEL_DIR,
            "algo": "kmeans",
            "features": final_meta["features"],
            "window": DEFAULT_WINDOW,
            "k": final_meta["k"],
            "scoring": "cblof",
            "large_clusters": final_meta["large"],
            "threshold": final_meta["threshold"],
            "threshold_pct": args.threshold_pct,
            "min_req_for_scoring": MIN_REQ_FOR_SCORING,
            "centers": [c.tolist() for c in model.stages[-1].clusterCenters()],
            "trained_on": f"{args.gold_root}/features_sim (< {cutoff})",
            "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        print(f"Da luu PipelineModel -> {MODEL_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
