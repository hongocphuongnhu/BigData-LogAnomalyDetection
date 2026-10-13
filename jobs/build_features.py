"""Phase 03: silver -> gold, feature theo cua so (ip, 5 phut).

Goi features_common.build_window_features - CUNG ham voi stream_detect.py,
nen model train tren gold nay cham diem dung tren du lieu streaming.

    ./scripts/submit.ps1 build_features.py --source sim     # simulator (co nhan, EXTENDED)
    ./scripts/submit.ps1 build_features.py --source nasa    # NASA that (CORE)

Ghi Parquet vao gold/features_{source} partition theo dt (ngay cua window_start),
in so cua so, so cua so bat thuong theo nhan (sim) va phan vi cua tung feature.
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from features_common import (
    CORE_FEATURES,
    DEFAULT_WINDOW,
    EXTENDED_FEATURES,
    GOLD_ROOT,
    SILVER_NASA,
    SILVER_SIM,
    build_window_features,
)

SILVER_BY_SOURCE = {"sim": SILVER_SIM, "nasa": SILVER_NASA}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", choices=sorted(SILVER_BY_SOURCE), required=True)
    p.add_argument("--silver-path", default=None, help="Mac dinh theo --source.")
    p.add_argument("--gold-path", default=None, help="Mac dinh gold/features_{source}.")
    p.add_argument("--window", default=DEFAULT_WINDOW)
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    silver_path = args.silver_path or SILVER_BY_SOURCE[args.source]
    gold_path = args.gold_path or f"{GOLD_ROOT}/features_{args.source}"
    # NASA khong co latency/cache/user-agent -> chi tinh duoc CORE.
    extended = args.source == "sim"
    feature_cols = EXTENDED_FEATURES if extended else CORE_FEATURES

    spark = SparkSession.builder.appName(f"build_features_{args.source}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    silver = spark.read.parquet(silver_path)
    feats = (
        build_window_features(silver, window=args.window, extended=extended)
        .withColumn("source", F.lit(args.source))
        .withColumn("dt", F.date_format("window_start", "yyyy-MM-dd"))
    )

    feats.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(gold_path)

    gold = spark.read.parquet(gold_path).cache()
    n_windows = gold.count()
    print(f"Da ghi {n_windows} cua so ({args.window}) -> {gold_path}")
    print(f"  so ip khac nhau: {gold.select('ip').distinct().count()}")

    if "is_anomaly" in gold.columns:
        print("  cua so theo nhan:")
        for row in gold.groupBy("anomaly_label").count().orderBy("anomaly_label").collect():
            print(f"    {str(row['anomaly_label']):12s}: {row['count']}")

    # Phan vi tung feature -> dung de dat nguong rule va viet muc "Trich xuat dac trung".
    quantiles = gold.approxQuantile(feature_cols, [0.5, 0.95, 0.99], 0.001)
    print(f"  {'feature':18s} {'p50':>10s} {'p95':>10s} {'p99':>10s}")
    for name, (p50, p95, p99) in zip(feature_cols, quantiles):
        print(f"  {name:18s} {p50:10.3f} {p95:10.3f} {p99:10.3f}")

    spark.stop()


if __name__ == "__main__":
    main()
