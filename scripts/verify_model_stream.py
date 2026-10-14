"""Kiem thu model CBLOF chinh thuc voi traffic offline va index rieng."""
from __future__ import annotations

import json
import subprocess
import uuid
import sys
import random
from datetime import datetime, timedelta, timezone

from stack_api import ROOT, request


def main() -> None:
    run = "qa-model-" + uuid.uuid4().hex[:8]
    sys.path.insert(0, str(ROOT))
    from generator.traffic_normal import generate_normal_traffic
    from generator.attacks import ATTACK_GENERATORS
    folder = ROOT / "data/stream" / run / "incoming"
    folder.mkdir(parents=True)
    rng = random.Random(73)
    start = datetime.now(timezone.utc) - timedelta(minutes=4)
    records = generate_normal_traffic(start, 180, 2, rng)
    records.extend(ATTACK_GENERATORS["ddos"](start + timedelta(seconds=60), 0, rng))
    (folder / "part.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
    feature_index, alert_index = run + "-features", run + "-alerts"
    for target, original in ((feature_index, "log-features"), (alert_index, "log-alerts")):
        mappings = request("GET", f"/{original}/_mapping")[original]["mappings"]
        request("PUT", f"/{target}", {"settings": {"number_of_replicas": 0}, "mappings": mappings})
    log = ROOT / "data/raw/setup" / (run + ".log")
    try:
        command = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
                   "--conf", "spark.sql.shuffle.partitions=4", "--py-files", "/opt/spark/jobs/features_common.py", "/opt/spark/jobs/stream_detect.py",
                   "--input-path", f"/opt/spark/data/stream/{run}/incoming", "--checkpoint-path", f"/opt/spark/data/stream/{run}/checkpoint",
                   "--detector", "model", "--features-index", feature_index, "--alerts-index", alert_index, "--duration", "45"]
        with log.open("w") as output:
            subprocess.run(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, check=True, timeout=120)
        data = request("POST", f"/{feature_index}/_search", {"size": 10})
        assert data["hits"]["total"]["value"] > 0
        assert all(hit["_source"]["detector"] == "kmeans_cblof" and "ref_cluster" in hit["_source"] for hit in data["hits"]["hits"])
        alert_count = request("GET", f"/{alert_index}/_count")["count"]
        assert alert_count > 0
        report = {"model_streaming_branch": "passed", "feature_windows": data["hits"]["total"]["value"],
                  "isolated_indices": True, "official_model": True, "alert_windows": alert_count}
        print(json.dumps(report, indent=2))
    finally:
        request("DELETE", f"/{feature_index}")
        request("DELETE", f"/{alert_index}")


if __name__ == "__main__":
    main()
