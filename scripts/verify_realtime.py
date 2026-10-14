"""E2E live: attack -> ES alert -> Kibana history, restart checkpoint khong trung."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from stack_api import ROOT, request


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attack", choices=["none", "ddos", "scan", "brute_force", "sqli"], default="ddos")
    parser.add_argument("--detector", choices=["model", "rule"], default="model")
    args = parser.parse_args()
    run = f"qa-{args.attack}-{uuid.uuid4().hex[:8]}"
    base = ROOT / "data" / "stream" / run
    (base / "incoming").mkdir(parents=True)
    logs = ROOT / "data" / "raw" / "setup"
    logs.mkdir(parents=True, exist_ok=True)
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    normal = args.attack == "none"
    duration = 180 if normal else 75
    stream_duration = duration + 50
    if not normal:
        # Doi rule recover sau bulk import, de lan kich hoat nay thuoc attack dang do.
        deadline = time.monotonic() + 110
        quiet_at = None
        while time.monotonic() < deadline:
            fresh = request("POST", "/log-alerts/_count", {"query": {"range": {"detected_at": {"gte": "now-1m"}}}})["count"]
            rules = request("GET", "/api/alerting/rules/_find?per_page=100", kibana=True)["data"]
            rule = next(item for item in rules if item["name"] == "Nhu - New anomaly detected")
            assert rule["enabled"], "Native rule dang bi tat"
            if fresh:
                quiet_at = None
            else:
                quiet_at = quiet_at or datetime.now(timezone.utc)
                last = rule.get("execution_status", {}).get("last_execution_date")
                if last and datetime.fromisoformat(last.replace("Z", "+00:00")) > quiet_at:
                    break
            time.sleep(2)
        else:
            raise RuntimeError("Native rule chua recover; chua bat dau attack")
    command = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
               "--conf", "spark.sql.shuffle.partitions=4", "--py-files", "/opt/spark/jobs/features_common.py", "/opt/spark/jobs/stream_detect.py",
               "--input-path", f"/opt/spark/data/stream/{run}/incoming", "--checkpoint-path", f"/opt/spark/data/stream/{run}/checkpoint", "--detector", args.detector]
    with (logs / f"{run}.log").open("w") as stream_log:
        stream = subprocess.Popen(command + ["--duration", str(stream_duration)], cwd=ROOT, stdout=stream_log, stderr=subprocess.STDOUT, creationflags=flags)
        start = time.monotonic()
        while "STREAM READY" not in (logs / f"{run}.log").read_text(errors="replace"):
            if stream.poll() is not None or time.monotonic() - start > 90:
                raise RuntimeError(f"Stream khong ready; xem {logs / (run + '.log')}")
            time.sleep(1)
        manifest = base / "manifest.json"
        with (logs / f"{run}-sim.log").open("w") as sim_log:
            sim = subprocess.Popen([sys.executable, "-B", "-m", "generator.stream_simulate", "--duration", str(duration), "--rate", "2", "--attack", args.attack,
                                    "--attack-after", "20", "--seed", "73", "--out-dir", str(base / "incoming"), "--manifest", str(manifest)],
                                   cwd=ROOT, stdout=sim_log, stderr=subprocess.STDOUT, creationflags=flags)
            first_observed = None
            hits = []
            history_before = request("GET", "/alert-history/_count")["count"]
            while not normal and time.monotonic() - start < stream_duration - 5:
                if manifest.exists():
                    try:
                        data = json.loads(manifest.read_text())
                    except json.JSONDecodeError:
                        time.sleep(1)
                        continue
                    filters = [{"term": {"source": "stream"}}, {"range": {"detected_at": {"gte": data["started_at"]}}}]
                    if not normal:
                        filters.append({"terms": {"ip": data["attack_ips"]}})
                    query = {"bool": {"filter": filters}}
                    response = request("POST", "/log-alerts/_search", {"query": query, "size": 1000})
                    hits = response["hits"]["hits"]
                    if hits and first_observed is None:
                        first_observed = datetime.now(timezone.utc)
                        print(f"First {args.attack} alert observed: {first_observed.isoformat()}", flush=True)
                    if not normal and first_observed and request("GET", "/alert-history/_count")["count"] > history_before:
                        break
                if stream.poll() is not None:
                    raise RuntimeError("Stream dung som; xem log")
                time.sleep(2)
            if sim.wait(timeout=duration + 15) != 0 or stream.wait(timeout=stream_duration + 30) != 0:
                raise RuntimeError("Simulator/stream failed")
    data = json.loads(manifest.read_text())
    if normal:
        query = {"bool": {"filter": [{"term": {"source": "stream"}}, {"range": {"detected_at": {"gte": data["started_at"]}}}]}}
        count = request("POST", "/log-alerts/_count", {"query": query})["count"]
        features = request("POST", "/log-features/_search", {"query": query, "size": 10})
        assert features["hits"]["total"]["value"] > 0, "Normal stream khong ghi features"
        expected = "kmeans_cblof" if args.detector == "model" else "rule_fallback"
        assert all(hit["_source"]["detector"] == expected for hit in features["hits"]["hits"])
        assert count <= 2, f"Qua nhieu canh bao gia: {count}"
        print(json.dumps({"normal_duration_seconds": duration, "records": data["published_records"],
                          "false_positive_windows": count, "detector": args.detector}))
        return
    if not first_observed:
        raise AssertionError(f"Khong co canh bao {args.attack}")
    latency = (first_observed - datetime.fromisoformat(data["first_attack_at"].replace("Z", "+00:00"))).total_seconds()
    assert 0 <= latency <= 60, latency
    expected_detector = "kmeans_cblof" if args.detector == "model" else "rule_fallback"
    assert all(hit["_source"]["detector"] == expected_detector and hit["_source"]["is_alert"] for hit in hits)
    if args.detector == "model":
        assert all(hit["_source"]["reason"] == hit["_source"]["top_feature"] for hit in hits)
    before = request("POST", "/log-alerts/_count", {"query": query})["count"]
    with (logs / f"{run}-restart.log").open("w") as restart_log:
        result = subprocess.run(command + ["--duration", "25"], cwd=ROOT, stdout=restart_log, stderr=subprocess.STDOUT, creationflags=flags, timeout=90)
    if result.returncode:
        raise RuntimeError("Checkpoint restart failed")
    after = request("POST", "/log-alerts/_count", {"query": query})["count"]
    assert before == after > 0
    history_after = request("GET", "/alert-history/_count")["count"]
    assert history_after > history_before, "Native Kibana rule chua ghi alert-history"
    rules = request("GET", "/api/alerting/rules/_find?per_page=100", kibana=True)["data"]
    rule = next(item for item in rules if item["name"] == "Nhu - New anomaly detected")
    report = {"attack": args.attack, "run_id": run, "detector": expected_detector, "latency_seconds": round(latency, 3),
              "alert_documents_before_restart": before, "alert_documents_after_restart": after,
              "history_before": history_before, "history_after": history_after,
              "rule_execution_status": rule.get("execution_status"), "rule_last_run": rule.get("last_run"),
              "manifest": data, "verified_at": datetime.now(timezone.utc).isoformat()}
    print(json.dumps({key: value for key, value in report.items() if key != "manifest"}, indent=2), flush=True)


if __name__ == "__main__":
    main()
