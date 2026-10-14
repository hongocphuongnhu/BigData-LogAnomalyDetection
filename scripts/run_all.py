"""Dieu phoi serving cua Nhu; chi export artifact that da co tren may."""
from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from pathlib import Path

from stack_api import ROOT


def run(*command: str) -> None:
    print("RUN: " + " ".join(command), flush=True)
    if "hdfs" in command:
        result = subprocess.run(command, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if result.returncode:
            print(result.stdout[-6000:])
            raise subprocess.CalledProcessError(result.returncode, command)
    else:
        subprocess.run(command, cwd=ROOT, check=True)


def submit(job: str, *args: str) -> None:
    command = ("docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
        "--conf", "spark.sql.shuffle.partitions=16", "--conf", "spark.hadoop.dfs.replication=1",
        "--py-files", "/opt/spark/jobs/features_common.py", f"/opt/spark/jobs/{job}", *args)
    logs = ROOT / "data/logs"
    logs.mkdir(parents=True, exist_ok=True)
    suffix = "-".join(args[args.index(flag) + 1] for flag in ("--what", "--source") if flag in args) or "demo"
    name = job.removesuffix(".py") + "-" + suffix
    path = logs / (name + ".log")
    print("RUN: " + " ".join(command) + f" (log: {path.relative_to(ROOT)})", flush=True)
    with path.open("w", encoding="utf-8") as output:
        result = subprocess.run(command, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT)
    if result.returncode:
        print("\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-40:]))
        raise subprocess.CalledProcessError(result.returncode, command)
    print("OK: " + job, flush=True)


def upload_hdfs(local: str, target: str) -> None:
    # Parquet ghi lai doi ten part-file. Ghep folder cu se nhan doi du lieu.
    # Dua vao staging, doi folder; giu ban cu trong _history de co the khoi phuc.
    command = ("docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs")
    identity = uuid.uuid4().hex
    staging, history = f"/lake/_staging/{identity}", f"/lake/_history/{identity}"
    name = Path(local).name
    destination = f"/lake/{target}/{name}"
    run(*command, "-mkdir", "-p", staging, history, f"/lake/{target}")
    run(*command, "-put", f"/data/{local}", staging + "/")
    exists = subprocess.run((*command, "-test", "-e", destination), cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    if exists:
        run(*command, "-mv", destination, history + "/")
    try:
        run(*command, "-mv", f"{staging}/{name}", destination)
    except subprocess.CalledProcessError:
        if exists:
            run(*command, "-mv", f"{history}/{name}", destination)
        raise


def prepare_batch() -> None:
    # Dung nguyen job cua Dinh; model SIM da chot chi load, khong train/ghi de.
    if sys.platform == "win32":
        run("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts/download_nasa.ps1")
    else:
        run("sh", "scripts/download_nasa.sh")
    bronze = ROOT / "data/bronze/demo_sim"
    marker = bronze / "_SUCCESS"
    if not marker.exists():
        if list(bronze.rglob("*.jsonl")):
            raise RuntimeError("Bronze demo_sim chua hoan tat. Kiem tra du lieu truoc khi sinh lai de tranh trung.")
        run(sys.executable, "-B", "-m", "generator.simulate", "--minutes", "1440", "--rate", "12",
            "--attack-repeat", "30", "--start", "2026-01-10T00:00:00Z", "--seed", "42", "--out-dir", str(bronze))
        marker.touch()
    submit("ingest_nasa.py", "--dq-report", "/opt/spark/data/raw/setup/demo_dq_nasa.json")
    submit("ingest_http_access.py", "--bronze-path", "/opt/spark/data/bronze/demo_sim", "--silver-path", "/opt/spark/data/silver/demo_sim")
    # Dynamic partition overwrite khong bao dam co _SUCCESS tai root.
    (ROOT / "data/silver/demo_sim/_DEMO_READY").touch()
    for source in ("nasa", "sim"):
        extra = ("--silver-path", "/opt/spark/data/silver/demo_sim") if source == "sim" else ()
        submit("build_features.py", *extra, "--source", source)
        submit("score_demo_batch.py", "--source", source)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--batch", action="store_true", help="Can job Phase 02-04 cua Dinh da duoc ban giao")
    parser.add_argument("--bootstrap", action="store_true", help="Build/up Docker stack truoc khi chay")
    parser.add_argument("--hdfs", action="store_true", help="Dua them NASA va gold batch len HDFS")
    args = parser.parse_args()
    if args.bootstrap:
        run("docker", "compose", "up", "-d", "--build")
    if args.batch:
        prepare_batch()
        if not args.serve:
            return
    full_sim = (ROOT / "data/silver/demo_sim/_DEMO_READY").exists()
    silver_sim = "demo_sim" if full_sim else "http_access"
    if not full_sim and not list((ROOT / "data" / "bronze" / "http_access").rglob("*.jsonl")):
        run(sys.executable, "-B", "-m", "generator.simulate", "--minutes", "30", "--rate", "2", "--seed", "42")
    # Upload boi Docker truc tiep de Linux va Windows dung chung orchestration.
    run("docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/lake/bronze", "/lake/raw/nasa", "/lake/silver", "/lake/gold")
    bronze_sim = "demo_sim" if full_sim else "http_access"
    upload_hdfs(f"bronze/{bronze_sim}", "bronze")
    if full_sim:
        upload_hdfs("silver/demo_sim", "silver")
    else:
        submit("ingest_http_access.py", "--bronze-path", "hdfs://namenode:9000/lake/bronze/http_access",
               "--silver-path", "hdfs://namenode:9000/lake/silver/http_access", "--rejected-path", "hdfs://namenode:9000/lake/silver/_rejected/http_access")
    if args.hdfs:
        for local, target in (("raw/nasa", "raw"), ("silver/nasa_access", "silver"),
                              *((f"gold/{kind}_{source}", "gold") for source in ("sim", "nasa") for kind in ("features", "scores", "alerts"))):
            if not (ROOT / "data" / local).is_dir():
                raise FileNotFoundError(local)
            upload_hdfs(local, target)
    run(sys.executable, "-B", "scripts/es_setup.py")
    submit("export_to_es.py", "--what", "logs-sim", "--replace-source", "--input-path", f"hdfs://namenode:9000/lake/silver/{silver_sim}")
    skipped = []
    for source, silver_name in (("sim", "http_access"), ("nasa", "nasa_access")):
        if source == "nasa":
            path = ROOT / "data" / "silver" / silver_name
            if list(path.rglob("*.parquet")):
                submit("export_to_es.py", "--what", "logs-nasa", "--replace-source", "--input-path", "/opt/spark/data/silver/nasa_access")
            else:
                skipped.append("silver NASA")
        for kind, directory in (("features", "scores"), ("alerts", "alerts")):
            path = ROOT / "data" / "gold" / f"{directory}_{source}"
            if list(path.rglob("*.parquet")):
                submit("export_to_es.py", "--what", kind, "--source", source, "--replace-source", "--input-path", f"/opt/spark/data/gold/{directory}_{source}")
            else:
                skipped.append(f"{directory}_{source}")
    run(sys.executable, "-B", "scripts/kibana_setup.py")
    if args.batch and skipped:
        raise RuntimeError("Demo batch chua du artifact: " + ", ".join(skipped))
    print("SERVING OK. Artifact chua ban giao: " + (", ".join(skipped) or "khong co"))
    print("Streaming chay rieng: scripts/submit.ps1 stream_detect.py; python -m generator.stream_simulate --attack ddos")


if __name__ == "__main__":
    main()
