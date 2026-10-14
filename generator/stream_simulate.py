"""Sinh log theo thoi gian thuc, publish JSONL bang atomic rename moi 5 giay."""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .attacks import ATTACK_GENERATORS
from .log_contract import FIELD_ORDER, validate
from .traffic_normal import generate_normal_traffic


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=2)
    parser.add_argument("--duration", type=float, default=180)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--attack", choices=["none", *ATTACK_GENERATORS], default="none")
    parser.add_argument("--attack-after", type=float, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", default="data/stream/incoming")
    parser.add_argument("--manifest", default="data/stream/last-run.json")
    args = parser.parse_args()
    if args.rate < 0 or args.duration <= 0 or args.interval <= 0 or args.attack_after < 0:
        parser.error("rate >= 0, duration/interval > 0, attack-after >= 0")
    if args.attack != "none" and args.attack_after >= args.duration:
        parser.error("attack-after phai nho hon duration")
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    rng = random.Random(args.seed)
    base = datetime.now(timezone.utc)
    end = base + timedelta(seconds=args.duration)
    records = generate_normal_traffic(base, args.duration, args.rate, rng)
    if args.attack != "none":
        records.extend(ATTACK_GENERATORS[args.attack](base + timedelta(seconds=args.attack_after), 0, rng))
    records = [record for record in records if datetime.fromisoformat(record["ts"].replace("Z", "+00:00")) < end]
    records.sort(key=lambda record: record["ts"])
    if any(validate(record) for record in records):
        raise ValueError("Record vi pham contract")
    attacks = [record for record in records if record["is_anomaly"]]
    manifest = {"run_id": run_id, "started_at": base.isoformat(), "attack": args.attack,
                "first_attack_at": attacks[0]["ts"] if attacks else None,
                "attack_ips": sorted({record["ip"] for record in attacks}),
                "normal_records": len(records) - len(attacks), "attack_records": len(attacks),
                "planned_duration_seconds": args.duration, "published_records": 0, "files": []}
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    monotonic_start = time.monotonic()
    cursor = 0
    try:
        while time.monotonic() - monotonic_start < args.duration or cursor < len(records):
            # Khong publish timestamp tuong lai: watermark phai tien theo traffic that.
            elapsed = min(args.duration, time.monotonic() - monotonic_start)
            cutoff = (base + timedelta(seconds=elapsed)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            upper = cursor
            while upper < len(records) and records[upper]["ts"] <= cutoff:
                upper += 1
            if upper > cursor:
                path = out / f"part-{run_id}-{len(manifest['files']):06d}.jsonl"
                temp = path.with_name("." + path.name + ".tmp")
                with temp.open("w", encoding="utf-8", newline="\n") as stream:
                    for record in records[cursor:upper]:
                        stream.write(json.dumps({key: record[key] for key in FIELD_ORDER}) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                temp.replace(path)
                manifest["files"].append(path.as_posix())
                manifest["published_records"] = upper
                manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
                print(f"Published {upper - cursor} records -> {path}", flush=True)
                cursor = upper
            if elapsed >= args.duration:
                break
            time.sleep(min(args.interval, args.duration - elapsed))
    finally:
        manifest["ended_at"] = datetime.now(timezone.utc).isoformat()
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"STREAM SIMULATOR OK: {cursor} records, run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
