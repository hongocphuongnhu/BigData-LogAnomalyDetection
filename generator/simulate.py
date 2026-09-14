"""CLI: sinh file log mau (JSONL) theo Log Contract v1 vao data/bronze/.

Chay tu thu muc goc repo:
    python -m generator.simulate --minutes 30 --rate 2.0 --seed 42
"""
from __future__ import annotations

import argparse
import itertools
import json
import random
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from .attacks import ATTACK_GENERATORS
from .log_contract import FIELD_ORDER, validate
from .traffic_normal import generate_normal_traffic

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "bronze" / "http_access"


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--minutes", type=float, default=30, help="Do dai timeline mo phong (phut). Default: 30")
    p.add_argument("--rate", type=float, default=2.0, help="So request/giay trung binh cua traffic binh thuong. Default: 2.0")
    p.add_argument("--seed", type=int, default=42, help="Seed cho RNG, de tai lap ket qua. Default: 42")
    p.add_argument(
        "--attacks",
        type=str,
        default="brute_force,sqli,scan,ddos",
        help="Danh sach loai attack can chen, phan cach boi dau phay. Rong ('') = khong chen attack.",
    )
    p.add_argument("--attack-repeat", type=int, default=1, help="So chien dich cho MOI loai attack. Default: 1")
    p.add_argument(
        "--start",
        type=str,
        default=None,
        help="Thoi diem bat dau timeline, ISO 8601 UTC (vi du 2026-01-15T00:00:00Z). Default: now.",
    )
    p.add_argument("--out-dir", type=str, default=str(DEFAULT_OUT_DIR), help="Thu muc goc bronze de ghi file.")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    rng = random.Random(args.seed)

    base_dt = (
        datetime.fromisoformat(args.start.replace("Z", "+00:00"))
        if args.start
        else datetime.now(timezone.utc)
    )
    duration_s = args.minutes * 60

    records = generate_normal_traffic(base_dt, duration_s, args.rate, rng)

    attack_types = [a.strip() for a in args.attacks.split(",") if a.strip()]
    for attack_type in attack_types:
        generator_fn = ATTACK_GENERATORS.get(attack_type)
        if generator_fn is None:
            raise SystemExit(
                f"Loai attack khong hop le: {attack_type!r}. "
                f"Cac loai duoc ho tro: {sorted(ATTACK_GENERATORS)}"
            )
        for _ in range(args.attack_repeat):
            records.extend(generator_fn(base_dt, duration_s, rng))

    records.sort(key=lambda r: r["ts"])

    errors = [e for r in records for e in validate(r)]
    if errors:
        raise SystemExit(f"Sinh log that bai, vi pham Log Contract v1: {errors[:10]}")

    # Rotation theo gio: dt=YYYY-MM-DD/hour=HH/part-*.jsonl - ts co dinh dang
    # "YYYY-MM-DDTHH:MM:SS.mmmZ" nen lay date/hour bang cach slice truc tiep,
    # khong can parse lai datetime.
    def hour_key(r: dict) -> tuple[str, str]:
        ts = r["ts"]
        return ts[0:10], ts[11:13]

    out_root = Path(args.out_dir)
    written_files = []
    for (date_str, hour_str), group_iter in itertools.groupby(records, key=hour_key):
        group = list(group_iter)
        out_dir = out_root / f"dt={date_str}" / f"hour={hour_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        file_name = f"part-{date_str.replace('-', '')}T{hour_str}0000-{uuid.uuid4().hex[:8]}.jsonl"
        out_path = out_dir / file_name

        with out_path.open("w", encoding="utf-8") as f:
            for r in group:
                f.write(json.dumps({k: r[k] for k in FIELD_ORDER}, ensure_ascii=False))
                f.write("\n")
        written_files.append((out_path, len(group)))

    normal_count = sum(1 for r in records if not r["is_anomaly"])
    counts = Counter(r["anomaly_label"] for r in records if r["is_anomaly"])
    cache_hits = sum(1 for r in records if r["cache_status"] == "HIT")

    print(f"Da sinh {len(records)} dong log, chia thanh {len(written_files)} file theo gio:")
    for path, n in written_files:
        print(f"  {path} ({n} dong)")
    print(f"  normal      : {normal_count}")
    for label in ("brute_force", "sqli", "scan", "ddos"):
        if counts.get(label):
            print(f"  {label:12s}: {counts[label]}")
    print(f"  cache HIT ratio: {cache_hits}/{len(records)} ({100 * cache_hits / len(records):.1f}%)")


if __name__ == "__main__":
    main()
