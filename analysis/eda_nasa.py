"""EDA cho silver NASA - ve hinh dua vao bao cao (muc Du lieu & tien xu ly).

Chay tren HOST (khong can Spark), sau khi da chay jobs/ingest_nasa.py:

    pip install pandas pyarrow matplotlib
    python analysis/eda_nasa.py

Doc thang Parquet partition dt=/hour= bang pyarrow, chi lay cac cot can ve.
Hinh ghi vao data/reports/figures/nasa_*.png.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # chay khong can man hinh
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SILVER = REPO_ROOT / "data" / "silver" / "nasa_access"
DEFAULT_OUT = REPO_ROOT / "data" / "reports" / "figures"

COLUMNS = ["event_time", "ip", "path", "status_code", "bytes_out"]


def load(silver: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(silver), format="parquet", partitioning="hive")
    return dataset.to_table(columns=COLUMNS).to_pandas()


def save(fig, out_dir: Path, name: str) -> None:
    fig.tight_layout()
    fig.savefig(out_dir / name, dpi=150)
    plt.close(fig)
    print(f"  {out_dir / name}")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver", type=Path, default=DEFAULT_SILVER)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = p.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)

    df = load(args.silver)
    print(f"Doc {len(df):,} dong tu {args.silver}")
    df["status_class"] = (df["status_code"] // 100).astype(str) + "xx"

    # 1. Luu luong theo gio, tach theo nhom status
    hourly = (
        df.groupby([pd.Grouper(key="event_time", freq="1h"), "status_class"])
        .size()
        .unstack("status_class")
        .fillna(0)
    )
    fig, ax = plt.subplots(figsize=(12, 4))
    hourly.plot(ax=ax, linewidth=0.8)
    ax.set_title("NASA 07/1995 - so request moi gio theo nhom status")
    ax.set_xlabel("Thoi gian (UTC)")
    ax.set_ylabel("Request / gio")
    save(fig, args.out, "nasa_requests_per_hour.png")

    # 2. Phan bo status code (log scale vi 200 ap dao)
    status = df["status_code"].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(7, 4))
    status.plot.bar(ax=ax, logy=True, color="#4C72B0")
    ax.set_title("Phan bo status code (thang log)")
    ax.set_xlabel("Status code")
    ax.set_ylabel("So request")
    save(fig, args.out, "nasa_status_distribution.png")

    # 3. Top 20 host theo so request
    top_hosts = df["ip"].value_counts().head(20).sort_values()
    fig, ax = plt.subplots(figsize=(8, 6))
    top_hosts.plot.barh(ax=ax, color="#55A868")
    ax.set_title("Top 20 host theo so request")
    ax.set_xlabel("So request")
    save(fig, args.out, "nasa_top_hosts.png")

    # 4. Top 15 path tra ve 404
    top_404 = df.loc[df["status_code"] == 404, "path"].value_counts().head(15).sort_values()
    fig, ax = plt.subplots(figsize=(9, 6))
    top_404.plot.barh(ax=ax, color="#C44E52")
    ax.set_title("Top 15 path bi 404")
    ax.set_xlabel("So request")
    save(fig, args.out, "nasa_top_404_paths.png")

    # 5. Histogram kich thuoc response (log10), bo cac response rong
    nonzero = df.loc[df["bytes_out"] > 0, "bytes_out"]
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(np.log10(nonzero), bins=60, color="#8172B2")
    ax.set_title("Phan bo kich thuoc response (log10 bytes)")
    ax.set_xlabel("log10(bytes_out)")
    ax.set_ylabel("So request")
    save(fig, args.out, "nasa_bytes_histogram.png")

    # 6. So request moi host (phan bo duoi dai - co so cho viec phat hien bat thuong)
    per_host = df["ip"].value_counts()
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(per_host, bins=100, log=True, color="#937860")
    ax.set_title("Phan bo so request moi host (truc y log)")
    ax.set_xlabel("So request cua mot host trong thang")
    ax.set_ylabel("So host")
    save(fig, args.out, "nasa_requests_per_host.png")

    print(
        f"Tong ket: {df['ip'].nunique():,} host, "
        f"{(df['status_code'] >= 400).mean():.2%} request loi, "
        f"median {per_host.median():.0f} request/host, max {per_host.max():,}"
    )


if __name__ == "__main__":
    main()
