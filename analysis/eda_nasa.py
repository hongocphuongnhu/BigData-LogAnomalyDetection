"""EDA cho silver NASA - ve hinh dua vao bao cao (muc Du lieu & tien xu ly).

Chay tren HOST (khong can Spark), sau khi da chay jobs/ingest_nasa.py:

    pip install pandas pyarrow matplotlib
    python analysis/eda_nasa.py

Doc thang Parquet partition dt=/hour= bang pyarrow, chi lay cac cot can ve.
Hinh ghi vao data/reports/figures/nasa_*.png (style chung: analysis/vizstyle.py).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from matplotlib.ticker import FuncFormatter

import vizstyle as vs

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SILVER = REPO_ROOT / "data" / "silver" / "nasa_access"
DEFAULT_OUT = REPO_ROOT / "data" / "reports" / "figures"

COLUMNS = ["event_time", "ip", "path", "status_code", "bytes_out"]
STATUS_CLASSES = ["2xx", "3xx", "4xx", "5xx"]
COMPACT = FuncFormatter(lambda v, _: f"{v / 1000:.0f}K" if abs(v) >= 1000 else f"{v:.0f}")


def load(silver: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(silver), format="parquet", partitioning="hive")
    return dataset.to_table(columns=COLUMNS).to_pandas()


def hbar(ax, series: pd.Series, title: str, sub: str, xlabel: str) -> None:
    """Bar ngang mot series (mot mau, khong legend), gia tri o dau bar."""
    series = series.sort_values()
    ax.barh(series.index.astype(str), series.values, height=0.6, **vs.bar_kwargs(vs.SERIES[0]))
    ax.grid(axis="y", visible=False)
    ax.xaxis.set_major_formatter(COMPACT)
    ax.set_xlabel(xlabel)
    ax.set_title(title, pad=18)
    vs.subtitle(ax, sub)
    top = series.max()
    for i, v in enumerate(series.values):
        ax.text(v + top * 0.01, i, f"{v:,}", va="center", fontsize=8, color=vs.INK_2)
    ax.set_xlim(0, top * 1.15)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver", type=Path, default=DEFAULT_SILVER)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = p.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    vs.apply()

    df = load(args.silver)
    print(f"Doc {len(df):,} dong tu {args.silver}")
    df["status_class"] = (df["status_code"] // 100).astype(str) + "xx"

    # 1. Request/gio theo nhom status: small multiples (moi nhom mot truc y rieng)
    #    vi 2xx lon gap ~1000 lan 5xx - chung mot truc thi 4xx/5xx thanh duong phang.
    hourly = (
        df.groupby([pd.Grouper(key="event_time", freq="1h"), "status_class"])
        .size()
        .unstack("status_class")
        .reindex(columns=STATUS_CLASSES)
        .fillna(0)
    )
    fig, axes = plt.subplots(4, 1, figsize=(12, 7.5), sharex=True)
    for ax, cls, color in zip(axes, STATUS_CLASSES, vs.SERIES):
        ax.plot(hourly.index, hourly[cls], color=color, linewidth=1.2)
        ax.fill_between(hourly.index, hourly[cls], color=color, alpha=0.1, linewidth=0)
        ax.yaxis.set_major_formatter(COMPACT)
        ax.set_ylim(bottom=0)
        ax.set_ylabel(cls, rotation=0, ha="right", va="center", color=vs.INK)
        ax.text(1.0, 0.85, f"tổng {int(hourly[cls].sum()):,}", transform=ax.transAxes,
                ha="right", fontsize=8, color=vs.INK_2)
    peak = hourly["2xx"].idxmax()
    axes[0].annotate(
        f"Đỉnh {int(hourly['2xx'].max()):,} req/giờ\n{peak:%d/%m %H:%M} — phóng tàu STS-70",
        xy=(peak, hourly["2xx"].max()), xytext=(25, -8), textcoords="offset points",
        fontsize=8, color=vs.INK_2, va="top",
        arrowprops={"arrowstyle": "-", "color": vs.MUTED, "linewidth": 0.8},
    )
    peak5 = hourly["5xx"].idxmax()
    axes[3].annotate(
        f"{int(hourly['5xx'].max())} lỗi 5xx lúc {peak5:%d/%m %H:%M}\n(cảnh báo #1 của mô hình: host 163.205.1.45)",
        xy=(peak5, hourly["5xx"].max()), xytext=(25, -4), textcoords="offset points",
        fontsize=8, color=vs.INK_2, va="top",
        arrowprops={"arrowstyle": "-", "color": vs.MUTED, "linewidth": 0.8},
    )
    axes[0].set_title("NASA 07/1995 — số request mỗi giờ theo nhóm status", pad=18)
    vs.subtitle(axes[0], "Mỗi nhóm một trục y riêng để thấy được 4xx/5xx; giờ UTC")
    vs.save(fig, args.out, "nasa_requests_per_hour.png")

    # 2. Phan bo status code (log scale vi 200 ap dao)
    status = df["status_code"].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(7.5, 4))
    x = np.arange(len(status))
    ax.bar(x, status.values, width=0.6, **vs.bar_kwargs(vs.SERIES[0]))
    ax.set_xticks(x, status.index.astype(str))
    ax.set_yscale("log")
    ax.grid(axis="x", visible=False)
    for xi, v in zip(x, status.values):
        ax.text(xi, v * 1.15, f"{v:,}", ha="center", fontsize=8, color=vs.INK_2)
    ax.set_xlabel("Status code")
    ax.set_ylabel("Số request (thang log)")
    ax.set_title("Phân bố status code", pad=18)
    vs.subtitle(ax, f"{len(df):,} request; lỗi 4xx/5xx chiếm {(df['status_code'] >= 400).mean():.2%}")
    vs.save(fig, args.out, "nasa_status_distribution.png")

    # 3. Top 20 host theo so request
    fig, ax = plt.subplots(figsize=(9, 6.5))
    hbar(ax, df["ip"].value_counts().head(20), "Top 20 host theo số request",
         "Nhiều host là proxy của nhà mạng (prodigy, aol) — một host đại diện nhiều người dùng", "Số request")
    vs.save(fig, args.out, "nasa_top_hosts.png")

    # 4. Top 15 path tra ve 404
    fig, ax = plt.subplots(figsize=(10, 6))
    hbar(ax, df.loc[df["status_code"] == 404, "path"].value_counts().head(15), "Top 15 path trả về 404",
         "Link hỏng lặp lại nhiều lần — nguồn chính của lỗi 4xx", "Số request")
    vs.save(fig, args.out, "nasa_top_404_paths.png")

    # 5. Histogram kich thuoc response (log10), bo cac response rong
    nonzero = df.loc[df["bytes_out"] > 0, "bytes_out"]
    fig, ax = plt.subplots(figsize=(7.5, 4))
    ax.hist(np.log10(nonzero), bins=60, color=vs.SERIES[0], edgecolor=vs.SURFACE, linewidth=0.5)
    ax.set_xticks(range(1, 8), ["10 B", "100 B", "1 KB", "10 KB", "100 KB", "1 MB", "10 MB"])
    ax.yaxis.set_major_formatter(COMPACT)
    ax.grid(axis="x", visible=False)
    ax.set_xlabel("Kích thước response (thang log)")
    ax.set_ylabel("Số request")
    ax.set_title("Phân bố kích thước response", pad=18)
    vs.subtitle(ax, f"Bỏ {int((df['bytes_out'] == 0).sum()):,} response rỗng (304/302, thiếu bytes)")
    vs.save(fig, args.out, "nasa_bytes_histogram.png")

    # 6. So request moi host - duoi dai (co so cho phat hien bat thuong theo host)
    per_host = df["ip"].value_counts()
    fig, ax = plt.subplots(figsize=(7.5, 4))
    bins = np.logspace(0, np.log10(per_host.max()), 50)
    ax.hist(per_host, bins=bins, color=vs.SERIES[0], edgecolor=vs.SURFACE, linewidth=0.5, log=True)
    ax.set_xscale("log")
    ax.grid(axis="x", visible=False)
    ax.set_xlabel("Số request của một host trong tháng (thang log)")
    ax.set_ylabel("Số host (thang log)")
    ax.set_title("Phân bố số request mỗi host", pad=18)
    vs.subtitle(ax, f"{len(per_host):,} host; trung vị {per_host.median():.0f}, lớn nhất {per_host.max():,} — đuôi rất dài")
    vs.save(fig, args.out, "nasa_requests_per_host.png")

    print(
        f"Tong ket: {df['ip'].nunique():,} host, "
        f"{(df['status_code'] >= 400).mean():.2%} request loi, "
        f"median {per_host.median():.0f} request/host, max {per_host.max():,}"
    )


if __name__ == "__main__":
    main()
