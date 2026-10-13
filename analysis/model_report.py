"""Ve hinh ket qua mo hinh cho bao cao (muc Ket qua & so sanh).

Chay tren HOST sau khi da chay jobs/detect_anomalies.py --source sim (va nasa):

    python analysis/model_report.py

Doc data/reports/metrics_{sim,nasa}.json + gold/scores_sim (Parquet), ghi
data/reports/figures/model_*.png va in bang so sanh dang Markdown de dan vao bao cao.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from sklearn.decomposition import PCA
from sklearn.metrics import precision_recall_curve
from sklearn.preprocessing import StandardScaler

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS = REPO_ROOT / "data" / "reports"
GOLD = REPO_ROOT / "data" / "gold"
LABELS = ["brute_force", "sqli", "scan", "ddos"]
CORE_FEATURES = [
    "n_req", "burst_rate", "n_distinct_paths", "ratio_4xx", "ratio_5xx", "ratio_404",
    "ratio_post", "ratio_sus_path", "ratio_static", "mean_bytes", "max_bytes",
]
LOG_FEATURES = {"n_req", "burst_rate", "n_distinct_paths", "mean_bytes", "max_bytes"}


def save(fig, out: Path, name: str) -> None:
    fig.tight_layout()
    fig.savefig(out / name, dpi=150)
    plt.close(fig)
    print(f"  {out / name}")


def method_name(r: dict) -> str:
    return r["method"] if r["method"] == "rule" else f"{r['method']} ({r['features']})"


def comparison(metrics: dict, out: Path) -> pd.DataFrame:
    rows = [
        {
            "method": method_name(r), "k": r.get("k", "-"), "precision": r["precision"],
            "recall": r["recall"], "f1": r["f1"], "pr_auc": r.get("pr_auc", np.nan),
            "fpr": r["fpr"], **{f"recall_{l}": r["recall_by_label"].get(l, np.nan) for l in LABELS},
        }
        for r in metrics["results"]
    ]
    df = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(9, 4))
    df.set_index("method")[["precision", "recall", "f1", "pr_auc"]].plot.bar(ax=ax, rot=15)
    ax.set_ylim(0, 1.05)
    ax.set_title("So sanh phuong phap tren tap test (cap cua so ip x 5 phut)")
    ax.legend(loc="lower right")
    save(fig, out, "model_comparison.png")

    fig, ax = plt.subplots(figsize=(8, 4))
    mat = df.set_index("method")[[f"recall_{l}" for l in LABELS]]
    mat.columns = LABELS
    im = ax.imshow(mat.values, cmap="RdYlGn", vmin=0, vmax=1, aspect="auto")
    ax.set_xticks(range(len(LABELS)), LABELS)
    ax.set_yticks(range(len(mat)), mat.index)
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat.values[i, j]
            ax.text(j, i, "-" if np.isnan(v) else f"{v:.2f}", ha="center", va="center")
    fig.colorbar(im, ax=ax, label="recall")
    ax.set_title("Recall theo tung loai tan cong")
    save(fig, out, "model_recall_by_attack.png")

    fig, ax = plt.subplots(figsize=(7, 4))
    for r in metrics["results"]:
        if "silhouette_by_k" in r:
            ks = sorted(r["silhouette_by_k"], key=int)
            ax.plot([int(k) for k in ks], [r["silhouette_by_k"][k] for k in ks], marker="o", label=method_name(r))
    ax.set_xlabel("k")
    ax.set_ylabel("silhouette")
    ax.set_title("Chon so cum k theo silhouette (tap train)")
    ax.legend()
    save(fig, out, "model_silhouette_by_k.png")
    return df


def load_scores(path: Path, sample: int, seed: int = 42) -> pd.DataFrame:
    cols = ["n_req", "score", "is_anomaly", "anomaly_label", "cluster", "threshold", *CORE_FEATURES]
    table = ds.dataset(str(path), format="parquet", partitioning="hive").to_table(columns=sorted(set(cols)))
    df = table.to_pandas()
    df = df[df["n_req"] >= 3]
    # Giu het cua so bat thuong (it) + lay mau cua so binh thuong cho de ve.
    normal = df[~df["is_anomaly"]]
    normal = normal.sample(min(sample, len(normal)), random_state=seed)
    return pd.concat([normal, df[df["is_anomaly"]]])


def score_plots(scores: pd.DataFrame, out: Path) -> None:
    thr = scores["threshold"].iloc[0]

    precision, recall, _ = precision_recall_curve(scores["is_anomaly"], scores["score"])
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(recall, precision)
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Duong Precision-Recall - KMeans (core)\n(mau: cua so binh thuong duoc lay mau)")
    save(fig, out, "model_pr_curve.png")

    fig, ax = plt.subplots(figsize=(8, 4))
    bins = np.linspace(0, scores["score"].quantile(0.999), 80)
    ax.hist(scores.loc[~scores["is_anomaly"], "score"], bins=bins, alpha=0.6, label="binh thuong", log=True)
    for label in LABELS:
        part = scores.loc[scores["anomaly_label"] == label, "score"]
        if len(part):
            ax.hist(part, bins=bins, alpha=0.7, label=label, log=True)
    ax.axvline(thr, color="black", linestyle="--", label=f"nguong = {thr:.2f}")
    ax.set_xlabel("Diem bat thuong (khoang cach toi tam cum)")
    ax.set_ylabel("So cua so (log)")
    ax.set_title("Phan bo diem bat thuong theo nhan that")
    ax.legend()
    save(fig, out, "model_score_distribution.png")

    x = scores[CORE_FEATURES].astype(float).copy()
    for c in LOG_FEATURES:
        x[c] = np.log1p(x[c])
    xy = PCA(n_components=2, random_state=42).fit_transform(StandardScaler().fit_transform(x))
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    sc = axes[0].scatter(xy[:, 0], xy[:, 1], c=scores["cluster"], cmap="tab10", s=4, alpha=0.6)
    axes[0].set_title("PCA 2D - to mau theo cum KMeans")
    axes[0].legend(*sc.legend_elements(), title="cum", fontsize=7, loc="best")
    normal_mask = ~scores["is_anomaly"].values
    axes[1].scatter(xy[normal_mask, 0], xy[normal_mask, 1], c="lightgray", s=4, alpha=0.5, label="binh thuong")
    for label in LABELS:
        m = (scores["anomaly_label"] == label).values
        if m.any():
            axes[1].scatter(xy[m, 0], xy[m, 1], s=12, label=label)
    axes[1].set_title("PCA 2D - to mau theo nhan that")
    axes[1].legend(fontsize=8)
    save(fig, out, "model_pca_clusters.png")


def nasa_plot(metrics: dict, out: Path) -> None:
    top = pd.DataFrame(metrics["top_alerts"])
    if top.empty:
        return
    top["label"] = top["ip"] + "  " + pd.to_datetime(top["window_start"]).dt.strftime("%d/%m %H:%M")
    top = top.sort_values("score")
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(top["label"], top["score"], color="#C44E52")
    for i, (s, reason) in enumerate(zip(top["score"], top["reason"])):
        ax.text(s, i, f" {reason}", va="center", fontsize=8)
    ax.set_xlabel("Diem bat thuong")
    ax.set_title("NASA 07/1995 - 20 cua so (host x 5 phut) bat thuong nhat va feature lech nhat")
    save(fig, out, "nasa_top_anomalies.png")


def to_markdown(df: pd.DataFrame) -> str:
    """Bang Markdown don gian (khong can cai tabulate)."""
    fmt = lambda v: f"{v:.3f}" if isinstance(v, float) else str(v)
    lines = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    lines += ["| " + " | ".join(fmt(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join(lines)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sample", type=int, default=60000, help="So cua so binh thuong lay mau de ve.")
    args = p.parse_args(argv)
    out = REPORTS / "figures"
    out.mkdir(parents=True, exist_ok=True)

    sim = json.loads((REPORTS / "metrics_sim.json").read_text(encoding="utf-8"))
    table = comparison(sim, out)
    score_plots(load_scores(GOLD / "scores_sim", args.sample), out)

    nasa_path = REPORTS / "metrics_nasa.json"
    if nasa_path.exists():
        nasa_plot(json.loads(nasa_path.read_text(encoding="utf-8")), out)

    print("\nBang so sanh (Markdown, dan vao bao cao):\n")
    print(to_markdown(table))


if __name__ == "__main__":
    main()
