"""Ve hinh ket qua mo hinh cho bao cao (muc Ket qua & so sanh).

Chay tren HOST sau khi da chay jobs/detect_anomalies.py --source sim (va nasa):

    python analysis/model_report.py

Doc data/reports/metrics_{sim,nasa}.json + gold/scores_sim (Parquet), ghi
data/reports/figures/model_*.png va in bang so sanh dang Markdown de dan vao
bao cao (bang = "table view" di kem moi hinh). Style chung: analysis/vizstyle.py.
"""
from __future__ import annotations

import argparse
import sys
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from sklearn.decomposition import PCA
from sklearn.metrics import precision_recall_curve
from sklearn.preprocessing import StandardScaler

import vizstyle as vs

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS = REPO_ROOT / "data" / "reports"
GOLD = REPO_ROOT / "data" / "gold"
LABELS = ["brute_force", "sqli", "scan", "ddos"]
CORE_FEATURES = [
    "n_req", "burst_rate", "n_distinct_paths", "ratio_4xx", "ratio_5xx", "ratio_404",
    "ratio_post", "ratio_sus_path", "ratio_static", "mean_bytes", "max_bytes",
]
LOG_FEATURES = {"n_req", "burst_rate", "n_distinct_paths", "mean_bytes", "max_bytes"}
METRICS = [("precision", "Precision"), ("recall", "Recall"), ("f1", "F1"), ("pr_auc", "PR-AUC")]


def method_name(r: dict) -> str:
    names = {"rule": "Rule (ngưỡng)", "kmeans": "KMeans + CBLOF", "kmeans_own": "KMeans (tâm cụm riêng)",
             "bisecting": "BisectingKMeans + CBLOF"}
    base = names.get(r["method"], r["method"])
    return base if r["method"] == "rule" else f"{base} · {r['features']}"


def comparison(metrics: dict, out: Path) -> pd.DataFrame:
    rows = [
        {
            "method": method_name(r), "k": r.get("k", "-"),
            **{m: r.get(m, np.nan) for m, _ in METRICS},
            "fpr": r["fpr"], **{f"recall_{l}": r["recall_by_label"].get(l, np.nan) for l in LABELS},
        }
        for r in metrics["results"]
    ]
    df = pd.DataFrame(rows)

    # 1. Bang so sanh dang grouped bar ngang: 4 chi so = 4 series (slot 1-4, legend).
    fig, ax = plt.subplots(figsize=(10, 6))
    n = len(df)
    h = 0.18
    y = np.arange(n)[::-1]
    for i, (col, label) in enumerate(METRICS):
        vals = df[col].fillna(0).values
        ax.barh(y + (1.5 - i) * h, vals, height=h, label=label, **vs.bar_kwargs(vs.SERIES[i]))
        for yi, v, raw in zip(y + (1.5 - i) * h, vals, df[col].values):
            ax.text(v + 0.01, yi, "–" if np.isnan(raw) else f"{v:.2f}", va="center", fontsize=7, color=vs.INK_2)
    ax.set_yticks(y, df["method"])
    ax.set_xlim(0, 1.1)
    ax.grid(axis="y", visible=False)
    ax.legend(ncol=4, loc="upper left", bbox_to_anchor=(0, -0.06))
    ax.set_title("So sánh phương pháp trên tập test", pad=18)
    vs.subtitle(ax, "Cấp cửa sổ (IP × 5 phút); ngưỡng = phân vị 99 điểm trên tập train, không dùng nhãn")
    vs.save(fig, out, "model_comparison.png")

    # 2. Recall theo loai tan cong: heatmap mot hue (xanh), chu trang/den theo do sang o.
    mat = df.set_index("method")[[f"recall_{l}" for l in LABELS]]
    fig, ax = plt.subplots(figsize=(8.5, 4.5))
    im = ax.imshow(mat.values, cmap=vs.SEQ_BLUE, vmin=0, vmax=1, aspect="auto")
    ax.set_xticks(range(len(LABELS)), LABELS)
    ax.set_yticks(range(len(mat)), mat.index)
    ax.grid(False)
    for s in ax.spines.values():
        s.set_visible(False)
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat.values[i, j]
            ax.text(j, i, "–" if np.isnan(v) else f"{v:.2f}", ha="center", va="center", fontsize=9,
                    color="white" if (not np.isnan(v) and v > 0.55) else vs.INK)
    cb = fig.colorbar(im, ax=ax, fraction=0.04)
    cb.outline.set_visible(False)
    cb.set_label("Recall", color=vs.INK_2)
    ax.set_title("Recall theo từng loại tấn công", pad=18)
    vs.subtitle(ax, "Rule bỏ sót DDoS (botnet chia nhỏ request ra nhiều IP); CBLOF bắt được")
    vs.save(fig, out, "model_recall_by_attack.png")

    # 3. Silhouette theo k (bo bien the *_own: cung model, chi khac cach cham diem).
    fig, ax = plt.subplots(figsize=(8, 4.5))
    series = [r for r in metrics["results"] if "silhouette_by_k" in r and not r["method"].endswith("_own")]
    for color, r in zip(vs.SERIES, series):
        ks = sorted(r["silhouette_by_k"], key=int)
        xs, ys = [int(k) for k in ks], [r["silhouette_by_k"][k] for k in ks]
        ax.plot(xs, ys, color=color, label=method_name(r))
        chosen = r["k"]
        ax.plot([chosen], [r["silhouette_by_k"][str(chosen)]], "o", color=color, markersize=8,
                markeredgecolor=vs.SURFACE, markeredgewidth=2)
    ax.set_xlabel("Số cụm k")
    ax.set_ylabel("Silhouette")
    ax.legend(loc="upper right")
    ax.set_title("Chọn số cụm k theo silhouette (tập train)", pad=18)
    vs.subtitle(ax, "Chấm tròn = k được chọn (silhouette cao nhất)")
    vs.save(fig, out, "model_silhouette_by_k.png")
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

    # 4. Duong Precision-Recall (mot series)
    precision, recall, thresholds = precision_recall_curve(scores["is_anomaly"], scores["score"])
    idx = np.searchsorted(thresholds, thr)
    fig, ax = plt.subplots(figsize=(6.5, 5))
    ax.plot(recall, precision, color=vs.SERIES[0])
    if idx < len(thresholds):
        ax.plot([recall[idx]], [precision[idx]], "o", color=vs.SERIES[0], markersize=8,
                markeredgecolor=vs.SURFACE, markeredgewidth=2)
        ax.annotate(f"ngưỡng p99 = {thr:.2f}\nP={precision[idx]:.2f}, R={recall[idx]:.2f}",
                    xy=(recall[idx], precision[idx]), xytext=(-110, -40), textcoords="offset points",
                    fontsize=8, color=vs.INK_2,
                    arrowprops={"arrowstyle": "-", "color": vs.MUTED, "linewidth": 0.8})
    ax.set_xlim(0, 1.02)
    ax.set_ylim(0, 1.02)
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Đường Precision–Recall · KMeans + CBLOF (core)", pad=18)
    vs.subtitle(ax, "Cửa sổ bình thường được lấy mẫu nên precision tuyệt đối mang tính minh hoạ")
    vs.save(fig, out, "model_pr_curve.png")

    # 5. Diem bat thuong theo nhan that: strip plot ngang, mot truc (log), duong nguong.
    groups = [("bình thường", ~scores["is_anomaly"], vs.NEUTRAL)] + [
        (l, scores["anomaly_label"] == l, vs.ATTACK_COLORS[l]) for l in LABELS
    ]
    rng = np.random.default_rng(0)
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for i, (name, mask, color) in enumerate(groups):
        vals = scores.loc[mask, "score"].clip(lower=1e-2).values
        if len(vals) > 4000:
            vals = rng.choice(vals, 4000, replace=False)
        ax.scatter(vals, i + rng.uniform(-0.25, 0.25, len(vals)), s=6, color=color, alpha=0.5, linewidths=0)
        ax.text(1.0, i, f" n={int(mask.sum()):,}", transform=ax.get_yaxis_transform(), va="center",
                fontsize=8, color=vs.INK_2)
    ax.axvline(thr, color=vs.INK_2, linewidth=1)
    ax.text(thr, -0.55, f" ngưỡng {thr:.2f}", fontsize=8, color=vs.INK_2, va="center")
    ax.set_xscale("log")
    ticks = [t for t in (0.1, 0.3, 1, 3, 10, 30, 100) if scores["score"].min() <= t * 3 and t <= scores["score"].max() * 1.5]
    ax.set_xticks(ticks, [f"{t:g}" for t in ticks])
    ax.set_ylim(-0.8, len(groups) - 0.5)
    ax.set_yticks(range(len(groups)), [g[0] for g in groups])
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Điểm bất thường (khoảng cách tới tâm cụm lớn gần nhất, thang log)")
    ax.set_title("Phân bố điểm bất thường theo nhãn thật", pad=18)
    vs.subtitle(ax, "Bên phải ngưỡng = bị cảnh báo; cửa sổ bình thường được lấy mẫu")
    vs.save(fig, out, "model_score_distribution.png")

    # 6. PCA 2D. Trai: to theo cum (k nho -> <= 3 mau). Phai: small multiples moi loai
    #    tan cong mot o, noi bat tren nen xam (scatter khong dung qua 3 mau mot luc).
    x = scores[CORE_FEATURES].astype(float).copy()
    for c in LOG_FEATURES:
        x[c] = np.log1p(x[c])
    xy = PCA(n_components=2, random_state=42).fit_transform(StandardScaler().fit_transform(x))

    fig, ax = plt.subplots(figsize=(7, 5.5))
    for c in sorted(scores["cluster"].unique()):
        m = (scores["cluster"] == c).values
        ax.scatter(xy[m, 0], xy[m, 1], s=5, alpha=0.5, linewidths=0, color=vs.SERIES[int(c) % 3],
                   label=f"cụm {c} ({m.sum():,} cửa sổ)")
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    ax.legend(markerscale=3, loc="best")
    ax.set_title("Các cụm hành vi KMeans (chiếu PCA 2D)", pad=18)
    vs.subtitle(ax, "Feature core sau log1p + chuẩn hoá")
    vs.save(fig, out, "model_pca_clusters.png")

    normal = ~scores["is_anomaly"].values
    fig, axes = plt.subplots(2, 2, figsize=(11, 8), sharex=True, sharey=True)
    for ax, label in zip(axes.flat, LABELS):
        m = (scores["anomaly_label"] == label).values
        ax.scatter(xy[normal, 0], xy[normal, 1], s=3, color=vs.NEUTRAL, alpha=0.35, linewidths=0)
        ax.scatter(xy[m, 0], xy[m, 1], s=12, color=vs.ATTACK_COLORS[label], linewidths=0)
        ax.set_title(f"{label} ({m.sum():,} cửa sổ)", fontsize=10)
    fig.suptitle("Vị trí từng loại tấn công so với traffic bình thường (xám) — PCA 2D",
                 x=0.01, ha="left", fontsize=12, fontweight="semibold", color=vs.INK)
    vs.save(fig, out, "model_pca_attacks.png")


def nasa_plot(metrics: dict, out: Path) -> None:
    top = pd.DataFrame(metrics["top_alerts"])
    if top.empty:
        return
    top["label"] = top["ip"] + "  ·  " + pd.to_datetime(top["window_start"]).dt.strftime("%d/%m %H:%M")
    top = top.sort_values("score")
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(top["label"], top["score"], height=0.6, **vs.bar_kwargs(vs.SERIES[0]))
    for i, (s, reason) in enumerate(zip(top["score"], top["reason"])):
        ax.text(s, i, f"  {reason}", va="center", fontsize=8, color=vs.INK_2)
    ax.set_xlim(0, top["score"].max() * 1.25)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Điểm bất thường")
    ax.set_title("NASA 07/1995 — 20 cửa sổ (host × 5 phút) bất thường nhất", pad=18)
    vs.subtitle(ax, "Nhãn bên phải = feature lệch khỏi tâm cụm nhiều nhất (lý do cảnh báo)")
    vs.save(fig, out, "nasa_top_anomalies.png")


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
    vs.apply()

    sim = json.loads((REPORTS / "metrics_sim.json").read_text(encoding="utf-8"))
    table = comparison(sim, out)
    score_plots(load_scores(GOLD / "scores_sim", args.sample), out)

    nasa_path = REPORTS / "metrics_nasa.json"
    if nasa_path.exists():
        nasa_plot(json.loads(nasa_path.read_text(encoding="utf-8")), out)

    md = to_markdown(table)
    md_path = REPORTS / "model_comparison.md"
    md_path.write_text(md + "\n", encoding="utf-8")
    # Console Windows mac dinh cp1252, khong in duoc tieng Viet co dau.
    sys.stdout.reconfigure(encoding="utf-8")
    print(f"\nBang so sanh (Markdown, dan vao bao cao) -> {md_path}:\n")
    print(md)


if __name__ == "__main__":
    main()
