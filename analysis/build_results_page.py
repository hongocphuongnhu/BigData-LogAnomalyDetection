"""Sinh trang ket qua nhanh batch (data/reports/index.html) tu cac file ket qua that.

Doc dq_nasa.json, metrics_sim.json, metrics_nasa.json, model_meta.json va hinh trong
data/reports/figures/ -> mot trang HTML tinh, mo offline duoc (dung luc demo phan batch).

    python analysis/build_results_page.py          # sinh trang
    python analysis/build_results_page.py --open   # sinh + mo bang trinh duyet mac dinh
"""
from __future__ import annotations

import argparse
import html
import json
import webbrowser
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS = REPO_ROOT / "data" / "reports"
LABELS = ["brute_force", "sqli", "scan", "ddos"]
METHOD_NAMES = {
    "rule": "Rule (ngưỡng viết tay)",
    "kmeans": "KMeans + CBLOF",
    "kmeans_own": "KMeans (tâm cụm được gán)",
    "bisecting": "BisectingKMeans + CBLOF",
}


def esc(v) -> str:
    return html.escape(str(v))


def num(v, digits: int = 3) -> str:
    if v is None or v == "-":
        return "–"
    if isinstance(v, float):
        return f"{v:.{digits}f}".replace(".", ",")
    if isinstance(v, int):
        return f"{v:,}".replace(",", ".")
    return esc(v)


def table(headers: list[str], rows: list[list[str]], highlight: int | None = None) -> str:
    head = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = "".join(
        f"<tr{' class=hl' if i == highlight else ''}>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>"
        for i, r in enumerate(rows)
    )
    return f'<div class="tbl"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def figure(name: str, caption: str) -> str:
    path = REPORTS / "figures" / name
    if not path.exists():
        return ""
    return f'<figure><img src="figures/{esc(name)}" alt="{esc(caption)}"><figcaption>{esc(caption)}</figcaption></figure>'


def tile(label: str, value: str, note: str = "") -> str:
    return f'<div class="tile"><div class="tl">{esc(label)}</div><div class="tv">{value}</div><div class="tn">{esc(note)}</div></div>'


def build() -> str:
    dq = json.loads((REPORTS / "dq_nasa.json").read_text(encoding="utf-8"))
    sim = json.loads((REPORTS / "metrics_sim.json").read_text(encoding="utf-8"))
    nasa_path = REPORTS / "metrics_nasa.json"
    nasa = json.loads(nasa_path.read_text(encoding="utf-8")) if nasa_path.exists() else None
    meta = json.loads((REPORTS / "model_meta.json").read_text(encoding="utf-8"))

    main = next(r for r in sim["results"] if r["method"] == "kmeans" and r["features"] == "core")
    rule = next(r for r in sim["results"] if r["method"] == "rule")

    tiles = "".join([
        tile("Log NASA thật đã làm sạch", num(dq["silver_rows"]), f"loại {sum(dq['rejected'].values())} dòng lỗi"),
        tile("Model streaming", "KMeans + CBLOF", f"k = {meta['k']}, ngưỡng p99 = {meta['threshold']:.2f}"),
        tile("F1 (tập test sim)", num(main["f1"]), f"Rule: {num(rule['f1'])}"),
        tile("Recall DDoS", num(main["recall_by_label"]["ddos"], 2), f"Rule: {num(rule['recall_by_label']['ddos'], 2)}"),
    ])

    # Bang chat luong du lieu NASA
    dq_rows = [
        ["Tổng số dòng đọc", num(dq["total_lines"])],
        ["Vào silver", num(dq["silver_rows"])],
        ["Loại: dòng bị cắt cụt (malformed_line)", num(dq["rejected"].get("malformed_line", 0))],
        ["Loại: request rác (malformed_request)", num(dq["rejected"].get("malformed_request", 0))],
        ["Dòng trùng tuyệt đối (giữ lại)", num(dq["exact_duplicate_lines_kept"])],
        ["Thiếu bytes (“-”) → 0", num(dq["bytes_missing"])],
        ["Request HTTP/0.9 (không có protocol)", num(dq["http09_no_protocol"])],
        ["Request không nhất quán (path có dấu cách / rác)", num(dq["request_inconsistent"])],
        ["Số host khác nhau (xấp xỉ)", num(dq["distinct_hosts_approx"])],
        ["Khoảng thời gian (UTC)", f"{esc(dq['first_event_utc'])} → {esc(dq['last_event_utc'])}"],
    ]

    # Bang so sanh mo hinh
    cmp_rows, hl = [], None
    for i, r in enumerate(sim["results"]):
        name = METHOD_NAMES.get(r["method"], r["method"])
        if r["method"] != "rule":
            name += f" · {r['features']}"
        if r is main:
            hl = i
            name += " <span class=tag>model demo</span>"
        cmp_rows.append([
            name if r is main else esc(name), num(r.get("k", "-")), num(r["precision"]), num(r["recall"]),
            f"<b>{num(r['f1'])}</b>", num(r.get("pr_auc")), num(r["fpr"], 4),
            *[num(r["recall_by_label"].get(l), 2) for l in LABELS],
        ])

    # Cum hanh vi
    def cluster_rows(m, labeled: bool):
        out = []
        for c in m["cluster_profile_kmeans_core"]:
            row = [num(c["cluster"]), num(c["windows"]), num(c["n_req"], 1), num(c["burst_rate"], 2),
                   num(c["n_distinct_paths"], 1), num(c["ratio_4xx"], 2), num(c["ratio_404"], 2), num(c["ratio_static"], 2)]
            if labeled:
                row.append(num(c.get("anomaly_rate"), 2))
            out.append(row)
        return out

    cl_head = ["Cụm", "Số cửa sổ", "TB request", "burst_rate", "Số path", "Tỉ lệ 4xx", "Tỉ lệ 404", "Tỉ lệ file tĩnh"]

    nasa_html = ""
    if nasa:
        top_rows = [
            [esc(a["ip"]), esc(a["window_start"][:16]), num(a["n_req"], 0), num(a["score"], 1),
             f'<span class="sev {esc(a["severity"])}">{esc(a["severity"])}</span>', esc(a["reason"]),
             esc(a["rule_reason"] or "–")]
            for a in nasa["top_alerts"]
        ]
        nasa_html = f"""
<section id="nasa">
  <h2>4. Áp dụng lên NASA thật</h2>
  <p class="lead">Không có nhãn, nên đánh giá định tính. KMeans chọn k = {nasa['results'][0]['k']};
  có {num(nasa['alerts'])} cửa sổ vượt ngưỡng. Cảnh báo số 1 (host <code>163.205.1.45</code>, 03/07) trùng
  với đỉnh lỗi 5xx trên biểu đồ EDA.</p>
  <h3>Các cụm hành vi</h3>
  {table(cl_head, cluster_rows(nasa, False))}
  <h3>20 cửa sổ bất thường nhất</h3>
  {table(["Host", "Cửa sổ (UTC)", "Request", "Điểm", "Mức", "Lý do (feature lệch nhất)", "Rule"], top_rows)}
  {figure("nasa_top_anomalies.png", "Top 20 cửa sổ bất thường NASA và feature lệch nhất")}
</section>"""

    return f"""<!doctype html>
<html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kết quả Batch</title>
<style>
:root {{ --surface:#fcfcfb; --page:#f9f9f7; --ink:#0b0b0b; --ink2:#52514e; --muted:#898781;
  --grid:#e1e0d9; --accent:#2a78d6; --accent-wash:#e8f1fc; --border:rgba(11,11,11,.10); }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--page); color:var(--ink); font:15px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif; }}
header {{ background:var(--surface); border-bottom:1px solid var(--border); padding:28px 16px 20px; }}
.wrap {{ max-width:1180px; margin:0 auto; padding:0 16px; }}
h1 {{ font-size:26px; margin:0 0 4px; }} h2 {{ font-size:20px; margin:40px 0 8px; }} h3 {{ font-size:16px; margin:24px 0 8px; color:var(--ink2); }}
.sub {{ color:var(--ink2); margin:0; }}
nav {{ display:flex; gap:16px; flex-wrap:wrap; margin-top:14px; font-size:14px; }}
nav a {{ color:var(--accent); text-decoration:none; }}
.tiles {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; margin:24px 0 8px; }}
.tile {{ background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:14px 16px; }}
.tl {{ color:var(--ink2); font-size:13px; }} .tv {{ font-size:26px; font-weight:600; margin:2px 0; }} .tn {{ color:var(--muted); font-size:13px; }}
.lead {{ color:var(--ink2); max-width:860px; }}
.tbl {{ overflow-x:auto; background:var(--surface); border:1px solid var(--border); border-radius:10px; }}
table {{ border-collapse:collapse; width:100%; font-size:14px; }}
th, td {{ padding:8px 12px; border-bottom:1px solid var(--grid); text-align:left; white-space:nowrap; }}
th {{ color:var(--ink2); font-weight:600; background:var(--page); }}
td {{ font-variant-numeric:tabular-nums; }} tr:last-child td {{ border-bottom:0; }}
tr.hl td {{ background:var(--accent-wash); }}
.tag {{ display:inline-block; font-size:11px; padding:1px 6px; border-radius:999px; background:var(--accent); color:#fff; margin-left:6px; vertical-align:1px; }}
.sev {{ font-size:12px; padding:1px 8px; border-radius:999px; border:1px solid var(--border); }}
.sev.high {{ background:#fbe4e4; }} .sev.medium {{ background:#fdf0dc; }} .sev.low {{ background:#eef2f6; }}
.grid2 {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(460px,1fr)); gap:16px; }}
figure {{ margin:16px 0 0; background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:12px; }}
figure img {{ width:100%; height:auto; display:block; }}
.narrow {{ max-width:760px; }}
figcaption {{ color:var(--muted); font-size:13px; margin-top:6px; }}
.note {{ background:var(--surface); border-left:3px solid var(--accent); padding:10px 14px; color:var(--ink2); border-radius:0 8px 8px 0; }}
code {{ background:var(--page); border:1px solid var(--grid); border-radius:4px; padding:0 4px; font-size:13px; }}
footer {{ color:var(--muted); font-size:13px; padding:40px 0 32px; }}
@media (max-width:520px) {{ .grid2 {{ grid-template-columns:1fr; }} h1 {{ font-size:22px; }} }}
</style></head>
<body>
<header><div class="wrap">
  <h1>Phát hiện bất thường trong log HTTP — kết quả Batch</h1>
  <p class="sub">Spark (cluster standalone) · NASA HTTP 07/1995 + simulator có nhãn · KMeans + CBLOF · phần của Định</p>
  <nav><a href="#data">1. Dữ liệu</a><a href="#eda">2. EDA</a><a href="#model">3. Mô hình</a><a href="#nasa">4. NASA</a></nav>
</div></header>
<main class="wrap">
  <div class="tiles">{tiles}</div>

<section id="data">
  <h2>1. Dữ liệu & làm sạch (NASA thật)</h2>
  <p class="lead">File gốc Apache Common Log Format (gzip), parse bằng Spark trên cluster, ghi Parquet phân vùng
  <code>dt/hour</code>. Simulator sinh thêm {num(1065283)} dòng có nhãn (4 loại tấn công) để đánh giá định lượng.</p>
  {table(["Chỉ số", "Giá trị"], dq_rows)}
</section>

<section id="eda">
  <h2>2. Khám phá dữ liệu</h2>
  {figure("nasa_requests_per_hour.png", "Request mỗi giờ theo nhóm status — đỉnh 13/07 ngày phóng tàu STS-70; đỉnh 5xx 03/07")}
  <div class="grid2">
    {figure("nasa_status_distribution.png", "Phân bố status code")}
    {figure("nasa_requests_per_host.png", "Số request mỗi host — đuôi rất dài")}
    {figure("nasa_top_hosts.png", "Top 20 host")}
    {figure("nasa_top_404_paths.png", "Top 15 path trả về 404")}
  </div>
  <div class="narrow">{figure("nasa_bytes_histogram.png", "Phân bố kích thước response")}</div>
</section>

<section id="model">
  <h2>3. Mô hình: KMeans + CBLOF</h2>
  <p class="lead">Feature theo cửa sổ <b>IP × 5 phút</b>. KMeans gom các cửa sổ thành nhóm hành vi. <b>CBLOF</b> chấm
  điểm: cửa sổ thuộc cụm nhỏ được đo khoảng cách tới <b>tâm cụm lớn (bình thường) gần nhất</b>. Ngưỡng = phân vị 99
  điểm trên tập train, không dùng nhãn. Đánh giá trên 30% thời gian cuối (tập test).</p>
  <div class="note">Vì sao CBLOF: tấn công tự gom thành một cụm nhỏ riêng, nên “khoảng cách tới tâm cụm được gán”
  chấm điểm chúng thấp (F1 {num(next(r for r in sim['results'] if r['method'] == 'kmeans_own')['f1'])}, bỏ sót DDoS).
  CBLOF đo tới cụm bình thường, nên bắt được (F1 {num(main['f1'])}).</div>
  <h3>Bảng so sánh (tập test simulator)</h3>
  {table(["Phương pháp", "k", "Precision", "Recall", "F1", "PR-AUC", "FPR", *[f"Recall {l}" for l in LABELS]], cmp_rows, hl)}
  <div class="grid2">
    {figure("model_comparison.png", "So sánh phương pháp")}
    {figure("model_recall_by_attack.png", "Recall theo loại tấn công")}
    {figure("model_score_distribution.png", "Điểm bất thường theo nhãn thật và ngưỡng")}
    {figure("model_pr_curve.png", "Đường Precision–Recall")}
    {figure("model_pca_clusters.png", "Các cụm KMeans (PCA 2D)")}
    {figure("model_silhouette_by_k.png", "Chọn k theo silhouette")}
  </div>
  {figure("model_pca_attacks.png", "Vị trí từng loại tấn công so với traffic bình thường")}
  <h3>Các cụm hành vi trên simulator</h3>
  {table(cl_head + ["Tỉ lệ tấn công (nhãn thật)"], cluster_rows(sim, True))}
</section>
{nasa_html}
<footer>Sinh tự động bởi <code>analysis/build_results_page.py</code> từ <code>data/reports/*.json</code>.
Model: <code>{esc(meta['model_dir'])}</code> · huấn luyện lúc {esc(meta['trained_at'])}.</footer>
</main></body></html>
"""


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--open", action="store_true", help="Mo trang bang trinh duyet mac dinh.")
    args = p.parse_args(argv)
    out = REPORTS / "index.html"
    out.write_text(build(), encoding="utf-8")
    print(f"Da sinh {out}")
    if args.open:
        webbrowser.open(out.as_uri())


if __name__ == "__main__":
    main()
