"""Dich mot document trong log-alerts thanh cau tieng Viet cho nguoi xem demo.

Model KMeans + CBLOF khong giam sat: no chi biet cua so (IP x 5 phut) lech xa
hanh vi binh thuong, KHONG biet do la loai tan cong nao. Vi vay cau giai thich
dua tren top_feature (feature lech nhieu nhat so voi tam cum tham chieu), con
nhan cua simulator (ground_truth) duoc hien rieng va ghi ro model khong dung no.

Feature co the lech theo CA HAI chieu (vi du DDoS pha cache tra loi 502 rat nho
nen max_bytes THAP bat thuong), nen cau luon so voi muc "binh thuong" (trung vi
cua cac cua so khong canh bao, xem alerts.typical_values) thay vi doan chieu.

Module thuan Python, khong phu thuoc server -> test rieng duoc.
"""
from __future__ import annotations


def _int_vi(value: float) -> str:
    return f"{value:,.0f}".replace(",", ".")


def _dec_vi(value: float, digits: int = 1) -> str:
    return f"{value:,.{digits}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def _pct_vi(value: float) -> str:
    return f"{value * 100:.0f}%"


def _bytes_vi(value: float) -> str:
    for unit in ("B", "KB", "MB"):
        if value < 1024:
            return f"{_int_vi(value)} B" if unit == "B" else f"{_dec_vi(value)} {unit}"
        value /= 1024
    return f"{_dec_vi(value)} GB"


FORMATS = {"count": _int_vi, "rate": lambda v: f"{_dec_vi(v)}/giây", "pct": _pct_vi, "bytes": _bytes_vi}

# feature -> (cach goi de hieu, kieu dinh dang). Thu tu = CORE_FEATURES.
FEATURE_VI: dict[str, tuple[str, str]] = {
    "n_req": ("Số request trong 5 phút", "count"),
    "burst_rate": ("Tốc độ gửi request", "rate"),
    "n_distinct_paths": ("Số đường dẫn khác nhau", "count"),
    "ratio_4xx": ("Tỷ lệ request bị từ chối (lỗi 4xx)", "pct"),
    "ratio_5xx": ("Tỷ lệ request làm máy chủ báo lỗi (5xx)", "pct"),
    "ratio_404": ("Tỷ lệ request vào trang không tồn tại (404)", "pct"),
    "ratio_post": ("Tỷ lệ request POST (gửi form, đăng nhập)", "pct"),
    "ratio_sus_path": ("Tỷ lệ đường dẫn chứa mẫu tấn công", "pct"),
    "ratio_static": ("Tỷ lệ tải ảnh/CSS/JS kèm trang", "pct"),
    "mean_bytes": ("Dung lượng trả về trung bình", "bytes"),
    "max_bytes": ("Phản hồi lớn nhất", "bytes"),
}

# Nguong theo serving_common.add_alert_fields: diem / nguong >= 2 cao, >= 1,5 trung binh.
SEVERITY_VI = {"high": "Cao", "medium": "Trung bình", "low": "Thấp"}

GROUND_TRUTH_VI = {
    "brute_force": "Dò mật khẩu",
    "sqli": "Chèn SQL",
    "scan": "Dò quét lỗ hổng",
    "ddos": "DDoS phá cache",
    "normal": "Bình thường",
}

SOURCE_VI = {"stream": "Thời gian thực", "sim": "Lô mô phỏng", "nasa": "Log NASA thật (1995)"}

FALLBACK = "Hành vi lệch xa so với người đọc bình thường"


def describe(feature: str | None, doc: dict, typical: dict | None = None) -> str | None:
    """'<ten> toi/chi <gia tri> — binh thuong khoang <trung vi>'; None neu thieu du lieu."""
    entry = FEATURE_VI.get(feature or "")
    value = doc.get(feature or "")
    if not entry or not isinstance(value, (int, float)):
        return None
    label, kind = entry
    fmt = FORMATS[kind]
    normal = (typical or {}).get(feature)
    if not isinstance(normal, (int, float)) or fmt(value) == fmt(normal):
        return f"{label}: {fmt(value)}"
    return f"{label} {'tới' if value > normal else 'chỉ'} {fmt(value)}, bình thường khoảng {fmt(normal)}"


def explain(doc: dict, typical: dict | None = None) -> dict:
    """Tra ve cac chuoi hien thi cho mot canh bao; khong nem loi voi document thieu truong."""
    feature = doc.get("top_feature") or doc.get("reason")
    headline = describe(feature, doc, typical) or FALLBACK

    # Them vai so lieu boi canh (khac feature chinh) de nguoi xem thay buc tranh.
    context = []
    for extra in ("n_req", "ratio_404", "ratio_5xx", "ratio_sus_path", "ratio_post"):
        value = doc.get(extra)
        if extra == feature or len(context) == 2 or not isinstance(value, (int, float)):
            continue
        if extra == "n_req" or value >= 0.2:
            sentence = describe(extra, doc, typical)
            if sentence:
                context.append(sentence)

    score, threshold = doc.get("score"), doc.get("threshold")
    numeric = isinstance(score, (int, float)) and isinstance(threshold, (int, float)) and threshold > 0
    ratio = score / threshold if numeric else None
    severity = doc.get("severity")
    truth = doc.get("ground_truth")
    return {
        "ip": doc.get("ip"),
        "window_start": doc.get("window_start"),
        "detected_at": doc.get("detected_at"),
        "source": doc.get("source"),
        "source_vi": SOURCE_VI.get(doc.get("source"), doc.get("source") or "Không rõ"),
        "feature": feature,
        "feature_vi": FEATURE_VI[feature][0] if feature in FEATURE_VI else None,
        "headline": headline,
        "context": context,
        "severity": severity,
        "severity_vi": SEVERITY_VI.get(severity, "Không rõ"),
        "score": score,
        "threshold": threshold,
        "ratio": ratio,
        "score_vi": f"Điểm {_dec_vi(score)} / ngưỡng {_dec_vi(threshold, 2)}" if numeric else None,
        "ratio_vi": f"gấp {_dec_vi(ratio)} lần ngưỡng" if numeric else None,
        "ground_truth": truth,
        "ground_truth_vi": GROUND_TRUTH_VI.get(truth) if truth else None,
        "doc_id": doc.get("doc_id"),
    }


if __name__ == "__main__":
    import json
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    sample = {"ip": "1.153.174.119", "n_req": 14.0, "max_bytes": 483.0, "ratio_404": 0.0, "ratio_5xx": 0.07,
              "score": 14.6, "threshold": 7.4929, "severity": "medium", "top_feature": "max_bytes",
              "ground_truth": "ddos", "source": "sim"}
    print(json.dumps(explain(sample, {"n_req": 6.0, "max_bytes": 24700.0}), indent=2, ensure_ascii=False))
