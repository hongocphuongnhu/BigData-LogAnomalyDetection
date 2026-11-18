"""Doc canh bao moi nhat tu ES index log-alerts va dich sang tieng Viet.

Goi urllib truc tiep (timeout 3s), khong dung stack_api.request: ham do timeout
60s x 3 lan retry cho _search, ES treo thi moi lan poll giam mot thread vai phut.
"""
from __future__ import annotations

import json
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .explain import FEATURE_VI, GROUND_TRUTH_VI, explain
from .probes import TIMEOUT_S, _friendly, endpoints

SOURCES = ("stream", "sim", "nasa")
MAX_LIMIT = 100
TYPICAL_TTL_S = 300
# Stream it cua so luc moi bat -> trung vi chua on dinh, muon tam cua lo mo phong
# (cung simulator, cung dinh nghia feature).
MIN_WINDOWS_FOR_TYPICAL = 200

_typical_cache: dict[str, tuple[float, dict]] = {}
_typical_lock = threading.Lock()


def _search(index: str, body: dict) -> dict:
    request = Request(f"{endpoints()['es']}/{index}/_search", data=json.dumps(body).encode(),
                      headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=TIMEOUT_S) as response:
        return json.loads(response.read())


def _medians(source: str) -> tuple[int, dict]:
    """Trung vi tung feature tren cac cua so KHONG canh bao cua mot nguon (log-features)."""
    body = {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": [{"term": {"source": source}}, {"term": {"is_alert": False}}]}},
        "aggs": {name: {"percentiles": {"field": name, "percents": [50]}} for name in FEATURE_VI},
    }
    result = _search("log-features", body)
    medians = {name: agg["values"].get("50.0") for name, agg in result["aggregations"].items()}
    return result["hits"]["total"]["value"], {k: v for k, v in medians.items() if v is not None}


def typical_values(source: str) -> dict:
    """Muc 'binh thuong' de so sanh trong cau giai thich; loi thi tra {} (cau van hien, chi thieu so sanh)."""
    now = time.monotonic()
    with _typical_lock:
        cached = _typical_cache.get(source)
        if cached and now - cached[0] < TYPICAL_TTL_S:
            return cached[1]
    try:
        windows, medians = _medians(source)
        if source == "stream" and windows < MIN_WINDOWS_FOR_TYPICAL:
            medians = typical_values("sim")
    except Exception:
        return {}
    with _typical_lock:
        _typical_cache[source] = (now, medians)
    return medians


def fetch_alerts(source: str, limit: int = 50, label: str | None = None, offset: int = 0) -> dict:
    if source not in SOURCES:
        return {"ok": False, "error": "Nguồn không hợp lệ"}
    if label is not None and label not in GROUND_TRUTH_VI:
        return {"ok": False, "error": "Nhãn không hợp lệ"}
    limit = max(1, min(limit, MAX_LIMIT))
    if offset < 0 or offset + limit > 10000:
        return {"ok": False, "error": "Trang không hợp lệ"}
    # Stream: moi nhat len dau de thay canh bao vua sinh. Batch: detected_at la luc
    # export (giong nhau ca lo) nen xep theo diem, nang nhat len dau.
    sort = [{"detected_at": "desc"}, {"score": "desc"}] if source == "stream" else [{"score": "desc"}]
    sort.extend([{"ip": "asc"}, {"window_start": "asc"}])
    hit_filter = [{"term": {"source": source}}] + ([{"term": {"ground_truth": label}}] if label else [])
    body = {
        "size": limit,
        "from": offset,
        "track_total_hits": True,
        # post_filter: aggs dem tren ca index de hien so cho ca 3 tab trong mot lan goi.
        "post_filter": {"bool": {"filter": hit_filter}},
        "sort": sort,
        "aggs": {
            "by_source": {"terms": {"field": "source", "size": 10}},
            # Phan bo nhan mo phong trong nguon dang xem: cho thay model bat du 4 kieu
            # tan cong va bao nham bao nhieu (nhan "normal"), du top theo diem chi toan SQLi.
            "in_source": {"filter": {"term": {"source": source}},
                          "aggs": {"by_label": {"terms": {"field": "ground_truth", "size": 10}}}},
        },
    }
    try:
        result = _search("log-alerts", body)
    except HTTPError as error:
        if error.code == 404:
            return {"ok": True, "missing_index": True, "total": 0, "by_source": {}, "items": []}
        return {"ok": False, "error": _friendly(error)}
    except Exception as error:
        return {"ok": False, "error": _friendly(error)}
    hits = result["hits"]
    typical = typical_values(source) if hits["hits"] else {}
    aggs = result["aggregations"]
    by_source = {b["key"]: b["doc_count"] for b in aggs["by_source"]["buckets"]}
    by_label = [{"label": b["key"], "label_vi": GROUND_TRUTH_VI.get(b["key"], b["key"]), "count": b["doc_count"]}
                for b in aggs["in_source"]["by_label"]["buckets"]]
    return {
        "ok": True,
        "total": hits["total"]["value"],
        "by_source": by_source,
        "by_label": by_label,
        "typical": typical,
        "items": [explain({**hit["_source"], "doc_id": hit["_id"]}, typical) for hit in hits["hits"]],
    }
