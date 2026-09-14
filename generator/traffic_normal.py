"""Sinh traffic HTTP binh thuong (is_anomaly == False) cho Log Contract v1.

Domain: trang doc bai viet (article site) co tang cache-aside truoc DB -
moi lan xem bai viet se tra cuu cache theo article_id (HIT/MISS).
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Optional

from .log_contract import make_record, new_id

BROWSER_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]

KNOWN_USER_IDS = [f"u{i}" for i in range(1, 31)]

# Bai viet "hot" duoc doc nhieu (de cache am len nhanh) vs. long-tail hiem gap.
HOT_ARTICLE_IDS = list(range(1, 21))
LONG_TAIL_ARTICLE_IDS = list(range(21, 301))

SEARCH_TERMS = [
    "thoi su", "the thao", "cong nghe", "kinh te", "giai tri",
    "suc khoe", "giao duc", "du lich", "am thuc", "oto xe may",
]

# (event_type, weight)
EVENT_WEIGHTS = [
    ("list_articles", 20),
    ("view_article", 45),
    ("search", 15),
    ("login", 4),
    ("logout", 3),
    ("static_asset", 10),
    ("other", 3),
]

STATIC_PATHS = ["/static/style.css", "/static/app.js", "/favicon.ico"]
OTHER_PATHS = ["/about", "/contact"]


def _random_ip_pool(rng: random.Random, size: int) -> list[str]:
    """Tao pool IP 'nguoi dung binh thuong' tu vai khoi /16 co dinh."""
    blocks = [(203, 0), (198, 51), (192, 0), (172, 217)]
    ips = []
    for _ in range(size):
        b0, b1 = rng.choice(blocks)
        ips.append(f"{b0}.{b1}.{rng.randint(0, 255)}.{rng.randint(1, 254)}")
    return ips


def _pick_article_id(rng: random.Random) -> int:
    if rng.random() < 0.8:
        return rng.choice(HOT_ARTICLE_IDS)
    return rng.choice(LONG_TAIL_ARTICLE_IDS)


def _cache_lookup(event_type: str, key: Optional[str], cache_state: dict, rng: random.Random) -> str:
    """Mo phong Cache-Aside: tra ve 'HIT' hoac 'MISS' va cap nhat cache_state."""
    if event_type == "static_asset":
        return "HIT" if rng.random() < 0.95 else "MISS"

    if event_type in ("login", "logout"):
        return "MISS"  # request mutating, khong qua cache

    if event_type == "list_articles":
        primed = cache_state["home_primed"]
        cache_state["home_primed"] = True
        return "HIT" if primed and rng.random() < 0.85 else "MISS"

    if event_type == "view_article":
        seen = cache_state["articles"]
        was_seen = key in seen
        seen.add(key)
        if was_seen:
            return "MISS" if rng.random() < 0.10 else "HIT"  # 10% eviction/TTL expiry
        return "MISS"

    if event_type == "search":
        seen = cache_state["searches"]
        was_seen = key in seen
        seen.add(key)
        if was_seen:
            return "HIT" if rng.random() < 0.30 else "MISS"
        return "MISS"

    return "MISS"  # "other"


def _status_code_for(event_type: str, rng: random.Random) -> int:
    if event_type == "view_article" and rng.random() < 0.03:
        return 404
    return rng.choices([200, 301, 404, 500], weights=[90, 3, 5, 2], k=1)[0]


def _latency_ms_for(event_type: str, cache_status: str, rng: random.Random) -> int:
    if event_type == "static_asset":
        return rng.randint(5, 30)
    base = rng.randint(20, 300)
    return base if cache_status == "HIT" else base + rng.randint(50, 200)


def _bytes_out_for(event_type: str, rng: random.Random) -> int:
    if event_type == "static_asset":
        return rng.randint(200, 5000)
    return rng.randint(500, 20000)


def generate_normal_traffic(
    base_dt: datetime,
    duration_s: float,
    rate_per_s: float,
    rng: random.Random,
) -> list[dict]:
    """Sinh traffic binh thuong bang mo hinh 'visitor session'.

    Moi session la mot IP gui 1-8 request lien tiep, cach nhau vai giay,
    voi session_id chung. Mot cache_state duoc chia se xuyen suot timeline
    de mo phong cache 'am' dan theo thoi gian (Cache-Aside).
    """
    target_total = max(0, int(duration_s * rate_per_s))
    if target_total == 0:
        return []

    ip_pool = _random_ip_pool(rng, size=max(10, target_total // 5))
    cache_state = {"home_primed": False, "articles": set(), "searches": set()}

    # Sinh moc thoi gian truoc, sort, de cache "am" dan dung theo thu tu thoi gian thuc.
    session_starts = sorted(rng.uniform(0, duration_s) for _ in range(target_total // 3 + 1))

    records: list[dict] = []
    for session_start_s in session_starts:
        if len(records) >= target_total:
            break

        ip = rng.choice(ip_pool)
        ua = rng.choice(BROWSER_USER_AGENTS)
        session_id = new_id()
        user_id = rng.choice(KNOWN_USER_IDS) if rng.random() < 0.4 else None
        session_len = rng.randint(1, 8)

        t = base_dt + timedelta(seconds=session_start_s)
        for _ in range(session_len):
            if len(records) >= target_total:
                break

            event_type = rng.choices(
                [e for e, _ in EVENT_WEIGHTS],
                weights=[w for _, w in EVENT_WEIGHTS],
                k=1,
            )[0]

            article_id = None
            cache_key = None
            if event_type == "view_article":
                article_id = _pick_article_id(rng)
                path = f"/article/{article_id}"
                method = "GET"
                cache_key = article_id
            elif event_type == "list_articles":
                path = "/" if rng.random() < 0.5 else "/articles"
                method = "GET"
            elif event_type == "search":
                term = rng.choice(SEARCH_TERMS)
                path = f"/search?q={term}"
                method = "GET"
                cache_key = term
            elif event_type == "login":
                path = "/login"
                method = "POST"
            elif event_type == "logout":
                path = "/logout"
                method = "POST"
            elif event_type == "static_asset":
                path = rng.choice(STATIC_PATHS)
                method = "GET"
            else:  # other
                path = rng.choice(OTHER_PATHS)
                method = "GET"

            cache_status = _cache_lookup(event_type, cache_key, cache_state, rng)

            records.append(
                make_record(
                    ts=t,
                    session_id=session_id,
                    event_type=event_type,
                    method=method,
                    path=path,
                    article_id=article_id,
                    status_code=_status_code_for(event_type, rng),
                    latency_ms=_latency_ms_for(event_type, cache_status, rng),
                    cache_status=cache_status,
                    bytes_out=_bytes_out_for(event_type, rng),
                    ip=ip,
                    user_agent=ua,
                    is_anomaly=False,
                    user_id=user_id,
                    anomaly_label=None,
                )
            )
            t = t + timedelta(seconds=rng.uniform(0.5, 5.0))

    records.sort(key=lambda r: r["ts"])
    return records[:target_total]
