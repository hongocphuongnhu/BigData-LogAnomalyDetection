"""Sinh traffic HTTP binh thuong (label == 'normal') cho Log Contract v1."""
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

# (path_template, weight, is_static)
NORMAL_PATHS = [
    ("/", 20, False),
    ("/home", 10, False),
    ("/products", 15, False),
    ("/product/{id}", 20, False),
    ("/cart", 8, False),
    ("/checkout", 4, False),
    ("/login", 5, False),
    ("/logout", 3, False),
    ("/api/search", 10, False),
    ("/about", 3, False),
    ("/contact", 3, False),
    ("/static/style.css", 5, True),
    ("/static/app.js", 5, True),
    ("/favicon.ico", 3, True),
]

SEARCH_TERMS = [
    "shoes", "laptop", "phone", "book", "chair", "table",
    "watch", "camera", "headphones", "bag",
]

EXTERNAL_REFERERS = [
    None,
    "https://www.google.com/",
    "https://www.bing.com/",
    None,
]


def _random_ip_pool(rng: random.Random, size: int) -> list[str]:
    """Tao pool IP 'nguoi dung binh thuong' tu vai khoi /16 co dinh."""
    blocks = [(203, 0), (198, 51), (192, 0), (172, 217)]
    ips = []
    for _ in range(size):
        b0, b1 = rng.choice(blocks)
        ips.append(f"{b0}.{b1}.{rng.randint(0, 255)}.{rng.randint(1, 254)}")
    return ips


def _pick_path(rng: random.Random) -> tuple[str, Optional[str], bool]:
    template, _, is_static = rng.choices(
        NORMAL_PATHS,
        weights=[w for _, w, _ in NORMAL_PATHS],
        k=1,
    )[0]

    query = None
    path = template
    if template == "/product/{id}":
        path = f"/product/{rng.randint(1, 500)}"
    elif template == "/api/search":
        query = f"q={rng.choice(SEARCH_TERMS)}"

    return path, query, is_static


def _status_for(path: str, is_static: bool, rng: random.Random) -> int:
    if is_static:
        return rng.choices([200, 304], weights=[80, 20], k=1)[0]
    if path.startswith("/product/") and rng.random() < 0.05:
        return 404
    return rng.choices([200, 301, 404, 500], weights=[90, 3, 5, 2], k=1)[0]


def _response_time_ms(is_static: bool, path: str, rng: random.Random) -> int:
    if is_static:
        return rng.randint(5, 30)
    if path == "/checkout":
        return rng.randint(100, 400)
    return rng.randint(20, 300)


def _bytes_for(is_static: bool, rng: random.Random) -> int:
    if is_static:
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
    voi session_id chung va referer noi tiep tu request truoc.
    """
    target_total = max(0, int(duration_s * rate_per_s))
    if target_total == 0:
        return []

    ip_pool = _random_ip_pool(rng, size=max(10, target_total // 5))

    records: list[dict] = []
    while len(records) < target_total:
        ip = rng.choice(ip_pool)
        ua = rng.choice(BROWSER_USER_AGENTS)
        session_id = new_id()
        session_start = base_dt + timedelta(seconds=rng.uniform(0, duration_s))
        session_len = rng.randint(1, 8)

        t = session_start
        referer = rng.choice(EXTERNAL_REFERERS)
        for _ in range(session_len):
            if len(records) >= target_total:
                break
            path, query, is_static = _pick_path(rng)
            status = _status_for(path, is_static, rng)
            method = "POST" if path in ("/login", "/checkout") and rng.random() < 0.5 else "GET"

            records.append(
                make_record(
                    timestamp=t,
                    src_ip=ip,
                    method=method,
                    path=path,
                    query=query,
                    protocol="HTTP/1.1",
                    status=status,
                    bytes_=_bytes_for(is_static, rng),
                    response_time_ms=_response_time_ms(is_static, path, rng),
                    user_agent=ua,
                    referer=referer,
                    session_id=session_id,
                    label="normal",
                    attack_id=None,
                )
            )
            referer = f"https://shop.example.com{path}"
            t = t + timedelta(seconds=rng.uniform(0.5, 5.0))

    return records
