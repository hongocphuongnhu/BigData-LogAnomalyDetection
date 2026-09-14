"""Cac attack injector sinh log CO NHAN cho Log Contract v1.

Moi injector tra ve list[dict] cho MOT chien dich tan cong. Tat ca deu
cache_status == "MISS" boi vi target (id/query) cua attacker khong trung
voi key da duoc cache "am" boi traffic binh thuong - day cung la mot dau
hieu phat hien bat thuong theo goc nhin Cache-Aside (ty le MISS bat thuong cao).
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta

from .log_contract import make_record, new_id

SCRIPT_USER_AGENTS = [
    "python-requests/2.31.0",
    "curl/8.6.0",
    "sqlmap/1.8.2#stable",
    "Nikto/2.5.0",
    "Go-http-client/1.1",
]

SQLI_PAYLOADS = [
    "' OR '1'='1",
    "' OR '1'='1' --",
    "1; DROP TABLE users;--",
    "' UNION SELECT username,password FROM users--",
    "1 AND SLEEP(5)",
    "admin'--",
]

SCAN_PATHS = [
    "/.env", "/.git/config", "/wp-login.php", "/phpmyadmin/", "/admin/",
    "/config.php", "/server-status", "/.aws/credentials", "/backup.sql",
    "/shell.php", "/xmlrpc.php", "/admin/login", "/debug", "/console",
    "/actuator/health", "/.ssh/id_rsa", "/db.sqlite3", "/.htpasswd",
    "/wp-admin/", "/administrator/", "/api/v1/users", "/vendor/",
    "/composer.json", "/.docker.env", "/robots.txt", "/sitemap.xml",
    "/telescope/requests", "/graphql", "/_profiler", "/HNAP1/",
]


def _random_public_ip(rng: random.Random) -> str:
    return f"{rng.randint(1, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def _attack_window(duration_s: float, rng: random.Random, span_s: float) -> float:
    """Chon thoi diem bat dau (giay, tinh tu base_dt) sao cho con du cho span_s."""
    max_start = max(0.0, duration_s - span_s)
    return rng.uniform(0, max_start) if max_start > 0 else 0.0


def generate_brute_force(base_dt: datetime, duration_s: float, rng: random.Random) -> list[dict]:
    ip = _random_public_ip(rng)
    ua = rng.choice(SCRIPT_USER_AGENTS)
    username = rng.choice(["admin", "root", "administrator", "test"])

    n_attempts = rng.randint(20, 80)
    spacing = rng.uniform(0.1, 0.8)
    start_offset = _attack_window(duration_s, rng, n_attempts * spacing)
    breached = rng.random() < 0.15

    records = []
    t = base_dt + timedelta(seconds=start_offset)
    session_id = new_id()
    for i in range(n_attempts):
        is_last = i == n_attempts - 1
        status_code = 200 if (breached and is_last) else 401
        records.append(
            make_record(
                ts=t,
                session_id=session_id,
                event_type="login",
                method="POST",
                path="/login",
                article_id=None,
                status_code=status_code,
                latency_ms=rng.randint(10, 60),
                cache_status="MISS",
                bytes_out=rng.randint(200, 600),
                ip=ip,
                user_agent=ua,
                is_anomaly=True,
                user_id=username if (breached and is_last) else None,
                anomaly_label="brute_force",
            )
        )
        t = t + timedelta(seconds=spacing)
    return records


def generate_sqli(base_dt: datetime, duration_s: float, rng: random.Random) -> list[dict]:
    ip = _random_public_ip(rng)
    ua = rng.choice(SCRIPT_USER_AGENTS)

    n = rng.randint(10, 40)
    spacing = rng.uniform(0.2, 2.0)
    start_offset = _attack_window(duration_s, rng, n * spacing)

    records = []
    t = base_dt + timedelta(seconds=start_offset)
    session_id = new_id()
    for _ in range(n):
        payload = rng.choice(SQLI_PAYLOADS)
        target_article = rng.random() < 0.5
        if target_article:
            event_type = "view_article"
            path = f"/article/{payload}"  # id khong hop le -> article_id=None
        else:
            event_type = "search"
            path = f"/search?q={payload}"
        status_code = rng.choices([200, 500, 403], weights=[70, 20, 10], k=1)[0]
        records.append(
            make_record(
                ts=t,
                session_id=session_id,
                event_type=event_type,
                method="GET",
                path=path,
                article_id=None,
                status_code=status_code,
                latency_ms=rng.randint(20, 400),
                cache_status="MISS",
                bytes_out=rng.randint(200, 3000),
                ip=ip,
                user_agent=ua,
                is_anomaly=True,
                user_id=None,
                anomaly_label="sqli",
            )
        )
        t = t + timedelta(seconds=spacing)
    return records


def generate_scan(base_dt: datetime, duration_s: float, rng: random.Random) -> list[dict]:
    ip = _random_public_ip(rng)
    ua = rng.choice(SCRIPT_USER_AGENTS)

    targets = SCAN_PATHS * rng.randint(1, 2)
    rng.shuffle(targets)
    spacing = rng.uniform(0.01, 0.1)
    start_offset = _attack_window(duration_s, rng, len(targets) * spacing)

    records = []
    t = base_dt + timedelta(seconds=start_offset)
    session_id = new_id()
    for path in targets:
        status_code = rng.choices([404, 200, 403], weights=[85, 8, 7], k=1)[0]
        records.append(
            make_record(
                ts=t,
                session_id=session_id,
                event_type="other",
                method="GET",
                path=path,
                article_id=None,
                status_code=status_code,
                latency_ms=rng.randint(5, 40),
                cache_status="MISS",
                bytes_out=rng.randint(100, 800),
                ip=ip,
                user_agent=ua,
                is_anomaly=True,
                user_id=None,
                anomaly_label="scan",
            )
        )
        t = t + timedelta(seconds=spacing)
    return records


def generate_ddos(base_dt: datetime, duration_s: float, rng: random.Random) -> list[dict]:
    """Cache-busting flood: moi request nham vao mot article_id CHUA TUNG duoc
    cache (id lay tu khong gian rieng >= 100000), nen cache_status luon la
    MISS - mo phong dung kieu tan cong khai thac diem yeu cua Cache-Aside
    (lam tran luong truy van xuong nguon du lieu goc)."""
    botnet = [_random_public_ip(rng) for _ in range(rng.randint(20, 60))]
    ua_pool = SCRIPT_USER_AGENTS + [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ]

    burst_s = rng.uniform(5, 20)
    total_requests = rng.randint(400, 1200)
    start_offset = _attack_window(duration_s, rng, burst_s)
    id_base = rng.randint(100000, 900000)

    records = []
    session_id = new_id()
    for i in range(total_requests):
        progress = i / total_requests  # 0.0 -> 1.0, mo phong server bi qua tai dan
        t = base_dt + timedelta(seconds=start_offset + progress * burst_s)
        status_code = 200 if rng.random() > progress else rng.choice([503, 499])
        latency_ms = int(50 + progress * rng.uniform(1500, 5000))
        article_id = id_base + i
        records.append(
            make_record(
                ts=t,
                session_id=session_id,
                event_type="view_article",
                method="GET",
                path=f"/article/{article_id}",
                article_id=article_id,
                status_code=status_code,
                latency_ms=latency_ms,
                cache_status="MISS",
                bytes_out=rng.randint(0, 500),
                ip=rng.choice(botnet),
                user_agent=rng.choice(ua_pool),
                is_anomaly=True,
                user_id=None,
                anomaly_label="ddos",
            )
        )
    return records


ATTACK_GENERATORS = {
    "brute_force": generate_brute_force,
    "sqli": generate_sqli,
    "scan": generate_scan,
    "ddos": generate_ddos,
}
