"""Cac attack injector sinh log CO NHAN cho Log Contract v1.

Moi injector tra ve list[dict] cho MOT chien dich tan cong (mot attack_id),
co the goc tu mot hoac nhieu src_ip (vi du ddos = botnet).
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
SQLI_TARGETS = [("/product", "id"), ("/search", "q"), ("/login", "username")]

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
    attack_id = new_id()
    src_ip = _random_public_ip(rng)
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
        status = 200 if (breached and is_last) else 401
        records.append(
            make_record(
                timestamp=t,
                src_ip=src_ip,
                method="POST",
                path="/login",
                query=None,
                protocol="HTTP/1.1",
                status=status,
                bytes_=rng.randint(200, 600),
                response_time_ms=rng.randint(10, 60),
                user_agent=ua,
                referer=None,
                session_id=session_id,
                label="brute_force",
                attack_id=attack_id,
            )
        )
        t = t + timedelta(seconds=spacing)
    return records


def generate_sqli(base_dt: datetime, duration_s: float, rng: random.Random) -> list[dict]:
    attack_id = new_id()
    src_ip = _random_public_ip(rng)
    ua = rng.choice(SCRIPT_USER_AGENTS)

    n = rng.randint(10, 40)
    spacing = rng.uniform(0.2, 2.0)
    start_offset = _attack_window(duration_s, rng, n * spacing)

    records = []
    t = base_dt + timedelta(seconds=start_offset)
    session_id = new_id()
    for _ in range(n):
        path, param = rng.choice(SQLI_TARGETS)
        payload = rng.choice(SQLI_PAYLOADS)
        method = "POST" if path == "/login" else "GET"
        status = rng.choices([200, 500, 403], weights=[70, 20, 10], k=1)[0]
        records.append(
            make_record(
                timestamp=t,
                src_ip=src_ip,
                method=method,
                path=path,
                query=f"{param}={payload}",
                protocol="HTTP/1.1",
                status=status,
                bytes_=rng.randint(200, 3000),
                response_time_ms=rng.randint(20, 400),
                user_agent=ua,
                referer=None,
                session_id=session_id,
                label="sqli",
                attack_id=attack_id,
            )
        )
        t = t + timedelta(seconds=spacing)
    return records


def generate_scan(base_dt: datetime, duration_s: float, rng: random.Random) -> list[dict]:
    attack_id = new_id()
    src_ip = _random_public_ip(rng)
    ua = rng.choice(SCRIPT_USER_AGENTS)

    targets = SCAN_PATHS * rng.randint(1, 2)
    rng.shuffle(targets)
    spacing = rng.uniform(0.01, 0.1)
    start_offset = _attack_window(duration_s, rng, len(targets) * spacing)

    records = []
    t = base_dt + timedelta(seconds=start_offset)
    session_id = new_id()
    for path in targets:
        status = rng.choices([404, 200, 403], weights=[85, 8, 7], k=1)[0]
        records.append(
            make_record(
                timestamp=t,
                src_ip=src_ip,
                method="GET",
                path=path,
                query=None,
                protocol="HTTP/1.1",
                status=status,
                bytes_=rng.randint(100, 800),
                response_time_ms=rng.randint(5, 40),
                user_agent=ua,
                referer=None,
                session_id=session_id,
                label="scan",
                attack_id=attack_id,
            )
        )
        t = t + timedelta(seconds=spacing)
    return records


def generate_ddos(base_dt: datetime, duration_s: float, rng: random.Random) -> list[dict]:
    attack_id = new_id()
    botnet = [_random_public_ip(rng) for _ in range(rng.randint(20, 60))]
    ua_pool = SCRIPT_USER_AGENTS + [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ]
    target_path = rng.choice(["/", "/api/search", "/product/1"])

    burst_s = rng.uniform(5, 20)
    total_requests = rng.randint(400, 1200)
    start_offset = _attack_window(duration_s, rng, burst_s)

    records = []
    session_id = new_id()
    for i in range(total_requests):
        progress = i / total_requests  # 0.0 -> 1.0, dung de mo phong server bi qua tai dan
        t = base_dt + timedelta(seconds=start_offset + progress * burst_s)
        status = 200 if rng.random() > progress else rng.choice([503, 499])
        response_time_ms = int(50 + progress * rng.uniform(1500, 5000))
        records.append(
            make_record(
                timestamp=t,
                src_ip=rng.choice(botnet),
                method="GET",
                path=target_path,
                query=None,
                protocol="HTTP/1.1",
                status=status,
                bytes_=rng.randint(0, 500),
                response_time_ms=response_time_ms,
                user_agent=rng.choice(ua_pool),
                referer=None,
                session_id=session_id,
                label="ddos",
                attack_id=attack_id,
            )
        )
    return records


ATTACK_GENERATORS = {
    "brute_force": generate_brute_force,
    "sqli": generate_sqli,
    "scan": generate_scan,
    "ddos": generate_ddos,
}
