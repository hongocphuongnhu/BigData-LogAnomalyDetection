"""HTTP client stdlib cho Elasticsearch/Kibana, dung chung cac script serving."""
from __future__ import annotations

import base64
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent


def env_config() -> dict[str, str]:
    values = {}
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip("\"'")
    values.update(os.environ)
    return values


def request(method: str, path: str, body=None, *, kibana: bool = False, raw: bool = False, content_type: str | None = None):
    values = env_config()
    base = values.get("KIBANA_URL" if kibana else "ES_URL", "http://localhost:5601" if kibana else "http://localhost:9200")
    headers = {"kbn-xsrf": "log-anomaly-setup"}
    if values.get("ELASTIC_PASSWORD"):
        token = base64.b64encode(f"elastic:{values['ELASTIC_PASSWORD']}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    if isinstance(body, bytes):
        data = body
        headers["Content-Type"] = content_type or "application/x-ndjson"
    elif body is not None:
        data = json.dumps(body, ensure_ascii=False).encode()
        headers["Content-Type"] = "application/json"
    else:
        data = None
    req = Request(base + path, data=data, headers=headers, method=method)
    # Chi retry truy van doc; POST tao rule/connector co the tao trung neu gui lai.
    readable = method in ("GET", "HEAD") or (method == "POST" and path.split("?", 1)[0].endswith(("/_search", "/_count")))
    attempts = 3 if readable else 1
    for attempt in range(attempts):
        try:
            with urlopen(req, timeout=60) as response:
                payload = response.read()
            return payload if raw else (json.loads(payload) if payload else {})
        except HTTPError as error:
            detail = error.read().decode(errors="replace")
            if error.code not in (429, 502, 503, 504) or attempt == attempts - 1:
                raise RuntimeError(f"{method} {path}: HTTP {error.code}: {detail}") from None
        except (URLError, ConnectionError, TimeoutError):
            if attempt == attempts - 1:
                raise
        time.sleep(0.5 * (attempt + 1))


def save_report(name: str, content: dict) -> None:
    output = ROOT / "data" / "reports" / name
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(content, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
