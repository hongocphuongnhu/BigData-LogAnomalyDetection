"""Log Contract v1 cho bronze layer (http_access_log).

Doi chieu voi contracts/http_access_log.v1.schema.json - moi thay doi field
phai duoc cap nhat o ca hai noi. article_id + cache_status gan truc tiep
voi co che Cache-Aside (mon NoSQL).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

VALID_METHODS = ("GET", "POST", "PUT", "DELETE", "HEAD")
VALID_EVENT_TYPES = (
    "view_article",
    "list_articles",
    "search",
    "login",
    "logout",
    "static_asset",
    "other",
)
VALID_CACHE_STATUSES = ("HIT", "MISS")
VALID_ANOMALY_LABELS = ("brute_force", "sqli", "scan", "ddos")

# Thu tu field co dinh (dung nguyen thu tu nay khi serialize JSONL).
FIELD_ORDER = (
    "ts",
    "trace_id",
    "session_id",
    "user_id",
    "event_type",
    "method",
    "path",
    "article_id",
    "status_code",
    "latency_ms",
    "cache_status",
    "bytes_out",
    "ip",
    "user_agent",
    "is_anomaly",
    "anomaly_label",
)


def new_id() -> str:
    return str(uuid.uuid4())


def iso_ms(dt: datetime) -> str:
    """Format datetime thanh ISO 8601 UTC, do chinh xac milisecond, hau to 'Z'."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def make_record(
    *,
    ts: datetime,
    session_id: str,
    event_type: str,
    method: str,
    path: str,
    status_code: int,
    latency_ms: int,
    cache_status: str,
    bytes_out: int,
    ip: str,
    user_agent: str,
    is_anomaly: bool,
    user_id: Optional[str] = None,
    article_id: Optional[int] = None,
    anomaly_label: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> dict:
    """Dung nhat mot dict theo dung Log Contract v1 (khong thieu/du field)."""
    record = {
        "ts": iso_ms(ts),
        "trace_id": trace_id or new_id(),
        "session_id": session_id,
        "user_id": user_id,
        "event_type": event_type,
        "method": method,
        "path": path,
        "article_id": article_id,
        "status_code": status_code,
        "latency_ms": latency_ms,
        "cache_status": cache_status,
        "bytes_out": bytes_out,
        "ip": ip,
        "user_agent": user_agent,
        "is_anomaly": is_anomaly,
        "anomaly_label": anomaly_label,
    }
    errors = validate(record)
    if errors:
        raise ValueError(f"Record vi pham Log Contract v1: {errors}")
    return record


def validate(record: dict) -> list[str]:
    """Tra ve danh sach loi (rong neu record hop le theo contract)."""
    errors: list[str] = []

    missing = [f for f in FIELD_ORDER if f not in record]
    if missing:
        errors.append(f"thieu field: {missing}")
        return errors  # khong kiem tra tiep neu thieu field co ban

    extra = [k for k in record if k not in FIELD_ORDER]
    if extra:
        errors.append(f"field khong nam trong contract: {extra}")

    if record["method"] not in VALID_METHODS:
        errors.append(f"method khong hop le: {record['method']!r}")

    if record["event_type"] not in VALID_EVENT_TYPES:
        errors.append(f"event_type khong hop le: {record['event_type']!r}")

    if record["cache_status"] not in VALID_CACHE_STATUSES:
        errors.append(f"cache_status khong hop le: {record['cache_status']!r}")

    if not isinstance(record["status_code"], int) or not (100 <= record["status_code"] <= 599):
        errors.append(f"status_code khong hop le: {record['status_code']!r}")

    if not isinstance(record["latency_ms"], int) or record["latency_ms"] < 0:
        errors.append(f"latency_ms khong hop le: {record['latency_ms']!r}")

    if not isinstance(record["bytes_out"], int) or record["bytes_out"] < 0:
        errors.append(f"bytes_out khong hop le: {record['bytes_out']!r}")

    if not isinstance(record["is_anomaly"], bool):
        errors.append(f"is_anomaly phai la bool: {record['is_anomaly']!r}")

    if record["is_anomaly"]:
        if record["anomaly_label"] not in VALID_ANOMALY_LABELS:
            errors.append(f"is_anomaly == true nhung anomaly_label khong hop le: {record['anomaly_label']!r}")
    else:
        if record["anomaly_label"] is not None:
            errors.append("is_anomaly == false nhung anomaly_label khac null")

    return errors
