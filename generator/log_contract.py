"""Log Contract v1 cho bronze layer (http_access_log).

Doi chieu voi contracts/http_access_log.v1.schema.json - moi thay doi field
phai duoc cap nhat o ca hai noi.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

SCHEMA_VERSION = "1.0"

VALID_METHODS = ("GET", "POST", "PUT", "DELETE", "HEAD")
VALID_LABELS = ("normal", "brute_force", "sqli", "scan", "ddos")

# Thu tu field co dinh, dung lam thu tu key khi serialize JSONL.
FIELD_ORDER = (
    "schema_version",
    "log_id",
    "timestamp",
    "src_ip",
    "method",
    "path",
    "query",
    "protocol",
    "status",
    "bytes",
    "response_time_ms",
    "user_agent",
    "referer",
    "session_id",
    "label",
    "attack_id",
)


def new_id() -> str:
    return str(uuid.uuid4())


def iso_ms(dt: datetime) -> str:
    """Format datetime thanh ISO 8601 UTC, do chinh xac milisecond, hau to 'Z'."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def make_record(
    *,
    timestamp: datetime,
    src_ip: str,
    method: str,
    path: str,
    protocol: str,
    status: int,
    bytes_: int,
    response_time_ms: int,
    user_agent: str,
    session_id: str,
    label: str,
    query: Optional[str] = None,
    referer: Optional[str] = None,
    attack_id: Optional[str] = None,
    log_id: Optional[str] = None,
) -> dict:
    """Dung nhat mot dict theo dung Log Contract v1 (khong thieu/du field)."""
    record = {
        "schema_version": SCHEMA_VERSION,
        "log_id": log_id or new_id(),
        "timestamp": iso_ms(timestamp),
        "src_ip": src_ip,
        "method": method,
        "path": path,
        "query": query,
        "protocol": protocol,
        "status": status,
        "bytes": bytes_,
        "response_time_ms": response_time_ms,
        "user_agent": user_agent,
        "referer": referer,
        "session_id": session_id,
        "label": label,
        "attack_id": attack_id,
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

    if record["schema_version"] != SCHEMA_VERSION:
        errors.append(f"schema_version phai la {SCHEMA_VERSION!r}")

    if record["method"] not in VALID_METHODS:
        errors.append(f"method khong hop le: {record['method']!r}")

    if not isinstance(record["status"], int) or not (100 <= record["status"] <= 599):
        errors.append(f"status khong hop le: {record['status']!r}")

    if not isinstance(record["bytes"], int) or record["bytes"] < 0:
        errors.append(f"bytes khong hop le: {record['bytes']!r}")

    if not isinstance(record["response_time_ms"], int) or record["response_time_ms"] < 0:
        errors.append(f"response_time_ms khong hop le: {record['response_time_ms']!r}")

    if record["label"] not in VALID_LABELS:
        errors.append(f"label khong hop le: {record['label']!r}")

    is_normal = record["label"] == "normal"
    if is_normal and record["attack_id"] is not None:
        errors.append("label == 'normal' nhung attack_id khac null")
    if not is_normal and record["attack_id"] is None:
        errors.append(f"label == {record['label']!r} nhung attack_id la null")

    return errors
