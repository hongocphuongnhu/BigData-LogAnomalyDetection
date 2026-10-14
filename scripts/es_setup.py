"""Tao mapping va data view idempotent, khong xoa du lieu hien co."""
from __future__ import annotations

from stack_api import request

DATES = ("ts", "event_time", "ingested_at", "window_start", "window_end", "detected_at", "@timestamp")
KEYWORDS = ("ip", "source", "trace_id", "session_id", "user_id", "event_type", "method", "cache_status", "anomaly_label", "ground_truth", "cluster", "ref_cluster", "top_feature", "severity", "reason", "rule_reason", "detector", "doc_id", "run_id")


def main() -> None:
    properties = {name: {"type": "date"} for name in DATES}
    properties.update({name: {"type": "keyword"} for name in KEYWORDS})
    properties.update({
        "path": {"type": "keyword", "ignore_above": 4096, "fields": {"text": {"type": "text"}}},
        "status_code": {"type": "short"},
        "is_anomaly": {"type": "boolean"}, "predicted_anomaly": {"type": "boolean"}, "is_alert": {"type": "boolean"},
        "rule_flag": {"type": "boolean"}, "bytes_missing": {"type": "boolean"},
        "latency_ms": {"type": "integer"}, "bytes_out": {"type": "long"},
        "score": {"type": "double"}, "threshold": {"type": "double"},
    })
    template = {
        "index_patterns": ["logs-sim", "logs-nasa", "log-features", "log-alerts", "alert-history"],
        "priority": 200,
        "template": {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {"properties": properties, "dynamic_templates": [
                {"ratios": {"match": "ratio_*", "mapping": {"type": "double"}}},
                {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 4096}}},
            ]},
        },
    }
    request("PUT", "/_index_template/log-anomaly-serving-v1", template)
    existing = {item["index"] for item in request("GET", "/_cat/indices?format=json")}
    for index in ("logs-sim", "logs-nasa", "log-features", "log-alerts", "alert-history"):
        if index not in existing:
            request("PUT", f"/{index}", {})
        request("PUT", f"/{index}/_mapping", {"properties": properties})
        request("PUT", f"/{index}/_settings", {"index.number_of_replicas": 0})
        # Dashboard loc theo thoi gian su kien cho ca NASA lich su va stream.
        # Native rule van dung detected_at de chi kich hoat voi alert vua ghi.
        time_field = "event_time" if index.startswith("logs-") else ("@timestamp" if index == "alert-history" else "window_start")
        request("POST", "/api/data_views/data_view", {"data_view": {"id": f"view-{index}", "title": index, "name": index, "timeFieldName": time_field}, "override": True}, kibana=True)
        print(f"Index + data view OK: {index}")


if __name__ == "__main__":
    main()
