"""Tao Lens dashboard va native Kibana alert rule, ID co dinh de chay lai."""
from __future__ import annotations

import json
import uuid

from stack_api import ROOT, env_config, request


def save(kind: str, identity: str, attributes: dict, references=None) -> None:
    request("POST", f"/api/saved_objects/{kind}/{identity}?overwrite=true", {"attributes": attributes, "references": references or []}, kibana=True)


def column(operation: str, field: str = "___records___", label: str = "Records") -> dict:
    bucketed = operation in ("terms", "date_histogram")
    result = {"label": label, "dataType": "date" if operation == "date_histogram" else "string" if operation == "terms" else "number",
              "operationType": operation, "sourceField": field, "isBucketed": bucketed}
    if operation == "terms":
        result["params"] = {"size": 10, "orderBy": {"type": "column", "columnId": "metric"}, "orderDirection": "desc", "otherBucket": False, "missingBucket": False}
    elif operation == "date_histogram":
        result["params"] = {"interval": "auto", "includeEmptyRows": True, "dropPartials": False}
    elif operation == "percentile":
        result["params"] = {"percentile": 95}
    else:
        result["params"] = {}
    return result


def lens(identity: str, title: str, view: str, *, operation="count", field="___records___", bucket=None, breakdown=None, query="", metric_filter=None, value_format=None, comparison=None) -> str:
    columns = {"metric": column(operation, field, title)}
    if metric_filter:
        columns["metric"]["filter"] = {"query": metric_filter, "language": "kuery"}
    if value_format:
        columns["metric"]["params"]["format"] = {"id": value_format}
    order = ["metric"]
    visualization = {"layerId": "layer", "layerType": "data", "metricAccessor": "metric", "secondaryTrend": {"type": "none"}}
    visualization_type = "lnsMetric"
    if bucket:
        operation_type, bucket_field = bucket
        columns["bucket"] = column(operation_type, bucket_field, bucket_field)
        order.insert(0, "bucket")
        layer = {"layerId": "layer", "layerType": "data", "accessors": ["metric"], "xAccessor": "bucket",
                 "seriesType": "line" if operation_type == "date_histogram" else "bar_horizontal", "position": "top", "showGridlines": False}
        if breakdown:
            columns["breakdown"] = column("terms", breakdown, breakdown)
            order.insert(1, "breakdown")
            layer["splitAccessor"] = "breakdown"
        if comparison:
            columns["comparison"] = column("max", comparison, comparison)
            order.append("comparison")
            layer["accessors"].append("comparison")
        visualization = {"legend": {"isVisible": bool(breakdown or comparison), "position": "right"}, "valueLabels": "hide",
                         "preferredSeriesType": layer["seriesType"], "layers": [layer],
                         "yLeftExtent": {"mode": "full"}, "yRightExtent": {"mode": "full"}}
        visualization_type = "lnsXY"
    state = {"visualization": visualization, "query": {"query": query, "language": "kuery"}, "filters": [],
             "datasourceStates": {"formBased": {"layers": {"layer": {"columns": columns, "columnOrder": order, "incompleteColumns": {}, "sampling": 1}}}},
             "internalReferences": [], "adHocDataViews": {}}
    save("lens", identity, {"title": title, "description": "Log Anomaly Detection - Nhu", "visualizationType": visualization_type, "state": state},
         [{"type": "index-pattern", "id": view, "name": "indexpattern-datasource-layer-layer"}])
    return identity


def dashboard(identity: str, title: str, panels: list, time_from: str, time_to: str, refresh: bool = False) -> None:
    layout, references = [], []
    for position, (kind, panel_id, x, y, width, height) in enumerate(panels):
        name = f"panel_{position}"
        layout.append({"panelIndex": str(position), "type": kind, "panelRefName": name, "embeddableConfig": {},
                       "gridData": {"x": x, "y": y, "w": width, "h": height, "i": str(position)}, "version": "8.19.14"})
        references.append({"type": kind, "id": panel_id, "name": name})
    save("dashboard", identity, {"title": title, "description": "HDFS / Spark / Elasticsearch / Kibana - Nhu",
         "panelsJSON": json.dumps(layout), "optionsJSON": json.dumps({"useMargins": True, "hidePanelTitles": False}),
         "timeRestore": True, "timeFrom": time_from, "timeTo": time_to,
         "refreshInterval": {"pause": not refresh, "value": 10000}, "version": 1,
         "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})}}, references)


def setup_rule() -> None:
    # Saved object ID can phai duy nhat giua cac space tu Kibana 8.
    namespace = env_config().get("KIBANA_URL", "")
    server_id = str(uuid.uuid5(uuid.NAMESPACE_URL, namespace + "log-anomaly/nhu-server-log"))
    index_id = str(uuid.uuid5(uuid.NAMESPACE_URL, namespace + "log-anomaly/nhu-alert-index"))
    connectors = [
        (server_id, {"name": "Nhu - Server log", "connector_type_id": ".server-log", "config": {}, "secrets": {}}),
        (index_id, {"name": "Nhu - Alert history", "connector_type_id": ".index", "config": {"index": "alert-history", "refresh": True, "executionTimeField": "@timestamp"}, "secrets": {}}),
    ]
    current = {item["id"] for item in request("GET", "/api/actions/connectors", kibana=True)}
    for identity, body in connectors:
        if identity not in current:
            request("POST", f"/api/actions/connector/{identity}", body, kibana=True)
    rule = {"name": "Nhu - New anomaly detected", "tags": ["log-anomaly", "nhu"], "rule_type_id": ".es-query", "consumer": "stackAlerts",
            "schedule": {"interval": "30s"}, "enabled": True,
            "params": {"searchType": "esQuery", "index": ["log-alerts"], "timeField": "detected_at",
                       "esQuery": json.dumps({"query": {"match_all": {}}}), "size": 10, "timeWindowSize": 1, "timeWindowUnit": "m",
                       "threshold": [0], "thresholdComparator": ">", "aggType": "count", "groupBy": "all", "excludeHitsFromPreviousRun": False},
            "actions": [
                {"group": "query matched", "id": server_id, "params": {"level": "warn", "message": "Log anomaly: {{context.value}} windows. {{context.message}}"},
                 "frequency": {"summary": False, "notify_when": "onActionGroupChange", "throttle": None}},
                {"group": "query matched", "id": index_id, "params": {"documents": [{"rule_name": "Nhu - New anomaly detected", "reason": "New anomalous windows in log-alerts", "message": "{{context.message}}", "source": "kibana-rule"}]},
                 "frequency": {"summary": False, "notify_when": "onActionGroupChange", "throttle": None}},
            ]}
    existing = request("GET", "/api/alerting/rules/_find?per_page=100", kibana=True)["data"]
    identity = next((item["id"] for item in existing if item["name"] == rule["name"]), None)
    if identity:
        update = {key: value for key, value in rule.items() if key not in ("enabled", "rule_type_id", "consumer")}
        request("PUT", f"/api/alerting/rule/{identity}", update, kibana=True)
    else:
        identity = request("POST", "/api/alerting/rule", rule, kibana=True)["id"]
    if "/s/" not in namespace:
        (ROOT / "kibana" / "rule-id.json").write_text(json.dumps({"rule_id": identity, "name": rule["name"]}, indent=2) + "\n")
    print(f"Kibana rule OK: {identity}")


def main() -> None:
    (ROOT / "kibana").mkdir(exist_ok=True)
    request("POST", "/api/data_views/data_view", {"data_view": {"id": "view-logs-all", "title": "logs-*", "name": "HTTP logs (sim + NASA)", "timeFieldName": "event_time"}, "override": True}, kibana=True)
    overview = []
    def panel(identity, title, view, x, y, w, h, **kwargs):
        lens(identity, title, view, **kwargs)
        return ("lens", identity, x, y, w, h)
    overview.extend([
        panel("nhu-total", "Requests", "view-logs-all", 0, 0, 12, 6),
        panel("nhu-errors", "HTTP error rate", "view-logs-all", 12, 0, 12, 6, operation="average", field="error_flag", value_format="percent"),
        panel("nhu-ips", "Unique clients", "view-logs-all", 24, 0, 12, 6, operation="unique_count", field="ip"),
        panel("nhu-alert-count", "Anomalous windows", "view-log-alerts", 36, 0, 12, 6),
        panel("nhu-requests-time", "Requests over time", "view-logs-all", 0, 6, 32, 12, bucket=("date_histogram", "event_time"), breakdown="status_code"),
        panel("nhu-status", "HTTP status distribution", "view-logs-all", 32, 6, 16, 12, bucket=("terms", "status_code")),
        panel("nhu-error-clients", "Top clients with 4xx errors", "view-logs-all", 0, 18, 24, 12, bucket=("terms", "ip"), query="status_code >= 400 and status_code < 500"),
        panel("nhu-404-paths", "Top 404 paths", "view-logs-all", 24, 18, 24, 12, bucket=("terms", "path"), query="status_code: 404"),
        panel("nhu-miss", "Cache MISS rate (sim)", "view-logs-sim", 0, 30, 24, 10, operation="average", field="cache_miss_flag", value_format="percent"),
        panel("nhu-p95", "Latency p95 (ms, sim)", "view-logs-sim", 24, 30, 24, 10, operation="percentile", field="latency_ms", bucket=("date_histogram", "event_time")),
    ])
    # Pham vi mac dinh theo du lieu batch that, khong dung last 15m cho log lich su.
    history_index = "logs-nasa" if request("GET", "/logs-nasa/_count")["count"] else "logs-sim"
    span = request("POST", f"/{history_index}/_search", {"size": 0, "aggs": {"lo": {"min": {"field": "event_time"}}, "hi": {"max": {"field": "event_time"}}}})["aggregations"]
    # Mac dinh hien NASA; khong gom 1995 va 2026 vao cung truc thoi gian.
    dashboard("nhu-log-overview", "Log Overview", overview, span["lo"].get("value_as_string") or "now-1y", span["hi"].get("value_as_string") or "now")
    save("search", "nhu-latest-alerts", {"title": "Latest anomalous windows", "columns": ["ip", "window_start", "score", "threshold", "cluster", "ref_cluster", "severity", "top_feature", "reason", "source"], "sort": [["detected_at", "desc"]],
         "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index", "query": {"query": "", "language": "kuery"}, "filter": []})}},
         [{"type": "index-pattern", "id": "view-log-alerts", "name": "kibanaSavedObjectMeta.searchSourceJSON.index"}])
    alerts = [
        panel("nhu-live-alert-count", "Anomalous windows", "view-log-alerts", 0, 0, 16, 6),
        panel("nhu-live-clients", "Affected clients", "view-log-alerts", 16, 0, 16, 6, operation="unique_count", field="ip"),
        panel("nhu-live-history", "Rule activations", "view-alert-history", 32, 0, 16, 6),
        ("search", "nhu-latest-alerts", 0, 6, 48, 14),
        panel("nhu-score", "Score and threshold", "view-log-features", 0, 20, 24, 12, operation="max", field="score", bucket=("date_histogram", "window_start"), comparison="threshold"),
        panel("nhu-clusters", "Cluster distribution (model only)", "view-log-features", 24, 20, 24, 12, bucket=("terms", "cluster")),
        panel("nhu-labels", "Ground truth vs prediction (simulator)", "view-log-features", 0, 32, 24, 12, bucket=("terms", "ground_truth"), breakdown="is_alert", query="source: sim or source: stream"),
        panel("nhu-reasons", "Alert reasons", "view-log-alerts", 24, 32, 24, 12, bucket=("terms", "reason")),
    ]
    dashboard("nhu-anomaly-alerts", "Anomaly & Alerts", alerts, "now-30m", "now", refresh=True)
    setup_rule()
    data = request("POST", "/api/saved_objects/_export", {"objects": [{"type": "dashboard", "id": "nhu-log-overview"}, {"type": "dashboard", "id": "nhu-anomaly-alerts"}], "includeReferencesDeep": True}, kibana=True, raw=True)
    (ROOT / "kibana" / "dashboards.ndjson").write_bytes(data)
    print("Dashboard export OK: kibana/dashboards.ndjson")


if __name__ == "__main__":
    main()
