"""Doc trang thai ha tang (Spark, HDFS, ES, Kibana) qua API JSON cua tung dich vu.

Moi probe tra ve dict co khoa "ok" va KHONG BAO GIO nem loi: mot dich vu chet
chi lam the cua no do, khong lam sap ca trang.

Goi urllib truc tiep voi timeout ngan thay vi stack_api.request: ham do timeout
60s va retry 3 lan, qua cham cho the trang thai lam moi moi vai giay.
"""
from __future__ import annotations

import json
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT / "scripts"))
from stack_api import env_config  # noqa: E402  (scripts/ khong phai package)

TIMEOUT_S = 3
LAKE_TIERS = ("raw", "bronze", "silver", "gold")


def endpoints(host: str = "127.0.0.1") -> dict[str, str]:
    """URL goc cua tung dich vu tren host, theo cong trong .env (giong docker-compose.yml).

    Probe goi 127.0.0.1 thay vi localhost: tren Windows ket noi toi cong dong mat ~2s
    moi dia chi, "localhost" thu ca ::1 lan 127.0.0.1 nen thanh ~4s moi lan poll.
    """
    env = env_config()
    return {
        "spark": f"http://{host}:{env.get('SPARK_MASTER_UI_PORT', '8080')}",
        "spark_app": f"http://{host}:{env.get('SPARK_DRIVER_UI_PORT', '4040')}",
        "hdfs": f"http://{host}:9870",
        "es": env.get("ES_URL", "http://localhost:9200").replace("//localhost", f"//{host}"),
        "kibana": env.get("KIBANA_URL", "http://localhost:5601").replace("//localhost", f"//{host}"),
    }


def _get_json(url: str):
    with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=TIMEOUT_S) as response:
        return json.loads(response.read())


def _friendly(error: Exception) -> str:
    if isinstance(error, HTTPError):
        return f"Dịch vụ trả lỗi HTTP {error.code}"
    reason = getattr(error, "reason", error)
    if isinstance(reason, (socket.timeout, TimeoutError)) or isinstance(error, (socket.timeout, TimeoutError)):
        return f"Quá thời gian chờ ({TIMEOUT_S} giây)"
    if isinstance(error, (URLError, ConnectionError)):
        return "Không kết nối được"
    return f"Lỗi đọc dữ liệu: {type(error).__name__}"


def _safe(fn):
    def wrapper(base: str) -> dict:
        try:
            return {"ok": True, **fn(base)}
        except Exception as error:  # probe khong duoc lam sap server
            return {"ok": False, "error": _friendly(error)}
    return wrapper


@_safe
def probe_spark(base: str) -> dict:
    data = _get_json(f"{base}/json/")
    return {
        "workers": data.get("aliveworkers", 0),
        "cores": data.get("cores", 0),
        "cores_used": data.get("coresused", 0),
        "memory_mb": data.get("memory", 0),
        "memory_used_mb": data.get("memoryused", 0),
        "apps": [
            {"name": app.get("name"), "cores": app.get("cores", 0), "state": app.get("state"),
             "duration_s": round(app.get("duration", 0) / 1000)}
            for app in data.get("activeapps", [])
        ],
    }


def probe_spark_app(base: str) -> dict:
    # Cong 4040 chi mo khi co driver dang chay; dong cong la trang thai binh thuong.
    try:
        apps = _get_json(f"{base}/api/v1/applications")
    except (URLError, ConnectionError, socket.timeout, TimeoutError):
        return {"ok": True, "running": False}
    except Exception as error:
        return {"ok": False, "error": _friendly(error)}
    return {"ok": True, "running": bool(apps), "apps": [app.get("name") for app in apps]}


@_safe
def probe_hdfs(base: str) -> dict:
    beans = _get_json(f"{base}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")["beans"]
    state = beans[0] if beans else {}
    lake = {}
    for tier in LAKE_TIERS:
        try:
            summary = _get_json(f"{base}/webhdfs/v1/lake/{tier}?op=GETCONTENTSUMMARY")["ContentSummary"]
            lake[tier] = {"bytes": summary.get("length", 0), "files": summary.get("fileCount", 0)}
        except HTTPError as error:
            if error.code != 404:
                raise
            lake[tier] = None  # thu muc chua duoc tao tren HDFS
    return {
        "live_nodes": state.get("NumLiveDataNodes", 0),
        "capacity_total": state.get("CapacityTotal", 0),
        "capacity_used": state.get("CapacityUsed", 0),
        "blocks": state.get("BlocksTotal", 0),
        "files": state.get("FilesTotal", 0),
        "lake": lake,
    }


@_safe
def probe_es(base: str) -> dict:
    health = _get_json(f"{base}/_cluster/health")
    rows = _get_json(f"{base}/_cat/indices?format=json&h=index,docs.count,store.size&bytes=b")
    # Index he thong (.kibana, .alerts...) khong phai du lieu cua do an.
    indices = {
        row["index"]: {"docs": int(row.get("docs.count") or 0), "bytes": int(row.get("store.size") or 0)}
        for row in rows if not row["index"].startswith(".")
    }
    return {"status": health.get("status"), "nodes": health.get("number_of_nodes", 0),
            "indices": dict(sorted(indices.items()))}


@_safe
def probe_kibana(base: str) -> dict:
    status = _get_json(f"{base}/api/status")
    return {"level": status.get("status", {}).get("overall", {}).get("level"),
            "version": status.get("version", {}).get("number")}


PROBES = {
    "spark": probe_spark,
    "spark_app": probe_spark_app,
    "hdfs": probe_hdfs,
    "es": probe_es,
    "kibana": probe_kibana,
}


def collect_infra() -> dict:
    """Goi song song moi probe; tong thoi gian ~ probe cham nhat, khong cong don."""
    urls = endpoints()
    with ThreadPoolExecutor(max_workers=len(PROBES)) as pool:
        futures = {name: pool.submit(fn, urls[name]) for name, fn in PROBES.items()}
        result = {name: future.result() for name, future in futures.items()}
    # Link cho trinh duyet de localhost cho de doc tren man hinh chieu.
    shown = endpoints("localhost")
    result["links"] = {
        "spark": shown["spark"],
        "spark_app": shown["spark_app"],
        "hdfs": f"{shown['hdfs']}/dfshealth.html#tab-overview",
        "hdfs_browse": f"{shown['hdfs']}/explorer.html#/lake",
        "es": f"{shown['es']}/_cat/indices?v",
        "kibana": shown["kibana"],
    }
    return result


if __name__ == "__main__":
    # Console Windows mac dinh cp1252, khong in duoc tieng Viet co dau.
    sys.stdout.reconfigure(encoding="utf-8")
    print(json.dumps(collect_infra(), indent=2, ensure_ascii=False))
