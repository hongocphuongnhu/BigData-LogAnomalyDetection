"""Quan ly tien trinh cua console: bo phat hien live (Spark stream), simulator va bai kiem tra.

Khoa "mot viec mot luc": worker chi co 4 core; stream console xin 2 core, stream cua
verify_realtime.py xin ca 4. Hai viec chay chong thi viec sau treo o WAITING (rui ro R1).

Kill client `docker compose exec` tren host KHONG kill driver Spark trong container
(da kiem chung 29/10), nen moi lan dung/don deu pkill theo marker la duong dan checkpoint.
Moi tien trinh ghi ra file log, khong dung PIPE: pipe khong ai doc se day va chan tien trinh.
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .alerts import _search
from .probes import endpoints, probe_spark

ROOT = Path(__file__).resolve().parent.parent
CONSOLE_DIR = ROOT / "data" / "stream" / "console"
SETUP_LOGS = ROOT / "data" / "raw" / "setup"
CHECKS_FILE = CONSOLE_DIR / "checks.jsonl"
# Marker de pgrep/pkill: chi driver cua console dung checkpoint nay.
LIVE_MARKER = "stream/console/checkpoint"
QA_MARKER = "/opt/spark/data/stream/qa-"
FLAGS = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0

STREAM_DURATION_S = 600  # chot chan: console chet thi driver tu tat sau 10 phut
BOOT_TIMEOUT_S = 120
CORES_TIMEOUT_S = 60  # sau STREAM READY ma van chua co core (rui ro R8)
SIM_DURATION_S = 90
ATTACK_AFTER_S = 20
WATCH_AFTER_SIM_S = 60  # cua so 5 phut cap nhat moi trigger, alert co the toi tre sau khi phat xong
MAX_FALSE_POSITIVES = 2  # cung nguong verify_realtime.py

STREAM_COMMAND = [
    "docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
    "--master", "spark://spark-master:7077",
    "--conf", "spark.cores.max=2", "--conf", "spark.sql.shuffle.partitions=4",
    "--py-files", "/opt/spark/jobs/features_common.py", "/opt/spark/jobs/stream_detect.py",
    "--input-path", "/opt/spark/data/stream/console/incoming",
    "--checkpoint-path", "/opt/spark/data/stream/console/checkpoint",
    "--detector", "model", "--duration", str(STREAM_DURATION_S),
]

# Cau chuyen theo chu de bao dien tu, dung voi generator/attacks.py.
SCENARIOS = {
    "none": ("Bình thường",
             "Chỉ có bạn đọc bình thường: vào trang chủ, đọc bài, tìm kiếm. Hệ thống không nên báo động "
             f"(cho phép tối đa {MAX_FALSE_POSITIVES} lần báo nhầm)."),
    "scan": ("Dò quét lỗ hổng",
             "Một IP gõ cửa hàng loạt đường dẫn nhạy cảm như /.env, /wp-admin, /phpmyadmin để tìm chỗ hở; "
             "phần lớn trả lỗi 404."),
    "sqli": ("Chèn SQL",
             "Một IP nhét câu lệnh SQL vào ô tìm kiếm và đường dẫn bài viết (' OR '1'='1, UNION SELECT…) "
             "để thử đọc trộm cơ sở dữ liệu."),
    "brute_force": ("Dò mật khẩu",
                    "Một IP thử đăng nhập trang quản trị hàng chục lần liên tiếp với các mật khẩu khác nhau."),
    "ddos": ("DDoS phá cache",
             "Vài chục IP cùng lúc mở các bài viết chưa ai đọc để né cache, dồn truy vấn xuống cơ sở dữ liệu "
             "làm trang chậm dần rồi trả lỗi 503."),
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _container(*args: str, timeout: float = 20) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(["docker", "compose", "exec", "-T", "spark-master", *args], cwd=ROOT,
                              capture_output=True, text=True, timeout=timeout, creationflags=FLAGS)
    except (OSError, subprocess.TimeoutExpired):
        return None


def driver_pids(marker: str) -> list[str]:
    result = _container("pgrep", "-f", marker)
    return result.stdout.split() if result and result.returncode == 0 else []


def _kill_tree(proc: subprocess.Popen | None) -> None:
    """verify_realtime.py tu sinh tien trinh con (docker client, simulator): kill ca cay."""
    if proc is None or proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True, creationflags=FLAGS)
    else:
        proc.kill()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


def _spark_apps() -> list[dict] | None:
    spark = probe_spark(endpoints()["spark"])
    return spark["apps"] if spark.get("ok") else None


def clear_drivers(marker: str, timeout: float) -> bool:
    """pkill driver theo marker roi cho toi khi Spark master khong con app nao (de nha khoa)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if driver_pids(marker):
            _container("pkill", "-f", marker)
        elif _spark_apps() == []:
            return True
        time.sleep(1)
    return False


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None  # simulator dang ghi do manifest


def _tail(path: Path, lines: int = 20) -> list[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]
    except OSError:
        return []


class Runner:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.job: str | None = None  # None | "live" | "check"
        self.notice = ""
        self.live: dict | None = None
        self.scenario: dict | None = None
        self.check: dict | None = None
        self._stop = threading.Event()
        CONSOLE_DIR.mkdir(parents=True, exist_ok=True)

    # --- chung -----------------------------------------------------------------

    def _release(self, notice: str) -> None:
        with self.lock:
            self.job = None
            self.notice = notice

    def cleanup_orphans(self) -> None:
        """Lan truoc dong cua so server khi stream dang chay -> driver con song, giu core va checkpoint."""
        if driver_pids(LIVE_MARKER):
            _container("pkill", "-f", LIVE_MARKER)
            self.notice = "Đã dọn bộ phát hiện còn sót từ lần chạy trước."

    def shutdown(self) -> None:
        self._stop.set()
        if self.scenario:
            _kill_tree(self.scenario.get("proc"))
        if self.live:
            _kill_tree(self.live.get("proc"))
        if self.check:
            self.check["cancelled"] = True
            _kill_tree(self.check.get("proc"))
        if self.job == "live":
            _container("pkill", "-f", LIVE_MARKER)
        elif self.job == "check":
            _container("pkill", "-f", QA_MARKER)

    # --- live: bo phat hien ------------------------------------------------------

    def live_start(self) -> tuple[bool, str]:
        with self.lock:
            if self.job == "live":
                return False, "Bộ phát hiện đã bật rồi."
            if self.job == "check":
                return False, "Đang chạy kiểm tra tự động, chờ xong hoặc bấm Hủy."
            self.job = "live"
            self.notice = ""
            self._stop.clear()
            self.scenario = None
            self.live = {"phase": "starting", "error": None, "proc": None, "started": time.monotonic(),
                         "ready_at": None, "log": CONSOLE_DIR / "stream.log"}
        threading.Thread(target=self._live_loop, daemon=True).start()
        return True, "Đang bật bộ phát hiện."

    def live_stop(self) -> tuple[bool, str]:
        with self.lock:
            if self.job != "live" or not self.live:
                return False, "Bộ phát hiện chưa bật."
            self.live["phase"] = "stopping"
        self._stop.set()
        return True, "Đang dừng bộ phát hiện."

    def _live_loop(self) -> None:
        live = self.live
        notice = "Bộ phát hiện đã dừng."
        try:
            # Khong bao gio de hai driver ghi chung mot checkpoint.
            clear_drivers(LIVE_MARKER, 20)
            (CONSOLE_DIR / "incoming").mkdir(parents=True, exist_ok=True)
            with live["log"].open("w", encoding="utf-8") as log:
                live["proc"] = subprocess.Popen(STREAM_COMMAND, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                                creationflags=FLAGS)
            ready_line_at = None
            while not self._stop.wait(2):
                if live["proc"].poll() is not None:
                    if live["phase"] == "ready":
                        notice = "Bộ phát hiện đã tự tắt sau 10 phút. Bấm Bật để chạy tiếp (không đọc lại log cũ)."
                    else:
                        live["error"] = "Bộ phát hiện dừng ngay khi khởi động. Xem data/stream/console/stream.log."
                        notice = live["error"]
                    break
                if live["phase"] == "starting":
                    elapsed = time.monotonic() - live["started"]
                    if ready_line_at is None and "STREAM READY" in live["log"].read_text(errors="replace"):
                        ready_line_at = time.monotonic()
                    apps = _spark_apps() or []
                    # STREAM READY in ra ngay sau query.start(), ca khi app con WAITING chua co core.
                    if ready_line_at and any(a["name"] == "stream_detect" and a["state"] == "RUNNING" and a["cores"] > 0
                                             for a in apps):
                        live["phase"] = "ready"
                        live["ready_at"] = time.monotonic()
                    elif (ready_line_at and time.monotonic() - ready_line_at > CORES_TIMEOUT_S) or elapsed > BOOT_TIMEOUT_S:
                        live["error"] = "Cụm Spark đang bận, bộ phát hiện không nhận được lõi CPU. Xem thẻ Hạ tầng."
                        notice = live["error"]
                        break
                if self.scenario:
                    self._watch_scenario(self.scenario)
        except Exception as error:  # thread nen khong duoc chet im lang
            live["error"] = f"Lỗi khi chạy bộ phát hiện: {error}"
            notice = live["error"]
        finally:
            live["phase"] = "stopping"
            if self.scenario:
                _kill_tree(self.scenario.get("proc"))
            _kill_tree(live.get("proc"))
            if not clear_drivers(LIVE_MARKER, 20):
                notice += " Spark master vẫn còn tác vụ, xem thẻ Hạ tầng."
            live["phase"] = "off"
            self._release(notice)

    # --- live: kich ban -----------------------------------------------------------

    def scenario_start(self, attack: str) -> tuple[bool, str]:
        with self.lock:
            if self.job != "live" or not self.live or self.live["phase"] != "ready":
                return False, "Hãy bật bộ phát hiện và chờ sẵn sàng trước."
            current = self.scenario
            if current and current["proc"].poll() is None:
                return False, f"Đang chạy kịch bản “{SCENARIOS[current['attack']][0]}”, chờ xong rồi chạy tiếp."
            run_id = uuid.uuid4().hex[:8]
            manifest = CONSOLE_DIR / f"run-{run_id}.json"
            log_path = CONSOLE_DIR / f"run-{run_id}.log"
            command = [sys.executable, "-B", "-m", "generator.stream_simulate", "--attack", attack,
                       "--duration", str(SIM_DURATION_S), "--attack-after", str(ATTACK_AFTER_S), "--rate", "2",
                       # Seed moi lan khac nhau: chay lai cung kich ban thi IP khac, khong lan voi alert cu.
                       "--seed", str(random.randrange(1, 10**6)),
                       "--out-dir", str(CONSOLE_DIR / "incoming"), "--manifest", str(manifest)]
            baseline = self._stream_alert_count()
            with log_path.open("w", encoding="utf-8") as log:
                proc = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, creationflags=FLAGS)
            self.scenario = {"attack": attack, "run_id": run_id, "proc": proc, "manifest": manifest,
                             "started": _now(), "baseline_count": baseline, "baseline_ids": None,
                             "first_seen": None, "latency_s": None, "new_alerts": 0, "flagged_ips": 0, "ended": None,
                             "data": None}
        return True, f"Đã bắt đầu kịch bản “{SCENARIOS[attack][0]}”."

    @staticmethod
    def _stream_alert_count(ips: list[str] | None = None) -> int | None:
        filters = [{"term": {"source": "stream"}}] + ([{"terms": {"ip": ips}}] if ips else [])
        try:
            result = _search("log-alerts", {"size": 0, "track_total_hits": True,
                                            "query": {"bool": {"filter": filters}}})
            return result["hits"]["total"]["value"]
        except Exception:
            return None

    @staticmethod
    def _stream_alert_ids(ips: list[str]) -> dict[str, str] | None:
        """doc_id -> ip cua alert stream thuoc cac IP tan cong."""
        body = {"size": 5000, "_source": ["ip"],
                "query": {"bool": {"filter": [{"term": {"source": "stream"}}, {"terms": {"ip": ips}}]}}}
        try:
            return {hit["_id"]: hit["_source"].get("ip") for hit in _search("log-alerts", body)["hits"]["hits"]}
        except Exception:
            return None

    def _watch_scenario(self, scenario: dict) -> None:
        data = _read_json(scenario["manifest"])
        if data:
            scenario["data"] = data
        if scenario["proc"].poll() is not None and scenario["ended"] is None:
            scenario["ended"] = time.monotonic()
        if scenario["ended"] and time.monotonic() - scenario["ended"] > WATCH_AFTER_SIM_S:
            return
        if scenario["attack"] == "none":
            count = self._stream_alert_count()
            if count is not None and scenario["baseline_count"] is not None:
                scenario["new_alerts"] = max(0, count - scenario["baseline_count"])
            return
        ips = (scenario["data"] or {}).get("attack_ips")
        if not ips:
            return
        ids = self._stream_alert_ids(ips)
        if ids is None:
            return
        if scenario["baseline_ids"] is None:
            # Manifest ghi truoc khi tan cong 20 giay: alert san co cua cac IP nay la cua luot khac.
            scenario["baseline_ids"] = set(ids)
            return
        fresh = {doc_id: ip for doc_id, ip in ids.items() if doc_id not in scenario["baseline_ids"]}
        scenario["new_alerts"] = len(fresh)
        # Cua so truot 1 phut: mot IP nam trong toi 5 cua so -> dem them so IP de de doc.
        scenario["flagged_ips"] = len(set(fresh.values()))
        first_attack = scenario["data"].get("first_attack_at")
        if fresh and scenario["first_seen"] is None and first_attack:
            # Do tre theo dong ho host luc console THAY alert, khong dung detected_at cua container (R9).
            scenario["first_seen"] = _now()
            scenario["latency_s"] = round((scenario["first_seen"] - _parse_ts(first_attack)).total_seconds(), 1)

    # --- kiem tra tu dong -----------------------------------------------------------

    def check_start(self, attack: str) -> tuple[bool, str]:
        with self.lock:
            if self.job == "live":
                return False, "Hãy dừng mô phỏng trước: bài kiểm tra tự chạy bộ phát hiện riêng."
            if self.job == "check":
                return False, "Đang có một bài kiểm tra chạy."
            apps = _spark_apps()
            if apps is None:
                return False, "Không kết nối được cụm Spark."
            if apps:
                return False, "Cụm Spark đang có tác vụ khác giữ lõi CPU. Chờ tác vụ xong (xem thẻ Hạ tầng)."
            self.job = "check"
            self.notice = ""
            check_id = uuid.uuid4().hex[:8]
            self.check = {"id": check_id, "attack": attack, "proc": None, "started": time.monotonic(),
                          "started_at": _now(), "log": CONSOLE_DIR / f"check-{check_id}.log",
                          "cancelled": False, "result": None}
        threading.Thread(target=self._check_loop, args=(self.check,), daemon=True).start()
        return True, f"Đã bắt đầu kiểm tra “{SCENARIOS[attack][0]}”."

    def check_cancel(self) -> tuple[bool, str]:
        with self.lock:
            if self.job != "check" or not self.check:
                return False, "Không có bài kiểm tra nào đang chạy."
            self.check["cancelled"] = True
        _kill_tree(self.check.get("proc"))
        return True, "Đang hủy bài kiểm tra."

    def _check_loop(self, check: dict) -> None:
        env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
        command = [sys.executable, "-B", "scripts/verify_realtime.py", "--attack", check["attack"], "--detector", "model"]
        try:
            with check["log"].open("w", encoding="utf-8") as log:
                check["proc"] = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                                 env=env, creationflags=FLAGS)
            code = check["proc"].wait()
            check["result"] = self._check_result(check, code)
        except Exception as error:
            check["result"] = {"passed": False, "error": f"Không chạy được bài kiểm tra: {error}", "tail": []}
        finally:
            check["finished"] = time.monotonic()
            check["cleaning"] = True
            # verify_realtime loi giua chung thi driver qa-* van giu ca 4 core toi het --duration.
            cleared = clear_drivers(QA_MARKER, 30)
            record = {"id": check["id"], "attack": check["attack"], "started_at": check["started_at"].isoformat(),
                      "duration_s": round(check["finished"] - check["started"]), **check["result"]}
            record.pop("tail", None)
            try:
                with CHECKS_FILE.open("a", encoding="utf-8") as history:
                    history.write(json.dumps(record, ensure_ascii=False) + "\n")
            except OSError:
                pass
            check["cleaning"] = False
            self._release("" if cleared else "Spark master vẫn còn tác vụ kiểm tra, xem thẻ Hạ tầng.")

    @staticmethod
    def _check_result(check: dict, code: int) -> dict:
        text = check["log"].read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        if check["cancelled"]:
            return {"passed": False, "cancelled": True, "error": "Đã hủy giữa chừng.", "tail": lines[-20:]}
        if code != 0:
            errors = [line for line in lines if "Error" in line or "assert" in line]
            return {"passed": False, "error": errors[-1].strip() if errors else f"Mã thoát {code}", "tail": lines[-20:]}
        # Luot tan cong in JSON nhieu dong (indent=2), luot none in mot dong: lay tu dong cuoi bat dau bang "{".
        starts = [index for index, line in enumerate(lines) if line.startswith("{")]
        try:
            report = json.loads("\n".join(lines[starts[-1]:])) if starts else {}
        except json.JSONDecodeError:
            report = {}
        keys = ("latency_seconds", "alert_documents_before_restart", "alert_documents_after_restart",
                "history_before", "history_after", "false_positive_windows", "records", "normal_duration_seconds")
        return {"passed": True, "report": {key: report[key] for key in keys if key in report}, "tail": []}

    @staticmethod
    def _check_step(check: dict) -> int:
        """Buoc hien tai (1-6) suy tu dong log cua verify_realtime.py va log qa-* no ghi ra."""
        if check["result"]:
            return 6
        attack = check["attack"]
        logs = [path for path in SETUP_LOGS.glob(f"qa-{attack}-*.log")
                if not path.stem.endswith(("-sim", "-restart"))
                and path.stat().st_mtime >= check["started_at"].timestamp() - 1]
        if not logs:
            return 2 if attack == "none" else 1
        qa_log = max(logs, key=lambda path: path.stat().st_mtime)
        if (qa_log.with_name(qa_log.stem + "-restart.log")).exists():
            return 5
        if "First" in check["log"].read_text(encoding="utf-8", errors="replace"):
            return 4
        return 3 if "STREAM READY" in qa_log.read_text(errors="replace") else 2

    # --- trang thai cho UI ------------------------------------------------------------

    def status(self) -> dict:
        live = self.live
        live_view = None
        if live and live["phase"] != "off":
            remaining = None
            if live["ready_at"]:
                remaining = max(0, round(STREAM_DURATION_S - (time.monotonic() - live["started"])))
            live_view = {"phase": live["phase"], "error": live["error"], "remaining_s": remaining,
                         "elapsed_s": round(time.monotonic() - live["started"])}
        return {"job": self.job, "notice": self.notice, "live": live_view, "scenario": self._scenario_view(),
                "scenarios": [{"key": key, "label": label, "story": story} for key, (label, story) in SCENARIOS.items()]}

    def _scenario_view(self) -> dict | None:
        scenario = self.scenario
        if not scenario:
            return None
        data = scenario["data"] or {}
        running = scenario["proc"].poll() is None
        attack = scenario["attack"]
        first_attack = data.get("first_attack_at")
        if running and not data:
            stage = "Đang chuẩn bị traffic"
        elif running:
            if attack != "none" and first_attack and _now() < _parse_ts(first_attack):
                stage = f"Đang phát traffic bình thường, tấn công bắt đầu sau {ATTACK_AFTER_S} giây"
            else:
                stage = "Đang phát traffic bình thường" if attack == "none" else "Đang phát traffic, tấn công đã bắt đầu"
        elif scenario["proc"].returncode == 0:
            stage = "Đã phát xong"
        else:
            stage = "Simulator dừng do lỗi hoặc bị dừng"
        watching_over = scenario["ended"] and time.monotonic() - scenario["ended"] > WATCH_AFTER_SIM_S
        if attack == "none":
            ok = scenario["new_alerts"] <= MAX_FALSE_POSITIVES
            verdict = (f"{scenario['new_alerts']} lần báo nhầm "
                       f"({'trong mức cho phép' if ok else 'vượt mức cho phép'} ≤ {MAX_FALSE_POSITIVES})")
        elif scenario["latency_s"] is not None:
            ok = True
            verdict = f"Có cảnh báo sau {scenario['latency_s']:.0f} giây kể từ khi tấn công bắt đầu"
        else:
            ok = None if not watching_over else False
            verdict = "Chưa thấy cảnh báo cho các IP tấn công" if watching_over else "Đang chờ cảnh báo…"
        elapsed = round((_now() - scenario["started"]).total_seconds())
        return {"attack": attack, "label": SCENARIOS[attack][0], "running": running, "stage": stage,
                "elapsed_s": elapsed, "duration_s": SIM_DURATION_S,
                "published": data.get("published_records", 0),
                "total": data.get("normal_records", 0) + data.get("attack_records", 0),
                "attack_records": data.get("attack_records", 0), "attack_ips": len(data.get("attack_ips", [])),
                "new_alerts": scenario["new_alerts"], "flagged_ips": scenario["flagged_ips"], "latency_s": scenario["latency_s"],
                "ok": ok, "verdict": verdict}

    def check_status(self) -> dict:
        check = self.check
        view = None
        if check:
            result = check["result"]
            view = {"attack": check["attack"], "label": SCENARIOS[check["attack"]][0],
                    "running": self.job == "check", "cleaning": bool(check.get("cleaning")),
                    "step": self._check_step(check),
                    "elapsed_s": round(check.get("finished", time.monotonic()) - check["started"]),
                    "result": result if result else None}
            if self.job == "check" and not result:
                view["tail"] = _tail(check["log"], 3)
        history = []
        try:
            history = [json.loads(line) for line in CHECKS_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
        except (OSError, json.JSONDecodeError):
            pass
        for item in history:
            item["label"] = SCENARIOS.get(item.get("attack"), (item.get("attack"),))[0]
        return {"job": self.job, "current": view, "history": history[-10:][::-1]}


runner = Runner()
