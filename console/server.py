"""Trung tam giam sat tieng Viet: server stdlib phuc vu trang demo va API JSON.

Chay tu goc repo:
    python -m console.server            # http://127.0.0.1:8765
    python -m console.server --port 9000

Chi bind 127.0.0.1. Cac phase sau cho server chay tien trinh (stream, simulator),
nen moi request deu qua bo chan Host/Origin (chong DNS rebinding va CSRF tu
trang web khac mo tren cung trinh duyet).
"""
from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .probes import collect_infra

STATIC_DIR = Path(__file__).resolve().parent / "static"
# Whitelist co dinh: khong ghep duong dan tu URL -> khong co path traversal.
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/style.css": ("style.css", "text/css; charset=utf-8"),
}
CONSOLE_HEADER = "X-Console"
MAX_BODY_BYTES = 16 * 1024


class ConsoleHandler(BaseHTTPRequestHandler):
    server_version = "TrungTamGiamSat/1.0"
    allowed_hosts: frozenset[str] = frozenset()

    # --- bo chan chung -----------------------------------------------------

    def _host_ok(self) -> bool:
        return self.headers.get("Host", "") in self.allowed_hosts

    def _origin_ok(self) -> bool:
        origin = self.headers.get("Origin")
        return origin is None or origin in {f"http://{host}" for host in self.allowed_hosts}

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload, status: int = HTTPStatus.OK) -> None:
        self._send(status, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def _error(self, status: int, message: str) -> None:
        self._json({"ok": False, "error": message}, status)

    def _read_json_body(self):
        """Body POST bat buoc la JSON + header X-Console: ca hai ep trinh duyet gui preflight,
        nen trang web la khong gui duoc "simple request" toi day."""
        if self.headers.get(CONSOLE_HEADER) != "1":
            self._error(HTTPStatus.FORBIDDEN, "Thiếu header X-Console")
            return None
        if self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
            self._error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "Chỉ nhận Content-Type: application/json")
            return None
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        if not 0 <= length <= MAX_BODY_BYTES:
            self._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Content-Length không hợp lệ hoặc body quá lớn")
            return None
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self._error(HTTPStatus.BAD_REQUEST, "Body không phải JSON hợp lệ")
            return None

    # --- route --------------------------------------------------------------

    def do_GET(self) -> None:
        if not self._host_ok():
            return self._error(HTTPStatus.FORBIDDEN, "Host không hợp lệ")
        path = self.path.split("?", 1)[0]
        if path == "/api/infra":
            return self._json(collect_infra())
        if path in STATIC_FILES:
            name, content_type = STATIC_FILES[path]
            return self._send(HTTPStatus.OK, (STATIC_DIR / name).read_bytes(), content_type)
        self._error(HTTPStatus.NOT_FOUND, "Không tìm thấy")

    def do_POST(self) -> None:
        if not self._host_ok() or not self._origin_ok():
            return self._error(HTTPStatus.FORBIDDEN, "Host hoặc Origin không hợp lệ")
        body = self._read_json_body()
        if body is None:
            return
        path = self.path.split("?", 1)[0]
        if path == "/api/ping":
            return self._json({"ok": True})
        self._error(HTTPStatus.NOT_FOUND, "Không tìm thấy")

    def do_OPTIONS(self) -> None:
        # Khong tra Access-Control-Allow-*: preflight tu origin khac luon that bai.
        self._error(HTTPStatus.FORBIDDEN, "Không hỗ trợ CORS")

    def log_request(self, code="-", size="-") -> None:
        # Bo log poll /api/* moi vai giay, chi giu request loi de terminal de doc khi demo.
        if str(getattr(code, "value", code)).startswith(("4", "5")):
            super().log_request(code, size)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Trung tam giam sat tieng Viet")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    ConsoleHandler.allowed_hosts = frozenset({f"127.0.0.1:{args.port}", f"localhost:{args.port}"})
    server = ThreadingHTTPServer(("127.0.0.1", args.port), ConsoleHandler)
    server.daemon_threads = True
    print(f"Trung tam giam sat: http://127.0.0.1:{args.port}  (Ctrl+C de dung)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
