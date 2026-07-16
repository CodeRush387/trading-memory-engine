from __future__ import annotations

import json
import mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .engine import MemoryEngine


class APIHandler(BaseHTTPRequestHandler):
    engine: MemoryEngine
    web_root: Path

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[API] {self.address_string()} {fmt % args}")

    def _json(self, status: int, value: object) -> None:
        body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store")
        self.end_headers(); self.wfile.write(body)

    def _body(self) -> dict:
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")

    def do_GET(self) -> None:
        try:
            u, q = urlparse(self.path), parse_qs(urlparse(self.path).query)
            parts = [p for p in u.path.split("/") if p]
            if u.path in {"/", "/index.html"}:
                return self._file("index.html")
            if u.path == "/health":
                return self._json(200, {"status": "ok", "service": "trading-memory-engine", "version": "1.0.0"})
            if u.path == "/v1/report": return self._json(200, self.engine.report())
            if u.path == "/v1/wallets": return self._json(200, self.engine.db.rows("SELECT * FROM wallets"))
            if len(parts) == 4 and parts[:2] == ["v1", "projection"]:
                return self._json(200, self.engine.projection(parts[2], parts[3]))
            if len(parts) == 3 and parts[:2] == ["v1", "projection"]:
                return self._json(200, self.engine.projection(parts[2]))
            if len(parts) == 3 and parts[:2] == ["v1", "raw"]:
                return self._json(200, self.engine.raw(parts[2], q.get("coin", [None])[0],
                    int(q["lifecycle"][0]) if "lifecycle" in q else None,
                    int(q.get("limit", [500])[0]), int(q.get("after", [0])[0])))
            self._json(404, {"error": "not_found"})
        except Exception as e: self._json(400, {"error": type(e).__name__, "message": str(e)})

    def do_POST(self) -> None:
        try:
            parts = [p for p in urlparse(self.path).path.split("/") if p]
            body = self._body()
            if parts == ["v1", "wallets"]: return self._json(201, self.engine.add_wallet(body["address"], body.get("label", "")))
            if parts == ["v1", "events", "fills"]: return self._json(201, self.engine.ingest_fill(body))
            if len(parts) == 4 and parts[:2] == ["v1", "wallets"]: return self._json(200, self.engine.command_wallet(parts[2], parts[3]))
            if len(parts) == 4 and parts[:2] == ["v1", "recovery"] and parts[3] == "run":
                return self._json(200, self.engine.recover(parts[2], body.get("current_state")))
            if len(parts) == 4 and parts[:2] == ["v1", "snapshots"] and parts[3] == "create":
                return self._json(201, self.engine.snapshot(parts[2]))
            self._json(404, {"error": "not_found"})
        except Exception as e: self._json(400, {"error": type(e).__name__, "message": str(e)})

    def _file(self, name: str) -> None:
        path = self.web_root / name
        if not path.is_file(): return self._json(404, {"error": "not_found"})
        body = path.read_bytes(); self.send_response(200)
        self.send_header("Content-Type", mimetypes.guess_type(path)[0] or "application/octet-stream")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)


def serve(engine: MemoryEngine, host: str, port: int, web_root: Path) -> None:
    handler = type("BoundAPIHandler", (APIHandler,), {"engine": engine, "web_root": web_root})
    print(f"Trading Memory Engine listening on http://{host}:{port}")
    ThreadingHTTPServer((host, port), handler).serve_forever()

