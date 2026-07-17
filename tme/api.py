from __future__ import annotations

import json
import os
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

    def _queue_authorized(self) -> bool:
        if not self.queue_token: return True
        return self.headers.get("Authorization", "") == f"Bearer {self.queue_token}"

    def _body(self) -> dict:
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")

    def do_GET(self) -> None:
        try:
            u, q = urlparse(self.path), parse_qs(urlparse(self.path).query)
            parts = [p for p in u.path.split("/") if p]
            if u.path in {"/", "/index.html"}:
                return self._file("index.html")
            if u.path == "/health":
                import time
                now=int(time.time()*1000); stale_ms=int(os.getenv("TME_HEARTBEAT_STALE_MS","30000")); components={}
                for row in self.engine.db.rows("SELECT * FROM operational_status"):
                    age=max(0,now-int(row["heartbeat_ms"])); components[row["component"]]={"status":row["status"],"heartbeat_age_ms":age,"last_error":row["last_error"],"details":json.loads(row["details"]),"healthy":age<=stale_ms and row["status"]=="LIVE"}
                required=["processor"]+(["collector"] if self.collector_enabled else [])
                queue=self.processor.queue.health() if self.processor else None
                ok=all(components.get(name,{}).get("healthy",False) for name in required)
                return self._json(200 if ok else 503,{"status":"ok" if ok else "unhealthy","service":"trading-memory-engine","version":"1.0.0","components":components,"queue":queue})
            if u.path == "/v1/report": return self._json(200, self.engine.report())
            if u.path.startswith("/v1/ready/") or u.path == "/v1/processing/health":
                if not self._queue_authorized(): return self._json(401,{"error":"unauthorized"})
            if u.path == "/v1/processing/health" and self.processor:
                return self._json(200, {"status":"ok","queue":self.processor.queue.health()})
            if u.path == "/v1/ready/next" and self.processor:
                consumer=q.get("consumer",[""])[0]
                if not consumer: return self._json(400,{"error":"consumer_required"})
                import time
                deadline=time.monotonic()+min(max(int(q.get("wait",["25"])[0]),0),30)
                while time.monotonic() <= deadline:
                    item=self.processor.queue.claim(consumer,int(q.get("lease_ms",["30000"])[0]))
                    if item: return self._json(200,item)
                    time.sleep(.1)
                return self._json(204,{})
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
            if (parts[:2]==["v1","ready"] or parts==["v1","processing","held"]) and not self._queue_authorized():
                return self._json(401,{"error":"unauthorized"})
            if parts == ["v1", "wallets"]: return self._json(201, self.engine.add_wallet(body["address"], body.get("label", "")))
            if parts == ["v1", "events", "fills"]: return self._json(201, self.engine.ingest_fill(body))
            if len(parts)==4 and parts[:2]==["v1","ready"] and parts[3] in {"ack","nack","renew"} and self.processor:
                if parts[3]=="ack": ok=self.processor.queue.ack(parts[2],body.get("consumer",""))
                elif parts[3]=="nack": ok=self.processor.queue.nack(parts[2],body.get("consumer",""),int(body.get("delay_ms",1000)))
                else: ok=self.processor.queue.renew(parts[2],body.get("consumer",""),int(body.get("lease_ms",90000)))
                return self._json(200 if ok else 409,{"ok":ok})
            if parts==["v1","processing","held"] and self.processor:
                self.processor.set_held(body["wallet"],body.get("asset"),body.get("side")); return self._json(200,{"ok":True})
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


def serve(engine: MemoryEngine, host: str, port: int, web_root: Path, processor: object | None = None, collector_enabled: bool = False) -> None:
    handler = type("BoundAPIHandler", (APIHandler,), {"engine": engine, "web_root": web_root, "processor": processor, "collector_enabled":collector_enabled,"queue_token": os.getenv("TME_QUEUE_TOKEN", "")})
    print(f"Trading Memory Engine listening on http://{host}:{port}")
    ThreadingHTTPServer((host, port), handler).serve_forever()

