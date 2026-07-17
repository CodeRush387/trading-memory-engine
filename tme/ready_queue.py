from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .database import Database

QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ready_queue (
 id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT NOT NULL UNIQUE,
 source_sequence INTEGER NOT NULL UNIQUE, wallet TEXT NOT NULL, coin TEXT NOT NULL,
 kind TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'READY',
 attempts INTEGER NOT NULL DEFAULT 0, available_at_ms INTEGER NOT NULL,
 lease_until_ms INTEGER, consumer TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 acknowledged_at TEXT
);
CREATE INDEX IF NOT EXISTS ready_queue_delivery ON ready_queue(status,available_at_ms,lease_until_ms,id);
CREATE TABLE IF NOT EXISTS processor_offsets (
 processor TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS processor_assets (
 processor TEXT NOT NULL, wallet TEXT NOT NULL, coin TEXT NOT NULL,
 signed_size TEXT NOT NULL DEFAULT '0', capital TEXT NOT NULL DEFAULT '0',
 share TEXT NOT NULL DEFAULT '0', last_sequence INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(processor,wallet,coin)
);
CREATE TABLE IF NOT EXISTS processor_wallet_runtime (
 processor TEXT NOT NULL, wallet TEXT NOT NULL, held_asset TEXT, held_side TEXT,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(processor,wallet)
);
"""

class ReadyQueue:
    def __init__(self, db: Database):
        self.db = db
        with db.transaction() as con: con.executescript(QUEUE_SCHEMA)

    def publish(self, con: Any, source_sequence: int, wallet: str, coin: str, kind: str, payload: dict[str, Any]) -> str:
        message_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"tme:{source_sequence}"))
        con.execute("INSERT OR IGNORE INTO ready_queue(message_id,source_sequence,wallet,coin,kind,payload,available_at_ms) VALUES(?,?,?,?,?,?,?)",
            (message_id,source_sequence,wallet,coin,kind,json.dumps(payload,separators=(",",":")),int(time.time()*1000)))
        return message_id

    def claim(self, consumer: str, lease_ms: int = 30_000) -> dict[str, Any] | None:
        now=int(time.time()*1000)
        with self.db.transaction() as con:
            row=con.execute("SELECT * FROM ready_queue WHERE available_at_ms<=? AND (status='READY' OR (status='INFLIGHT' AND lease_until_ms<=?)) ORDER BY id LIMIT 1",(now,now)).fetchone()
            if not row: return None
            lease=now+max(1000,lease_ms)
            con.execute("UPDATE ready_queue SET status='INFLIGHT',consumer=?,lease_until_ms=?,attempts=attempts+1 WHERE id=?",(consumer,lease,row["id"]))
            result=dict(row); result.update(status="INFLIGHT",consumer=consumer,lease_until_ms=lease); result["payload"]=json.loads(result["payload"])
            return result

    def ack(self, message_id: str, consumer: str) -> bool:
        with self.db.transaction() as con:
            cur=con.execute("UPDATE ready_queue SET status='ACKED',acknowledged_at=CURRENT_TIMESTAMP,lease_until_ms=NULL WHERE message_id=? AND consumer=? AND status='INFLIGHT'",(message_id,consumer))
            return cur.rowcount==1

    def nack(self, message_id: str, consumer: str, delay_ms: int = 1000) -> bool:
        with self.db.transaction() as con:
            cur=con.execute("UPDATE ready_queue SET status='READY',available_at_ms=?,lease_until_ms=NULL,consumer=NULL WHERE message_id=? AND consumer=? AND status='INFLIGHT'",(int(time.time()*1000)+max(0,delay_ms),message_id,consumer))
            return cur.rowcount==1

    def renew(self,message_id:str,consumer:str,lease_ms:int=90_000)->bool:
        now=int(time.time()*1000)
        with self.db.transaction() as con:
            cur=con.execute("UPDATE ready_queue SET lease_until_ms=? WHERE message_id=? AND consumer=? AND status='INFLIGHT' AND lease_until_ms>?",(now+max(1000,lease_ms),message_id,consumer,now))
            return cur.rowcount==1

    def depth(self) -> dict[str,int]:
        rows=self.db.rows("SELECT status,COUNT(*) count FROM ready_queue GROUP BY status")
        result={"READY":0,"INFLIGHT":0,"ACKED":0}; result.update({r["status"]:r["count"] for r in rows}); return result
    def quarantine_pending(self)->int:
        with self.db.transaction() as con:
            cur=con.execute("UPDATE ready_queue SET status='QUARANTINED',lease_until_ms=NULL,consumer=NULL WHERE status IN ('READY','INFLIGHT')")
            return cur.rowcount

    def health(self) -> dict[str,Any]:
        now=int(time.time()*1000); depth=self.depth()
        row=self.db.connection().execute("SELECT MIN(available_at_ms) oldest FROM ready_queue WHERE status IN ('READY','INFLIGHT')").fetchone()
        journal=self.db.connection().execute("SELECT COALESCE(MAX(sequence),0) n FROM event_journal").fetchone()["n"]
        offset=self.db.connection().execute("SELECT COALESCE(MAX(last_sequence),0) n FROM processor_offsets").fetchone()["n"]
        return {"depth":depth,"oldest_age_ms":max(0,now-int(row["oldest"])) if row and row["oldest"] is not None else 0,"sequence_lag":max(0,int(journal)-int(offset))}
