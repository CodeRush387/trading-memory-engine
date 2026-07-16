from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from .database import Database
from .models import D, Fill


class MemoryEngine:
    def __init__(self, db: Database):
        self.db = db

    def add_wallet(self, address: str, label: str = "") -> dict[str, Any]:
        address = address.lower()
        with self.db.transaction() as c:
            c.execute("INSERT INTO wallets(address,label,status) VALUES(?,?,'DISCOVERING') "
                      "ON CONFLICT(address) DO UPDATE SET label=excluded.label, updated_at=CURRENT_TIMESTAMP", (address, label))
        return self.wallet(address)

    def command_wallet(self, address: str, command: str) -> dict[str, Any] | None:
        address, command = address.lower(), command.upper()
        mapping = {"PAUSE": "PAUSED", "RESUME": "LIVE", "ARCHIVE": "ARCHIVED",
                   "DISCOVER": "DISCOVERING", "SYNC": "SYNCING", "READY": "READY", "LIVE": "LIVE"}
        if command == "REMOVE":
            with self.db.transaction() as c:
                c.execute("UPDATE wallets SET status='REMOVED', updated_at=CURRENT_TIMESTAMP WHERE address=?", (address,))
            return self.wallet(address)
        if command == "PURGE":
            # Journal stays immutable. Purge removes derived state and archives identity.
            with self.db.transaction() as c:
                c.execute("DELETE FROM projections WHERE wallet=?", (address,))
                c.execute("DELETE FROM snapshots WHERE wallet=?", (address,))
                c.execute("UPDATE wallets SET status='PURGED', updated_at=CURRENT_TIMESTAMP WHERE address=?", (address,))
            return self.wallet(address)
        if command not in mapping:
            raise ValueError(f"unsupported wallet command: {command}")
        with self.db.transaction() as c:
            c.execute("UPDATE wallets SET status=?, updated_at=CURRENT_TIMESTAMP WHERE address=?", (mapping[command], address))
        return self.wallet(address)

    def wallet(self, address: str) -> dict[str, Any] | None:
        rows = self.db.rows("SELECT * FROM wallets WHERE address=?", (address.lower(),))
        return rows[0] if rows else None

    def ingest_fill(self, raw: dict[str, Any]) -> dict[str, Any]:
        fill = Fill.parse(raw)
        with self.db.transaction() as c:
            w = c.execute("SELECT status FROM wallets WHERE address=?", (fill.wallet,)).fetchone()
            if not w:
                raise KeyError("wallet is not registered")
            if w["status"] in {"PAUSED", "ARCHIVED", "REMOVED", "PURGED"}:
                raise RuntimeError(f"wallet is {w['status']}")
            exists = c.execute("SELECT sequence FROM event_journal WHERE event_id=?", (fill.event_id,)).fetchone()
            if exists:
                return {"accepted": False, "duplicate": True, "sequence": exists["sequence"]}
            cur = c.execute("INSERT INTO event_journal(event_id,wallet,coin,event_type,event_time_ms,payload) "
                            "VALUES(?,?,?,'FILL',?,?)", (fill.event_id, fill.wallet, fill.coin,
                            fill.timestamp_ms, self.db.encode(fill.dict())))
            sequence = cur.lastrowid
            self._project(c, fill, sequence)
            c.execute("UPDATE wallets SET last_fill_ms=?, status=CASE WHEN status IN ('DISCOVERING','SYNCING','READY') "
                      "THEN 'LIVE' ELSE status END, updated_at=CURRENT_TIMESTAMP WHERE address=?",
                      (fill.timestamp_ms, fill.wallet))
        return {"accepted": True, "duplicate": False, "sequence": sequence,
                "projection": self.projection(fill.wallet, fill.coin)}

    def _project(self, c: Any, f: Fill, seq: int) -> None:
        row = c.execute("SELECT * FROM projections WHERE wallet=? AND coin=?", (f.wallet, f.coin)).fetchone()
        old = D(row["size"]) if row else D(0)
        delta = D(f.size) if f.side == "BUY" else -D(f.size)
        new = old + delta
        lifecycle = int(row["lifecycle_id"]) if row else 0
        old_avg = D(row["average_price"]) if row and row["average_price"] else D(0)
        opened = row["opened_at_ms"] if row else None
        realized = D(row["realized_pnl"]) if row else D(0)
        if old == 0 or (old > 0) != (new > 0) and new != 0:
            lifecycle += 1
            avg, opened = D(f.price), f.timestamp_ms
        elif new == 0:
            avg = None
        elif (old > 0) == (delta > 0):
            avg = ((abs(old) * old_avg) + (abs(delta) * D(f.price))) / abs(new)
        else:
            avg = old_avg if (old > 0) == (new > 0) else D(f.price)
            closed = min(abs(old), abs(delta))
            realized += closed * (D(f.price) - old_avg) * (D(1) if old > 0 else D(-1))
        status = "OPEN" if new else "CLOSED"
        side = "LONG" if new > 0 else "SHORT" if new < 0 else None
        capital = abs(new) * (avg or D(0))
        c.execute("INSERT INTO projections(wallet,coin,lifecycle_id,status,side,size,average_price,capital,realized_pnl,opened_at_ms,updated_at_ms,last_sequence) "
                  "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(wallet,coin) DO UPDATE SET "
                  "lifecycle_id=excluded.lifecycle_id,status=excluded.status,side=excluded.side,size=excluded.size,"
                  "average_price=excluded.average_price,capital=excluded.capital,realized_pnl=excluded.realized_pnl,"
                  "opened_at_ms=excluded.opened_at_ms,updated_at_ms=excluded.updated_at_ms,last_sequence=excluded.last_sequence",
                  (f.wallet, f.coin, lifecycle, status, side, str(new), str(avg) if avg is not None else None,
                   str(capital), str(realized), opened, f.timestamp_ms, seq))
        c.execute("INSERT OR IGNORE INTO lifecycle_fills(wallet,coin,lifecycle_id,sequence) VALUES(?,?,?,?)",
                  (f.wallet, f.coin, lifecycle, seq))

    def projection(self, wallet: str, coin: str | None = None) -> Any:
        if coin:
            rows = self.db.rows("SELECT * FROM projections WHERE wallet=? AND coin=?", (wallet.lower(), coin.upper()))
            return rows[0] if rows else None
        return self.db.rows("SELECT * FROM projections WHERE wallet=? ORDER BY capital DESC", (wallet.lower(),))

    def raw(self, wallet: str, coin: str | None = None, lifecycle: int | None = None,
            limit: int = 500, after: int = 0) -> list[dict[str, Any]]:
        sql = "SELECT j.sequence,j.event_id,j.wallet,j.coin,j.event_type,j.event_time_ms,j.received_at,j.payload"
        args: list[Any] = []
        if lifecycle is not None:
            sql += " FROM event_journal j JOIN lifecycle_fills l ON l.sequence=j.sequence WHERE l.wallet=? AND l.lifecycle_id=?"
            args += [wallet.lower(), lifecycle]
        else:
            sql += " FROM event_journal j WHERE j.wallet=?"
            args += [wallet.lower()]
        if coin:
            sql += " AND j.coin=?"; args.append(coin.upper())
        sql += " AND j.sequence>? ORDER BY j.sequence LIMIT ?"; args += [after, min(limit, 5000)]
        rows = self.db.rows(sql, tuple(args))
        for row in rows:
            row["payload"] = json.loads(row["payload"])
        return rows

    def snapshot(self, wallet: str) -> dict[str, Any]:
        state = self.projection(wallet)
        last = max((int(x["last_sequence"]) for x in state), default=0)
        with self.db.transaction() as c:
            cur = c.execute("INSERT INTO snapshots(wallet,last_sequence,state) VALUES(?,?,?)",
                            (wallet.lower(), last, self.db.encode(state)))
        return {"id": cur.lastrowid, "wallet": wallet.lower(), "last_sequence": last}

    def recover(self, wallet: str, authoritative: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        wallet = wallet.lower()
        with self.db.transaction() as c:
            c.execute("UPDATE wallets SET recovery_status='RECOVERING', status='SYNCING' WHERE address=?", (wallet,))
            snap = c.execute("SELECT * FROM snapshots WHERE wallet=? ORDER BY id DESC LIMIT 1", (wallet,)).fetchone()
            if snap:
                c.execute("DELETE FROM projections WHERE wallet=?", (wallet,))
                for p in json.loads(snap["state"]):
                    cols = "wallet,coin,lifecycle_id,status,side,size,average_price,capital,realized_pnl,opened_at_ms,updated_at_ms,last_sequence"
                    c.execute(f"INSERT INTO projections({cols}) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", tuple(p[k] for k in cols.split(',')))
            start = int(snap["last_sequence"]) if snap else 0
            events = c.execute("SELECT sequence,payload FROM event_journal WHERE wallet=? AND sequence>? ORDER BY sequence", (wallet, start)).fetchall()
            for event in events:
                self._project(c, Fill.parse(json.loads(event["payload"])), event["sequence"])
            # Authoritative current state is accepted as a reconciliation signal, not as synthetic history.
            mismatches = self._compare_authoritative(c, wallet, authoritative or [])
            status = "READY" if not mismatches else "SYNCING"
            recovery = "HEALTHY" if not mismatches else "MISMATCH"
            c.execute("UPDATE wallets SET status=?, recovery_status=?, last_error=? WHERE address=?",
                      (status, recovery, self.db.encode(mismatches) if mismatches else None, wallet))
        return {"wallet": wallet, "snapshot_sequence": start, "replayed": len(events),
                "status": status, "mismatches": mismatches}

    def _compare_authoritative(self, c: Any, wallet: str, states: list[dict[str, Any]]) -> list[dict[str, Any]]:
        mismatches = []
        for state in states:
            row = c.execute("SELECT size FROM projections WHERE wallet=? AND coin=?", (wallet, state["coin"].upper())).fetchone()
            actual = D(row["size"]) if row else D(0)
            expected = D(str(state["size"]))
            if actual != expected:
                mismatches.append({"coin": state["coin"].upper(), "projection": str(actual), "authoritative": str(expected)})
        return mismatches

    def report(self) -> dict[str, Any]:
        wallets = self.db.rows("SELECT * FROM wallets ORDER BY created_at")
        positions = self.db.rows("SELECT * FROM projections WHERE status='OPEN'")
        for w in wallets:
            ps = [p for p in positions if p["wallet"] == w["address"]]
            w["capital"] = str(sum((D(p["capital"]) for p in ps), D(0)))
            w["current_positions"] = len(ps)
            w["coverage"] = self._coverage(w["address"])
        dominant = max(positions, key=lambda p: D(p["capital"]), default=None)
        active = max(wallets, key=lambda w: w["last_fill_ms"] or 0, default=None)
        return {"wallets": wallets, "market": {"dominant_coin": dominant["coin"] if dominant else None,
                "most_active_wallet": active["address"] if active else None,
                "largest_exposure": dominant, "top_long": self._top(positions, "LONG"),
                "top_short": self._top(positions, "SHORT")}}

    def _top(self, positions: list[dict[str, Any]], side: str) -> dict[str, Any] | None:
        return max((p for p in positions if p["side"] == side), key=lambda p: D(p["capital"]), default=None)

    def _coverage(self, wallet: str) -> dict[str, Any]:
        rows = self.db.rows("SELECT COUNT(*) events, MIN(sequence) first_sequence, MAX(sequence) last_sequence FROM event_journal WHERE wallet=?", (wallet,))
        return {**rows[0], "missing_events": 0, "health": self.wallet(wallet)["recovery_status"]}

