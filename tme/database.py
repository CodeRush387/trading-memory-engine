from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS wallets (
  address TEXT PRIMARY KEY, label TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_fill_ms INTEGER, last_error TEXT, recovery_status TEXT NOT NULL DEFAULT 'IDLE'
);
CREATE TABLE IF NOT EXISTS event_journal (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
  wallet TEXT NOT NULL, coin TEXT NOT NULL, event_type TEXT NOT NULL,
  event_time_ms INTEGER NOT NULL, received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  payload TEXT NOT NULL, FOREIGN KEY(wallet) REFERENCES wallets(address)
);
CREATE INDEX IF NOT EXISTS journal_path ON event_journal(wallet, coin, sequence);
CREATE TABLE IF NOT EXISTS projections (
  wallet TEXT NOT NULL, coin TEXT NOT NULL, lifecycle_id INTEGER NOT NULL,
  status TEXT NOT NULL, side TEXT, size TEXT NOT NULL, average_price TEXT,
  capital TEXT NOT NULL, realized_pnl TEXT NOT NULL DEFAULT '0',
  opened_at_ms INTEGER, updated_at_ms INTEGER NOT NULL, last_sequence INTEGER NOT NULL,
  PRIMARY KEY(wallet, coin), FOREIGN KEY(wallet) REFERENCES wallets(address)
);
CREATE TABLE IF NOT EXISTS lifecycle_fills (
  wallet TEXT NOT NULL, coin TEXT NOT NULL, lifecycle_id INTEGER NOT NULL,
  sequence INTEGER NOT NULL UNIQUE, PRIMARY KEY(wallet, coin,lifecycle_id,sequence),
  FOREIGN KEY(sequence) REFERENCES event_journal(sequence)
);
CREATE TABLE IF NOT EXISTS snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT, wallet TEXT NOT NULL, last_sequence INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collector_offsets (
  source TEXT NOT NULL, wallet TEXT NOT NULL, cursor TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(source,wallet)
);
CREATE TABLE IF NOT EXISTS operational_status (
  component TEXT PRIMARY KEY, status TEXT NOT NULL, heartbeat_ms INTEGER NOT NULL,
  details TEXT NOT NULL DEFAULT '{}', last_error TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS wallet_onboarding (
  wallet TEXT PRIMARY KEY, joined_at_ms INTEGER NOT NULL, snapshot TEXT NOT NULL,
  legacy_total INTEGER NOT NULL DEFAULT 0, legacy_remaining INTEGER NOT NULL DEFAULT 0,
  coverage_pct TEXT NOT NULL DEFAULT '0', ready_for_execution INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(wallet) REFERENCES wallets(address)
);
CREATE TABLE IF NOT EXISTS wallet_asset_coverage (
  wallet TEXT NOT NULL, coin TEXT NOT NULL, state TEXT NOT NULL,
  baseline_size TEXT NOT NULL, current_size TEXT NOT NULL, updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY(wallet,coin), FOREIGN KEY(wallet) REFERENCES wallets(address)
);
CREATE INDEX IF NOT EXISTS coverage_state ON wallet_asset_coverage(wallet,state);


CREATE TRIGGER IF NOT EXISTS journal_no_update BEFORE UPDATE ON event_journal
BEGIN SELECT RAISE(ABORT, 'event_journal is append-only'); END;
CREATE TRIGGER IF NOT EXISTS journal_no_delete BEFORE DELETE ON event_journal
BEGIN SELECT RAISE(ABORT, 'event_journal is append-only'); END;
"""


class Database:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._local = threading.local()
        with self.transaction() as con:
            con.executescript(SCHEMA)

    def connection(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(self.path, isolation_level=None, timeout=30)
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("PRAGMA busy_timeout=30000")
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=FULL")
            self._local.con = con
        return con

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        con = self.connection()
        # SQLite has a single writer. Absorb transient writer contention here.
        for attempt in range(8):
            try:
                con.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 7:
                    raise
                time.sleep(min(0.025 * (2**attempt), 0.5))
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise

    def rows(self, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        return [dict(r) for r in self.connection().execute(sql, args).fetchall()]

    def close(self) -> None:
        con = getattr(self._local, "con", None)
        if con is not None:
            con.close()
            self._local.con = None

    @staticmethod
    def encode(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

