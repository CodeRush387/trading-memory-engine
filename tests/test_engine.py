import sqlite3
import tempfile
import unittest
from pathlib import Path

from tme.database import Database
from tme.engine import MemoryEngine


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tmp.name) / "test.db"))
        self.e = MemoryEngine(self.db)
        self.wallet = "0xabc"
        self.e.add_wallet(self.wallet, "Alpha")

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def fill(self, event, side, size, price, ts):
        return self.e.ingest_fill({"wallet": self.wallet, "coin": "BTC", "side": side,
            "size": size, "price": price, "timestamp_ms": ts, "event_id": event})

    def test_weighted_projection_close_and_reversal(self):
        self.fill("1", "BUY", "2", "100", 1)
        self.fill("2", "BUY", "1", "130", 2)
        p = self.e.projection(self.wallet, "BTC")
        self.assertEqual(p["average_price"], "110")
        self.assertEqual(p["capital"], "330")
        self.fill("3", "SELL", "4", "120", 3)
        p = self.e.projection(self.wallet, "BTC")
        self.assertEqual((p["side"], p["size"], p["average_price"]), ("SHORT", "-1", "120"))
        self.assertEqual(p["lifecycle_id"], 2)

    def test_idempotency_and_raw_preservation(self):
        a = self.fill("same", "BUY", "1", "100", 1)
        b = self.fill("same", "BUY", "1", "100", 1)
        self.assertTrue(a["accepted"]); self.assertTrue(b["duplicate"])
        self.assertEqual(len(self.e.raw(self.wallet, "BTC", 1)), 1)

    def test_journal_is_append_only(self):
        self.fill("1", "BUY", "1", "100", 1)
        with self.assertRaises(sqlite3.IntegrityError):
            with self.db.transaction() as c: c.execute("DELETE FROM event_journal")

    def test_snapshot_recovery(self):
        self.fill("1", "BUY", "1", "100", 1)
        snap = self.e.snapshot(self.wallet)
        self.fill("2", "BUY", "1", "120", 2)
        result = self.e.recover(self.wallet, [{"coin": "BTC", "size": "2"}])
        self.assertEqual(result["snapshot_sequence"], snap["last_sequence"])
        self.assertEqual(result["replayed"], 1)
        self.assertEqual(self.e.projection(self.wallet, "BTC")["average_price"], "110")

    def test_pause_and_purge_keep_raw(self):
        self.fill("1", "BUY", "1", "100", 1)
        self.e.command_wallet(self.wallet, "PAUSE")
        with self.assertRaises(RuntimeError): self.fill("2", "BUY", "1", "100", 2)
        self.e.command_wallet(self.wallet, "PURGE")
        self.assertEqual(len(self.e.raw(self.wallet)), 1)
        self.assertIsNone(self.e.projection(self.wallet, "BTC"))


if __name__ == "__main__": unittest.main()

