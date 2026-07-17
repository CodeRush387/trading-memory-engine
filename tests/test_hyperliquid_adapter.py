import asyncio,tempfile,unittest
from pathlib import Path
from tme.database import Database
from tme.engine import MemoryEngine
from tme.collector_service import HyperliquidCollectorService
from tme.hyperliquid.adapter import HyperliquidAdapter

W="0x1111111111111111111111111111111111111111"
def raw(tid="1",start="0",size="1"):
    return {"coin":"BTC","px":"100","sz":size,"side":"B","time":1000,"startPosition":start,"tid":tid,"oid":tid,"dir":"Open Long"}
class FakeAdapter:
    def historical_fills(self,wallet):return [raw()]
    def current_state(self,wallet,dexes=(None,)):return [{"coin":"BTC","size":"1","entry_price":"100"}]
    @staticmethod
    def canonical(wallet,value):return HyperliquidAdapter.canonical(wallet,value)
class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def test_canonical_preserves_raw_and_start_position(self):
        event=HyperliquidAdapter.canonical(W,raw());self.assertEqual(event["event_id"],"tid:1");self.assertEqual(event["side"],"BUY");self.assertEqual(event["raw"]["startPosition"],"0")
    def test_stream_payload_variants(self):
        import json
        payload=json.dumps({"events":[[W,raw()],{"user":W,"data":raw("2")} ]});items=HyperliquidAdapter.stream_events(payload);self.assertEqual([x[1]["tid"] for x in items],["1","2"])
    async def test_bootstrap_enters_journal_projection_and_live(self):
        tmp=tempfile.TemporaryDirectory();db=Database(str(Path(tmp.name)/"db.sqlite"));MemoryEngine(db).add_wallet(W);service=HyperliquidCollectorService(db,FakeAdapter());await service.bootstrap(W)
        self.assertEqual(db.rows("SELECT COUNT(*) n FROM event_journal")[0]["n"],1);self.assertEqual(MemoryEngine(db).projection(W,"BTC")["size"],"1");self.assertEqual(MemoryEngine(db).wallet(W)["status"],"LIVE");db.close();tmp.cleanup()
    async def test_stream_failure_forces_all_wallets_to_reconcile(self):
        tmp=tempfile.TemporaryDirectory();db=Database(str(Path(tmp.name)/"db.sqlite"));MemoryEngine(db).add_wallet(W)
        service=HyperliquidCollectorService(db,FakeAdapter(),refresh_seconds=.01);service.bootstrapped.add(W);calls=[]
        async def subscription(wallets):
            calls.append(set(service.bootstrapped))
            if len(calls)==1: raise RuntimeError("stream disconnected")
            service.stop.set()
        service.run_subscription=subscription
        await asyncio.wait_for(service.run(),2)
        self.assertEqual(calls[0],{W})
        self.assertEqual(calls[1],set())
        row=db.rows("SELECT status,last_error FROM operational_status WHERE component='collector'")[0]
        self.assertEqual(row["status"],"RECONNECTING")
        db.close();tmp.cleanup()

if __name__=="__main__":unittest.main()