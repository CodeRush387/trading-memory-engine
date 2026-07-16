import tempfile,unittest
from decimal import Decimal
from pathlib import Path
from tme.database import Database
from tme.engine import MemoryEngine
from tme.processing import ProcessingEngine

A="0x1111111111111111111111111111111111111111"; B="0x2222222222222222222222222222222222222222"
class ProcessingIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.path=str(Path(self.tmp.name)/"tme.db"); self.db=Database(self.path); self.memory=MemoryEngine(self.db); self.memory.add_wallet(A); self.memory.add_wallet(B); self.processor=ProcessingEngine(self.db)
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def fill(self,wallet,coin,px,sz,side,time,start,tid):
        raw={"coin":coin,"px":str(px),"sz":str(sz),"side":side,"time":time,"startPosition":str(start),"tid":tid,"oid":tid,"dir":"test"}
        return self.memory.ingest_fill({"wallet":wallet,"coin":coin,"price":str(px),"size":str(sz),"side":"BUY" if side=="B" else "SELL","timestamp_ms":time,"event_id":"tid:"+tid,"order_id":tid,"raw":raw})
    def make_signal(self,wallet,prefix):
        self.fill(wallet,"BTC",100,1,"B",500,0,prefix+"b")
        self.fill(wallet,"ETH",100,1,"B",1000,0,prefix+"1")
        self.fill(wallet,"ETH",120,1,"B",3000,1,prefix+"2")
        self.fill(wallet,"ETH",180,1,"B",4000,2,prefix+"3")
    def test_fill_journal_projection_and_exact_ready_intent(self):
        self.make_signal(A,"a"); self.assertEqual(self.processor.process_available(),4)
        item=self.processor.queue.claim("hrs"); self.assertEqual(item["kind"],"ENTRY"); self.assertEqual(item["wallet"],A); self.assertEqual(item["payload"]["asset"],"ETH"); self.assertEqual(item["payload"]["evidence"]["velocity"],"180"); self.assertEqual(item["payload"]["evidence"]["acceleration"],"120")
        self.assertEqual(self.memory.projection(A,"ETH")["size"],"3")
    def test_wallets_are_isolated_and_dynamic_pause_resume(self):
        self.make_signal(A,"a"); self.make_signal(B,"b"); self.processor.process_available(); first=self.processor.queue.claim("hrs"); self.processor.queue.ack(first["message_id"],"hrs"); second=self.processor.queue.claim("hrs")
        self.assertEqual({first["wallet"],second["wallet"]},{A,B})
        self.memory.command_wallet(A,"PAUSE"); self.assertEqual(self.db.rows("SELECT status FROM wallets WHERE address=?",(A,))[0]["status"],"PAUSED"); self.memory.command_wallet(A,"RESUME"); self.assertEqual(self.db.rows("SELECT status FROM wallets WHERE address=?",(A,))[0]["status"],"LIVE")
    def test_restart_does_not_duplicate_decision(self):
        self.make_signal(A,"a"); self.processor.process_available(); self.db.close(); db2=Database(self.path); restarted=ProcessingEngine(db2); self.assertEqual(restarted.process_available(),0); self.assertEqual(restarted.queue.depth()["READY"],1); db2.close()
    def test_held_feedback_changes_entry_to_rotation(self):
        self.processor.set_held(A,"BTC","LONG"); self.make_signal(A,"a"); self.processor.process_available(); item=self.processor.queue.claim("hrs"); self.assertEqual(item["payload"]["action"],"ROTATE")
if __name__=="__main__":unittest.main()