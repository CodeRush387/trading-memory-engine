import importlib.util
import os
import sys
import unittest
from decimal import Decimal
from pathlib import Path

HRS_ROOT = Path(os.getenv("HRS_PROJECT_PATH", r"C:\Users\microsoft\hyperliquid-copy-bot"))
os.environ.setdefault("HRS_LEADER_ADDRESSES", "0x1111111111111111111111111111111111111111")
os.environ.setdefault("HC_GRPC_ENDPOINT", "example.com:443")
os.environ.setdefault("HC_GRPC_TOKEN", "test-token")
os.environ.setdefault("HC_GRPC_SERVER_NAME", "example.com")
os.environ.setdefault("HRS_EXECUTION_ENABLED", "false")
sys.path.insert(0, str(HRS_ROOT))
spec = importlib.util.spec_from_file_location("reference_hrs", HRS_ROOT / "hrs.py")
reference = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = reference
spec.loader.exec_module(reference)

from tme.hrs_processing import Fill, ProcessingCore

WALLET = "0x1111111111111111111111111111111111111111"

def raw(coin, price, size, side, timestamp, start, tid):
    return {"coin":coin,"px":str(price),"sz":str(size),"side":side,"time":timestamp,
            "startPosition":str(start),"tid":tid,"oid":tid,"dir":"test"}

def reference_fill(data): return reference.Fill.from_raw(WALLET, data)
def candidate_fill(data): return Fill.from_raw(WALLET, data)

def lifecycle_view(item):
    return {"status":item.status.value,"direction":item.direction,"final_size":item.final_size,
            "capital":item.current_capital,"velocity":item.velocity,"acceleration":item.acceleration}

class HrsParityTests(unittest.TestCase):
    def setUp(self):
        self.full=[raw("BTC",100,1,"B",1000,0,"1"),raw("BTC",120,1,"B",3000,1,"2"),raw("BTC",180,1,"B",4000,2,"3"),raw("BTC",200,1,"A",5000,3,"4")]
        self.partial=[raw("ETH",100,1,"B",1000,1,"e1"),raw("ETH",120,1,"B",3000,2,"e2"),raw("ETH",180,1,"B",4000,3,"e3")]

    def compare_lifecycle(self, coin, values, size, capital=None):
        old=reference.reconstruct_lifecycle(coin,[reference_fill(x) for x in values],Decimal(size),Decimal(capital) if capital else None)
        new=ProcessingCore.reconstruct_lifecycle(coin,[candidate_fill(x) for x in values],Decimal(size),Decimal(capital) if capital else None)
        self.assertEqual(lifecycle_view(new),lifecycle_view(old))

    def test_full_lifecycle_capital_velocity_acceleration(self): self.compare_lifecycle("BTC",self.full,"2")
    def test_partial_lifecycle(self): self.compare_lifecycle("ETH",self.partial,"4","500")
    def test_insufficient_lifecycle(self): self.compare_lifecycle("ETH",self.partial[:1],"2","350")

    def test_wallet_share_challenger_and_decision(self):
        old=reference.WalletState(wallet=WALLET,snapshot_positions={"BTC":Decimal("2"),"ETH":Decimal("4")},snapshot_capital={"BTC":Decimal("400"),"ETH":Decimal("500")},fills_by_coin={"BTC":[reference_fill(x) for x in self.full],"ETH":[reference_fill(x) for x in self.partial]})
        new=ProcessingCore.wallet_state(WALLET,{"BTC":Decimal("2"),"ETH":Decimal("4")},{"BTC":Decimal("400"),"ETH":Decimal("500")},{"BTC":[candidate_fill(x) for x in self.full],"ETH":[candidate_fill(x) for x in self.partial]})
        previous={"BTC":Decimal("0.6"),"ETH":Decimal("0.4")}; reference.rebuild_wallet(old,previous=previous); ProcessingCore.rebuild_wallet(new,previous=previous)
        trigger_old=old.lifecycles["ETH"].latest_fill; trigger_new=new.lifecycles["ETH"].latest_fill
        self.assertEqual({k:v.share for k,v in new.lifecycles.items()},{k:v.share for k,v in old.lifecycles.items()})
        self.assertEqual([x.coin for x in ProcessingCore.leaderboard(new,trigger_new)],[x.coin for x in reference.leaderboard(old,trigger_old)])
        self.assertEqual(ProcessingCore.evaluate(new,trigger_new,Decimal("3")).decision.value,ProcessingCore.reference_decision(old,trigger_old,Decimal("3"),reference))

if __name__ == "__main__": unittest.main()