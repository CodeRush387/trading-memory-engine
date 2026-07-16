from __future__ import annotations
import asyncio,logging,os
from decimal import Decimal
from .collector import Collector
from .database import Database
from .engine import MemoryEngine
from .hyperliquid import HyperliquidAdapter

log=logging.getLogger("tme.collector")
ACTIVE={"DISCOVERING","SYNCING","READY","LIVE"}
class HyperliquidCollectorService:
    def __init__(self,db:Database,adapter:HyperliquidAdapter,refresh_seconds:float=1,dexes:tuple[str|None,...]=(None,)):
        self.db=db;self.adapter=adapter;self.collector=Collector(MemoryEngine(db));self.refresh_seconds=refresh_seconds;self.dexes=dexes;self.bootstrapped=set();self.stop=asyncio.Event()
    def wallets(self)->set[str]:return {r["address"] for r in self.db.rows("SELECT address FROM wallets WHERE status IN ('DISCOVERING','SYNCING','READY','LIVE')")}
    async def bootstrap(self,wallet:str)->None:
        with self.db.transaction() as con:con.execute("UPDATE wallets SET status='SYNCING',recovery_status='RECOVERING' WHERE address=?",(wallet,))
        fills,state=await asyncio.gather(asyncio.to_thread(self.adapter.historical_fills,wallet),asyncio.to_thread(self.adapter.current_state,wallet,self.dexes))
        for raw in fills:
            try:self.collector.accept_fill(self.adapter.canonical(wallet,raw))
            except RuntimeError as exc:
                if "wallet is" not in str(exc):raise
        self.collector.engine.snapshot(wallet)
        result=self.collector.accept_state(wallet,state)
        if result["mismatches"]:log.warning("wallet=%s startup mismatch=%s",wallet,result["mismatches"])
        else:
            self.collector.engine.command_wallet(wallet,"LIVE");self.bootstrapped.add(wallet);log.info("wallet=%s bootstrap complete fills=%s",wallet,len(fills))
    async def run_subscription(self,wallets:set[str])->None:
        queue=asyncio.Queue()
        async def reader():
            async for item in self.adapter.trades(sorted(wallets)):await queue.put(item)
        task=asyncio.create_task(reader(),name="hyperliquid-grpc-reader")
        try:
            for wallet in sorted(wallets-self.bootstrapped):await self.bootstrap(wallet)
            while not self.stop.is_set():
                current=self.wallets()
                if current!=wallets:return
                try:wallet,raw=await asyncio.wait_for(queue.get(),timeout=self.refresh_seconds)
                except asyncio.TimeoutError:continue
                if wallet in current and "px" in raw and "sz" in raw:self.collector.accept_fill(self.adapter.canonical(wallet,raw))
        finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
    async def run(self)->None:
        backoff=1.0
        while not self.stop.is_set():
            wallets=self.wallets()
            self.bootstrapped.intersection_update(wallets)
            if not wallets:await asyncio.sleep(self.refresh_seconds);continue
            try:await self.run_subscription(wallets);backoff=1.0
            except asyncio.CancelledError:raise
            except Exception:log.exception("collector stream failed; reconnecting in %.1fs",backoff);await asyncio.sleep(backoff);backoff=min(backoff*2,30)

def from_env(db:Database)->HyperliquidCollectorService|None:
    endpoint=os.getenv("HC_GRPC_ENDPOINT","").strip();token=os.getenv("HC_GRPC_TOKEN","").strip()
    if not endpoint or not token:return None
    server=os.getenv("HC_GRPC_SERVER_NAME","").strip() or endpoint.rsplit(":",1)[0];info=os.getenv("QUICKNODE_INFO_URL","").strip() or f"https://{server}/{token}/info"
    raw_dexes=os.getenv("TME_PERP_DEXES","").strip();dexes=(None,)+tuple(x.strip() for x in raw_dexes.split(",") if x.strip())
    return HyperliquidCollectorService(db,HyperliquidAdapter(endpoint,token,server,info),float(os.getenv("TME_WALLET_REFRESH_SECONDS","1")),dexes)