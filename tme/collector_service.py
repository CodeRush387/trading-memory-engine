from __future__ import annotations
import asyncio,json,logging,os,time
from decimal import Decimal
from .collector import Collector
from .database import Database
from .engine import MemoryEngine
from .hyperliquid import HyperliquidAdapter

log=logging.getLogger("tme.collector")
ACTIVE={"DISCOVERING","SYNCING","READY","LIVE"}
class HyperliquidCollectorService:
    def __init__(self,db:Database,adapter:HyperliquidAdapter,refresh_seconds:float=1,dexes:tuple[str|None,...]=(None,)):
        self.db=db;self.adapter=adapter;self.collector=Collector(MemoryEngine(db));self.refresh_seconds=refresh_seconds;self.dexes=dexes;self.bootstrapped=set();self.stop=asyncio.Event();self.last_sync_ms=0;self.last_event_ms=0
    def heartbeat(self,status:str="LIVE",error:str|None=None,**details)->None:
        with self.db.transaction() as con:
            details={"last_sync_ms":self.last_sync_ms,"last_event_ms":self.last_event_ms,**details}
            details["last_fill_age_ms"]=max(0,int(time.time()*1000)-self.last_event_ms) if self.last_event_ms else None
            con.execute("INSERT INTO operational_status(component,status,heartbeat_ms,details,last_error) VALUES('collector',?,?,?,?) ON CONFLICT(component) DO UPDATE SET status=excluded.status,heartbeat_ms=excluded.heartbeat_ms,details=excluded.details,last_error=excluded.last_error,updated_at=CURRENT_TIMESTAMP",(status,int(time.time()*1000),json.dumps(details,separators=(",",":")),error))
    def wallets(self)->set[str]:return {r["address"] for r in self.db.rows("SELECT address FROM wallets WHERE status IN ('DISCOVERING','SYNCING','READY','LIVE')")}
    async def bootstrap(self,wallet:str)->None:
        self.heartbeat("SYNCING",wallet=wallet)
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
            self.collector.engine.command_wallet(wallet,"LIVE");self.bootstrapped.add(wallet);self.last_sync_ms=int(time.time()*1000);self.heartbeat("LIVE",wallet=wallet,fills=len(fills));log.info("wallet=%s bootstrap complete fills=%s",wallet,len(fills))
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
                except asyncio.TimeoutError:
                    if task.done(): await task
                    self.heartbeat("LIVE",wallet_count=len(current))
                    continue
                if wallet in current and "px" in raw and "sz" in raw:
                    self.collector.accept_fill(self.adapter.canonical(wallet,raw))
                    self.last_event_ms=int(time.time()*1000)
        finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
    async def run(self)->None:
        backoff=1.0
        while not self.stop.is_set():
            wallets=self.wallets()
            self.bootstrapped.intersection_update(wallets)
            if not wallets:await asyncio.sleep(self.refresh_seconds);continue
            try:await self.run_subscription(wallets);backoff=1.0
            except asyncio.CancelledError:raise
            except Exception as exc:
                self.bootstrapped.difference_update(wallets)
                self.heartbeat("RECONNECTING",error=str(exc),wallet_count=len(wallets),backoff_seconds=backoff)
                log.exception("collector stream failed; reconciling before reconnect in %.1fs",backoff)
                await asyncio.sleep(backoff);backoff=min(backoff*2,30)

def from_env(db:Database)->HyperliquidCollectorService|None:
    endpoint=os.getenv("HC_GRPC_ENDPOINT","").strip();token=os.getenv("HC_GRPC_TOKEN","").strip()
    if not endpoint or not token:return None
    server=os.getenv("HC_GRPC_SERVER_NAME","").strip() or endpoint.rsplit(":",1)[0];info=os.getenv("QUICKNODE_INFO_URL","").strip() or f"https://{server}/{token}/info"
    raw_dexes=os.getenv("TME_PERP_DEXES","").strip();dexes=(None,)+tuple(x.strip() for x in raw_dexes.split(",") if x.strip())
    return HyperliquidCollectorService(db,HyperliquidAdapter(endpoint,token,server,info),float(os.getenv("TME_WALLET_REFRESH_SECONDS","1")),dexes)