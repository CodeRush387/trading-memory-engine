from __future__ import annotations
import argparse,asyncio,json,os,threading,time
from pathlib import Path
from .api import serve
from .database import Database
from .engine import MemoryEngine
from .processing import ProcessingEngine
from .collector_service import from_env as collector_from_env

def processing_loop(processor:ProcessingEngine,interval:float)->None:
    while True:
        count=processor.process_available()
        if count<500: time.sleep(interval)

def main()->None:
    p=argparse.ArgumentParser(prog="tme"); p.add_argument("--db",default=os.getenv("TME_DB","data/tme.db")); sub=p.add_subparsers(dest="command",required=True)
    s=sub.add_parser("serve"); s.add_argument("--host",default=os.getenv("TME_HOST","0.0.0.0")); s.add_argument("--port",type=int,default=int(os.getenv("PORT",os.getenv("TME_PORT","8080")))); s.add_argument("--processor-interval",type=float,default=float(os.getenv("PROCESSOR_INTERVAL_SECONDS","0.05")))
    w=sub.add_parser("wallet-add"); w.add_argument("address"); w.add_argument("--label",default="")
    c=sub.add_parser("wallet-command"); c.add_argument("address"); c.add_argument("action")
    f=sub.add_parser("ingest-fill"); f.add_argument("json"); sub.add_parser("report")
    args=p.parse_args(); db=Database(args.db); engine=MemoryEngine(db)
    if args.command=="serve":
        processor=ProcessingEngine(db,allocation_gap_pct=__import__("decimal").Decimal(os.getenv("HRS_ALLOCATION_GAP_PCT","3")))
        threading.Thread(target=processing_loop,args=(processor,args.processor_interval),name="hrs-processing",daemon=True).start()
        collector_service=collector_from_env(db)
        if collector_service:
            threading.Thread(target=lambda: asyncio.run(collector_service.run()),name="hyperliquid-collector",daemon=True).start()
        serve(engine,args.host,args.port,Path(__file__).parent.parent/"web",processor)
    elif args.command=="wallet-add":print(json.dumps(engine.add_wallet(args.address,args.label),indent=2))
    elif args.command=="wallet-command":print(json.dumps(engine.command_wallet(args.address,args.action),indent=2))
    elif args.command=="ingest-fill":print(json.dumps(engine.ingest_fill(json.loads(args.json)),indent=2))
    elif args.command=="report":print(json.dumps(engine.report(),indent=2))
if __name__=="__main__":main()