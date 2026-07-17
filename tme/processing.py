from __future__ import annotations
import json,time
from decimal import Decimal
from typing import Any
from .database import Database
from .hrs_processing import Decision,Fill,FillKind,ProcessingCore,ZERO
from .ready_queue import ReadyQueue

ACTIVE={"DISCOVERING","SYNCING","READY","LIVE"}
class ProcessingEngine:
    def __init__(self,db:Database,name:str="hrs-v1",allocation_gap_pct:Decimal=Decimal("3")):
        self.db,self.name,self.allocation_gap_pct=db,name,allocation_gap_pct; self.queue=ReadyQueue(db)
    def set_held(self,wallet:str,asset:str|None,side:str|None=None):
        with self.db.transaction() as con: con.execute("INSERT INTO processor_wallet_runtime(processor,wallet,held_asset,held_side) VALUES(?,?,?,?) ON CONFLICT(processor,wallet) DO UPDATE SET held_asset=excluded.held_asset,held_side=excluded.held_side,updated_at=CURRENT_TIMESTAMP",(self.name,wallet.lower(),asset,side))
    def process_available(self,limit:int=500)->int:
        with self.db.transaction() as con:
            off=con.execute("SELECT last_sequence FROM processor_offsets WHERE processor=?",(self.name,)).fetchone(); after=int(off["last_sequence"]) if off else 0
            events=con.execute("SELECT j.sequence,j.wallet,j.payload,w.status FROM event_journal j JOIN wallets w ON w.address=j.wallet WHERE j.sequence>? AND j.event_type='FILL' ORDER BY j.sequence LIMIT ?",(after,min(max(limit,1),5000))).fetchall()
            for event in events:
                sequence=int(event["sequence"])
                if event["status"] in ACTIVE:self._process_event(con,event)
                con.execute("INSERT INTO processor_offsets(processor,last_sequence) VALUES(?,?) ON CONFLICT(processor) DO UPDATE SET last_sequence=excluded.last_sequence,updated_at=CURRENT_TIMESTAMP",(self.name,sequence))
            last_sequence=int(events[-1]["sequence"]) if events else after
            details=json.dumps({"last_sequence":last_sequence,"processed":len(events)},separators=(",",":"))
            con.execute("INSERT INTO operational_status(component,status,heartbeat_ms,details,last_error) VALUES('processor','LIVE',?,?,NULL) ON CONFLICT(component) DO UPDATE SET status=excluded.status,heartbeat_ms=excluded.heartbeat_ms,details=excluded.details,last_error=NULL,updated_at=CURRENT_TIMESTAMP",(int(time.time()*1000),details))
            return len(events)
    def _source_fill(self,wallet,payload,prior_size=ZERO):
        canonical=json.loads(payload); raw=canonical.get("raw") if isinstance(canonical.get("raw"),dict) else canonical
        if "px" in raw:return Fill.from_raw(wallet,raw)
        side="B" if str(canonical.get("side","")).upper() in {"BUY","B"} else "A"
        mapped={"coin":canonical["coin"],"px":canonical["price"],"sz":canonical["size"],"side":side,"time":canonical["timestamp_ms"],"startPosition":str(raw.get("startPosition",prior_size)),"tid":raw.get("tid") or canonical.get("event_id",""),"oid":canonical.get("order_id") or raw.get("oid","") ,"dir":raw.get("dir","")}
        return Fill.from_raw(wallet,mapped)
    def _all_fills(self,con,wallet,coin,through):
        rows=con.execute("SELECT payload FROM event_journal WHERE wallet=? AND coin=? AND event_type='FILL' AND sequence<=? ORDER BY sequence",(wallet,coin,through)).fetchall(); result=[]; prior=ZERO
        for row in rows:
            fill=self._source_fill(wallet,row["payload"],prior); result.append(fill); prior=fill.after_position
        return result
    def _process_event(self,con,event):
        wallet=event["wallet"]; sequence=int(event["sequence"]); prior=con.execute("SELECT signed_size,capital FROM processor_assets WHERE processor=? AND wallet=? AND coin=(SELECT coin FROM event_journal WHERE sequence=?)",(self.name,wallet,sequence)).fetchone(); prior_size=Decimal(prior["signed_size"]) if prior else ZERO
        fill=self._source_fill(wallet,event["payload"],prior_size); prior_capital=Decimal(prior["capital"]) if prior else None
        if fill.kind in {FillKind.OPEN,FillKind.REVERSAL}:capital=abs(fill.after_position)*fill.price
        elif fill.kind==FillKind.INCREASE and prior_capital is not None:capital=prior_capital+fill.quantity*fill.price
        elif fill.kind==FillKind.REDUCTION and prior_capital is not None and fill.start_position!=ZERO:capital=prior_capital*abs(fill.after_position)/abs(fill.start_position)
        elif fill.kind==FillKind.FULL_CLOSE:capital=ZERO
        else:capital=ZERO
        previous={r["coin"]:Decimal(r["share"]) for r in con.execute("SELECT coin,share FROM processor_assets WHERE processor=? AND wallet=?",(self.name,wallet))}
        if fill.after_position==ZERO:con.execute("DELETE FROM processor_assets WHERE processor=? AND wallet=? AND coin=?",(self.name,wallet,fill.coin))
        else:con.execute("INSERT INTO processor_assets(processor,wallet,coin,signed_size,capital,last_sequence) VALUES(?,?,?,?,?,?) ON CONFLICT(processor,wallet,coin) DO UPDATE SET signed_size=excluded.signed_size,capital=excluded.capital,last_sequence=excluded.last_sequence",(self.name,wallet,fill.coin,str(fill.after_position),str(capital),sequence))
        assets=con.execute("SELECT coin,signed_size,capital FROM processor_assets WHERE processor=? AND wallet=?",(self.name,wallet)).fetchall(); positions={r["coin"]:Decimal(r["signed_size"]) for r in assets}; capitals={r["coin"]:Decimal(r["capital"]) for r in assets}; fills={coin:self._all_fills(con,wallet,coin,sequence) for coin in positions}
        state=ProcessingCore.wallet_state(wallet,positions,capitals,fills); runtime=con.execute("SELECT held_asset,held_side FROM processor_wallet_runtime WHERE processor=? AND wallet=?",(self.name,wallet)).fetchone()
        if runtime:state.held_asset,state.held_side=runtime["held_asset"],runtime["held_side"]
        ProcessingCore.rebuild_wallet(state,previous=previous)
        for coin,item in state.lifecycles.items():con.execute("UPDATE processor_assets SET share=? WHERE processor=? AND wallet=? AND coin=?",(str(item.share),self.name,wallet,coin))
        result=ProcessingCore.evaluate(state,fill,self.allocation_gap_pct)
        if result.decision not in {Decision.INITIAL_ENTRY,Decision.ROTATE_SIGNAL} or result.challenger is None:return
        item=result.challenger; action="ENTRY" if result.decision==Decision.INITIAL_ENTRY else "ROTATE"; generation=item.latest_capital_event.fill.event_id
        payload={"schema_version":1,"decision_key":f"{wallet}:{item.coin}:{generation}:{action}","leader":wallet,"wallet":wallet,"asset":item.coin,"side":item.direction,"action":action,"lifecycle_generation":generation,"trigger":{"event_id":fill.event_id,"source_sequence":sequence},"evidence":{"lifecycle_status":item.status.value,"capital":str(item.current_capital),"share":str(item.share),"previous_share":str(item.previous_share),"share_change":str(item.share_change),"velocity":str(item.velocity) if item.velocity is not None else None,"acceleration":str(item.acceleration) if item.acceleration is not None else None,"held_asset":state.held_asset,"held_share":str(result.held_share),"allocation_gap_pct":str(result.gap_pct)}}
        self.queue.publish(con,sequence,wallet,item.coin,action,payload)