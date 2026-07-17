from __future__ import annotations
import json,time
from decimal import Decimal
from typing import Any
from .database import Database

ZERO=Decimal("0")

class WalletCoverage:
    def __init__(self,db:Database):self.db=db
    def onboard(self,wallet:str,state:list[dict[str,Any]])->dict[str,Any]:
        wallet=wallet.lower();now=int(time.time()*1000)
        positions=[x for x in state if Decimal(str(x.get("size","0")))!=ZERO]
        with self.db.transaction() as con:
            con.execute("DELETE FROM wallet_asset_coverage WHERE wallet=?",(wallet,))
            con.execute("DELETE FROM lifecycle_fills WHERE wallet=?",(wallet,))
            con.execute("DELETE FROM projections WHERE wallet=?",(wallet,))
            if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='processor_assets'").fetchone():
                con.execute("DELETE FROM processor_assets WHERE wallet=?",(wallet,))
            last=int(con.execute("SELECT COALESCE(MAX(sequence),0) n FROM event_journal").fetchone()["n"])
            for item in positions:
                coin=str(item["coin"]).upper();size=Decimal(str(item["size"]));entry=Decimal(str(item.get("entry_price") or "0"))
                con.execute("INSERT INTO wallet_asset_coverage(wallet,coin,state,baseline_size,current_size,updated_at_ms) VALUES(?,?,?,?,?,?)",(wallet,coin,"LEGACY",str(size),str(size),now))
                con.execute("INSERT INTO projections(wallet,coin,lifecycle_id,status,side,size,average_price,capital,realized_pnl,opened_at_ms,updated_at_ms,last_sequence) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",(wallet,coin,0,"OPEN","LONG" if size>0 else "SHORT",str(size),str(entry),str(abs(size)*entry),"0",None,now,last))
            total=len(positions);ready=total==0
            con.execute("INSERT INTO wallet_onboarding(wallet,joined_at_ms,snapshot,legacy_total,legacy_remaining,coverage_pct,ready_for_execution) VALUES(?,?,?,?,?,?,?) ON CONFLICT(wallet) DO UPDATE SET joined_at_ms=excluded.joined_at_ms,snapshot=excluded.snapshot,legacy_total=excluded.legacy_total,legacy_remaining=excluded.legacy_remaining,coverage_pct=excluded.coverage_pct,ready_for_execution=excluded.ready_for_execution,updated_at=CURRENT_TIMESTAMP",(wallet,now,json.dumps(state,separators=(",",":")),total,total,"100" if ready else "0",int(ready)))
            con.execute("UPDATE wallets SET status=?,recovery_status='HEALTHY',last_error=NULL,updated_at=CURRENT_TIMESTAMP WHERE address=?",("LIVE" if ready else "SYNCING",wallet))
        return self.report(wallet)
    def observe(self,wallet:str,canonical:dict[str,Any])->dict[str,Any]:
        wallet=wallet.lower();coin=str(canonical["coin"]).upper();raw=canonical.get("raw") or {};start=Decimal(str(raw.get("startPosition","0")));qty=Decimal(str(canonical["size"]));after=start+(qty if canonical["side"]=="BUY" else -qty);now=int(time.time()*1000)
        with self.db.transaction() as con:
            row=con.execute("SELECT state FROM wallet_asset_coverage WHERE wallet=? AND coin=?",(wallet,coin)).fetchone();old=row["state"] if row else "ARMED"
            state=old
            if old=="LEGACY" and (after==ZERO or start*after<ZERO):state="ARMED" if after==ZERO else "OWNED"
            elif old=="ARMED" and after!=ZERO and start==ZERO:state="OWNED"
            elif old=="OWNED" and after==ZERO:state="ARMED"
            con.execute("INSERT INTO wallet_asset_coverage(wallet,coin,state,baseline_size,current_size,updated_at_ms) VALUES(?,?,?,?,?,?) ON CONFLICT(wallet,coin) DO UPDATE SET state=excluded.state,current_size=excluded.current_size,updated_at_ms=excluded.updated_at_ms",(wallet,coin,state,"0",str(after),now))
            total=int(con.execute("SELECT legacy_total FROM wallet_onboarding WHERE wallet=?",(wallet,)).fetchone()["legacy_total"]);remaining=int(con.execute("SELECT COUNT(*) n FROM wallet_asset_coverage WHERE wallet=? AND state='LEGACY'",(wallet,)).fetchone()["n"]);coverage=Decimal("100") if total==0 else Decimal(total-remaining)*Decimal("100")/Decimal(total);ready=remaining==0
            con.execute("UPDATE wallet_onboarding SET legacy_remaining=?,coverage_pct=?,ready_for_execution=?,updated_at=CURRENT_TIMESTAMP WHERE wallet=?",(remaining,str(coverage),int(ready),wallet))
            con.execute("UPDATE wallets SET status=?,recovery_status='HEALTHY',updated_at=CURRENT_TIMESTAMP WHERE address=?",("LIVE" if ready else "SYNCING",wallet))
        return self.report(wallet)
    def report(self,wallet:str)->dict[str,Any]:
        rows=self.db.rows("SELECT w.address wallet,w.status,w.recovery_status,o.coverage_pct,o.ready_for_execution,o.legacy_total,o.legacy_remaining,o.joined_at_ms FROM wallets w LEFT JOIN wallet_onboarding o ON o.wallet=w.address WHERE w.address=?",(wallet.lower(),))
        return rows[0] if rows else {}
    def wallets(self)->list[dict[str,Any]]:
        return self.db.rows("SELECT w.*,COALESCE(o.coverage_pct,'0') coverage_pct,COALESCE(o.ready_for_execution,0) ready_for_execution,COALESCE(o.legacy_total,0) legacy_total,COALESCE(o.legacy_remaining,0) legacy_remaining,o.joined_at_ms FROM wallets w LEFT JOIN wallet_onboarding o ON o.wallet=w.address ORDER BY w.created_at")
