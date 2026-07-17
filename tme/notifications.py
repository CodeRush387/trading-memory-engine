from __future__ import annotations
import html,os,time
import requests
from .database import Database

class EmailNotifier:
    def __init__(self,db:Database):
        self.db=db
        self.api_key=os.getenv("RESEND_API_KEY","").strip()
        self.recipient=os.getenv("TME_ALERT_EMAIL","").strip()
        self.sender=os.getenv("TME_ALERT_FROM","Trading Memory Engine <onboarding@resend.dev>").strip()
    @property
    def enabled(self)->bool:return bool(self.api_key and self.recipient)
    def send_coverage_report_if_due(self,interval_hours:float=5)->bool:
        if not self.enabled:return False
        now=int(time.time()*1000);interval_ms=max(1,int(interval_hours*3600000))
        rows=self.db.rows("SELECT cursor FROM collector_offsets WHERE source='coverage_email' AND wallet='*'")
        last=int(rows[0]["cursor"]) if rows and str(rows[0]["cursor"]).isdigit() else 0
        if last and now-last<interval_ms:return False
        wallets=self.db.rows("SELECT w.address,w.status,w.recovery_status,COALESCE(o.coverage_pct,'0') coverage_pct,COALESCE(o.legacy_total,0) legacy_total,COALESCE(o.legacy_remaining,0) legacy_remaining,COALESCE(o.ready_for_execution,0) ready_for_execution FROM wallets w LEFT JOIN wallet_onboarding o ON o.wallet=w.address ORDER BY w.created_at")
        table=[];text_rows=[]
        for w in wallets:
            pct=f'{float(w["coverage_pct"]):.2f}%';short=f'{w["address"][:10]}...{w["address"][-6:]}'
            table.append(f"<tr><td><code>{html.escape(short)}</code></td><td>{pct}</td><td>{w['legacy_remaining']} / {w['legacy_total']}</td><td>{html.escape(w['status'])}</td><td>{'YES' if w['ready_for_execution'] else 'NO'}</td></tr>")
            text_rows.append(f"{w['address']} | {pct} | {w['legacy_remaining']}/{w['legacy_total']} | {w['status']} | {'YES' if w['ready_for_execution'] else 'NO'}")
        body="<h2>Trading Memory Engine - Wallet Coverage</h2><table border='1' cellpadding='7' cellspacing='0'><thead><tr><th>Wallet</th><th>Coverage</th><th>Legacy remaining</th><th>Status</th><th>Execution ready</th></tr></thead><tbody>"+"".join(table)+"</tbody></table>"
        response=requests.post("https://api.resend.com/emails",headers={"Authorization":f"Bearer {self.api_key}","Content-Type":"application/json"},json={"from":self.sender,"to":[self.recipient],"subject":"TME wallet coverage report (5h)","html":body,"text":"Wallet | Coverage | Legacy | Status | Ready\n"+ "\n".join(text_rows)},timeout=15)
        response.raise_for_status()
        with self.db.transaction() as con:
            con.execute("INSERT INTO collector_offsets(source,wallet,cursor) VALUES('coverage_email','*',?) ON CONFLICT(source,wallet) DO UPDATE SET cursor=excluded.cursor,updated_at=CURRENT_TIMESTAMP",(str(now),))
        return True
    def drain_one(self)->bool:
        if not self.enabled:return False
        now=int(time.time()*1000)
        rows=self.db.rows("SELECT id,wallet,attempts FROM notification_outbox WHERE status='PENDING' AND next_attempt_ms<=? ORDER BY id LIMIT 1",(now,))
        if not rows:return False
        item=rows[0];wallet=item["wallet"]
        try:
            response=requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization":f"Bearer {self.api_key}","Content-Type":"application/json"},
                json={
                    "from":self.sender,
                    "to":[self.recipient],
                    "subject":f"TME wallet LIVE: {wallet}",
                    "text":f"Wallet coverage is now 100% and execution is enabled.\n\nWallet: {wallet}\nStatus: LIVE",
                },
                timeout=10,
            )
            response.raise_for_status()
            with self.db.transaction() as con:
                con.execute("UPDATE notification_outbox SET status='SENT',sent_at=CURRENT_TIMESTAMP,attempts=attempts+1,last_error=NULL WHERE id=? AND status='PENDING'",(item["id"],))
        except Exception as exc:
            attempts=int(item["attempts"])+1;delay=min(3600,30*(2**min(attempts-1,7)))
            with self.db.transaction() as con:
                con.execute("UPDATE notification_outbox SET attempts=?,last_error=?,next_attempt_ms=? WHERE id=? AND status='PENDING'",(attempts,str(exc)[:500],now+delay*1000,item["id"]))
        return True