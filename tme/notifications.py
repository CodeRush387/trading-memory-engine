from __future__ import annotations
import os,time
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
