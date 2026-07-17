from __future__ import annotations
import time
from .database import Database

class ExecutorLease:
    NAME="hrs-executor"
    def __init__(self,db:Database):self.db=db
    def acquire(self,owner_id:str,signer_address:str,ttl_ms:int)->dict:
        owner_id=owner_id.strip();signer_address=signer_address.strip().lower()
        if not owner_id or not signer_address:return {"acquired":False,"error":"identity_required"}
        ttl_ms=min(max(int(ttl_ms),15000),120000);now=int(time.time()*1000)
        with self.db.transaction() as con:
            row=con.execute("SELECT * FROM executor_leases WHERE lease_name=?",(self.NAME,)).fetchone()
            if row and int(row["expires_at_ms"])>now and row["owner_id"]!=owner_id:
                return {"acquired":False,"owner_id":row["owner_id"],"expires_at_ms":int(row["expires_at_ms"])}
            acquired=int(row["acquired_at_ms"]) if row and row["owner_id"]==owner_id else now
            con.execute("INSERT INTO executor_leases(lease_name,owner_id,signer_address,acquired_at_ms,renewed_at_ms,expires_at_ms) VALUES(?,?,?,?,?,?) ON CONFLICT(lease_name) DO UPDATE SET owner_id=excluded.owner_id,signer_address=excluded.signer_address,acquired_at_ms=excluded.acquired_at_ms,renewed_at_ms=excluded.renewed_at_ms,expires_at_ms=excluded.expires_at_ms",(self.NAME,owner_id,signer_address,acquired,now,now+ttl_ms))
        return {"acquired":True,"owner_id":owner_id,"signer_address":signer_address,"expires_at_ms":now+ttl_ms}
    def renew(self,owner_id:str,signer_address:str,ttl_ms:int)->dict:
        now=int(time.time()*1000);owner_id=owner_id.strip();signer_address=signer_address.strip().lower();ttl_ms=min(max(int(ttl_ms),15000),120000)
        with self.db.transaction() as con:
            row=con.execute("SELECT * FROM executor_leases WHERE lease_name=?",(self.NAME,)).fetchone()
            if not row or row["owner_id"]!=owner_id or row["signer_address"]!=signer_address or int(row["expires_at_ms"])<=now:
                return {"acquired":False,"error":"lease_not_owned"}
            con.execute("UPDATE executor_leases SET renewed_at_ms=?,expires_at_ms=? WHERE lease_name=?",(now,now+ttl_ms,self.NAME))
        return {"acquired":True,"owner_id":owner_id,"signer_address":signer_address,"expires_at_ms":now+ttl_ms}
    def release(self,owner_id:str)->bool:
        with self.db.transaction() as con:
            return con.execute("DELETE FROM executor_leases WHERE lease_name=? AND owner_id=?",(self.NAME,owner_id.strip())).rowcount==1
    def status(self)->dict:
        rows=self.db.rows("SELECT * FROM executor_leases WHERE lease_name=?",(self.NAME,))
        if not rows:return {"active":False}
        row=rows[0];now=int(time.time()*1000)
        return {"active":int(row["expires_at_ms"])>now,"owner_id":row["owner_id"],"signer_address":row["signer_address"],"expires_at_ms":int(row["expires_at_ms"]),"renewed_at_ms":int(row["renewed_at_ms"])}
