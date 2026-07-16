from __future__ import annotations
import asyncio,json,time
from typing import Any,AsyncIterator
import grpc,requests
from .proto import hyperliquid_pb2 as pb2
from .proto import hyperliquid_pb2_grpc as pb2_grpc

class HyperliquidAdapter:
    def __init__(self,endpoint:str,token:str,server_name:str,info_url:str,timeout:float=20):
        self.endpoint=endpoint;self.token=token;self.server_name=server_name;self.info_url=info_url;self.timeout=timeout
    def info(self,payload:dict[str,Any])->Any:
        for attempt in range(6):
            r=requests.post(self.info_url,json=payload,timeout=self.timeout)
            if r.status_code!=429:r.raise_for_status();return r.json()
            time.sleep(min(1.5*(attempt+1),10))
        r.raise_for_status()
    def current_state(self,wallet:str,dexes:tuple[str|None,...]=(None,))->list[dict[str,str]]:
        result=[]
        for dex in dexes:
            payload={"type":"clearinghouseState","user":wallet}
            if dex is not None:payload["dex"]=dex
            data=self.info(payload)
            for item in data.get("assetPositions",[]) or []:
                p=item.get("position",{}) or {};coin=str(p.get("coin","")).strip();size=str(p.get("szi","0"))
                if coin and size not in {"0","0.0",""}:result.append({"coin":coin,"size":size,"entry_price":str(p.get("entryPx") or "0")})
        return result
    def historical_fills(self,wallet:str)->list[dict[str,Any]]:
        cursor=0;seen={};pages=0
        while pages<100:
            data=self.info({"type":"userFillsByTime","user":wallet,"startTime":cursor,"aggregateByTime":False})
            if not isinstance(data,list):raise RuntimeError("unexpected userFillsByTime response")
            pages+=1
            for raw in data:
                key=str(raw.get("tid") or "|".join(str(raw.get(k,"")) for k in ("coin","oid","time","side","sz","px")));seen[key]=raw
            if len(data)<2000:break
            newest=max(int(x.get("time",0) or 0) for x in data)
            if newest<cursor:break
            cursor=newest+1
        twap=self.info({"type":"userTwapSliceFills","user":wallet})
        if isinstance(twap,list):
            for item in reversed(twap):
                raw=item.get("fill") if isinstance(item,dict) else None
                if isinstance(raw,dict):
                    key=str(raw.get("tid") or "|".join(str(raw.get(k,"")) for k in ("coin","oid","time","side","sz","px")));seen[key]=raw
        return sorted(seen.values(),key=lambda x:int(x.get("time",0) or 0))
    @staticmethod
    def canonical(wallet:str,raw:dict[str,Any])->dict[str,Any]:
        tid=str(raw.get("tid","")).strip();oid=str(raw.get("oid","")).strip();ts=int(raw.get("time",0) or 0)
        event_id=f"tid:{tid}" if tid else "|".join([str(raw.get("coin","")),oid,str(ts),str(raw.get("side","")),str(raw.get("sz","")),str(raw.get("px",""))])
        return {"wallet":wallet.lower(),"coin":str(raw.get("coin","")).upper(),"side":"BUY" if str(raw.get("side","")).upper()=="B" else "SELL","size":str(raw.get("sz","0")),"price":str(raw.get("px","0")),"timestamp_ms":ts,"event_id":event_id,"order_id":oid or None,"fee":str(raw.get("fee","0")),"raw":raw}
    @staticmethod
    def stream_events(payload:str)->list[tuple[str,dict[str,Any]]]:
        decoded=json.loads(payload)
        if isinstance(decoded,dict) and isinstance(decoded.get("data"),dict):decoded=decoded["data"]
        events=decoded.get("events",[]) if isinstance(decoded,dict) else decoded;result=[]
        if not isinstance(events,list):return result
        for event in events:
            if isinstance(event,list) and len(event)==2 and isinstance(event[1],dict):result.append((str(event[0]).lower(),event[1]))
            elif isinstance(event,dict):
                user=str(event.get("user","")).lower();data=event.get("data") if isinstance(event.get("data"),dict) else event
                if user:result.append((user,data))
        return result
    async def trades(self,users:list[str])->AsyncIterator[tuple[str,dict[str,Any]]]:
        credentials=grpc.ssl_channel_credentials();options=[("grpc.ssl_target_name_override",self.server_name),("grpc.default_authority",self.server_name),("grpc.max_receive_message_length",100*1024*1024),("grpc.keepalive_time_ms",20000),("grpc.keepalive_timeout_ms",10000)]
        channel=grpc.aio.secure_channel(self.endpoint,credentials,options=options);await asyncio.wait_for(channel.channel_ready(),timeout=self.timeout);stub=pb2_grpc.StreamingStub(channel)
        async def requests_iter():
            yield pb2.SubscribeRequest(subscribe=pb2.StreamSubscribe(stream_type=pb2.TRADES,filters={"user":pb2.FilterValues(values=users)},filter_name="TME_WALLETS"))
            while True:await asyncio.sleep(25);yield pb2.SubscribeRequest(ping=pb2.Ping(timestamp=int(time.time()*1000)))
        try:
            responses=stub.StreamData(requests_iter(),metadata=(("x-token",self.token),))
            async for update in responses:
                if update.HasField("data"):
                    for item in self.stream_events(update.data.data):yield item
        finally:await channel.close()