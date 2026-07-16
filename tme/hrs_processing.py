from __future__ import annotations
from dataclasses import dataclass,field
from decimal import Decimal
from enum import Enum
from typing import Any
ZERO=Decimal("0")
def dec(v): return ZERO if v is None or v=="" else Decimal(str(v))
def sign(v): return 1 if v>ZERO else -1 if v<ZERO else 0
class LifecycleStatus(str,Enum):
    FULL="FULL_HISTORY"; PARTIAL="PARTIAL_HISTORY"; INSUFFICIENT="INSUFFICIENT_HISTORY"; CLOSED="CLOSED_POSITION"
class FillKind(str,Enum):
    OPEN="OPEN"; INCREASE="INCREASE"; REDUCTION="REDUCTION"; FULL_CLOSE="FULL_CLOSE"; REVERSAL="REVERSAL"; UNKNOWN="UNKNOWN"
class Decision(str,Enum):
    NO_ACTION="NO_ACTION_NO_VALID_SIGNAL"; INITIAL_ENTRY="INITIAL_ENTRY_SIGNAL"; HOLD="HOLD_CURRENT"; ROTATE_SIGNAL="ROTATE_SIGNAL"
@dataclass(frozen=True)
class Fill:
    wallet:str; coin:str; price:Decimal; quantity:Decimal; side:str; timestamp_ms:int; start_position:Decimal; tid:str; oid:str; direction_label:str; raw:dict[str,Any]=field(compare=False,repr=False)
    @classmethod
    def from_raw(cls,wallet,raw): return cls(wallet.lower(),str(raw.get("coin","")).strip(),dec(raw.get("px")),abs(dec(raw.get("sz"))),str(raw.get("side","")).upper(),int(raw.get("time",0) or 0),dec(raw.get("startPosition")),str(raw.get("tid","")),str(raw.get("oid","")),str(raw.get("dir","")),raw)
    @property
    def after_position(self):
        if self.side=="B": return self.start_position+self.quantity
        if self.side=="A": return self.start_position-self.quantity
        raise ValueError(f"Unknown fill side {self.side!r}")
    @property
    def event_id(self): return f"tid:{self.tid}" if self.tid else "|".join([self.coin,self.oid,str(self.timestamp_ms),self.side,str(self.quantity),str(self.price)])
    @property
    def kind(self):
        before,after=self.start_position,self.after_position
        if before==ZERO and after!=ZERO:return FillKind.OPEN
        if after==ZERO and before!=ZERO:return FillKind.FULL_CLOSE
        if sign(before)!=0 and sign(after)!=0 and sign(before)!=sign(after):return FillKind.REVERSAL
        if sign(before)==sign(after) and abs(after)>abs(before):return FillKind.INCREASE
        if sign(before)==sign(after) and abs(after)<abs(before):return FillKind.REDUCTION
        return FillKind.UNKNOWN
@dataclass
class CapitalEvent:
    fill:Fill; capital_added:Decimal; interval_seconds:Decimal|None=None; velocity:Decimal|None=None; acceleration:Decimal|None=None
@dataclass
class Lifecycle:
    coin:str; direction:str; status:LifecycleStatus; reason:str; fills:list[Fill]=field(default_factory=list); capital_events:list[CapitalEvent]=field(default_factory=list); final_size:Decimal=ZERO; current_capital:Decimal=ZERO; share:Decimal=ZERO; previous_share:Decimal=ZERO; share_change:Decimal=ZERO
    @property
    def valid(self):return self.status in {LifecycleStatus.FULL,LifecycleStatus.PARTIAL}
    @property
    def latest_fill(self):return self.fills[-1] if self.fills else None
    @property
    def latest_capital_event(self):return self.capital_events[-1] if self.capital_events else None
    @property
    def velocity(self):return self.latest_capital_event.velocity if self.latest_capital_event else None
    @property
    def acceleration(self):return self.latest_capital_event.acceleration if self.latest_capital_event else None
@dataclass
class WalletState:
    wallet:str; snapshot_positions:dict[str,Decimal]=field(default_factory=dict); snapshot_capital:dict[str,Decimal]=field(default_factory=dict); fills_by_coin:dict[str,list[Fill]]=field(default_factory=dict); lifecycles:dict[str,Lifecycle]=field(default_factory=dict); held_asset:str|None=None; held_side:str|None=None; race_ready:bool=False; race_reason:str="startup reconstruction has not completed"
@dataclass(frozen=True)
class Evaluation:
    decision:Decision; challenger:Lifecycle|None; held_share:Decimal; gap_pct:Decimal; conditions_met:bool
class ProcessingCore:
    @staticmethod
    def wallet_state(wallet,positions,capital,fills): return WalletState(wallet,snapshot_positions=positions,snapshot_capital=capital,fills_by_coin=fills)
    @staticmethod
    def reconstruct_lifecycle(coin,fills,expected_size,authoritative_capital=None):
        if expected_size==ZERO:return Lifecycle(coin,"FLAT",LifecycleStatus.CLOSED,"authoritative position is closed")
        direction="LONG" if expected_size>ZERO else "SHORT"; ordered=sorted(fills,key=lambda f:f.timestamp_ms)
        if not ordered:return Lifecycle(coin,direction,LifecycleStatus.INSUFFICIENT,"no verified fills are available for the open position",final_size=expected_size,current_capital=authoritative_capital or ZERO)
        if abs(ordered[-1].after_position-expected_size)>Decimal("0.000001"):return Lifecycle(coin,direction,LifecycleStatus.INSUFFICIENT,f"latest verified fill ends at {ordered[-1].after_position}, not authoritative exposure {expected_size}",final_size=ordered[-1].after_position,current_capital=authoritative_capital or ZERO)
        suffix_start=len(ordered)-1
        while suffix_start>0 and ordered[suffix_start-1].after_position==ordered[suffix_start].start_position:suffix_start-=1
        suffix=ordered[suffix_start:]; expected_sign=sign(expected_size); boundary=None
        for i,f in enumerate(suffix):
            if sign(f.after_position)==expected_sign and f.after_position!=ZERO and sign(f.start_position)!=expected_sign:boundary=i
        full=boundary is not None; lifecycle_fills=suffix[boundary:] if boundary is not None else suffix; current=lifecycle_fills[0].start_position; reconstructed=ZERO; events=[]; previous=None
        for f in lifecycle_fills:
            if f.start_position!=current:return Lifecycle(coin,direction,LifecycleStatus.INSUFFICIENT,f"verified history gap before {f.event_id}: expected {current}, received {f.start_position}",fills=lifecycle_fills,final_size=current,current_capital=authoritative_capital or ZERO)
            before,after,kind=f.start_position,f.after_position,f.kind
            if kind in {FillKind.OPEN,FillKind.INCREASE,FillKind.REVERSAL}:
                added=abs(after)*f.price if kind==FillKind.REVERSAL else f.quantity*f.price
                if previous is not None and previous.fill.timestamp_ms==f.timestamp_ms:
                    event=previous; event.fill=f; event.capital_added+=added; event.interval_seconds=event.velocity=event.acceleration=None; prior=events[-2] if len(events)>1 else None
                else: event=CapitalEvent(f,added); events.append(event); prior=previous
                if prior is not None:
                    seconds=Decimal(f.timestamp_ms-prior.fill.timestamp_ms)/Decimal(1000)
                    if seconds>ZERO:
                        event.interval_seconds=seconds; event.velocity=event.capital_added/seconds
                        if prior.velocity is not None:event.acceleration=(event.velocity-prior.velocity)/seconds
                previous=event; reconstructed=added if kind==FillKind.REVERSAL else reconstructed+added
            elif kind==FillKind.REDUCTION:
                if before==ZERO:return Lifecycle(coin,direction,LifecycleStatus.INSUFFICIENT,"verified reduction has zero starting exposure",fills=lifecycle_fills,current_capital=authoritative_capital or ZERO)
                reconstructed*=abs(after)/abs(before)
            elif kind==FillKind.FULL_CLOSE:reconstructed=ZERO
            else:return Lifecycle(coin,direction,LifecycleStatus.INSUFFICIENT,f"unclassifiable verified fill {f.event_id}",fills=lifecycle_fills,current_capital=authoritative_capital or ZERO)
            current=after
        if current!=expected_size:return Lifecycle(coin,direction,LifecycleStatus.INSUFFICIENT,f"verified suffix reconstructs {current}, not authoritative exposure {expected_size}",fills=lifecycle_fills,capital_events=events,final_size=current,current_capital=authoritative_capital or ZERO)
        if full:status=LifecycleStatus.FULL; reason="zero/open or reversal boundary and every subsequent fill are verified"; current_capital=authoritative_capital if authoritative_capital is not None and authoritative_capital>ZERO else reconstructed
        elif authoritative_capital is not None and authoritative_capital>ZERO and events and events[-1].velocity is not None and events[-1].acceleration is not None:status=LifecycleStatus.PARTIAL; reason=f"verified contiguous suffix of {len(lifecycle_fills)} fills provides authoritative allocation, velocity, and acceleration"; current_capital=authoritative_capital
        elif authoritative_capital is not None and authoritative_capital>ZERO:status=LifecycleStatus.INSUFFICIENT; reason="verified partial suffix and authoritative allocation are available, but timed capital-entry events are insufficient for acceleration"; current_capital=authoritative_capital
        else:status=LifecycleStatus.INSUFFICIENT; reason="partial fills are verified but authoritative current allocation is unavailable"; current_capital=ZERO
        return Lifecycle(coin,direction,status,reason,lifecycle_fills,events,current,current_capital)
    @classmethod
    def rebuild_wallet(cls,state,startup=False,previous=None):
        previous=previous or {}; rebuilt={}
        for coin,size in state.snapshot_positions.items():
            item=cls.reconstruct_lifecycle(coin,state.fills_by_coin.get(coin,[]),size,state.snapshot_capital.get(coin)); item.previous_share=previous.get(coin,ZERO); rebuilt[coin]=item
        reliable=[x for x in rebuilt.values() if x.current_capital>ZERO]; total=sum((x.current_capital for x in reliable),ZERO); state.race_ready=total>ZERO
        state.race_reason=f"maximum reliable universe allocations={len(reliable)}/{len(rebuilt)}" if state.race_ready else "no authoritative open capital is available"
        for item in rebuilt.values():item.share=item.current_capital/total if item.current_capital>ZERO and total>ZERO else ZERO; item.share_change=ZERO if startup else item.share-item.previous_share
        state.lifecycles=rebuilt
    @staticmethod
    def is_eligible(item,trigger,held):
        latest=item.latest_capital_event
        return bool(item.coin!=held and item.valid and item.acceleration is not None and item.acceleration>ZERO and item.share_change>ZERO and latest is not None and latest.fill.event_id==trigger.event_id and trigger.kind in {FillKind.OPEN,FillKind.INCREASE})
    @staticmethod
    def leaderboard(state,trigger):return sorted(state.lifecycles.values(),key=lambda x:(x.acceleration if x.acceleration is not None else Decimal("-Infinity"),x.current_capital),reverse=True)
    @classmethod
    def evaluate(cls,state,trigger,gap_required):
        ranked=cls.leaderboard(state,trigger); eligible=[x for x in ranked if state.race_ready and cls.is_eligible(x,trigger,state.held_asset)]; challenger=eligible[0] if eligible else None; held=state.lifecycles.get(state.held_asset or ""); held_share=held.share if held else ZERO; gap=(challenger.share-held_share)*100 if challenger else ZERO
        if state.held_asset is None:decision=Decision.NO_ACTION if challenger is None else Decision.INITIAL_ENTRY; met=challenger is not None
        elif challenger is None:decision=Decision.HOLD; met=False
        else:met=gap>=gap_required; decision=Decision.ROTATE_SIGNAL if met else Decision.HOLD
        return Evaluation(decision,challenger,held_share,gap,met)
    @staticmethod
    def reference_decision(state,trigger,gap_required,reference):
        held=state.held_asset; ranked=reference.leaderboard(state,trigger); eligible=[x for x in ranked if state.race_ready and reference.is_eligible(x,trigger,held)]; challenger=eligible[0] if eligible else None; held_item=state.lifecycles.get(held or ""); held_share=held_item.share if held_item else ZERO; gap=(challenger.share-held_share)*100 if challenger else ZERO
        if held is None:return reference.Decision.NO_ACTION.value if challenger is None else reference.Decision.INITIAL_ENTRY.value
        if challenger is None:return reference.Decision.HOLD.value
        return reference.Decision.ROTATE_SIGNAL.value if gap>=gap_required else reference.Decision.HOLD.value