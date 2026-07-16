from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any


D = Decimal


@dataclass(frozen=True)
class Fill:
    wallet: str
    coin: str
    side: str
    size: str
    price: str
    timestamp_ms: int
    event_id: str
    order_id: str | None = None
    fee: str = "0"
    raw: dict[str, Any] | None = None

    @classmethod
    def parse(cls, data: dict[str, Any]) -> "Fill":
        side = str(data["side"]).upper()
        if side in {"B", "BUY", "LONG"}:
            side = "BUY"
        elif side in {"A", "S", "SELL", "SHORT"}:
            side = "SELL"
        else:
            raise ValueError("side must be BUY or SELL")
        size, price = D(str(data["size"])), D(str(data["price"]))
        if size <= 0 or price <= 0:
            raise ValueError("size and price must be positive")
        wallet = str(data["wallet"]).lower()
        coin = str(data["coin"]).upper()
        event_id = str(data.get("event_id") or data.get("hash") or "")
        if not wallet or not coin or not event_id:
            raise ValueError("wallet, coin and event_id are required")
        return cls(wallet, coin, side, str(size), str(price), int(data["timestamp_ms"]),
                   event_id, data.get("order_id"), str(data.get("fee", "0")), data.get("raw", data))

    def dict(self) -> dict[str, Any]:
        return asdict(self)

