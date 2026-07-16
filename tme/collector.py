"""Collector boundary.

Network-specific adapters translate gRPC TRADES/ORDERS/STATE messages to these
canonical calls. They deliberately contain no lifecycle or trading logic.
"""
from __future__ import annotations

from collections.abc import AsyncIterable
from typing import Any

from .engine import MemoryEngine


class Collector:
    def __init__(self, engine: MemoryEngine):
        self.engine = engine

    async def consume_fills(self, stream: AsyncIterable[dict[str, Any]]) -> None:
        async for message in stream:
            self.engine.ingest_fill(message)

    def accept_fill(self, message: dict[str, Any]) -> dict[str, Any]:
        return self.engine.ingest_fill(message)

    def accept_order(self, message: dict[str, Any]) -> None:
        raise NotImplementedError("ORDER journal support is reserved for v1.1")

    def accept_state(self, wallet: str, state: list[dict[str, Any]]) -> dict[str, Any]:
        return self.engine.recover(wallet, state)

