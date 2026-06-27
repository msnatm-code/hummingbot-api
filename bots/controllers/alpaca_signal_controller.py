from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional

from controllers.base_controller import BaseController
from orchestration.router import BridgeRouter, TradingSignal


class AlpacaSignalController(BaseController):
    """
    Consumes AlpacaTradingAgent decision output and routes orders
    via BridgeRouter to the configured venue.

    AlpacaTradingAgent decision schema:
        {
            "action":   "BUY" | "SELL" | "HOLD",
            "ticker":   "AAPL" | "BTC-USDT" | ...,
            "quantity": float,
            "order_type": "market" | "limit",
            "price":    float | None,
            "reasoning": str
        }

    Usage (inline / programmatic):
        ctrl = AlpacaSignalController(router, venue="bybit")
        await ctrl.process_agent_decision(decision_dict)

    Usage (streaming loop with asyncio.Queue):
        queue = asyncio.Queue()
        ctrl = AlpacaSignalController(router, venue="bybit", signal_queue=queue)
        await ctrl.start()
        ...
        await queue.put(decision_dict)
    """

    def __init__(
        self,
        router: BridgeRouter,
        venue: str,
        signal_queue: Optional[asyncio.Queue] = None,
        ticker_map: Optional[Dict[str, str]] = None,
    ):
        super().__init__(router, venue)
        self._queue: asyncio.Queue = signal_queue or asyncio.Queue()
        # ticker_map: normalise AlpacaAgent tickers to venue symbols
        # e.g. {"AAPL": "AAPL-USD", "BTC": "BTC-USDT"}
        self._ticker_map: Dict[str, str] = ticker_map or {}

    async def _run_loop(self):
        while self._running:
            try:
                raw = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                signal = await self._on_signal(raw)
                if signal:
                    await self._dispatch(signal)
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._logger.error(f"AlpacaSignalController loop error: {e}")

    async def _on_signal(self, raw: Dict[str, Any]) -> Optional[TradingSignal]:
        action = raw.get("action", "HOLD").upper()
        if action == "HOLD":
            self._logger.info(f"HOLD signal for {raw.get('ticker')} — no order dispatched")
            return None

        ticker = raw.get("ticker", "")
        symbol = self._ticker_map.get(ticker, ticker)
        qty_raw = raw.get("quantity", 0)
        qty = Decimal(str(qty_raw)) if qty_raw else Decimal("0")
        if qty <= 0:
            self._logger.warning(f"Signal qty={qty} ≤ 0 for {ticker}, skipped")
            return None

        price_raw = raw.get("price")
        price = Decimal(str(price_raw)) if price_raw else None
        order_type = raw.get("order_type", "market").lower()

        return TradingSignal(
            symbol=symbol,
            venue=self._venue,
            side=action,
            order_type=order_type,
            qty=qty,
            price=price,
            correlation_id=str(uuid.uuid4())[:8],
        )

    async def process_agent_decision(self, decision: Dict[str, Any]):
        """Direct call — bypasses queue, processes immediately."""
        signal = await self._on_signal(decision)
        if signal:
            return await self._dispatch(signal)
        return None

    async def enqueue(self, decision: Dict[str, Any]):
        """Enqueue a decision for async processing via _run_loop."""
        await self._queue.put(decision)
