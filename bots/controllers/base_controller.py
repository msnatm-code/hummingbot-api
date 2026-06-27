from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from orchestration.router import BridgeRouter, TradingSignal


class BaseController(ABC):
    """
    Abstract base for all strategy controllers.
    Subclasses implement _on_signal() to process incoming AlpacaTradingAgent signals
    and convert them to TradingSignal instances for BridgeRouter dispatch.
    """

    def __init__(self, router: BridgeRouter, venue: str):
        self._router = router
        self._venue = venue
        self._logger = logging.getLogger(self.__class__.__name__)
        self._running = False

    async def start(self):
        self._running = True
        self._logger.info(f"{self.__class__.__name__} started on venue={self._venue}")
        await self._run_loop()

    async def stop(self):
        self._running = False
        self._logger.info(f"{self.__class__.__name__} stopped")

    @abstractmethod
    async def _run_loop(self):
        """Main loop — subclass polls or subscribes to signal source."""
        ...

    @abstractmethod
    async def _on_signal(self, raw_signal: Dict[str, Any]) -> Optional[TradingSignal]:
        """Convert raw AlpacaTradingAgent output to TradingSignal. Return None to skip."""
        ...

    async def _dispatch(self, signal: TradingSignal):
        try:
            result = await self._router.submit_signal(signal)
            self._logger.info(
                f"[{signal.correlation_id}] dispatched → {signal.venue} "
                f"{signal.side} {signal.qty} {signal.symbol} → {result}"
            )
            return result
        except Exception as e:
            self._logger.error(f"Dispatch error for {signal}: {e}")
            raise
