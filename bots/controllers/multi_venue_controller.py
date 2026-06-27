from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from controllers.alpaca_signal_controller import AlpacaSignalController
from orchestration.router import BridgeRouter


class MultiVenueController:
    """
    Fans out a single AlpacaTradingAgent decision to multiple venues simultaneously.

    Usage:
        ctrl = MultiVenueController(
            router,
            venues=["bybit", "okx"],
            ticker_maps={
                "bybit": {"BTC": "BTC-USDT"},
                "okx":   {"BTC": "BTC-USDT"},
            }
        )
        await ctrl.process_agent_decision(decision)
    """

    def __init__(
        self,
        router: BridgeRouter,
        venues: List[str],
        ticker_maps: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        self._controllers = {
            venue: AlpacaSignalController(
                router=router,
                venue=venue,
                ticker_map=(ticker_maps or {}).get(venue, {}),
            )
            for venue in venues
        }

    async def process_agent_decision(self, decision: Dict[str, Any]):
        """Fan out to all venues concurrently."""
        tasks = [
            ctrl.process_agent_decision(decision)
            for ctrl in self._controllers.values()
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def process_for_venue(self, venue: str, decision: Dict[str, Any]):
        """Route to a single specific venue."""
        ctrl = self._controllers.get(venue)
        if not ctrl:
            raise ValueError(f"No controller registered for venue: {venue}")
        return await ctrl.process_agent_decision(decision)
