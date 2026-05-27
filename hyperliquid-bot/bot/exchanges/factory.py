from bot import db
from bot.exchanges.base import BaseExchangeClient


def create_exchange_client(profile_id: int = 1) -> BaseExchangeClient:
    """Build the exchange client for a given profile.

    Reads `selected_exchange` from the global config (every profile shares the
    same exchange backend today; mixed Lighter+HL profiles are out of scope).
    The Lighter client receives `profile_id` so its persisted COI counter is
    scoped per profile.
    """
    cfg = db.get_all_config()
    selected = cfg.get("selected_exchange", "hyperliquid")

    if selected == "lighter":
        from bot.exchanges.lighter import LighterExchangeClient
        return LighterExchangeClient(profile_id=profile_id)
    else:
        from bot.exchanges.hyperliquid import HyperliquidClient
        return HyperliquidClient()
