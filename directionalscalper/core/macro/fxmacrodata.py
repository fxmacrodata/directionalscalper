"""FXMacroData release-calendar helpers for event-risk filters."""

from __future__ import annotations

import os
from typing import Any, Optional

import requests

FXMACRODATA_BASE_URL = "https://fxmacrodata.com/api/v1"


def fetch_release_calendar(
    currency: str = "usd",
    *,
    limit: int = 50,
    min_tier: Optional[int] = 1,
    api_key: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Fetch official macro release events from FXMacroData."""

    limit_count = max(1, min(int(limit), 100))
    params: dict[str, str] = {"limit": str(limit_count)}
    token = api_key or os.getenv("FXMACRODATA_API_KEY")
    if token:
        params["api_key"] = token

    response = requests.get(
        f"{FXMACRODATA_BASE_URL}/calendar/{currency.lower()}",
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    events = response.json().get("data", [])
    if min_tier is None:
        return events[:limit_count]

    return [
        event
        for event in events
        if int(event.get("market_tier") or 99) <= min_tier
    ][:limit_count]


def release_date_set(events: list[dict[str, Any]]) -> set[str]:
    """Convert FXMacroData events into ISO release dates."""

    return {event["date"] for event in events if event.get("date")}
