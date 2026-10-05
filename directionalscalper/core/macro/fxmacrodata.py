"""FXMacroData release-calendar helpers for event-risk filters."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

import requests

FXMACRODATA_BASE_URL = "https://api.fxmacrodata.com/v1"


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
    headers: dict[str, str] = {}
    token = api_key or os.getenv("FXMACRODATA_API_KEY")
    if token:
        headers["X-API-Key"] = token

    response = requests.get(
        f"{FXMACRODATA_BASE_URL}/calendar/{currency.lower()}",
        params=params,
        headers=headers,
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
    """Convert FXMacroData events into ISO release dates (UTC).

    Calendar rows carry the release time in ``announcement_datetime_utc``
    (or as a Unix timestamp in ``announcement_datetime``). The optional
    ``date`` field is the reference period the release covers, not the day
    it is published, so it is not used here.
    """

    dates: set[str] = set()
    for event in events:
        utc = event.get("announcement_datetime_utc")
        if isinstance(utc, str) and len(utc) >= 10:
            dates.add(utc[:10])
            continue
        epoch = event.get("announcement_datetime")
        if isinstance(epoch, (int, float)) and not isinstance(epoch, bool):
            dates.add(datetime.fromtimestamp(epoch, tz=timezone.utc).date().isoformat())
    return dates
