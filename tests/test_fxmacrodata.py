"""Offline tests for the FXMacroData release-calendar helpers."""

from directionalscalper.core.macro import fxmacrodata
from directionalscalper.core.macro.fxmacrodata import (
    fetch_release_calendar,
    release_date_set,
)


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _capture_get(monkeypatch, payload):
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append({"url": url, "params": params, "headers": headers})
        return _Response(payload)

    monkeypatch.setattr(fxmacrodata.requests, "get", fake_get)
    return calls


EVENTS = [
    {
        "release": "non_farm_payrolls",
        "announcement_datetime": 1791549000,
        "announcement_datetime_utc": "2026-10-09T12:30:00+00:00",
        "date": "2026-09-30",
        "market_tier": 1,
    },
    {
        "release": "trade_balance",
        "announcement_datetime": 1791289800,
        "announcement_datetime_utc": "2026-10-06T12:30:00+00:00",
        "market_tier": 2,
    },
]


def test_key_is_sent_as_header_not_query(monkeypatch):
    calls = _capture_get(monkeypatch, {"data": EVENTS})
    fetch_release_calendar("USD", api_key="test-key")

    assert calls[0]["url"] == "https://api.fxmacrodata.com/v1/calendar/usd"
    assert calls[0]["headers"] == {"X-API-Key": "test-key"}
    assert "api_key" not in calls[0]["params"]


def test_no_key_sends_no_auth_header(monkeypatch):
    monkeypatch.delenv("FXMACRODATA_API_KEY", raising=False)
    calls = _capture_get(monkeypatch, {"data": EVENTS})
    fetch_release_calendar("usd")

    assert calls[0]["headers"] == {}


def test_min_tier_filters_events(monkeypatch):
    _capture_get(monkeypatch, {"data": EVENTS})
    events = fetch_release_calendar("usd", min_tier=1)

    assert [e["release"] for e in events] == ["non_farm_payrolls"]


def test_release_date_set_uses_release_time_not_reference_period():
    assert release_date_set(EVENTS) == {"2026-10-09", "2026-10-06"}


def test_release_date_set_falls_back_to_epoch():
    events = [{"announcement_datetime": 1791289800}, {"release": "no_time"}]
    assert release_date_set(events) == {"2026-10-06"}
