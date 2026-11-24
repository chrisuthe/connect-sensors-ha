"""Fixtures: a network entry with one proxy and one device, a simulated proxy and sensor."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator
from datetime import datetime, timedelta
import logging
import time
from typing import Any
from unittest.mock import patch

import connectsensors as cs
from homeassistant.const import CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.connect_sensors.const import (
    CONF_DEV_EUI,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
)

from .sim import SimProxy, SimSensor, free_udp_port

PROXY_KEY = "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a"
PROXY_ID = bytes.fromhex("348518fffe000001")
DEV_EUI = "c5c5000000000001"
K_ROOT = "000102030405060708090a0b0c0d0e0f"
PAIRING_CODE = cs.qr_encode(bytes.fromhex(DEV_EUI), bytes.fromhex(K_ROOT), "US915", "CS.CONTACT-1")
SECRETS = (PROXY_KEY, K_ROOT, PAIRING_CODE)


@pytest.fixture(autouse=True)
def auto_enable(enable_custom_integrations: None, socket_enabled: None) -> None:
    """Load the custom integration; allow real UDP on 127.0.0.1."""


@pytest.fixture
def port() -> int:
    """The server's UDP port."""
    return free_udp_port()


class Clock:
    """The server's monotonic clock, ahead of real time by a settable offset."""

    def __init__(self) -> None:
        self.offset = 0

    def __call__(self) -> int:
        return time.monotonic_ns() // 1_000_000 + self.offset

    def advance(self, ms: int) -> None:
        """Jump ahead, as if the time had passed."""
        self.offset += ms


@pytest.fixture
def clock() -> Generator[Clock]:
    """Control the server's monotonic clock (retransmission timing, proxy timeouts)."""
    c = Clock()
    with patch("custom_components.connect_sensors.server._now_ms", c):
        yield c


class WallClock:
    """The server's wall clock, ahead of real time by a settable offset; advancing it
    advances the monotonic clock too.

    Not freezegun: frozen time stalls the real UDP traffic these tests run on.
    """

    def __init__(self, hass: HomeAssistant, clock: Clock) -> None:
        self.hass = hass
        self.clock = clock
        self.offset = timedelta()

    def __call__(self) -> datetime:
        return dt_util.utcnow() + self.offset

    async def advance(self, delta: timedelta) -> None:
        """Let the time pass, and run Home Assistant's timers due by then."""
        # Firing timers early would also expire in-flight TX_RESULT waits.
        await asyncio.sleep(0.1)
        self.offset += delta
        self.clock.advance(int(delta.total_seconds() * 1000))
        async_fire_time_changed(self.hass, self())
        await self.hass.async_block_till_done()


@pytest.fixture
def wall(hass: HomeAssistant, clock: Clock) -> Generator[WallClock]:
    """Control the server's wall clock (last seen, supervision)."""
    c = WallClock(hass, clock)
    with patch("custom_components.connect_sensors.server._utcnow", c):
        yield c


@pytest.fixture
def entry(port: int) -> MockConfigEntry:
    """The network, with one proxy and one device in SCANNED."""
    return MockConfigEntry(
        domain=DOMAIN,
        version=2,
        title="ConnectSensors",
        data={CONF_REGION: "US915", CONF_PORT: port},
        subentries_data=[
            {
                "data": {CONF_KEY: PROXY_KEY},
                "subentry_type": SUBENTRY_PROXY,
                "title": "Garage proxy",
                "subentry_id": "proxy1",
                "unique_id": None,
            },
            {
                "data": {
                    CONF_DEV_EUI: DEV_EUI,
                    CONF_K_ROOT: K_ROOT,
                    CONF_PROFILE: "CS.CONTACT-1",
                    CONF_REGION: "US915",
                },
                "subentry_type": SUBENTRY_DEVICE,
                "title": "Front door",
                "subentry_id": "device1",
                "unique_id": DEV_EUI,
            },
        ],
    )


@pytest.fixture
async def loaded(hass: HomeAssistant, entry: MockConfigEntry) -> MockConfigEntry:
    """The entry, set up."""
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


@pytest.fixture
async def proxy(port: int) -> AsyncGenerator[SimProxy]:
    """A simulated proxy with the entry's key."""
    p = SimProxy(bytes.fromhex(PROXY_KEY), PROXY_ID, port)
    await p.start()
    yield p
    p.close()


@pytest.fixture
def sensor() -> SimSensor:
    """The simulated sensor for the entry's device."""
    return SimSensor(bytes.fromhex(K_ROOT), bytes.fromhex(DEV_EUI))


@pytest.fixture(autouse=True)
def debug_logs(caplog: pytest.LogCaptureFixture) -> Generator[pytest.LogCaptureFixture]:
    """Capture debug logs, and check that no secret ever reaches them."""
    caplog.set_level(logging.DEBUG, logger="custom_components.connect_sensors")
    yield caplog
    assert_no_secrets(caplog.text)


def assert_no_secrets(text: str, extra: tuple[str, ...] = ()) -> None:
    """No key or pairing code, in any case, appears in text."""
    lowered = text.lower()
    for secret in (*SECRETS, *extra):
        assert secret.lower() not in lowered, "a secret leaked"


def state_of(hass: HomeAssistant, entity_id: str) -> Any:
    """The state of an entity, which must exist."""
    state = hass.states.get(entity_id)
    assert state is not None, entity_id
    return state


class Unix:
    """The server's Unix time, ahead of real time by a settable offset."""

    def __init__(self) -> None:
        self.offset = 0

    def __call__(self) -> int:
        return int(time.time()) + self.offset


@pytest.fixture
def unix() -> Generator[Unix]:
    """Control the server's Unix time (epochs and tags)."""
    u = Unix()
    with patch("custom_components.connect_sensors.server._unix", u):
        yield u


async def tick(hass: HomeAssistant) -> None:
    """Run the server's 5-second tick."""
    # Firing timers early would also expire in-flight TX_RESULT waits.
    await asyncio.sleep(0.1)
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=6))
    await hass.async_block_till_done()
