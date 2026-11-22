"""Choosing the proxy that answers, and RX2 fallback (07 section 6), with three proxies."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator
from typing import Any
from unittest.mock import patch

import connectsensors as cs
from homeassistant.const import CONF_PORT
from homeassistant.core import HomeAssistant
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors import server
from custom_components.connect_sensors.const import (
    CONF_DEV_EUI,
    CONF_K_NET,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
)

from .common import full_state, pair
from .conftest import DEV_EUI, K_NET, K_ROOT
from .sim import SimProxy, SimSensor, downlink_body

# Name, key, proxy_id, base of its rx_time_us (so a DOWNLINK shows which copy it targets).
PROXIES = (
    ("a", "a1" * 16, "348518fffe0000a1", 1_000_000_000),
    ("b", "b2" * 16, "348518fffe0000b2", 2_000_000_000),
    ("c", "c3" * 16, "348518fffe0000c3", 3_000_000_000),
)
RX1, RX2 = 1_000_000, 3_000_000
SENT, TOO_LATE, BUSY, DUTY_CYCLE = 0, 1, 2, 3


@pytest.fixture
def entry(port: int) -> MockConfigEntry:
    """The network, with proxies a, b and c and one device."""
    return MockConfigEntry(
        domain=DOMAIN,
        title="ConnectSensors",
        data={CONF_REGION: "US915", CONF_PORT: port, CONF_K_NET: K_NET},
        subentries_data=[
            *(
                {
                    "data": {CONF_KEY: key},
                    "subentry_type": SUBENTRY_PROXY,
                    "title": f"Proxy {name}",
                    "subentry_id": name,
                    "unique_id": None,
                }
                for name, key, _, _ in PROXIES
            ),
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
async def proxies(port: int) -> AsyncGenerator[dict[str, SimProxy]]:
    """Simulated proxies a, b and c, not connected yet."""
    sims = {
        name: SimProxy(bytes.fromhex(key), bytes.fromhex(pid), port, rx_time_us=base)
        for name, key, pid, base in PROXIES
    }
    for sim in sims.values():
        await sim.start()
    yield sims
    for sim in sims.values():
        sim.close()


@pytest.fixture
def short_wait() -> Generator[None]:
    """Give up on a missing TX_RESULT sooner than the real 1.5 s."""
    with patch("custom_components.connect_sensors.server.TX_RESULT_WAIT_MS", 400):
        yield


async def paired(
    hass: HomeAssistant, proxies: dict[str, SimProxy], sensor: SimSensor, via: str = "a"
) -> None:
    """Connect the proxies that are not connected yet, and pair through one of them."""
    for sim in proxies.values():
        if sim.seq == 0:
            await sim.connect()
    await pair(hass, proxies[via], sensor)


def heard(
    proxies: dict[str, SimProxy], frame: bytes, snr: dict[str, float], **kw: Any
) -> dict[str, int]:
    """Forward the frame from each named proxy with its SNR; their rx_time_us by name."""
    rssi: dict[str, float] = kw.get("rssi", {})
    flags: dict[str, int] = kw.get("flags", {})
    return {
        name: proxies[name].uplink(
            frame, snr=value, rssi=rssi.get(name, -80.0), flags=flags.get(name, 0)
        )
        for name, value in snr.items()
    }


async def downlink(sim: SimProxy, rx_time_us: int, offset: int) -> bytes:
    """The next DOWNLINK on this proxy, which must target this copy in this window."""
    m = await sim.recv(cs.M_DOWNLINK)
    assert m["verdict"] == "accepted"
    dl = downlink_body(m)
    assert dl["ref_rx_time_us"] == rx_time_us
    assert dl["tx_offset_us"] == offset
    frame: bytes = dl["frame"]
    return frame


async def silent(proxies: dict[str, SimProxy], *names: str, wait: float = 0.4) -> None:
    """None of these proxies is asked to transmit (anything else they get is ignored)."""
    await asyncio.sleep(wait)
    for name in names:
        inbox = proxies[name].inbox
        while not inbox.empty():
            assert inbox.get_nowait()["type"] != cs.M_DOWNLINK, f"proxy {name} was asked"


async def record(sim: SimProxy, *results: tuple[int, int]) -> None:
    """Give a proxy a timing record (07 section 6.2): TX_RESULTs for earlier downlinks."""
    for result, offset in results:
        sim.send(cs.M_TX_RESULT, cs.tx_result_body(0xFFFF, result, offset))
    await asyncio.sleep(0.1)


def confirmed(sensor: SimSensor) -> bytes:
    """A door change that needs an answer."""
    return sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(0, 0x01, 1))


async def test_rx1_best_snr(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxies: dict[str, SimProxy],
    sensor: SimSensor,
) -> None:
    """RX1 goes through the copy with the best SNR, and no other proxy is asked."""
    await paired(hass, proxies, sensor)
    rx = heard(proxies, confirmed(sensor), {"a": 2.0, "b": 9.0, "c": 5.0})
    ack = sensor.downlink(await downlink(proxies["b"], rx["b"], RX1))
    assert ack["reply_to"] == sensor.fcnt
    # RX1 was SENT: no RX2 anywhere, not even after the TX_RESULT wait.
    await silent(proxies, "a", "b", "c", wait=1.8)

    # Within 1 dB of the best SNR, the higher RSSI wins.
    rx = heard(
        proxies,
        confirmed(sensor),
        {"a": 8.5, "b": 9.0, "c": 2.0},
        rssi={"a": -50.0, "b": -70.0},
    )
    sensor.downlink(await downlink(proxies["a"], rx["a"], RX1))
    await silent(proxies, "b", "c")


async def test_ineligible_proxies_never_used(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxies: dict[str, SimProxy],
    sensor: SimSensor,
) -> None:
    """No downlink capability, or another radio profile: never asked, in any window."""
    await proxies["a"].connect(caps=0)
    await proxies["b"].connect(region="EU868")
    await paired(hass, proxies, sensor, via="c")

    rx = heard(proxies, confirmed(sensor), {"a": 10.0, "b": 9.0, "c": 1.0})
    sensor.downlink(await downlink(proxies["c"], rx["c"], RX1))
    await silent(proxies, "a", "b")

    # Even when the only eligible proxy fails, they are not the fallback.
    proxies["c"].tx_result = (BUSY, 0)
    rx = heard(proxies, confirmed(sensor), {"a": 10.0, "b": 9.0, "c": 1.0})
    first = await downlink(proxies["c"], rx["c"], RX1)
    assert await downlink(proxies["c"], rx["c"], RX2) == first
    await silent(proxies, "a", "b")

    # Heard only by ineligible proxies: not answered at all.
    heard(proxies, confirmed(sensor), {"a": 10.0, "b": 9.0})
    await silent(proxies, "a", "b", "c")


async def test_timing_excludes_from_rx1(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxies: dict[str, SimProxy],
    sensor: SimSensor,
) -> None:
    """A late SENT, more than 2 TOO_LATE or BUSY, or an uncertain timestamp: no RX1."""
    await paired(hass, proxies, sensor, via="c")
    await record(proxies["a"], (SENT, 1800), (SENT, 90_000))
    await record(proxies["b"], (TOO_LATE, 0), (BUSY, 0), (BUSY, 0))

    rx = heard(proxies, confirmed(sensor), {"a": 10.0, "b": 9.0, "c": 1.0})
    sensor.downlink(await downlink(proxies["c"], rx["c"], RX1))
    await silent(proxies, "a", "b")

    # c's timestamp is uncertain, so nobody qualifies for RX1: RX2 through the best.
    rx = heard(
        proxies,
        confirmed(sensor),
        {"a": 10.0, "b": 9.0, "c": 12.0},
        flags={"c": cs.UPF_TS_UNCERTAIN},
    )
    sensor.downlink(await downlink(proxies["c"], rx["c"], RX2))
    await silent(proxies, "a", "b", "c")

    # Two TOO_LATE or BUSY are still tolerated.
    await record(proxies["c"], (TOO_LATE, 0), (BUSY, 0))
    rx = heard(proxies, confirmed(sensor), {"a": 10.0, "c": 1.0})
    sensor.downlink(await downlink(proxies["c"], rx["c"], RX1))


async def test_rx2_after_failed_rx1(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxies: dict[str, SimProxy],
    sensor: SimSensor,
) -> None:
    """An RX1 TX_RESULT other than SENT: the same frame in RX2 through the next best."""
    await paired(hass, proxies, sensor)
    proxies["b"].tx_result = (TOO_LATE, 0)
    rx = heard(proxies, confirmed(sensor), {"a": 2.0, "b": 9.0, "c": 5.0})
    first = await downlink(proxies["b"], rx["b"], RX1)
    second = await downlink(proxies["c"], rx["c"], RX2)
    assert second == first  # same fcnt_down: the sensor accepts at most one (6.4)
    ack = sensor.downlink(second)
    assert ack["reply_to"] == sensor.fcnt
    await silent(proxies, "a", "b", "c", wait=1.0)
    assert loaded.runtime_data.proxies["b"].tx_results[-1] == ("too_late", 0)


async def test_rx2_after_tx_result_timeout(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxies: dict[str, SimProxy],
    sensor: SimSensor,
    short_wait: None,
) -> None:
    """No TX_RESULT in time from the RX1 proxy: RX2 through the next best."""
    await paired(hass, proxies, sensor)
    proxies["b"].tx_result = None
    rx = heard(proxies, confirmed(sensor), {"a": 2.0, "b": 9.0, "c": 5.0})
    first = await downlink(proxies["b"], rx["b"], RX1)
    assert await downlink(proxies["c"], rx["c"], RX2) == first
    await silent(proxies, "a", "b", "c", wait=0.6)
    assert loaded.runtime_data.proxies["b"].tx_timeouts == 1


async def test_single_proxy_falls_back_to_itself(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxies: dict[str, SimProxy],
    sensor: SimSensor,
) -> None:
    """The only proxy that heard it answers RX2 itself after a failed RX1."""
    await paired(hass, proxies, sensor)
    proxies["a"].tx_result = (BUSY, 0)
    rx = heard(proxies, sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0)), {"a": 3.0})
    first = await downlink(proxies["a"], rx["a"], RX1)
    assert await downlink(proxies["a"], rx["a"], RX2) == first
    await silent(proxies, "a", "b", "c", wait=1.0)


async def test_airtime_budget(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxies: dict[str, SimProxy],
    sensor: SimSensor,
) -> None:
    """DUTY_CYCLE, or the server's own airtime count, takes a proxy out for a while."""
    await paired(hass, proxies, sensor)
    proxies["b"].tx_result = (DUTY_CYCLE, 0)
    rx = heard(proxies, confirmed(sensor), {"a": 2.0, "b": 9.0, "c": 5.0})
    await downlink(proxies["b"], rx["b"], RX1)
    sensor.downlink(await downlink(proxies["c"], rx["c"], RX2))

    # b is left alone now, so c answers in RX1.
    rx = heard(proxies, confirmed(sensor), {"a": 2.0, "b": 9.0, "c": 5.0})
    sensor.downlink(await downlink(proxies["c"], rx["c"], RX1))
    await silent(proxies, "a", "b")

    # c's budget only has room for one more acknowledgement; a's is untouched.
    runtime = loaded.runtime_data
    runtime.proxies["a"].airtime.clear()
    used = runtime.proxies["c"].airtime_used_ms(server._now_ms())
    budget = "custom_components.connect_sensors.server.PROXY_AIRTIME_MS_PER_HOUR"
    with patch(budget, used + 70):
        rx = heard(proxies, confirmed(sensor), {"a": 2.0, "c": 5.0})
        sensor.downlink(await downlink(proxies["c"], rx["c"], RX1))
        rx = heard(proxies, confirmed(sensor), {"a": 2.0, "c": 5.0})
        sensor.downlink(await downlink(proxies["a"], rx["a"], RX1))
        await silent(proxies, "b", "c")
