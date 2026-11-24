"""Pairing ends on the first session uplink, within the provisional time (04 section 4)."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import connectsensors as cs
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ServiceValidationError
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors.server import NetworkServer

from .common import answer, full_state, pair, uplink_answered
from .conftest import DEV_EUI, WallClock, state_of
from .sim import SimProxy, SimSensor

PAIRING = "sensor.front_door_pairing"
START_PAIRING = "button.front_door_start_pairing"
IGNORED = "button pressed after its pairing expired; ignored"


async def reconnect(proxy: SimProxy) -> None:
    """A HELLO, so the proxy is online again after time was advanced."""
    proxy.hello()
    await proxy.recv(cs.M_SERVER_STATUS)


async def start_pairing(hass: HomeAssistant) -> None:
    """Press the device's Start pairing button."""
    await hass.services.async_call("button", "press", {"entity_id": START_PAIRING}, blocking=True)


async def test_first_uplink_without_dl_ack(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
) -> None:
    """A first session uplink without DL_ACK still pairs the device (07 section 7)."""
    await proxy.connect()
    rx = proxy.uplink(sensor.join())
    assert sensor.key_exchange(await answer(proxy, rx)) == "accept"
    rx = proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0)))
    sensor.downlink(await answer(proxy, rx))
    await hass.async_block_till_done()
    assert sensor.pairing.state == "PAIRED"
    assert state_of(hass, PAIRING).state == "paired"
    server: NetworkServer = loaded.runtime_data
    assert server.devices[DEV_EUI].join.pending is None


async def test_provisional_timeout(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """Two minutes after KEY_EXCHANGE without a session uplink, the device is SCANNED
    again and its pending k_session is not recognized (04 section 4.2): the sensor's
    provisional session fails, and the next button press pairs it."""
    await proxy.connect()
    join = sensor.join()
    rx = proxy.uplink(join)
    key_exchange = await answer(proxy, rx)
    await hass.async_block_till_done()
    assert state_of(hass, PAIRING).state == "exchanged"
    assert sensor.key_exchange(key_exchange) == "accept"

    await wall.advance(timedelta(minutes=2, seconds=5))
    assert state_of(hass, PAIRING).state == "scanned"
    server: NetworkServer = loaded.runtime_data
    foreign = server.foreign_frames
    # The sensor's session uplink is not answered: it gives up and returns to UNPAIRED.
    proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0), cs.UF_DL_ACK))
    await proxy.nothing()
    assert server.foreign_frames == foreign + 1
    sensor.pairing.undelivered()
    assert sensor.pairing.state == "UNPAIRED"

    proxy.hello()  # time has passed: it would be offline otherwise
    await proxy.recv(cs.M_SERVER_STATUS)

    # The user presses the button again: a new JOIN, a new exchange, and pairing.
    rx = proxy.uplink(sensor.join())
    second = await answer(proxy, rx)
    assert second != key_exchange
    assert sensor.key_exchange(second) == "accept"
    await uplink_answered(hass, proxy, sensor, cs.T_HEARTBEAT_CONFIRMED, cs.UF_DL_ACK)
    assert sensor.pairing.state == "PAIRED"
    assert state_of(hass, PAIRING).state == "paired"

    # Paired devices do not time out.
    await wall.advance(timedelta(minutes=3))
    assert state_of(hass, PAIRING).state == "paired"


async def test_timeout_while_uplink_waits(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A session uplink identified just before the exchange times out, and acted on
    after, is dropped: its keys are no longer in use."""
    await proxy.connect()
    rx = proxy.uplink(sensor.join())
    assert sensor.key_exchange(await answer(proxy, rx)) == "accept"
    server: NetworkServer = loaded.runtime_data
    device = server.devices[DEV_EUI]

    async with device.lock:
        proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(1), cs.UF_DL_ACK))
        await asyncio.sleep(0.2)  # identified; its task waits for the lock
        await wall.advance(timedelta(minutes=2, seconds=5))
        assert device.state == "scanned"
    await proxy.nothing()
    assert device.readings == {}
    assert "Error processing an uplink" not in caplog.text


async def test_pairing_expires(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """10 minutes after the code was entered the pairing expires (04 section 4.2): its
    root key is no longer tried, and a JOIN is ignored and logged once."""
    await proxy.connect()
    server: NetworkServer = loaded.runtime_data
    device = server.devices[DEV_EUI]
    assert device.pairing_deadline is not None
    assert state_of(hass, PAIRING).attributes["deadline"] == device.pairing_deadline.isoformat()

    await wall.advance(timedelta(minutes=9, seconds=50))
    assert state_of(hass, PAIRING).state == "scanned"
    await wall.advance(timedelta(seconds=15))
    assert state_of(hass, PAIRING).state == "expired"
    assert device.join.root_fcnt_down == 0

    await reconnect(proxy)
    foreign = server.foreign_frames
    proxy.uplink(sensor.join())
    await proxy.nothing()
    assert caplog.text.count(IGNORED) == 1
    # Once seen, the root key is not tried at all: the next JOIN is foreign.
    proxy.uplink(sensor.join())
    await proxy.nothing()
    assert caplog.text.count(IGNORED) == 1
    assert server.foreign_frames == foreign + 1
    assert server.trial_decryptions == 0
    assert state_of(hass, PAIRING).state == "expired"


async def test_exchanged_timeout_keeps_deadline(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """An EXCHANGED timeout returns to SCANNED with the original deadline; expiry then
    discards the pending exchange and keeps the root counter (04 section 4.2)."""
    await proxy.connect()
    server: NetworkServer = loaded.runtime_data
    device = server.devices[DEV_EUI]
    deadline = device.pairing_deadline
    rx = proxy.uplink(sensor.join())
    await answer(proxy, rx)
    await hass.async_block_till_done()
    assert state_of(hass, PAIRING).state == "exchanged"

    await wall.advance(timedelta(minutes=2, seconds=5))
    assert state_of(hass, PAIRING).state == "scanned"
    assert device.pairing_deadline == deadline
    assert device.join.pending is not None

    await wall.advance(timedelta(minutes=8))
    assert state_of(hass, PAIRING).state == "expired"
    record = device.to_store()
    assert record["pending"] is None and record["expired"]
    assert device.join.root_fcnt_down == 1


async def test_start_pairing_rearms(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """Start pairing gives another 10 minutes, and the next button press pairs. A
    paired device refuses it."""
    await proxy.connect()
    server: NetworkServer = loaded.runtime_data
    device = server.devices[DEV_EUI]
    await wall.advance(timedelta(minutes=10, seconds=5))
    assert state_of(hass, PAIRING).state == "expired"
    await reconnect(proxy)
    proxy.uplink(sensor.join())
    await proxy.nothing()

    await start_pairing(hass)
    assert state_of(hass, PAIRING).state == "scanned"
    assert device.pairing_deadline is not None
    assert abs(device.pairing_deadline - wall() - timedelta(minutes=10)) < timedelta(seconds=5)
    await pair(hass, proxy, sensor)
    assert state_of(hass, PAIRING).state == "paired"
    assert state_of(hass, PAIRING).attributes["deadline"] is None

    with pytest.raises(ServiceValidationError):
        await start_pairing(hass)
    assert state_of(hass, PAIRING).state == "paired"


async def test_deadline_survives_restart(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    wall: WallClock,
) -> None:
    """The deadline, and the expiry once past it, are kept across a restart."""
    server: NetworkServer = loaded.runtime_data
    deadline = server.devices[DEV_EUI].pairing_deadline
    await wall.advance(timedelta(minutes=5))
    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()
    server = loaded.runtime_data
    assert server.devices[DEV_EUI].pairing_deadline == deadline

    await wall.advance(timedelta(minutes=5, seconds=10))
    assert state_of(hass, PAIRING).state == "expired"
    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()
    server = loaded.runtime_data
    device = server.devices[DEV_EUI]
    assert device.expired and device.pairing_deadline == deadline
    assert state_of(hass, PAIRING).state == "expired"


async def test_exchange_in_progress_outlives_the_deadline(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """The deadline ends SCANNED only: an EXCHANGED device still answers a JOIN past
    it, and pairs (04 section 4.2)."""
    await proxy.connect()
    await wall.advance(timedelta(minutes=9))
    await reconnect(proxy)
    rx = proxy.uplink(sensor.join())
    await answer(proxy, rx)
    await hass.async_block_till_done()
    assert state_of(hass, PAIRING).state == "exchanged"

    await wall.advance(timedelta(minutes=1, seconds=30))
    await reconnect(proxy)
    await pair(hass, proxy, sensor)
    assert state_of(hass, PAIRING).state == "paired"
