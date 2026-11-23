"""Pairing ends on the first session uplink, within the provisional time (04 section 4)."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import connectsensors as cs
from homeassistant.core import HomeAssistant
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors.server import NetworkServer

from .common import answer, full_state, uplink_answered
from .conftest import DEV_EUI, WallClock, state_of
from .sim import SimProxy, SimSensor

PAIRING = "sensor.front_door_pairing"


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
