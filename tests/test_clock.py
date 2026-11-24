"""Clock corrections: skew from a neighbouring epoch's tag (07 section 4, step 7) and
TIME_UNSURE are answered even on unconfirmed uplinks (07 section 5)."""

from __future__ import annotations

import time

import connectsensors as cs
from homeassistant.core import HomeAssistant
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors.server import NetworkServer

from .common import pair, uplink_answered, uplink_unanswered
from .conftest import DEV_EUI, Clock, Unix, tick
from .sim import SimProxy, SimSensor

EPOCH = cs.EPOCH_SECONDS
LIMITED = "correcting it at most every 10 minutes"


async def paired_at(
    hass: HomeAssistant, proxy: SimProxy, sensor: SimSensor, unix: Unix, into: int
) -> int:
    """Pair with the server's clock `into` seconds into an epoch; returns the sensor's
    clock offset, which pairing set right."""
    e = cs.epoch_of(int(time.time())) + 5
    unix.offset = e * EPOCH + into - int(time.time())
    await tick(hass)
    await proxy.connect()
    await pair(hass, proxy, sensor)
    return sensor.clock_offset


@pytest.mark.parametrize("epochs", [-1, 1])
async def test_skew_is_corrected(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    unix: Unix,
    epochs: int,
) -> None:
    """An unconfirmed uplink tagged for e - 1 or e + 1, mid-epoch, is answered, and the
    answer's server_time sets the sensor's clock right."""
    right = await paired_at(hass, proxy, sensor, unix, EPOCH // 2)
    sensor.clock_offset = right + epochs * EPOCH
    await uplink_answered(hass, proxy, sensor)
    assert abs(sensor.clock_offset - right) <= 2

    server: NetworkServer = loaded.runtime_data
    skew = server.devices[DEV_EUI].clock_skew
    assert skew is not None and skew["epochs"] == epochs
    assert server.diagnostics()["devices"][DEV_EUI]["clock_skew"] == skew
    # Its clock is right now: an unconfirmed uplink needs no answer.
    await uplink_unanswered(hass, proxy, sensor)


@pytest.mark.parametrize(("into", "epochs"), [(120, -1), (EPOCH - 120, 1)])
async def test_skew_near_a_boundary_is_ignored(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    unix: Unix,
    into: int,
    epochs: int,
) -> None:
    """Within 5 minutes of a boundary a neighbouring epoch's tag is normal drift."""
    right = await paired_at(hass, proxy, sensor, unix, into)
    sensor.clock_offset = right + epochs * EPOCH
    await uplink_unanswered(hass, proxy, sensor)
    server: NetworkServer = loaded.runtime_data
    assert server.devices[DEV_EUI].clock_skew is None


async def test_time_unsure_is_answered(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    unix: Unix,
) -> None:
    """An unconfirmed uplink with TIME_UNSURE is answered in RX1 (07 section 5)."""
    await paired_at(hass, proxy, sensor, unix, EPOCH // 2)
    await uplink_answered(hass, proxy, sensor, uflags=cs.UF_TIME_UNSURE)
    await uplink_unanswered(hass, proxy, sensor)


async def test_corrections_are_limited(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    unix: Unix,
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A sensor that stays skewed gets a correction at most every 10 minutes, and the
    limit is logged once."""
    right = await paired_at(hass, proxy, sensor, unix, EPOCH // 2)
    sensor.clock_offset = right - EPOCH
    await uplink_answered(hass, proxy, sensor)

    # The sensor does not take the correction.
    sensor.clock_offset = right - EPOCH
    await uplink_unanswered(hass, proxy, sensor)
    await uplink_unanswered(hass, proxy, sensor, uflags=cs.UF_TIME_UNSURE)
    assert caplog.text.count(LIMITED) == 1

    clock.advance(10 * 60 * 1000 + 1000)
    proxy.hello()  # time has passed: it would be offline otherwise
    await proxy.recv(cs.M_SERVER_STATUS)
    await uplink_answered(hass, proxy, sensor)
    assert caplog.text.count(LIMITED) == 1
