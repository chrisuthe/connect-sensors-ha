"""The tag table (07 section 3) and bootstrap identification (02 section 11.3)."""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from datetime import timedelta
import time
from unittest.mock import patch

import connectsensors as cs
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.connect_sensors.server import NetworkServer

from .common import answer, full_state, pair, uplink_answered, uplink_unanswered
from .conftest import DEV_EUI
from .sim import SimProxy, SimSensor


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


async def test_rotation_at_epoch_boundary(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    unix: Unix,
) -> None:
    """At an epoch boundary the table moves on: the new epoch's tags are found, and
    tags more than one epoch old are not tried at all (02 section 6.3)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    server: NetworkServer = loaded.runtime_data
    # The JOIN was found by trial decryption, as every JOIN is (02 section 11.3).
    tried = server.trial_decryptions
    assert tried == 1
    e = cs.epoch_of(unix())

    # Two epochs later, 10 s past the boundary; the sensor's clock agrees.
    unix.offset = (e + 2) * cs.EPOCH_SECONDS + 10 - int(time.time())
    sensor.clock_offset = unix.offset
    await tick(hass)
    dl = await uplink_answered(hass, proxy, sensor, cs.T_HEARTBEAT_CONFIRMED)
    assert cs.epoch_of(int(time.time()) + sensor.clock_offset) == e + 2
    assert dl["tag"] == cs.normal_tag(sensor.session.k_dev_tag, e + 2)

    # A sensor whose clock still says epoch e, now e - 2, is not recognized.
    foreign = server.foreign_frames
    sensor.clock_offset = 0
    await uplink_unanswered(hass, proxy, sensor, cs.T_HEARTBEAT_CONFIRMED)
    assert server.foreign_frames == foreign + 1
    assert server.trial_decryptions == tried

    # One epoch behind is still in the window.
    sensor.clock_offset = unix.offset - cs.EPOCH_SECONDS
    await uplink_answered(hass, proxy, sensor, cs.T_HEARTBEAT_CONFIRMED)


async def test_sync_by_block_tag(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
) -> None:
    """A SYNC in the blocks around last_fcnt is found by its tag, without trial
    decryption, and its answer sets the sensor's clock (02 section 11.2)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    server: NetworkServer = loaded.runtime_data
    tried = server.trial_decryptions

    # The sensor lost its clock: the SYNC tag comes from its counter, two blocks on.
    sensor.fcnt += 2 * cs.TAG_BLOCK
    sensor.clock_offset = -86_400
    frame = sensor.sync(full_state(1))
    assert cs.parse_header(frame).tag == cs.bootstrap_tag(  # type: ignore[union-attr]
        sensor.session.k_dev_tag, sensor.fcnt
    )
    rx = proxy.uplink(frame)
    dl = sensor.downlink(await answer(proxy, rx))
    assert dl["reply_to"] == sensor.fcnt
    assert abs(sensor.now() - time.time()) < 5
    assert server.trial_decryptions == tried
    await hass.async_block_till_done()
    assert server.devices[DEV_EUI].readings["0:contact"] == 1


async def test_sync_by_fallback(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
) -> None:
    """A SYNC past the table's blocks, for example after many reboots out of range, is
    found by trial decryption; the table then moves to its block (07 section 3)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    server: NetworkServer = loaded.runtime_data
    tried = server.trial_decryptions

    sensor.fcnt += 10 * cs.TAG_BLOCK
    rx = proxy.uplink(sensor.sync())
    assert sensor.downlink(await answer(proxy, rx))["reply_to"] == sensor.fcnt
    assert server.trial_decryptions == tried + 1

    # The next SYNC is in the table's new blocks.
    rx = proxy.uplink(sensor.sync())
    assert sensor.downlink(await answer(proxy, rx))["reply_to"] == sensor.fcnt
    assert server.trial_decryptions == tried + 1


async def test_fallback_limit(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
) -> None:
    """Trial decryption is limited per minute (02 section 11.3): beyond the limit a
    bootstrap frame that misses the table is dropped unprocessed."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    server: NetworkServer = loaded.runtime_data
    tried = server.trial_decryptions

    with patch("custom_components.connect_sensors.server.BOOTSTRAP_PER_MINUTE", 0):
        sensor.fcnt += 10 * cs.TAG_BLOCK
        proxy.uplink(sensor.sync())
        await proxy.nothing()
    assert server.trial_decryptions == tried
