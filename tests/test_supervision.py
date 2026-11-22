"""Supervision (07 section 8) and repair issues (07 section 11.5)."""

from __future__ import annotations

from datetime import timedelta
import time
from types import MappingProxyType

import connectsensors as cs
from homeassistant.config_entries import ConfigSubentry
from homeassistant.const import STATE_OFF, STATE_ON, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.connect_sensors.const import CONF_HEARTBEAT, CONF_KEY, DOMAIN

from .common import answer, full_state, pair
from .conftest import DEV_EUI, Clock, WallClock, state_of
from .sim import SimProxy, SimSensor

DOOR = "binary_sensor.front_door_door"
TEMPERATURE = "sensor.front_door_temperature"
REACHABLE = "binary_sensor.front_door_reachable"
PROXIES = "sensor.front_door_proxies_hearing_it"
UNREACHABLE = f"device_unreachable_{DEV_EUI}"
SINGLE_PATH = f"single_path_{DEV_EUI}"
# CS.CONTACT-1 heartbeats every 30 minutes: unreachable after 3 x 30 + 5.
SILENCE = timedelta(minutes=95)


def issue(hass: HomeAssistant, issue_id: str) -> ir.IssueEntry | None:
    """A repair issue of this integration."""
    return ir.async_get(hass).async_get_issue(DOMAIN, issue_id)


async def heartbeat(
    hass: HomeAssistant, proxy: SimProxy, sensor: SimSensor, readings: bytes | None = None
) -> None:
    """A confirmed heartbeat at the current time, answered in RX1."""
    proxy.hello()  # time has passed: it would be offline otherwise
    await proxy.recv(cs.M_SERVER_STATUS)
    sensor.server_time = int(time.time())
    rx = proxy.uplink(
        sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0) if readings is None else readings)
    )
    sensor.downlink(await answer(proxy, rx))
    await hass.async_block_till_done()


async def test_unreachable_and_recovery(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """Silent for 3 heartbeats and 5 minutes: unavailable, never a stale value."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    assert state_of(hass, REACHABLE).state == STATE_ON
    assert state_of(hass, DOOR).state == STATE_OFF

    await wall.advance(SILENCE - timedelta(minutes=1))
    assert state_of(hass, DOOR).state == STATE_OFF
    assert issue(hass, UNREACHABLE) is None

    await wall.advance(timedelta(minutes=2))
    assert state_of(hass, REACHABLE).state == STATE_OFF
    assert state_of(hass, DOOR).state == STATE_UNAVAILABLE
    assert state_of(hass, TEMPERATURE).state == STATE_UNAVAILABLE
    found = issue(hass, UNREACHABLE)
    assert found is not None
    assert found.translation_placeholders is not None
    assert found.translation_placeholders["heartbeat"] == "30"

    # A door change: reachable again, but only the readings it carries come back.
    await heartbeat(hass, proxy, sensor, cs.encode_reading(0, 0x01, 1))
    assert state_of(hass, REACHABLE).state == STATE_ON
    assert state_of(hass, DOOR).state == STATE_ON
    assert state_of(hass, TEMPERATURE).state == STATE_UNAVAILABLE
    assert issue(hass, UNREACHABLE) is None

    await heartbeat(hass, proxy, sensor)
    assert state_of(hass, TEMPERATURE).state == "21.53"
    assert state_of(hass, DOOR).state == STATE_OFF


async def test_heartbeat_setting(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """The device's heartbeat setting sets the silence allowed."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    subentry = loaded.subentries["device1"]
    hass.config_entries.async_update_subentry(
        loaded, subentry, data={**subentry.data, CONF_HEARTBEAT: 2}
    )
    await hass.async_block_till_done()

    await wall.advance(timedelta(minutes=10))
    assert state_of(hass, DOOR).state == STATE_OFF
    await wall.advance(timedelta(minutes=2))
    assert state_of(hass, DOOR).state == STATE_UNAVAILABLE
    found = issue(hass, UNREACHABLE)
    assert found is not None and found.translation_placeholders is not None
    assert found.translation_placeholders["heartbeat"] == "2"


async def test_unpaired_device_is_not_supervised(
    hass: HomeAssistant, loaded: MockConfigEntry, wall: WallClock
) -> None:
    """A device waiting for its button press sends no heartbeats yet."""
    await wall.advance(timedelta(days=2))
    assert state_of(hass, REACHABLE).state == STATE_UNKNOWN
    assert issue(hass, UNREACHABLE) is None
    assert issue(hass, SINGLE_PATH) is None


async def test_restart(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """Silence while stopped does not count; an unreachable device stays unreachable."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await hass.config_entries.async_unload(loaded.entry_id)
    wall.offset += SILENCE * 2
    wall.clock.advance(int(SILENCE.total_seconds() * 2000))
    assert await hass.config_entries.async_setup(loaded.entry_id)
    await hass.async_block_till_done()
    await wall.advance(timedelta(minutes=5))
    assert state_of(hass, DOOR).state == STATE_OFF

    await wall.advance(SILENCE)
    assert state_of(hass, DOOR).state == STATE_UNAVAILABLE

    # Repair issues are not restored by Home Assistant: the server raises them again.
    ir.async_delete_issue(hass, DOMAIN, UNREACHABLE)
    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()
    assert state_of(hass, DOOR).state == STATE_UNAVAILABLE
    assert issue(hass, UNREACHABLE) is not None


async def test_single_path(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
    port: int,
) -> None:
    """Heard by one proxy only: flagged at once, a repair issue after a day of it."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await wall.advance(timedelta(seconds=5))
    state = state_of(hass, PROXIES)
    assert state.state == "1"
    assert state.attributes["single_path"] is True
    assert issue(hass, SINGLE_PATH) is None

    for _ in range(2):
        await wall.advance(timedelta(hours=12))
        await heartbeat(hass, proxy, sensor)
    await wall.advance(timedelta(seconds=5))
    found = issue(hass, SINGLE_PATH)
    assert found is not None and found.translation_placeholders is not None
    assert found.translation_placeholders["proxy"] == "Garage proxy"

    # A second proxy hears it too.
    hass.config_entries.async_add_subentry(loaded, _proxy_subentry("proxy2", "b2" * 16))
    await hass.async_block_till_done()
    second = SimProxy(bytes.fromhex("b2" * 16), bytes.fromhex("348518fffe000002"), port)
    await second.start()
    try:
        await second.connect()
        proxy.hello()
        await proxy.recv(cs.M_SERVER_STATUS)
        sensor.server_time = int(time.time())
        frame = sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0))
        rx = proxy.uplink(frame, snr=9.0)
        second.uplink(frame, snr=1.0)
        sensor.downlink(await answer(proxy, rx))
        await hass.async_block_till_done()
    finally:
        second.close()
    state = state_of(hass, PROXIES)
    assert state.state == "2"
    assert state.attributes["single_path"] is False
    await wall.advance(timedelta(seconds=5))
    assert issue(hass, SINGLE_PATH) is None


def _proxy_subentry(subentry_id: str, key: str) -> ConfigSubentry:
    return ConfigSubentry(
        data=MappingProxyType({CONF_KEY: key}),
        subentry_type="proxy",
        title="Second proxy",
        subentry_id=subentry_id,
        unique_id=None,
    )


async def test_proxy_mismatch_issue(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy
) -> None:
    """Another radio profile raises a repair issue, cleared by a matching HELLO."""
    await proxy.connect(region="EU868")
    found = issue(hass, "proxy_mismatch_proxy1")
    assert found is not None and found.translation_placeholders is not None
    assert found.translation_placeholders["proxy_settings"] == "EU868 865.2 MHz SF9 125 kHz"
    assert found.translation_placeholders["network_settings"] == "US915 918.5 MHz SF9 500 kHz"

    proxy.hello()
    await proxy.recv(cs.M_SERVER_STATUS)
    await hass.async_block_till_done()
    assert issue(hass, "proxy_mismatch_proxy1") is None

    # Removing the proxy clears it too.
    proxy.hello(region="EU868")
    await proxy.recv(cs.M_SERVER_STATUS)
    assert issue(hass, "proxy_mismatch_proxy1") is not None
    hass.config_entries.async_remove_subentry(loaded, "proxy1")
    await hass.async_block_till_done()
    assert issue(hass, "proxy_mismatch_proxy1") is None


async def test_stall_issue_survives_restart_and_clears_on_removal(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    clock: Clock,
) -> None:
    """The pairing-stall issue is raised again after a restart, and goes with the device."""
    await proxy.connect()
    join = sensor.join()
    rx = proxy.uplink(join)
    await answer(proxy, rx)
    for _ in range(3):
        clock.advance(5_000)
        rx = proxy.uplink(join)
        await answer(proxy, rx)
    await hass.async_block_till_done()
    stall = f"pairing_stalled_{DEV_EUI}"
    assert issue(hass, stall) is not None

    ir.async_delete_issue(hass, DOMAIN, stall)
    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()
    assert issue(hass, stall) is not None

    hass.config_entries.async_remove_subentry(loaded, "device1")
    await hass.async_block_till_done()
    assert issue(hass, stall) is None


async def test_issues_cleared_when_device_removed(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """Removing an unreachable device takes its issue with it."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await wall.advance(SILENCE + timedelta(minutes=1))
    assert issue(hass, UNREACHABLE) is not None
    hass.config_entries.async_remove_subentry(loaded, "device1")
    await hass.async_block_till_done()
    assert issue(hass, UNREACHABLE) is None


async def test_wall_clock_jump(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """A wall-clock jump (NTP setting the clock after boot) is not silence."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    wall.offset += timedelta(hours=6)
    async_fire_time_changed(hass, wall())
    await hass.async_block_till_done()
    assert state_of(hass, DOOR).state == STATE_OFF
    assert issue(hass, UNREACHABLE) is None


async def test_issues_withdrawn_on_unload(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """Nothing watches the issues once the integration is unloaded or removed."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await wall.advance(SILENCE + timedelta(minutes=1))
    assert issue(hass, UNREACHABLE) is not None
    assert await hass.config_entries.async_unload(loaded.entry_id)
    assert issue(hass, UNREACHABLE) is None

    ir.async_create_issue(
        hass,
        DOMAIN,
        UNREACHABLE,
        is_fixable=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key="device_unreachable",
    )
    assert await hass.config_entries.async_remove(loaded.entry_id)
    await hass.async_block_till_done()
    assert issue(hass, UNREACHABLE) is None
