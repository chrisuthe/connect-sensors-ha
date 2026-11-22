"""The command queue (07 section 7), its actions (07 section 11.4) and LEAVE (04 6.1)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import patch

import connectsensors as cs
from homeassistant.const import STATE_OFF, STATE_ON, STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import device_registry as dr
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors.const import CONF_HEARTBEAT, DOMAIN
from custom_components.connect_sensors.server import NetworkServer

from .common import answer, full_state, pair, uplink_answered, uplink_unanswered
from .conftest import DEV_EUI, K_ROOT, WallClock, state_of
from .sim import SimProxy, SimSensor

COMMAND = "sensor.front_door_command"
DOOR = "binary_sensor.front_door_door"
SET_HEARTBEAT_2 = (cs.CMD_SET_HEARTBEAT, b"\x00\x02")


def device_id(hass: HomeAssistant, entry: MockConfigEntry) -> str:
    """The registry id of the entry's sensor."""
    device = dr.async_get(hass).async_get_device_by_identifier((DOMAIN, DEV_EUI), entry.entry_id)
    assert device is not None
    return device.id


async def set_heartbeat(hass: HomeAssistant, entry: MockConfigEntry, minutes: int) -> None:
    """Call the set_heartbeat action for the entry's sensor."""
    await hass.services.async_call(
        DOMAIN,
        "set_heartbeat",
        {"device_id": device_id(hass, entry), "minutes": minutes},
        blocking=True,
    )


def server_of(entry: MockConfigEntry) -> NetworkServer:
    """The entry's network server."""
    server: NetworkServer = entry.runtime_data
    return server


async def test_set_heartbeat_resent_until_acknowledged(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """SET_HEARTBEAT rides on every answer until DL_ACK, then supervision follows it."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await set_heartbeat(hass, loaded, 5)
    # A second one replaces the first (07 section 7).
    await set_heartbeat(hass, loaded, 2)
    assert state_of(hass, COMMAND).state == "pending"
    assert state_of(hass, COMMAND).attributes["queued"] == ["set_heartbeat"]

    # An unconfirmed heartbeat is answered because a command waits (07 section 5).
    dl = await uplink_answered(hass, proxy, sensor)
    assert dl["parsed"] == [SET_HEARTBEAT_2]
    assert not dl["flags"] & cs.DF_PENDING
    # The sensor missed it: the next uplink has no DL_ACK, so it is sent again.
    dl = await uplink_answered(hass, proxy, sensor)
    assert dl["parsed"] == [SET_HEARTBEAT_2]

    await uplink_unanswered(hass, proxy, sensor, uflags=cs.UF_DL_ACK)
    assert state_of(hass, COMMAND).state == "delivered"
    assert state_of(hass, COMMAND).attributes["last_command"] == "set_heartbeat"
    assert loaded.subentries["device1"].data[CONF_HEARTBEAT] == 2
    # Nothing is left to send.
    await uplink_unanswered(hass, proxy, sensor)

    # Unreachable after 3 x 2 + 5 minutes now, not 3 x 30 + 5.
    await wall.advance(timedelta(minutes=10))
    assert state_of(hass, DOOR).state == STATE_OFF
    await wall.advance(timedelta(minutes=2))
    assert state_of(hass, DOOR).state == STATE_UNAVAILABLE


async def test_late_dl_ack_settles_nothing(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy, sensor: SimSensor
) -> None:
    """DL_ACK on a frame sent before the downlink does not deliver its commands
    (chrisuthe/ConnectSensors#8)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    early = sensor.uplink(cs.T_HEARTBEAT, full_state(0), cs.UF_DL_ACK)
    await set_heartbeat(hass, loaded, 2)
    dl = await uplink_answered(hass, proxy, sensor)
    assert dl["parsed"] == [SET_HEARTBEAT_2]

    # The earlier frame arrives late with DL_ACK set (02 section 8.5): not answered,
    # since the sensor would not accept it, and the command is still owed.
    proxy.uplink(early)
    await proxy.nothing()
    await hass.async_block_till_done()
    assert state_of(hass, COMMAND).state == "pending"
    dl = await uplink_answered(hass, proxy, sensor)
    assert dl["parsed"] == [SET_HEARTBEAT_2]


async def test_given_up_after_the_bound(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A command sent the most times without DL_ACK is dropped, and that is shown."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    with patch("custom_components.connect_sensors.server.COMMAND_MAX_SENDS", 3):
        await set_heartbeat(hass, loaded, 2)
        for _ in range(3):
            dl = await uplink_answered(hass, proxy, sensor)
            assert dl["parsed"] == [SET_HEARTBEAT_2]
        await uplink_unanswered(hass, proxy, sensor)
    assert state_of(hass, COMMAND).state == "failed"
    assert "did not acknowledge set_heartbeat" in caplog.text
    assert loaded.subentries["device1"].data.get(CONF_HEARTBEAT) is None


async def test_given_up_after_seven_days(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """A command not delivered within 7 days is dropped (07 section 7)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await set_heartbeat(hass, loaded, 2)
    await wall.advance(timedelta(days=7) - timedelta(minutes=1))
    assert state_of(hass, COMMAND).state == "pending"
    await wall.advance(timedelta(minutes=2))
    assert state_of(hass, COMMAND).attributes["queued"] == []
    assert state_of(hass, COMMAND).attributes["last_command"] == "set_heartbeat"


async def test_queue_survives_restart(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy, sensor: SimSensor
) -> None:
    """The queue is persisted whenever it changes (07 section 10)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await set_heartbeat(hass, loaded, 2)
    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()
    await proxy.connect()
    dl = await uplink_answered(hass, proxy, sensor)
    assert dl["parsed"] == [SET_HEARTBEAT_2]


async def test_actions_refused(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy, sensor: SimSensor
) -> None:
    """Unpaired devices and channels the profile does not list as writable."""
    with pytest.raises(ServiceValidationError) as err:
        await set_heartbeat(hass, loaded, 2)
    assert err.value.translation_key == "not_paired"

    await proxy.connect()
    await pair(hass, proxy, sensor)
    with pytest.raises(ServiceValidationError) as err:
        await hass.services.async_call(
            DOMAIN,
            "set_value",
            {"device_id": device_id(hass, loaded), "channel": 0, "kind": "contact", "value": 1},
            blocking=True,
        )
    assert err.value.translation_key == "not_writable"
    with pytest.raises(ServiceValidationError) as err:
        await hass.services.async_call(
            DOMAIN, "set_heartbeat", {"device_id": "nope", "minutes": 2}, blocking=True
        )
    assert err.value.translation_key == "unknown_device"


async def test_leave_on_removal(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    hass_storage: dict[str, Any],
) -> None:
    """Removing a reachable device sends LEAVE; its DL_ACK deletes the record."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await set_heartbeat(hass, loaded, 2)
    subentry = loaded.subentries["device1"]
    hass.config_entries.async_remove_subentry(loaded, "device1")
    await hass.async_block_till_done()
    assert hass.states.get(DOOR) is None
    server = server_of(loaded)
    assert DEV_EUI in server.leaving

    # Kept across a restart while LEAVE is outstanding.
    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()
    server = server_of(loaded)
    assert DEV_EUI in server.leaving
    await proxy.connect()

    # LEAVE alone: the queued SET_HEARTBEAT no longer matters.
    dl = await uplink_answered(hass, proxy, sensor, readings=cs.encode_reading(0, 0x01, 1))
    assert dl["parsed"] == [(cs.CMD_LEAVE, b"")]
    assert hass.states.get(DOOR) is None
    # 08 section 8: the DL_ACK goes in one final unconfirmed HEARTBEAT.
    await uplink_unanswered(hass, proxy, sensor, uflags=cs.UF_DL_ACK)
    assert DEV_EUI not in server.leaving
    await hass.async_block_till_done()
    assert hass_storage["connect_sensors.state"]["data"]["leaving"] == {}

    # Added again: pairing continues the root counter (02 section 8.4).
    hass.config_entries.async_add_subentry(loaded, subentry)
    await hass.async_block_till_done()
    rx = proxy.uplink(sensor.join())
    header, _ = cs.open_frame(
        cs.keys_from(bytes.fromhex(K_ROOT), bytes.fromhex(DEV_EUI)), await answer(proxy, rx)
    )
    assert header.fcnt == 2


async def test_unreachable_device_removed_at_once(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """LEAVE is only sent to a reachable device (04 section 6.1)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await wall.advance(timedelta(hours=2))
    hass.config_entries.async_remove_subentry(loaded, "device1")
    await hass.async_block_till_done()
    assert server_of(loaded).leaving == {}


async def test_leave_given_up_after_48_hours(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """The record is deleted 48 hours after removal without DL_ACK (04 section 6.1)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    hass.config_entries.async_remove_subentry(loaded, "device1")
    await hass.async_block_till_done()
    await wall.advance(timedelta(hours=47))
    assert DEV_EUI in server_of(loaded).leaving
    await wall.advance(timedelta(hours=2))
    assert server_of(loaded).leaving == {}


async def test_added_again_before_leave(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy, sensor: SimSensor
) -> None:
    """Adding the device back before it acknowledges LEAVE drops LEAVE and keeps its
    session: the sensor never left, so it carries on paired."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    subentry = loaded.subentries["device1"]
    hass.config_entries.async_remove_subentry(loaded, "device1")
    await hass.async_block_till_done()
    hass.config_entries.async_add_subentry(loaded, subentry)
    await hass.async_block_till_done()
    assert server_of(loaded).leaving == {}

    dl = await uplink_answered(
        hass, proxy, sensor, cs.T_DATA_CONFIRMED, readings=cs.encode_reading(0, 0x01, 1)
    )
    assert dl["parsed"] == []
    assert state_of(hass, DOOR).state == STATE_ON


async def test_pending_when_more_remain(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy, sensor: SimSensor
) -> None:
    """Commands beyond 55 bytes wait for the next downlink, flagged PENDING (02 10.1)."""
    await proxy.connect()
    entry = loaded.runtime_data
    await pair(hass, proxy, sensor)
    device = entry.devices[DEV_EUI]
    # Ten SET_VALUEs of 6 bytes each: nine fit in 55 bytes.
    for channel in range(10):
        assert await entry.async_queue(
            device,
            "set_value",
            f"value_{channel}",
            0x40,
            cs.encode_reading(channel, 0x40, 2000 + channel),
        )
    dl = await uplink_answered(hass, proxy, sensor)
    assert len(dl["parsed"]) == 9
    assert dl["flags"] & cs.DF_PENDING
    dl = await uplink_answered(hass, proxy, sensor, uflags=cs.UF_DL_ACK)
    assert dl["parsed"] == [(0x40, cs.encode_reading(9, 0x40, 2009))]
    assert not dl["flags"] & cs.DF_PENDING
    await uplink_unanswered(hass, proxy, sensor, uflags=cs.UF_DL_ACK)
    assert device.commands == []


async def test_send_counted_only_when_transmitted(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy, sensor: SimSensor
) -> None:
    """An answer no proxy could transmit does not count towards giving up."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    with patch("custom_components.connect_sensors.server.COMMAND_MAX_SENDS", 2):
        await set_heartbeat(hass, loaded, 2)
        # Uncertain timestamps rule the only proxy out of RX1, and an unconfirmed
        # uplink has no RX2 (08 section 7).
        for _ in range(4):
            proxy.uplink(sensor.uplink(cs.T_HEARTBEAT, full_state(0)), flags=cs.UPF_TS_UNCERTAIN)
            await proxy.nothing()
        await hass.async_block_till_done()
        assert state_of(hass, COMMAND).state == "pending"
        dl = await uplink_answered(hass, proxy, sensor)
        assert dl["parsed"] == [SET_HEARTBEAT_2]


async def test_longer_heartbeat_not_unreachable(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """A longer heartbeat is allowed for from when it is queued: the sensor applies it
    on receipt and acknowledges it one new interval later."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await set_heartbeat(hass, loaded, 120)
    dl = await uplink_answered(hass, proxy, sensor)
    assert dl["parsed"] == [(cs.CMD_SET_HEARTBEAT, (120).to_bytes(2, "big"))]
    # Past 3 x 30 + 5 minutes, still within 3 x 120 + 5.
    await wall.advance(timedelta(minutes=120))
    assert state_of(hass, DOOR).state == STATE_OFF
