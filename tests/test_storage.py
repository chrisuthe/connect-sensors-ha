"""Stored state from earlier releases still loads (07 section 10)."""

from __future__ import annotations

import time
from typing import Any

import connectsensors as cs
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors.server import NetworkServer

from .common import answer, full_state
from .conftest import DEV_EUI, K_NET, K_ROOT, state_of
from .sim import SimProxy, SimSensor

PAIRING = "sensor.front_door_pairing"
COMMAND = "sensor.front_door_command"


def store(hass_storage: dict[str, Any], record: dict[str, Any], root_counter: int = 0) -> None:
    """Put a device record, and its root counter, where the server loads them."""
    hass_storage["connect_sensors.state"] = {
        "version": 1,
        "minor_version": 1,
        "key": "connect_sensors.state",
        "data": {"proxies": {}, "devices": {DEV_EUI: record}, "leaving": {}},
    }
    hass_storage["connect_sensors.root_counters"] = {
        "version": 1,
        "minor_version": 1,
        "key": "connect_sensors.root_counters",
        "data": {DEV_EUI: root_counter},
    }


def milestone_2(**changes: Any) -> dict[str, Any]:
    """A device record as connectsensors 0.2.0 releases wrote it."""
    return {
        "state": "paired",
        "k_session": None,
        "fcnt_down_reserved": 0,
        "last_fcnt": 0,
        "seen": [],
        "pending": None,
        "stalled": False,
        "readings": {"0:contact": 0},
        "sw_version": "0.1.0",
        "last_seen": "2026-11-01T12:00:00+00:00",
        "rssi": -61.5,
        "snr": 7.25,
        "unreachable": False,
        "stale": [],
        "heard_by": {},
        "single_path_since": None,
        "events": [],
        "commands": [],
        "in_flight": None,
        "next_command_id": 1,
        "last_command": None,
    } | changes


async def start(hass: HomeAssistant, entry: MockConfigEntry) -> NetworkServer:
    """Set up the entry from storage."""
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    server: NetworkServer = entry.runtime_data
    return server


async def test_pending_exchange(
    hass: HomeAssistant,
    entry: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    hass_storage: dict[str, Any],
) -> None:
    """A pending exchange stored without "sent" is restored as sent, and the sensor
    that took its KEY_EXCHANGE before the restart finishes pairing."""
    join = sensor.join()
    responder = cs.JoinResponder(bytes.fromhex(K_ROOT), bytes.fromhex(DEV_EUI))
    r = responder.join(join, bytes([0xB5]) * 32, int(time.time()))
    assert responder.pending is not None
    pending = responder.pending.to_dict()
    del pending["sent"]
    record = milestone_2(
        state="exchanged",
        k_session=r["k_session"].hex(),
        last_fcnt=sensor.fcnt,
        pending=pending,
        readings={},
        sw_version=None,
    )
    store(hass_storage, record, root_counter=1)
    server = await start(hass, entry)
    restored = server.devices[DEV_EUI].join.pending
    assert restored is not None
    assert restored.frame == r["frame"] and restored.sent
    assert state_of(hass, PAIRING).state == "exchanged"

    await proxy.connect()
    assert sensor.key_exchange(r["frame"]) == "accept"
    rx = proxy.uplink(sensor.sync())
    assert sensor.downlink(await answer(proxy, rx))["commands"][0] == cs.CMD_SET_NETWORK
    rx = proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0), cs.UF_DL_ACK))
    sensor.downlink(await answer(proxy, rx))
    await hass.async_block_till_done()
    assert state_of(hass, PAIRING).state == "paired"


async def test_commands_in_flight(
    hass: HomeAssistant,
    entry: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    hass_storage: dict[str, Any],
) -> None:
    """A queued command and its "in_flight" record are restored: the next uplink with
    DL_ACK delivers it, and the late arrivals stored in "seen" stay rejected."""
    k_session = bytes(range(100, 116))
    last_fcnt = 500
    record = milestone_2(
        k_session=k_session.hex(),
        fcnt_down_reserved=64,
        last_fcnt=last_fcnt,
        seen=[497],
        commands=[
            {
                "id": 1,
                "name": "set_heartbeat",
                "slot": "heartbeat",
                "cmd": cs.CMD_SET_HEARTBEAT,
                "args": "0002",
                "queued_at": dt_util.utcnow().isoformat(),
                "sends": 1,
            }
        ],
        in_flight=[last_fcnt, [1]],
        next_command_id=2,
    )
    store(hass_storage, record)
    server = await start(hass, entry)
    device = server.devices[DEV_EUI]
    assert device.acks.reply_to == last_fcnt and device.acks.awaiting == [1]
    assert device.uplinks.counter.seen == {497}

    sensor.pairing.k_session = k_session
    sensor.k_net_route = cs.derive_net_route(bytes.fromhex(K_NET))
    sensor.server_time = int(time.time())
    await proxy.connect()
    sensor.fcnt = 496
    proxy.uplink(sensor.uplink(cs.T_DATA_CONFIRMED, full_state(0)))
    await proxy.nothing()

    sensor.fcnt = last_fcnt
    proxy.uplink(sensor.uplink(cs.T_HEARTBEAT, full_state(0), cs.UF_DL_ACK))
    await proxy.nothing()
    await hass.async_block_till_done()
    assert device.commands == []
    assert state_of(hass, COMMAND).state == "delivered"
