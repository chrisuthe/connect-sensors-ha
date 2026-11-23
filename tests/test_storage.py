"""Stored state loads (07 section 10), and state from connectsensors 0.3.x is migrated."""

from __future__ import annotations

import time
from typing import Any

import connectsensors as cs
from homeassistant.components.repairs import repairs_flow_manager
from homeassistant.const import CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import issue_registry as ir
from homeassistant.setup import async_setup_component
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry

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
from custom_components.connect_sensors.server import NetworkServer

from .common import answer, full_state
from .conftest import DEV_EUI, K_ROOT, PROXY_KEY, state_of
from .sim import SimProxy, SimSensor

PAIRING = "sensor.front_door_pairing"
COMMAND = "sensor.front_door_command"


def store(
    hass_storage: dict[str, Any],
    record: dict[str, Any],
    root_counter: int = 0,
    version: int = 2,
    leaving: dict[str, Any] | None = None,
) -> None:
    """Put a device record, and its root counter, where the server loads them."""
    hass_storage["connect_sensors.state"] = {
        "version": version,
        "minor_version": 1,
        "key": "connect_sensors.state",
        "data": {"proxies": {}, "devices": {DEV_EUI: record}, "leaving": leaving or {}},
    }
    hass_storage["connect_sensors.root_counters"] = {
        "version": 1,
        "minor_version": 1,
        "key": "connect_sensors.root_counters",
        "data": {DEV_EUI: root_counter},
    }


def milestone_2(**changes: Any) -> dict[str, Any]:
    """A device record with the fields milestone 2 stored; later fields are optional."""
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
    """A pending exchange is restored, and the sensor that took its KEY_EXCHANGE
    before the restart finishes pairing. One stored without "sent" counts as sent."""
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
        exchanged_at=dt_util.utcnow().isoformat(),
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

    sensor.restore(k_session, 496)
    await proxy.connect()
    proxy.uplink(sensor.uplink(cs.T_DATA_CONFIRMED, full_state(0)))
    await proxy.nothing()

    sensor.fcnt = last_fcnt
    proxy.uplink(sensor.uplink(cs.T_HEARTBEAT, full_state(0), cs.UF_DL_ACK))
    await proxy.nothing()
    await hass.async_block_till_done()
    assert device.commands == []
    assert state_of(hass, COMMAND).state == "delivered"


async def test_migration_from_0_3(
    hass: HomeAssistant,
    port: int,
    proxy: SimProxy,
    sensor: SimSensor,
    hass_storage: dict[str, Any],
) -> None:
    """A network from connectsensors 0.3.x: the entry loses its network key, and the
    paired device, whose sensor dropped its pairing in the firmware upgrade, is SCANNED
    with a fixable repair issue. Its JOIN starts a new exchange on the kept root
    counter, whatever uplink counter the new firmware resumes from."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=1,
        title="ConnectSensors",
        data={CONF_REGION: "US915", CONF_PORT: port, "k_net": "f0" * 16},
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
    gone = "c5c5000000000099"
    record = milestone_2(
        k_session=bytes(range(100, 116)).hex(),
        fcnt_down_reserved=128,
        last_fcnt=5000,
        seen=[4998],
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
        in_flight=[5000, [1]],
        next_command_id=2,
    )
    leaving = {
        gone: {
            "subentry_id": "old",
            "name": "Old door",
            "data": {CONF_DEV_EUI: gone, CONF_K_ROOT: K_ROOT, CONF_PROFILE: "CS.CONTACT-1"},
            "since": dt_util.utcnow().isoformat(),
            "record": milestone_2(k_session=bytes(range(16)).hex()),
        }
    }
    store(hass_storage, record, root_counter=3, version=1, leaving=leaving)
    server = await start(hass, entry)

    assert entry.version == 2
    assert "k_net" not in entry.data
    device = server.devices[DEV_EUI]
    assert device.state == "scanned" and device.re_pair
    assert device.k_session is None and device.join.pending is None
    assert device.commands == [] and device.acks.reply_to is None
    assert device.join.root_fcnt_down == 3
    assert server.leaving == {}
    assert state_of(hass, PAIRING).state == "scanned"
    assert state_of(hass, "binary_sensor.front_door_door").state == "unavailable"
    issue_id = f"re_pair_{DEV_EUI}"
    issue = ir.async_get(hass).async_get_issue(DOMAIN, issue_id)
    assert issue is not None and issue.is_fixable
    assert issue.translation_placeholders == {"name": "Front door"}
    stored = hass_storage["connect_sensors.state"]
    assert stored["version"] == 2

    # The repair asks for the button press and is not done before the device pairs.
    assert await async_setup_component(hass, "repairs", {})
    manager = repairs_flow_manager(hass)
    assert manager is not None
    result = await manager.async_init(DOMAIN, context={"issue_id": issue_id})
    assert result["type"] is FlowResultType.FORM
    result = await manager.async_configure(result["flow_id"], {})
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "not_paired"}

    # The upgraded sensor's counter is far below the stored one.
    await proxy.connect()
    sensor.fcnt = 10
    rx = proxy.uplink(sensor.join())
    key_exchange = await answer(proxy, rx)
    root = cs.keys_from(bytes.fromhex(K_ROOT), bytes.fromhex(DEV_EUI))
    assert cs.open_frame(root, key_exchange)[0].fcnt == 4
    assert sensor.key_exchange(key_exchange) == "accept"
    rx = proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(1), cs.UF_DL_ACK))
    sensor.downlink(await answer(proxy, rx))
    await hass.async_block_till_done()
    assert state_of(hass, PAIRING).state == "paired"
    assert state_of(hass, "binary_sensor.front_door_door").state == "on"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None
    assert device.to_store()["re_pair"] is False

    result = await manager.async_configure(result["flow_id"], {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
