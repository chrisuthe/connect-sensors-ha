"""The network server end to end: a simulated proxy over UDP and a simulated sensor."""

from __future__ import annotations

from datetime import timedelta
import json
import struct
import time
from typing import Any

import connectsensors as cs
from homeassistant.const import EVENT_HOMEASSISTANT_STOP, STATE_OFF, STATE_ON
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr, issue_registry as ir
from homeassistant.util import dt as dt_util
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed
from pytest_homeassistant_custom_component.components.diagnostics import (
    get_diagnostics_for_config_entry,
)
from pytest_homeassistant_custom_component.typing import ClientSessionGenerator

from custom_components.connect_sensors.const import DOMAIN

from .common import answer, full_state, pair
from .conftest import (
    DEV_EUI,
    K_NET,
    K_ROOT,
    PROXY_ID,
    Clock,
    assert_no_secrets,
    state_of,
)
from .sim import SimProxy, SimSensor, downlink_body

DOOR = "binary_sensor.front_door_door"
PAIRING = "sensor.front_door_pairing"
RETRY_MS = 5_000  # a sensor's first retry comes 4-6 s after the original (08 section 5)


async def test_proxy_session(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy, port: int
) -> None:
    """HELLO_REQUEST handshake, SERVER_STATUS, the proxy's device and entities."""
    status = await proxy.connect()
    tags = cs.parse_server_status(bytes.fromhex(status["body"]))["tags"]
    route = cs.derive_net_route(bytes.fromhex(K_NET))
    assert cs.net_tag(route, cs.epoch_of(int(time.time()))) in tags
    await hass.async_block_till_done()

    device = dr.async_get(hass).async_get_device_by_identifier(
        (DOMAIN, PROXY_ID.hex()), loaded.entry_id
    )
    assert device is not None
    assert device.name == "Garage proxy"
    assert (dr.CONNECTION_NETWORK_MAC, "34:85:18:00:00:01") in device.connections
    assert device.sw_version == "cs_proxy test"
    assert state_of(hass, "binary_sensor.garage_proxy_connectivity").state == STATE_ON
    assert state_of(hass, "sensor.garage_proxy_packets_heard").state == "10"

    # A periodic HELLO in the session is answered directly.
    hello = proxy.hello()
    status = await proxy.recv(cs.M_SERVER_STATUS)
    assert cs.parse_server_status(bytes.fromhex(status["body"]))["hello_seq"] == hello["seq"]

    # The proxy reboots: a new boot is only accepted after it echoes a nonce.
    rebooted = SimProxy(proxy.key, PROXY_ID, port, boot_id=0x0B007002)
    await rebooted.start()
    try:
        await rebooted.connect()
    finally:
        rebooted.close()

    # A message under another key is dropped without an answer.
    stranger = SimProxy(bytes(16), PROXY_ID, port)
    await stranger.start()
    try:
        stranger.hello()
        await stranger.nothing()
    finally:
        stranger.close()


async def test_server_restart_resync(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy
) -> None:
    """After a restart the proxy's next message draws a HELLO_REQUEST (05 section 3.2)."""
    await proxy.connect()
    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()

    proxy.uplink(b"\xc5\x11" + bytes(30))  # any frame: not this session's boot any more
    request = await proxy.recv(cs.M_HELLO_REQUEST)
    assert request["seq"] == 0
    (nonce,) = struct.unpack(">I", bytes.fromhex(request["body"]))
    answer_hello = proxy.hello(nonce)
    status = await proxy.recv(cs.M_SERVER_STATUS)
    assert status["verdict"] in ("accepted", "resync")
    assert cs.parse_server_status(bytes.fromhex(status["body"]))["hello_seq"] == answer_hello["seq"]


async def test_pairing_contact_and_acks(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    clock: Clock,
) -> None:
    """Pair from JOIN, then contact changes update the binary_sensor and are acked in RX1."""
    await proxy.connect()
    assert state_of(hass, PAIRING).state == "scanned"

    join = sensor.join()
    rx = proxy.uplink(join)
    key_exchange = await answer(proxy, rx)
    await hass.async_block_till_done()
    assert state_of(hass, PAIRING).state == "exchanged"

    # The sensor missed it and retries: the identical frame comes back (04 section 4.2).
    clock.advance(RETRY_MS)
    rx = proxy.uplink(join)
    assert await answer(proxy, rx) == key_exchange
    root_header, _ = cs.open_frame(
        cs.keys_from(bytes.fromhex(K_ROOT), bytes.fromhex(DEV_EUI)), key_exchange
    )
    assert root_header.fcnt == 1  # the first root downlink counter value

    assert sensor.key_exchange(key_exchange) == "accept"
    rx = proxy.uplink(sensor.sync())
    assert sensor.downlink(await answer(proxy, rx))["commands"][0] == cs.CMD_SET_NETWORK
    rx = proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0), cs.UF_DL_ACK))
    sensor.downlink(await answer(proxy, rx))
    await hass.async_block_till_done()

    assert state_of(hass, PAIRING).state == "paired"
    door = state_of(hass, DOOR)
    assert door.state == STATE_OFF
    assert door.attributes["device_class"] == "door"
    assert state_of(hass, "sensor.front_door_temperature").state == "21.53"
    assert state_of(hass, "sensor.front_door_battery").state == "95.0"
    assert state_of(hass, "sensor.front_door_signal_strength").state == "-61.5"
    device = dr.async_get(hass).async_get_device_by_identifier((DOMAIN, DEV_EUI), loaded.entry_id)
    assert device is not None and device.sw_version == "0.1.0"

    # The door opens: a confirmed uplink, acked in RX1 through the proxy that heard it.
    frame = sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(0, 0x01, 1))
    rx = proxy.uplink(frame)
    ack = sensor.downlink(await answer(proxy, rx))
    assert ack["reply_to"] == sensor.fcnt
    assert ack["commands"] == b""
    await hass.async_block_till_done()
    assert state_of(hass, DOOR).state == STATE_ON

    # A second copy of the same transmission is not answered again.
    proxy.uplink(frame)
    await proxy.nothing()

    # The sensor's retry is a new transmission: answered again with a new counter,
    # and not applied twice.
    clock.advance(RETRY_MS)
    rx = proxy.uplink(frame)
    again = sensor.downlink(await answer(proxy, rx))
    assert again["reply_to"] == ack["reply_to"]
    assert again["fcnt"] > ack["fcnt"]

    # The door closes, unconfirmed: applied, not answered.
    proxy.uplink(sensor.uplink(cs.T_DATA, cs.encode_reading(0, 0x01, 0)))
    await proxy.nothing()
    assert state_of(hass, DOOR).state == STATE_OFF

    # The proxy reports the downlink as sent.
    m = await _last_downlink_seq(proxy, sensor, clock)
    proxy.send(cs.M_TX_RESULT, cs.tx_result_body(m, 0, 1800))
    await proxy.nothing(0.2)
    assert loaded.runtime_data.proxies["proxy1"].tx_results[-1] == ("sent", 1800)


async def _last_downlink_seq(proxy: SimProxy, sensor: SimSensor, clock: Clock) -> int:
    """Draw one more ack and return its DOWNLINK seq."""
    rx = proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0)))
    m = await proxy.recv(cs.M_DOWNLINK)
    assert downlink_body(m)["ref_rx_time_us"] == rx
    seq: int = m["seq"]
    return seq


async def test_pairing_stalled(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    clock: Clock,
) -> None:
    """A JOIN repeated after its answer stalls the pairing and raises a repair issue."""
    await proxy.connect()
    join = sensor.join()
    rx = proxy.uplink(join)
    first = await answer(proxy, rx)
    for _ in range(3):
        clock.advance(RETRY_MS)
        rx = proxy.uplink(join)
        assert await answer(proxy, rx) == first
    await hass.async_block_till_done()
    issue_id = f"pairing_stalled_{DEV_EUI}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None
    assert state_of(hass, PAIRING).state == "stalled"

    # The user resets the sensor and presses the button: a new JOIN, the next counter.
    rx = proxy.uplink(sensor.join())
    second = await answer(proxy, rx)
    header, _ = cs.open_frame(cs.keys_from(bytes.fromhex(K_ROOT), bytes.fromhex(DEV_EUI)), second)
    assert header.fcnt == 2
    await hass.async_block_till_done()
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None
    assert state_of(hass, PAIRING).state == "exchanged"


async def test_restart_restores_state(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    clock: Clock,
    hass_storage: dict[str, Any],
) -> None:
    """Sessions, counters and readings survive a restart; old frames stay rejected."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    assert hass_storage["connect_sensors.root_counters"]["data"] == {DEV_EUI: 1}

    frame = sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(0, 0x01, 1))
    rx = proxy.uplink(frame)
    before = sensor.downlink(await answer(proxy, rx))
    await hass.async_block_till_done()
    assert state_of(hass, DOOR).state == STATE_ON

    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()
    assert state_of(hass, DOOR).state == STATE_ON
    assert state_of(hass, PAIRING).state == "paired"
    assert state_of(hass, "sensor.front_door_temperature").state == "21.53"

    await proxy.connect()
    # A replay of a frame from before the restart is neither applied nor answered.
    clock.advance(RETRY_MS)
    proxy.uplink(frame)
    await proxy.nothing()

    # New frames are accepted, and downlink counters continue above the old ones.
    rx = proxy.uplink(sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(0, 0x01, 0)))
    after = sensor.downlink(await answer(proxy, rx))
    assert after["fcnt"] > before["fcnt"]
    await hass.async_block_till_done()
    assert state_of(hass, DOOR).state == STATE_OFF


async def test_removed_device_keeps_root_counter(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    hass_storage: dict[str, Any],
) -> None:
    """Removing a device keeps its root downlink counter (02 section 8.4)."""
    await proxy.connect()
    rx = proxy.uplink(sensor.join())
    await answer(proxy, rx)
    subentry = loaded.subentries["device1"]

    hass.config_entries.async_remove_subentry(loaded, "device1")
    await hass.async_block_till_done()
    assert (
        dr.async_get(hass).async_get_device_by_identifier((DOMAIN, DEV_EUI), loaded.entry_id)
        is None
    )
    proxy.uplink(sensor.join())
    await proxy.nothing()

    hass.config_entries.async_add_subentry(loaded, subentry)
    await hass.async_block_till_done()
    rx = proxy.uplink(sensor.join())
    header, _ = cs.open_frame(
        cs.keys_from(bytes.fromhex(K_ROOT), bytes.fromhex(DEV_EUI)), await answer(proxy, rx)
    )
    assert header.fcnt == 2
    await hass.config_entries.async_unload(loaded.entry_id)
    assert hass_storage["connect_sensors.root_counters"]["data"] == {DEV_EUI: 2}


@pytest.mark.parametrize("region", ["EU868"])
async def test_mismatched_proxy_gets_no_downlinks(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    region: str,
) -> None:
    """A proxy on another radio profile never transmits (05 section 5.2)."""
    proxy.hello(region=region)
    request = await proxy.recv(cs.M_HELLO_REQUEST)
    proxy.hello(struct.unpack(">I", bytes.fromhex(request["body"]))[0], region=region)
    await proxy.recv(cs.M_SERVER_STATUS)
    proxy.uplink(sensor.join())
    await proxy.nothing()


async def test_diagnostics_redacted(
    hass: HomeAssistant,
    hass_client: ClientSessionGenerator,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
) -> None:
    """Diagnostics show the state but no key (07 section 11.6)."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    diag = await get_diagnostics_for_config_entry(hass, hass_client, loaded)

    assert_no_secrets(json.dumps(diag), extra=(sensor.k_session.hex(),))
    device = diag["server"]["devices"][DEV_EUI]
    assert device["state"] == "paired"
    assert device["k_session"] == "**REDACTED**"
    assert diag["server"]["proxies"]["proxy1"]["proxy_id"] == PROXY_ID.hex()
    assert diag["entry"]["data"]["k_net"] == "**REDACTED**"


async def test_start_from_storage(
    hass: HomeAssistant,
    entry: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    hass_storage: dict[str, Any],
) -> None:
    """A start after a crash resumes counters from storage, never below a reservation."""
    k_session = bytes(range(100, 116))
    reserved, last_fcnt = 130, 500
    record = {
        "state": "paired",
        "k_session": k_session.hex(),
        "fcnt_down_reserved": reserved,
        "last_fcnt": last_fcnt,
        "seen": [],
        "pending": None,
        "stalled": False,
        "readings": {"0:contact": 1},
        "sw_version": None,
        "last_seen": None,
        "rssi": None,
        "snr": None,
    }
    hass_storage["connect_sensors.state"] = {
        "version": 1,
        "minor_version": 1,
        "key": "connect_sensors.state",
        "data": {"proxies": {}, "devices": {DEV_EUI: record}},
    }
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert state_of(hass, DOOR).state == STATE_ON

    untyped: Any = sensor.pairing
    untyped.k_session = k_session
    sensor.k_net_route = cs.derive_net_route(bytes.fromhex(K_NET))
    sensor.server_time = int(time.time())
    await proxy.connect()

    # The last accepted counter is not accepted again.
    sensor.fcnt = last_fcnt - 1
    proxy.uplink(sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(0, 0x01, 0)))
    await proxy.nothing()
    assert state_of(hass, DOOR).state == STATE_ON

    rx = proxy.uplink(sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(0, 0x01, 0)))
    ack = sensor.downlink(await answer(proxy, rx))
    assert ack["fcnt"] == reserved
    await hass.async_block_till_done()
    assert state_of(hass, DOOR).state == STATE_OFF
    stored = hass_storage["connect_sensors.state"]["data"]["devices"][DEV_EUI]
    assert stored["fcnt_down_reserved"] == reserved + 64


async def test_counters_saved_under_steady_traffic(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    hass_storage: dict[str, Any],
) -> None:
    """Uplink counters reach storage within 10 s even while frames keep arriving."""
    await proxy.connect()
    await pair(hass, proxy, sensor)

    def stored_fcnt() -> int:
        value: int = hass_storage["connect_sensors.state"]["data"]["devices"][DEV_EUI]["last_fcnt"]
        return value

    start = dt_util.utcnow()
    proxy.uplink(sensor.uplink(cs.T_DATA, cs.encode_reading(0, 0x01, 1)))
    await proxy.nothing(0.2)
    first = sensor.fcnt
    assert stored_fcnt() < first
    async_fire_time_changed(hass, start + timedelta(seconds=6))
    await hass.async_block_till_done()
    proxy.uplink(sensor.uplink(cs.T_DATA, cs.encode_reading(0, 0x01, 0)))
    await proxy.nothing(0.2)
    async_fire_time_changed(hass, start + timedelta(seconds=11))
    await hass.async_block_till_done()
    assert stored_fcnt() >= first


async def test_stops_with_home_assistant(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy
) -> None:
    """At shutdown the server stops answering (entries are not unloaded then)."""
    await proxy.connect()
    hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP)
    await hass.async_block_till_done()
    proxy.hello()
    await proxy.nothing()
