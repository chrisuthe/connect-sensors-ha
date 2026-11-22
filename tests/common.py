"""Helpers shared by the end-to-end tests."""

from __future__ import annotations

from typing import Any

import connectsensors as cs
from homeassistant.core import HomeAssistant

from .conftest import K_NET
from .sim import SimProxy, SimSensor, downlink_body


def full_state(contact: int) -> bytes:
    """A contact sensor's heartbeat readings (03 section 5)."""
    return (
        cs.encode_reading(0, 0x01, contact)
        + cs.encode_reading(0, 0x40, 2153)
        + cs.encode_reading(0xFF, 0x07, 95)
        + cs.encode_reading(0xFF, 0xC2, bytes([0, 1, 0]))
    )


async def answer(proxy: SimProxy, rx_time_us: int) -> bytes:
    """The next DOWNLINK, which must target RX1 of the given uplink (05 section 5.4)."""
    m = await proxy.recv(cs.M_DOWNLINK)
    assert m["verdict"] == "accepted"
    dl = downlink_body(m)
    assert dl["ref_rx_time_us"] == rx_time_us
    assert dl["tx_offset_us"] == 1_000_000
    frame: bytes = dl["frame"]
    return frame


async def pair(hass: HomeAssistant, proxy: SimProxy, sensor: SimSensor) -> dict[str, Any]:
    """JOIN, KEY_EXCHANGE, SYNC, SET_NETWORK, then a heartbeat (04 section 4.4)."""
    rx = proxy.uplink(sensor.join())
    assert sensor.key_exchange(await answer(proxy, rx)) == "accept"
    rx = proxy.uplink(sensor.sync())
    set_network = sensor.downlink(await answer(proxy, rx))
    assert set_network["commands"][0] == cs.CMD_SET_NETWORK
    assert sensor.k_net_route == cs.derive_net_route(bytes.fromhex(K_NET))
    rx = proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0), cs.UF_DL_ACK))
    ack = sensor.downlink(await answer(proxy, rx))
    assert ack["reply_to"] == sensor.fcnt
    await hass.async_block_till_done()
    return ack
