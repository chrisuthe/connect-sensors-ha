"""Helpers shared by the end-to-end tests."""

from __future__ import annotations

from typing import Any

import connectsensors as cs
from homeassistant.core import HomeAssistant

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
    """JOIN, KEY_EXCHANGE, then the first session uplink, whose answer completes
    pairing on the sensor (04 section 4.4)."""
    rx = proxy.uplink(sensor.join())
    assert sensor.key_exchange(await answer(proxy, rx)) == "accept"
    assert sensor.pairing.state == "SESSION"
    rx = proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0), cs.UF_DL_ACK))
    ack = sensor.downlink(await answer(proxy, rx))
    assert ack["reply_to"] == sensor.fcnt
    assert sensor.pairing.state == "PAIRED"
    await hass.async_block_till_done()
    return ack


def parse_commands(data: bytes) -> list[tuple[int, bytes]]:
    """The commands of a downlink payload, as (cmd_id, args) (02 section 10.3)."""
    out: list[tuple[int, bytes]] = []
    i = 0
    while i + 2 <= len(data):
        n = data[i + 1]
        out.append((data[i], data[i + 2 : i + 2 + n]))
        i += 2 + n
    return out


async def uplink_answered(
    hass: HomeAssistant,
    proxy: SimProxy,
    sensor: SimSensor,
    ftype: int = cs.T_HEARTBEAT,
    uflags: int = 0,
    readings: bytes | None = None,
) -> dict[str, Any]:
    """Send an uplink that must be answered in RX1; returns the accepted downlink."""
    rx = proxy.uplink(sensor.uplink(ftype, full_state(0) if readings is None else readings, uflags))
    dl = sensor.downlink(await answer(proxy, rx))
    assert dl["reply_to"] == sensor.fcnt
    dl["parsed"] = parse_commands(dl["commands"])
    await hass.async_block_till_done()
    return dl


async def uplink_unanswered(
    hass: HomeAssistant,
    proxy: SimProxy,
    sensor: SimSensor,
    ftype: int = cs.T_HEARTBEAT,
    uflags: int = 0,
    readings: bytes | None = None,
) -> None:
    """Send an uplink that the server must not answer."""
    proxy.uplink(sensor.uplink(ftype, full_state(0) if readings is None else readings, uflags))
    await proxy.nothing()
    await hass.async_block_till_done()
