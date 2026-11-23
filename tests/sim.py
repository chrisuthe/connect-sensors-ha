"""A simulated proxy (over real UDP) and a simulated sensor, built on connectsensors."""

from __future__ import annotations

import asyncio
import struct
import time
from typing import Any

import connectsensors as cs

COUNTERS = [10, 9, 1, 0, 0, 2, 0]
# EU868 has no frequency yet: 865.2 MHz is the candidate in spec 01 section 3.2.
FREQUENCY_HZ = {"US915": 918_500_000, "EU868": 865_200_000}


def free_udp_port() -> int:
    """A port nothing listens on right now."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


class SimProxy(asyncio.DatagramProtocol):
    """A cs_proxy: sends envelopes to the server and checks what comes back (05 3.2)."""

    def __init__(
        self,
        key: bytes,
        proxy_id: bytes,
        server_port: int,
        boot_id: int = 0x0B007001,
        rx_time_us: int = 5_000_000,
    ) -> None:
        self.key = key
        self.proxy_id = proxy_id
        self.server_port = server_port
        self.boot_id = boot_id
        self.link = cs.ProxyLink(boot_id)
        self.seq = 0
        self.inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.transport: asyncio.DatagramTransport | None = None
        self.rx_time_us = rx_time_us
        # The TX_RESULT answering each DOWNLINK: (result, offset_us), or None for none.
        self.tx_result: tuple[int, int] | None = (0, 1800)

    async def start(self) -> None:
        """Open a socket towards the server."""
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: self, remote_addr=("127.0.0.1", self.server_port)
        )
        assert isinstance(transport, asyncio.DatagramTransport)
        self.transport = transport

    def close(self) -> None:
        """Close the socket."""
        if self.transport:
            self.transport.close()

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        """Queue every authenticated message from the server, with the proxy's verdict."""
        m = cs.open_envelope(self.key, data)
        m["verdict"] = self.link.receive(m, _ms())["result"]
        self.inbox.put_nowait(m)
        if m["type"] == cs.M_DOWNLINK and m["verdict"] == "accepted" and self.tx_result:
            self.send(cs.M_TX_RESULT, cs.tx_result_body(m["seq"], *self.tx_result))

    def send(self, msg_type: int, body: bytes) -> dict[str, Any]:
        """Send one message in this boot."""
        assert self.transport is not None
        self.seq += 1
        data = cs.envelope(self.key, msg_type, self.proxy_id, self.boot_id, self.seq, body)
        m = cs.open_envelope(self.key, data)
        self.link.sent(m, _ms())
        self.transport.sendto(data)
        return m

    def hello(
        self, nonce: int = 0, region: str = "US915", caps: int = cs.CAP_DOWNLINK
    ) -> dict[str, Any]:
        """Send a HELLO (05 section 5.2), with the region profile's radio settings."""
        sf, bw_hz = cs.REGIONS[region]
        return self.send(
            cs.M_HELLO,
            cs.hello_body(
                caps,
                region,
                FREQUENCY_HZ[region],
                sf,
                bw_hz // 1000,
                42,
                COUNTERS,
                12,
                "cs_proxy test",
                nonce,
            ),
        )

    def uplink(self, frame: bytes, rssi: float = -61.5, snr: float = 7.25, flags: int = 0) -> int:
        """Forward a frame; returns its rx_time_us."""
        self.rx_time_us += 7_000_000
        self.send(cs.M_UPLINK, cs.uplink_body(self.rx_time_us, rssi, snr, flags, frame))
        return self.rx_time_us

    async def recv(self, msg_type: int, within: float = 3.0) -> dict[str, Any]:
        """The next message of a type, skipping others."""
        async with asyncio.timeout(within):
            while True:
                m = await self.inbox.get()
                if m["type"] == msg_type:
                    return m

    async def nothing(self, wait: float = 0.4) -> None:
        """Assert the server sends nothing for a while."""
        await asyncio.sleep(wait)
        assert self.inbox.empty(), self.inbox.get_nowait()

    async def connect(self, region: str = "US915", caps: int = cs.CAP_DOWNLINK) -> dict[str, Any]:
        """Boot handshake: HELLO, HELLO_REQUEST, HELLO echoing the nonce, SERVER_STATUS."""
        self.hello(region=region, caps=caps)
        request = await self.recv(cs.M_HELLO_REQUEST)
        (nonce,) = struct.unpack(">I", bytes.fromhex(request["body"]))
        answer = self.hello(nonce, region=region, caps=caps)
        status = await self.recv(cs.M_SERVER_STATUS)
        assert status["verdict"] in ("accepted", "resync")
        assert cs.parse_server_status(bytes.fromhex(status["body"]))["hello_seq"] == answer["seq"]
        return status


class SimSensor:
    """A sensor: pairs with SensorPairing, then sends frames under its session keys.

    Its clock is real time plus clock_offset, which each downlink's server_time sets.
    """

    def __init__(self, k_root: bytes, dev_eui: bytes) -> None:
        self.dev_eui = dev_eui
        self.root = cs.keys_from(k_root, dev_eui)
        self.pairing = cs.SensorPairing(k_root, dev_eui)
        self.fcnt = 200
        self.clock_offset = 0
        self.session_floor = 0
        self._priv = 0xA1

    @property
    def k_session(self) -> bytes:
        """The session key, once KEY_EXCHANGE was accepted."""
        assert self.pairing.k_session is not None
        return self.pairing.k_session

    @property
    def session(self) -> cs.DeviceKeys:
        """Session key set."""
        return cs.keys_from(self.k_session, self.dev_eui)

    def now(self) -> int:
        """The sensor's clock."""
        return int(time.time()) + self.clock_offset

    def set_clock(self, server_time: int) -> None:
        """Take a downlink's server_time."""
        self.clock_offset = server_time - int(time.time())

    def restore(self, k_session: bytes, fcnt: int) -> None:
        """A sensor paired under k_session that last sent fcnt."""
        self.pairing.k_session = k_session
        self.pairing.state = "PAIRED"
        self.fcnt = fcnt

    def join(self) -> bytes:
        """A JOIN with a fresh ephemeral key (02 section 11.1)."""
        self.fcnt += 1
        self._priv += 1
        return self.pairing.join(self.fcnt, bytes([self._priv]) * 32)

    def key_exchange(self, frame: bytes) -> str:
        """Take the server's answer to JOIN: SESSION, with the clock it sets."""
        result = self.pairing.downlink(frame)
        if result == "accept":
            self.session_floor = 0
            _, pt = cs.open_frame(self.root, frame)
            self.set_clock(struct.unpack(">I", pt[4:8])[0])
        return result

    def uplink(self, ftype: int, readings: bytes, uflags: int = 0) -> bytes:
        """A session frame: normal tag, or the bootstrap tag for SYNC (02 section 6.1)."""
        self.fcnt += 1
        return self.pairing.uplink(ftype, self.fcnt, self.now(), bytes([uflags]) + readings)

    def sync(self, readings: bytes = b"", uflags: int = 0) -> bytes:
        """A SYNC: clock unknown, or lost contact (02 section 11.2)."""
        return self.uplink(cs.T_SYNC, readings, uflags | cs.UF_TIME_UNSURE)

    def downlink(self, frame: bytes) -> dict[str, Any]:
        """Accept a session downlink as a sensor does (02 section 10.2). The first one,
        in the provisional SESSION state, makes the sensor PAIRED (04 section 4.3)."""
        if self.pairing.state == "SESSION":
            assert self.pairing.downlink(frame) == "accept"
        h, pt = cs.open_frame(self.session, frame)
        assert h.ftype == cs.T_DOWNLINK
        assert h.fcnt > self.session_floor, "downlink counter not above the floor"
        self.session_floor = h.fcnt
        reply_to, server_time, flags = struct.unpack(">IIB", pt[:9])
        self.set_clock(server_time)
        return {
            "fcnt": h.fcnt,
            "tag": h.tag,
            "reply_to": reply_to,
            "flags": flags,
            "commands": pt[9:],
        }


def downlink_body(m: dict[str, Any]) -> dict[str, Any]:
    """Parse a DOWNLINK message body (05 section 5.4)."""
    body = bytes.fromhex(m["body"])
    ref, offset, n = struct.unpack(">QIB", body[:13])
    return {"ref_rx_time_us": ref, "tx_offset_us": offset, "frame": body[13 : 13 + n]}


def _ms() -> int:
    return time.monotonic_ns() // 1_000_000
