"""The ConnectSensors network server (spec 07), on Home Assistant's event loop.

Protocol work (frames, envelopes, proxy sessions, dedupe, JOIN answers) is done by the
connectsensors package. This module keeps the state around it: which proxies and
devices exist, what is persisted when, and which proxy answers which uplink.

Pairing codes, keys and decrypted payloads are never logged (07 section 11.6).
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
import secrets
import struct
import time
from typing import Any, cast

import connectsensors as cs
from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.const import CONF_PORT, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import CALLBACK_TYPE, CoreState, Event, HomeAssistant, callback
from homeassistant.helpers import device_registry as dr, issue_registry as ir
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_call_later, async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .const import (
    BOOTSTRAP_PER_MINUTE,
    COLLECTION_WINDOW_MS,
    CONF_DEV_EUI,
    CONF_K_NET,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    PROXY_TIMEOUT_S,
    REGIONS,
    RESERVE_BLOCK,
    RX1_DEADLINE_MS,
    RX1_OFFSET_US,
    RX2_OFFSET_US,
    SAVE_DELAY_S,
    SIGNAL_DEVICE_ADDED,
    SIGNAL_NEW_READING,
    SIGNAL_PROXY_BOUND,
    STATE_EXCHANGED,
    STATE_PAIRED,
    STATE_SCANNED,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
    TX_RESULTS,
    signal_device_update,
    signal_proxy_update,
)
from .readings import reading_key

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
# Root downlink counters outlive devices and the config entry (02 section 8.4).
ROOT_STORAGE_KEY = f"{DOMAIN}.root_counters"
STATE_STORAGE_KEY = f"{DOMAIN}.state"

CONFIRMED_TYPES = frozenset({cs.T_DATA_CONFIRMED, cs.T_HEARTBEAT_CONFIRMED, cs.T_JOIN, cs.T_SYNC})
TICK = timedelta(seconds=5)
TX_HISTORY = 20


def _now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def _new_nonce() -> int:
    return secrets.randbits(32) or 1


# connectsensors 0.1.0 leaves ServerLink.boot_id and JoinResponder.pending unannotated,
# so type checkers see them as always None (chrisuthe/ConnectSensors#54).
def _boot_id(link: cs.ServerLink) -> int | None:
    untyped: Any = link
    return cast(int | None, untyped.boot_id)


def _pending(join: cs.JoinResponder) -> dict[str, Any] | None:
    untyped: Any = join
    return cast(dict[str, Any] | None, untyped.pending)


def _set_pending(join: cs.JoinResponder, pending: dict[str, Any]) -> None:
    untyped: Any = join
    untyped.pending = pending


@dataclass(slots=True)
class Reception:
    """One proxy's copy of a transmission (07 section 4, step 3)."""

    proxy: Proxy
    boot_id: int
    rx_time_us: int
    rssi: float
    snr: float
    flags: int


@dataclass(slots=True)
class Transmission:
    """A transmission being collected and answered (07 section 4)."""

    frame: bytes
    header: cs.ParsedHeader
    device: Device
    keys: cs.DeviceKeys
    plaintext: bytes
    counter: str
    answer: bool
    started_ms: int
    receptions: list[Reception] = field(default_factory=list)


class Proxy:
    """A proxy the user added: its key, and once it has said HELLO, its identity."""

    def __init__(self, subentry: ConfigSubentry, proxy_id: bytes | None) -> None:
        self.subentry_id = subentry.subentry_id
        self.name = subentry.title
        self.key = bytes.fromhex(subentry.data[CONF_KEY])
        self.proxy_id = proxy_id
        self.link = cs.ServerLink(_new_nonce)
        self.addr: tuple[str, int] | None = None
        self.hello: dict[str, Any] | None = None
        self.last_hello_ms: int | None = None
        self.last_hello: datetime | None = None
        self.radio_mismatch = False
        self.uplinks = 0
        self.downlinks_sent = 0
        self.tx_results: deque[tuple[str, int]] = deque(maxlen=TX_HISTORY)
        self.pending_downlinks: dict[int, str] = {}

    @property
    def proxy_id_hex(self) -> str | None:
        """The proxy_id as 16 hex digits."""
        return self.proxy_id.hex() if self.proxy_id is not None else None

    @property
    def connected(self) -> bool:
        """A HELLO within the last 180 s (07 section 8.2)."""
        return (
            self.last_hello_ms is not None
            and _now_ms() - self.last_hello_ms < PROXY_TIMEOUT_S * 1000
        )

    @property
    def can_downlink(self) -> bool:
        """Candidate for answering at all (07 section 6.1)."""
        return (
            self.connected
            and self.hello is not None
            and bool(self.hello["caps"] & cs.CAP_DOWNLINK)
            and not self.radio_mismatch
            and self.addr is not None
        )


class Device:
    """A sensor added by pairing code, in any pairing state."""

    def __init__(self, subentry: ConfigSubentry, root_fcnt_down: int) -> None:
        data = subentry.data
        self.subentry_id = subentry.subentry_id
        self.name = subentry.title
        self.dev_eui = bytes.fromhex(data[CONF_DEV_EUI])
        self.eui = data[CONF_DEV_EUI]
        self.profile: str = data[CONF_PROFILE]
        k_root = bytes.fromhex(data[CONF_K_ROOT])
        self.root_keys = cs.keys_from(k_root, self.dev_eui)
        self.join = cs.JoinResponder(k_root, self.dev_eui, root_fcnt_down)
        self.state = STATE_SCANNED
        self.k_session: bytes | None = None
        self.session_keys: cs.DeviceKeys | None = None
        self.fcnt_down = 0
        self.fcnt_down_reserved = 0
        self.uplinks = cs.UplinkCollector(0)
        self.readings: dict[str, Any] = {}
        self.sw_version: str | None = None
        self.last_seen: datetime | None = None
        self.rssi: float | None = None
        self.snr: float | None = None
        self.stalled = False
        self.lock = asyncio.Lock()

    @property
    def last_fcnt(self) -> int:
        """Highest accepted uplink counter."""
        return int(self.uplinks.counter.last)

    def set_session(self, k_session: bytes) -> None:
        """Start using a new k_session; its downlink counter starts at 1 (02 section 8.4)."""
        self.k_session = k_session
        self.session_keys = cs.keys_from(k_session, self.dev_eui)
        self.fcnt_down = 0
        self.fcnt_down_reserved = 0

    def to_store(self) -> dict[str, Any]:
        """The persisted record (07 section 2.2)."""
        pending = _pending(self.join)
        return {
            "state": self.state,
            "k_session": self.k_session.hex() if self.k_session else None,
            "fcnt_down_reserved": self.fcnt_down_reserved,
            "last_fcnt": self.last_fcnt,
            "seen": sorted(self.uplinks.counter.seen),
            "pending": None
            if pending is None
            else {
                "sensor_pub": pending["sensor_pub"].hex(),
                "frame": pending["frame"].hex(),
                "k_session": pending["k_session"].hex(),
                "retransmissions": pending["retransmissions"],
            },
            "stalled": self.stalled,
            "readings": self.readings,
            "sw_version": self.sw_version,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "rssi": self.rssi,
            "snr": self.snr,
        }

    def load_store(self, d: dict[str, Any]) -> None:
        """Restore the persisted record."""
        self.state = d["state"]
        if d["k_session"]:
            self.set_session(bytes.fromhex(d["k_session"]))
            # Resume above every value that may have been used (02 section 8.2).
            self.fcnt_down_reserved = d["fcnt_down_reserved"]
            self.fcnt_down = max(0, self.fcnt_down_reserved - 1)
        self.uplinks = cs.UplinkCollector(d["last_fcnt"])
        self.uplinks.counter.seen = set(d["seen"])
        if (p := d["pending"]) is not None:
            _set_pending(
                self.join,
                {
                    "sensor_pub": bytes.fromhex(p["sensor_pub"]),
                    "frame": bytes.fromhex(p["frame"]),
                    "k_session": bytes.fromhex(p["k_session"]),
                    "retransmissions": p["retransmissions"],
                },
            )
        self.stalled = d["stalled"]
        self.readings = d["readings"]
        self.sw_version = d["sw_version"]
        self.last_seen = dt_util.parse_datetime(d["last_seen"]) if d["last_seen"] else None
        self.rssi = d["rssi"]
        self.snr = d["snr"]


class NetworkServer(asyncio.DatagramProtocol):
    """One ConnectSensors network: its UDP endpoint, proxies and devices."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self.entry = entry
        self.port: int = entry.data[CONF_PORT]
        self.region: str = entry.data[CONF_REGION]
        self.k_net_route = cs.derive_net_route(bytes.fromhex(entry.data[CONF_K_NET]))
        self.proxies: dict[str, Proxy] = {}
        self.devices: dict[str, Device] = {}
        self._by_proxy_id: dict[bytes, Proxy] = {}
        # Atomic writes (fsync and rename): a counter lost to a power cut would be reused.
        self._root_store: Store[dict[str, int]] = Store(
            hass, STORAGE_VERSION, ROOT_STORAGE_KEY, atomic_writes=True
        )
        self._state_store: Store[dict[str, Any]] = Store(
            hass, STORAGE_VERSION, STATE_STORAGE_KEY, atomic_writes=True
        )
        self._root_counters: dict[str, int] = {}
        self._proxy_ids: dict[str, str] = {}
        self._stored_devices: dict[str, Any] = {}
        self._transport: asyncio.DatagramTransport | None = None
        self._tags: dict[tuple[bytes, bytes], list[Device]] = {}
        self._tag_epoch: int | None = None
        self._tag_time: int = 0
        self._collecting: dict[bytes, Transmission] = {}
        self._identified: dict[bytes, tuple[Device, str, int]] = {}
        self._bootstrap: deque[int] = deque()
        self._tasks: set[asyncio.Task[None]] = set()
        self._unsub_tick: CALLBACK_TYPE | None = None
        self._unsub_save: CALLBACK_TYPE | None = None
        self._unsub_stop: CALLBACK_TYPE | None = None
        self._stopped = False
        self._closed: asyncio.Future[None] | None = None
        self.foreign_frames = 0

    # ------------------------------------------------------------------ lifecycle

    async def async_start(self) -> None:
        """Load state and open the UDP endpoint. Raises OSError if the port is taken."""
        self._root_counters = await self._root_store.async_load() or {}
        stored = await self._state_store.async_load() or {}
        self._proxy_ids = stored.get("proxies", {})
        self._stored_devices = stored.get("devices", {})
        self.async_sync_subentries(initial=True)
        self._rebuild_tags()
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: self, local_addr=("0.0.0.0", self.port)
        )
        self._transport = transport
        self._unsub_tick = async_track_time_interval(self.hass, self._tick, TICK)
        # Config entries are not unloaded at shutdown: stop answering and flush here.
        self._unsub_stop = self.hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP, self._async_on_stop
        )
        _LOGGER.debug("Listening for proxies on UDP %s", self.port)

    async def _async_on_stop(self, _event: Event) -> None:
        self._unsub_stop = None
        await self.async_stop()

    async def async_stop(self) -> None:
        """Close the endpoint and flush state (07 section 10: on clean shutdown)."""
        self._stopped = True
        for unsub in (self._unsub_tick, self._unsub_save, self._unsub_stop):
            if unsub:
                unsub()
        self._unsub_tick = self._unsub_save = self._unsub_stop = None
        if self._transport:
            self._closed = asyncio.get_running_loop().create_future()
            self._transport.close()
            self._transport = None
            # The port is free only once the socket is closed, which a reload needs.
            await self._closed
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._async_save()

    @callback
    def async_sync_subentries(self, initial: bool = False) -> None:
        """Bring proxies and devices in line with the config entry's subentries."""
        subentries = self.entry.subentries
        for sid in [s for s in self.proxies if s not in subentries]:
            proxy = self.proxies.pop(sid)
            if proxy.proxy_id is not None:
                self._by_proxy_id.pop(proxy.proxy_id, None)
            self._proxy_ids.pop(sid, None)
        removed = [eui for eui, d in self.devices.items() if d.subentry_id not in subentries]
        for eui in removed:
            self.devices.pop(eui)
            self._stored_devices.pop(eui, None)
            ir.async_delete_issue(self.hass, DOMAIN, f"pairing_stalled_{eui}")
        if removed:
            self._identified.clear()
            self._rebuild_tags()

        for sid, sub in subentries.items():
            if sub.subentry_type == SUBENTRY_PROXY:
                if sid in self.proxies:
                    self.proxies[sid].name = sub.title
                    continue
                pid = self._proxy_ids.get(sid)
                proxy = Proxy(sub, bytes.fromhex(pid) if pid else None)
                self.proxies[sid] = proxy
                if proxy.proxy_id is not None:
                    self._by_proxy_id[proxy.proxy_id] = proxy
            elif sub.subentry_type == SUBENTRY_DEVICE:
                eui = sub.data[CONF_DEV_EUI]
                if eui in self.devices:
                    self.devices[eui].name = sub.title
                    continue
                device = Device(sub, self._root_counters.get(eui, 0))
                if (record := self._stored_devices.get(eui)) is not None:
                    device.load_store(record)
                self.devices[eui] = device
                if not initial:
                    async_dispatcher_send(self.hass, SIGNAL_DEVICE_ADDED, device)
        if not initial:
            self._schedule_save()

    # ------------------------------------------------------------------ persistence

    def _state_data(self) -> dict[str, Any]:
        self._stored_devices.update({eui: d.to_store() for eui, d in self.devices.items()})
        return {"proxies": self._proxy_ids, "devices": self._stored_devices}

    def _root_data(self) -> dict[str, int]:
        for eui, d in self.devices.items():
            self._root_counters[eui] = int(d.join.root_fcnt_down)
        return self._root_counters

    async def _async_save(self) -> bool:
        """Persist everything now and wait for the write.

        Returns False while Home Assistant is stopping: Store then defers the write
        to the final write, so nothing that depends on it may be sent.
        """
        await self._root_store.async_save(self._root_data())
        await self._state_store.async_save(self._state_data())
        # Checked after the writes: stopping may begin while they run.
        return self.hass.state is not CoreState.stopping

    @callback
    def _schedule_save(self) -> None:
        """Persist within SAVE_DELAY_S of the first unsaved change (07 section 10).

        Not Store.async_delay_save: it pushes the write back on every call, so steady
        traffic could postpone it indefinitely.
        """
        # After stop, a late timer could overwrite a reloaded server's newer state.
        if self._unsub_save is None and not self._stopped:
            self._unsub_save = async_call_later(self.hass, SAVE_DELAY_S, self._async_save_later)

    async def _async_save_later(self, _now: datetime) -> None:
        self._unsub_save = None
        await self._async_save()

    # ------------------------------------------------------------------ tags

    def _rebuild_tags(self) -> None:
        """The tag table for epochs e-1, e, e+1 (07 section 3)."""
        now = int(time.time())
        e = cs.epoch_of(now)
        table: dict[tuple[bytes, bytes], list[Device]] = {}
        for device in self.devices.values():
            if device.session_keys is None:
                continue
            for epoch in (e - 1, e, e + 1):
                key = (
                    cs.net_tag(self.k_net_route, epoch),
                    cs.dev_tag(device.session_keys.k_dev_tag, epoch),
                )
                table.setdefault(key, []).append(device)
        self._tags = table
        self._tag_epoch = e
        self._tag_time = now

    def net_tags(self) -> list[bytes]:
        """The net_tags this server owns, for SERVER_STATUS (05 section 5.3)."""
        e = cs.epoch_of(int(time.time()))
        return [cs.net_tag(self.k_net_route, x) for x in (e - 1, e, e + 1)]

    @callback
    def _tick(self, _now: datetime) -> None:
        now = int(time.time())
        e = cs.epoch_of(now)
        jumped = abs(now - self._tag_time - TICK.total_seconds()) > 60
        if e != self._tag_epoch or jumped:
            self._rebuild_tags()
            # Tell connected proxies the new tags within 10 s of the boundary.
            for proxy in self.proxies.values():
                if proxy.connected:
                    self._send(
                        proxy, cs.M_SERVER_STATUS, cs.server_status_body(now, self.net_tags())
                    )
        else:
            self._tag_time = now
        for proxy in self.proxies.values():
            async_dispatcher_send(self.hass, signal_proxy_update(proxy.subentry_id))

    # ------------------------------------------------------------------ UDP

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Keep the transport (set again in async_start)."""
        assert isinstance(transport, asyncio.DatagramTransport)
        self._transport = transport

    def connection_lost(self, exc: Exception | None) -> None:
        """The socket is closed."""
        if self._closed is not None and not self._closed.done():
            self._closed.set_result(None)

    def error_received(self, exc: Exception) -> None:
        """Log socket errors; the endpoint stays open."""
        _LOGGER.debug("UDP error: %s", exc)

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        """Handle one envelope (05 section 3)."""
        try:
            self._handle(data, addr)
        except Exception:
            _LOGGER.exception("Error handling a message from %s", addr[0])

    def _open(self, data: bytes) -> tuple[Proxy, dict[str, Any]] | None:
        """Find the proxy whose key authenticates this message."""
        if len(data) < cs.ENV_HEADER + cs.ENV_MAC or data[0] != cs.ENV_VERSION:
            return None
        pid = data[2:10]
        known = self._by_proxy_id.get(pid)
        candidates = [known] if known else [p for p in self.proxies.values() if p.proxy_id is None]
        for proxy in candidates:
            try:
                m = cs.open_envelope(proxy.key, data)
            except ValueError:
                continue
            if proxy.proxy_id is None and m["type"] == cs.M_HELLO:
                self._bind(proxy, pid)
            return proxy, m
        return None

    def _bind(self, proxy: Proxy, pid: bytes) -> None:
        """Store the key against the proxy_id of its first HELLO (05 section 4.2)."""
        proxy.proxy_id = pid
        self._by_proxy_id[pid] = proxy
        self._proxy_ids[proxy.subentry_id] = pid.hex()
        self._schedule_save()
        _LOGGER.info("Proxy %s is %s", proxy.name, pid.hex())
        async_dispatcher_send(self.hass, SIGNAL_PROXY_BOUND, proxy)

    def _handle(self, data: bytes, addr: tuple[str, int]) -> None:
        opened = self._open(data)
        if opened is None:
            _LOGGER.debug("Dropped %s bytes from %s: no proxy key matches", len(data), addr[0])
            return
        proxy, m = opened
        now_ms = _now_ms()
        got = proxy.link.receive(m, now_ms)
        result = got["result"]
        if result == "hello_request":
            # A boot we have no session with: ask it to prove itself, without
            # touching the session or the proxy's address (05 section 3.2).
            self._sendto(
                cs.envelope(
                    proxy.key,
                    cs.M_HELLO_REQUEST,
                    data[2:10],
                    m["boot_id"],
                    0,
                    cs.hello_request_body(got["nonce"]),
                ),
                addr,
            )
            _LOGGER.debug(
                "%s: boot %08x is not the session's; sent HELLO_REQUEST", proxy.name, m["boot_id"]
            )
            return
        if result == "ignored":
            return
        if result == "switched":
            # Start above anything an earlier instance sent this boot (05 section 7).
            proxy.link.seq = int(time.time()) & 0xFFFFFFFF
            _LOGGER.debug("%s: session with boot %08x", proxy.name, m["boot_id"])
        proxy.addr = addr
        body = bytes.fromhex(m["body"])
        try:
            if m["type"] == cs.M_HELLO:
                self._hello(proxy, m, body, now_ms)
            elif m["type"] == cs.M_UPLINK:
                self._uplink(proxy, m["boot_id"], body, now_ms)
            elif m["type"] == cs.M_TX_RESULT:
                self._tx_result(proxy, body)
        except (ValueError, IndexError, struct.error) as err:
            _LOGGER.debug("%s: malformed message type 0x%02x: %s", proxy.name, m["type"], err)

    def _sendto(self, data: bytes, addr: tuple[str, int]) -> None:
        if self._transport is not None:
            self._transport.sendto(data, addr)

    def _send(self, proxy: Proxy, msg_type: int, body: bytes) -> int:
        """Send a message in the proxy's session; returns its seq."""
        boot_id = _boot_id(proxy.link)
        assert proxy.proxy_id is not None and proxy.addr is not None and boot_id is not None
        seq = proxy.link.next_seq() & 0xFFFFFFFF
        self._sendto(
            cs.envelope(proxy.key, msg_type, proxy.proxy_id, boot_id, seq, body), proxy.addr
        )
        return seq

    # ------------------------------------------------------------------ proxy messages

    def _hello(self, proxy: Proxy, m: dict[str, Any], body: bytes, now_ms: int) -> None:
        hello = cs.parse_hello(body)
        previous, proxy.hello = proxy.hello, hello
        proxy.last_hello_ms = now_ms
        proxy.last_hello = dt_util.utcnow()
        freq, sf, bw = REGIONS[self.region]
        mismatch = (
            hello["region"] != self.region
            or hello["sf"] != sf
            or hello["bw_khz"] != bw
            or (freq is not None and hello["frequency_hz"] != freq)
        )
        if mismatch and not proxy.radio_mismatch:
            _LOGGER.warning(
                "Proxy %s uses %s %s Hz SF%s %s kHz, not the network's %s profile; "
                "no downlinks go through it",
                proxy.name,
                hello["region"],
                hello["frequency_hz"],
                hello["sf"],
                hello["bw_khz"],
                self.region,
            )
        proxy.radio_mismatch = mismatch
        if proxy.proxy_id is not None and (previous is None or previous["fw"] != hello["fw"]):
            self._set_sw_version(proxy.proxy_id.hex(), hello["fw"])
        now = int(time.time())
        self._send(proxy, cs.M_SERVER_STATUS, cs.server_status_body(now, self.net_tags(), m["seq"]))
        async_dispatcher_send(self.hass, signal_proxy_update(proxy.subentry_id))

    def _tx_result(self, proxy: Proxy, body: bytes) -> None:
        t = cs.parse_tx_result(body)
        result = TX_RESULTS.get(t["result"], str(t["result"]))
        proxy.tx_results.append((result, t["offset_us"]))
        eui = proxy.pending_downlinks.pop(t["downlink_seq"], None)
        _LOGGER.debug(
            "%s: downlink %s to %s: %s, %.1f ms after target",
            proxy.name,
            t["downlink_seq"],
            eui,
            result,
            t["offset_us"] / 1000,
        )
        async_dispatcher_send(self.hass, signal_proxy_update(proxy.subentry_id))

    # ------------------------------------------------------------------ uplinks

    def _uplink(self, proxy: Proxy, boot_id: int, body: bytes, now_ms: int) -> None:
        """07 section 4, steps 2-5; steps 6-8 run in a task."""
        u = cs.parse_uplink(body)
        frame: bytes = u["frame"]
        h = cs.parse_header(frame)
        if h is None or cs.is_downlink(h.ftype):
            return
        proxy.uplinks += 1
        reception = Reception(proxy, boot_id, u["rx_time_us"], u["rssi"], u["snr"], u["flags"])
        identified = self._identify(frame, h, now_ms)
        if identified is None:
            return
        device, keys, plaintext = identified
        got = device.uplinks.receive(frame, now_ms)
        if got["result"] == "copy":
            if (tx := self._collecting.get(frame)) is not None:
                tx.receptions.append(reception)
            return
        if got["counter"] == "too_old":
            _LOGGER.debug("%s: frame %s too old", device.name, h.fcnt)
            return
        tx = Transmission(
            frame, h, device, keys, plaintext, got["counter"], got["answer"], now_ms, [reception]
        )
        self._collecting[frame] = tx
        task = self.hass.async_create_background_task(
            self._process(tx), f"{DOMAIN} uplink {device.eui}"
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _identify(
        self, frame: bytes, h: cs.ParsedHeader, now_ms: int
    ) -> tuple[Device, cs.DeviceKeys, bytes] | None:
        """07 section 4, step 4. Copies reuse the first copy's identification."""
        self._identified = {
            f: v for f, v in self._identified.items() if now_ms - v[2] < cs.FRAME_MEMORY_MS
        }
        if (known := self._identified.get(frame)) is not None:
            device, keyset, _ = known
            keys = self._keys_for(device, h.ftype, keyset)
            if keys is not None:
                try:
                    _, plaintext = cs.open_frame(keys, frame)
                except Exception:  # InvalidTag or ValueError: not this device
                    pass
                else:
                    return device, keys, plaintext

        bootstrap = h.net_tag == cs.ZERO_TAG and h.dev_tag == cs.ZERO_TAG
        candidates: list[tuple[Device, str]]
        if not bootstrap:
            candidates = [(d, "session") for d in self._tags.get((h.net_tag, h.dev_tag), [])]
        else:
            # Spec 02 section 11.3: at most 50 bootstrap frames a minute.
            while self._bootstrap and now_ms - self._bootstrap[0] > 60_000:
                self._bootstrap.popleft()
            if len(self._bootstrap) >= BOOTSTRAP_PER_MINUTE:
                return None
            self._bootstrap.append(now_ms)
            keyset = "root" if h.ftype == cs.T_JOIN else "session"
            candidates = [(d, keyset) for d in self.devices.values()]
        for device, keyset in candidates:
            keys = self._keys_for(device, h.ftype, keyset)
            if keys is None:
                continue
            try:
                _, plaintext = cs.open_frame(keys, frame)
            except Exception:  # trial decryption: try the next device
                continue
            self._identified[frame] = (device, keyset, now_ms)
            return device, keys, plaintext
        self.foreign_frames += 1
        return None

    @staticmethod
    def _keys_for(device: Device, ftype: int, keyset: str) -> cs.DeviceKeys | None:
        """The key set a frame type may use for a device in its current state."""
        if ftype == cs.T_JOIN:
            # JOIN: root keys of devices waiting to be paired (02 section 11.3).
            if keyset == "root" and device.state in (STATE_SCANNED, STATE_EXCHANGED):
                return device.root_keys
            return None
        if keyset == "session" and device.state in (STATE_EXCHANGED, STATE_PAIRED):
            return device.session_keys
        return None

    async def _process(self, tx: Transmission) -> None:
        """Act on a transmission, then answer it after the collection window."""
        try:
            async with tx.device.lock:
                downlink = await self._act(tx)
            if downlink is None:
                return
            wait = tx.started_ms + COLLECTION_WINDOW_MS - _now_ms()
            if wait > 0:
                await asyncio.sleep(wait / 1000)
            self._answer(tx, downlink)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("Error processing an uplink from %s", tx.device.name)
        finally:
            self._collecting.pop(tx.frame, None)

    async def _act(self, tx: Transmission) -> bytes | None:
        """07 section 4, steps 6 and 7. Returns the downlink to send, if any."""
        device, h = tx.device, tx.header
        fresh = tx.counter in ("accept", "accept_late")
        if fresh:
            best = max(tx.receptions, key=lambda r: r.snr)
            device.last_seen = dt_util.utcnow()
            device.rssi, device.snr = best.rssi, best.snr

        if h.ftype == cs.T_JOIN:
            return await self._join(tx) if tx.answer else None

        uflags = tx.plaintext[0]
        commands = b""
        if device.state == STATE_EXCHANGED and h.ftype == cs.T_SYNC:
            # 04 section 4.4, step 6.
            commands = cs.command(cs.CMD_SET_NETWORK, self.k_net_route)
        elif (
            device.state == STATE_EXCHANGED
            and fresh
            and h.net_tag != cs.ZERO_TAG
            and uflags & cs.UF_DL_ACK
        ):
            # Step 7: an uplink with normal tags acknowledges SET_NETWORK.
            device.state = STATE_PAIRED
            device.join.paired()
            self._clear_stall(device)
            _LOGGER.info("%s paired", device.name)
            await self._async_save()

        if fresh:
            self._apply_readings(device, tx.plaintext[1:])
            self._schedule_save()
            async_dispatcher_send(self.hass, signal_device_update(device.eui))

        if not (tx.answer or (fresh and uflags & cs.UF_TIME_UNSURE)):
            return None
        # The device record must be on disk before SET_NETWORK goes out (04 section 4.6).
        if commands and not await self._async_save():
            return None
        assert device.session_keys is not None
        fcnt = await self._next_fcnt_down(device)
        if fcnt is None:
            return None
        return cs.seal(
            device.session_keys,
            cs.T_DOWNLINK,
            h.net_tag,
            h.dev_tag,
            fcnt,
            cs.downlink_payload(h.fcnt, int(time.time()), 0, commands),
        )

    async def _join(self, tx: Transmission) -> bytes | None:
        """Answer a JOIN (07 section 5.1)."""
        device = tx.device
        r = device.join.join(tx.frame, secrets.token_bytes(32), int(time.time()))
        if r["action"] == "new_exchange":
            device.set_session(r["k_session"])
            device.state = STATE_EXCHANGED
            self._clear_stall(device)
            self._rebuild_tags()
            # Root counter and pending exchange are on disk before KEY_EXCHANGE is sent.
            if not await self._async_save():
                return None
            _LOGGER.info("%s: key exchange started", device.name)
        else:
            self._schedule_save()
            if r["stalled"] and not device.stalled:
                device.stalled = True
                _LOGGER.warning(
                    "%s keeps repeating JOIN although it was answered: pairing stalled",
                    device.name,
                )
                ir.async_create_issue(
                    self.hass,
                    DOMAIN,
                    f"pairing_stalled_{device.eui}",
                    is_fixable=False,
                    severity=ir.IssueSeverity.WARNING,
                    translation_key="pairing_stalled",
                    translation_placeholders={"name": device.name},
                )
        async_dispatcher_send(self.hass, signal_device_update(device.eui))
        frame: bytes = r["frame"]
        return frame

    def _set_sw_version(self, identifier: str, version: str) -> None:
        """Keep the device registry's software version current."""
        registry = dr.async_get(self.hass)
        entry = registry.async_get_device_by_identifier((DOMAIN, identifier), self.entry.entry_id)
        if entry is not None:
            registry.async_update_device(entry.id, sw_version=version)

    def _clear_stall(self, device: Device) -> None:
        if device.stalled:
            device.stalled = False
            ir.async_delete_issue(self.hass, DOMAIN, f"pairing_stalled_{device.eui}")

    async def _next_fcnt_down(self, device: Device) -> int | None:
        """Next session downlink counter, reserved in blocks (02 section 8.2 and 8.4).

        None if the reservation could not be written first.
        """
        value = device.fcnt_down + 1
        if value >= device.fcnt_down_reserved:
            device.fcnt_down_reserved = value + RESERVE_BLOCK
            if not await self._async_save():
                return None
        device.fcnt_down = value
        return value

    def _apply_readings(self, device: Device, data: bytes) -> None:
        """Update reading values (03 section 6). An invalid payload applies nothing."""
        if not data:
            return
        try:
            readings = cs.decode_readings(data)
        except ValueError as err:
            _LOGGER.warning("%s sent invalid readings: %s", device.name, err)
            return
        for r in readings:
            name, value = r["name"], r["value"]
            if name == "firmware_version":
                raw = bytes.fromhex(value)
                version = ".".join(str(b) for b in raw)
                if len(raw) == 3 and version != device.sw_version:
                    device.sw_version = version
                    self._set_sw_version(device.eui, version)
                continue
            if name in ("unknown", "raw", "text", "button"):
                continue
            key = reading_key(r["channel"], name)
            new = key not in device.readings
            device.readings[key] = value
            if new:
                async_dispatcher_send(self.hass, SIGNAL_NEW_READING, device, key)

    def _answer(self, tx: Transmission, downlink: bytes) -> None:
        """Send the downlink through the best proxy that heard it (07 section 6)."""
        candidates = [
            r
            for r in tx.receptions
            # A proxy removed while the uplink was collected is no longer asked.
            if self.proxies.get(r.proxy.subentry_id) is r.proxy
            and r.proxy.can_downlink
            and r.boot_id == _boot_id(r.proxy.link)
        ]
        in_time = _now_ms() - tx.started_ms <= RX1_DEADLINE_MS
        rx1 = [r for r in candidates if not r.flags & cs.UPF_TS_UNCERTAIN]
        if rx1 and in_time:
            pool, offset = rx1, RX1_OFFSET_US
        elif candidates:
            pool, offset = candidates, RX2_OFFSET_US
        else:
            _LOGGER.debug("%s: no proxy can answer frame %s", tx.device.name, tx.header.fcnt)
            return
        best = max(pool, key=lambda r: (r.snr, r.rssi))
        proxy = best.proxy
        seq = self._send(
            proxy, cs.M_DOWNLINK, cs.downlink_msg_body(best.rx_time_us, offset, downlink)
        )
        proxy.downlinks_sent += 1
        proxy.pending_downlinks[seq] = tx.device.eui
        while len(proxy.pending_downlinks) > TX_HISTORY:
            del proxy.pending_downlinks[next(iter(proxy.pending_downlinks))]
        _LOGGER.debug(
            "%s: answered frame %s from %s in %s",
            proxy.name,
            tx.header.fcnt,
            tx.device.name,
            "RX1" if offset == RX1_OFFSET_US else "RX2",
        )

    # ------------------------------------------------------------------ diagnostics

    def diagnostics(self) -> dict[str, Any]:
        """Runtime state for the diagnostics download; redacted by the caller."""
        return {
            "port": self.port,
            "region": self.region,
            "foreign_frames": self.foreign_frames,
            "proxies": {
                p.subentry_id: {
                    "proxy_id": p.proxy_id_hex,
                    "connected": p.connected,
                    "radio_mismatch": p.radio_mismatch,
                    "boot_id": _boot_id(p.link),
                    "hello": p.hello,
                    "uplinks": p.uplinks,
                    "downlinks_sent": p.downlinks_sent,
                    "tx_results": list(p.tx_results),
                }
                for p in self.proxies.values()
            },
            "devices": {eui: d.to_store() for eui, d in self.devices.items()},
            "root_counters": dict(self._root_data()),
        }
