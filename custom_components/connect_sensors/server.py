"""The ConnectSensors network server (spec 07), on Home Assistant's event loop.

Protocol work (frames, envelopes, proxy sessions, dedupe, JOIN answers) is done by the
connectsensors package. This module keeps the state around it: which proxies and
devices exist, what is persisted when, and which proxy answers which uplink.

Pairing codes, keys and decrypted payloads are never logged (07 section 11.6).
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
import secrets
import struct
import time
from typing import Any, cast

import connectsensors as cs

# airtime_ms is not in the package's public API yet (chrisuthe/ConnectSensors#56).
from connectsensors._csframe import airtime_ms
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
    BUTTON_EVENTS,
    COLLECTION_WINDOW_MS,
    COMMAND_EXPIRY_H,
    COMMAND_MAX_SENDS,
    COMMAND_QUEUE_MAX,
    COMMANDS_MAX_BYTES,
    CONF_DEV_EUI,
    CONF_HEARTBEAT,
    CONF_K_NET,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    DUTY_CYCLE_BACKOFF_S,
    EVENT_DEVICE_REACHABLE,
    EVENT_DEVICE_UNREACHABLE,
    EVENT_READING,
    LEAVE_EXPIRY_H,
    PROXY_AIRTIME_MS_PER_HOUR,
    PROXY_TIMEOUT_S,
    REGIONS,
    RESERVE_BLOCK,
    RX1_DEADLINE_MS,
    RX1_MAX_LATE_OR_BUSY,
    RX1_MAX_OFFSET_US,
    RX1_OFFSET_US,
    RX2_OFFSET_US,
    SAVE_DELAY_S,
    SIGNAL_DEVICE_ADDED,
    SIGNAL_NEW_READING,
    SIGNAL_PROXY_BOUND,
    SINGLE_PATH_REPAIR_H,
    SINGLE_PATH_WINDOW_H,
    SNR_TIE_DB,
    STATE_EXCHANGED,
    STATE_PAIRED,
    STATE_SCANNED,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
    TX_RESULT_MARGIN_MS,
    TX_RESULTS,
    UNREACHABLE_GRACE_MIN,
    signal_device_event,
    signal_device_update,
    signal_proxy_update,
)
from .profiles import Profile, default_heartbeat, get_profile
from .readings import reading_key

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
# Root downlink counters outlive devices and the config entry (02 section 8.4).
ROOT_STORAGE_KEY = f"{DOMAIN}.root_counters"
STATE_STORAGE_KEY = f"{DOMAIN}.state"

CONFIRMED_TYPES = frozenset({cs.T_DATA_CONFIRMED, cs.T_HEARTBEAT_CONFIRMED, cs.T_JOIN, cs.T_SYNC})
TICK = timedelta(seconds=5)
TX_HISTORY = 20
HOUR_MS = 3_600_000
SINGLE_PATH_WINDOW = timedelta(hours=SINGLE_PATH_WINDOW_H)
SINGLE_PATH_REPAIR = timedelta(hours=SINGLE_PATH_REPAIR_H)
DEVICE_ISSUES = ("pairing_stalled", "device_unreachable", "single_path")
COMMAND_EXPIRY = timedelta(hours=COMMAND_EXPIRY_H)
LEAVE_EXPIRY = timedelta(hours=LEAVE_EXPIRY_H)


def _now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def _utcnow() -> datetime:
    """Wall-clock time, for last-seen times and supervision."""
    return dt_util.utcnow()


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


def _radio(region: str, freq: int | None, sf: int, bw: int) -> str:
    mhz = f" {freq / 1e6:g} MHz" if freq is not None else ""
    return f"{region}{mhz} SF{sf} {bw} kHz"


def _parse_time(value: str | None) -> datetime | None:
    return dt_util.parse_datetime(value) if value else None


@dataclass(slots=True)
class PendingDownlink:
    """A DOWNLINK waiting for its TX_RESULT; result is set if someone waits for it."""

    eui: str
    result: asyncio.Future[str] | None


@dataclass(slots=True)
class Command:
    """A queued command (07 section 7). `slot` names what it sets: a later command for
    the same slot replaces it."""

    id: int
    name: str
    slot: str
    cmd: int
    args: bytes
    queued_at: datetime
    sends: int = 0

    @property
    def encoded(self) -> bytes:
        """Its TLV entry (02 section 10.3)."""
        return cs.command(self.cmd, self.args)

    def to_store(self) -> dict[str, Any]:
        """The persisted entry (07 section 10)."""
        return {
            "id": self.id,
            "name": self.name,
            "slot": self.slot,
            "cmd": self.cmd,
            "args": self.args.hex(),
            "queued_at": self.queued_at.isoformat(),
            "sends": self.sends,
        }

    @classmethod
    def from_store(cls, d: dict[str, Any]) -> Command:
        """Restore a persisted entry."""
        queued_at = _parse_time(d["queued_at"])
        assert queued_at is not None
        return cls(
            d["id"], d["name"], d["slot"], d["cmd"], bytes.fromhex(d["args"]), queued_at, d["sends"]
        )


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
        self.tx_timeouts = 0
        self.pending_downlinks: dict[int, PendingDownlink] = {}
        # Airtime of downlinks sent, by DOWNLINK seq: (sent at ms, airtime ms).
        self.airtime: dict[int, tuple[int, float]] = {}
        self.duty_cycle_until_ms: int | None = None

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

    @property
    def timing_ok(self) -> bool:
        """Trusted for RX1 by its last 20 results (07 section 6.2)."""
        late = sum(1 for result, _ in self.tx_results if result in ("too_late", "busy"))
        slow = any(
            result == "sent" and offset > RX1_MAX_OFFSET_US for result, offset in self.tx_results
        )
        return late <= RX1_MAX_LATE_OR_BUSY and not slow

    @property
    def failures(self) -> int:
        """Results other than SENT among the last 20, a missing one included."""
        return sum(1 for result, _ in self.tx_results if result != "sent")

    def airtime_used_ms(self, now_ms: int) -> float:
        """Downlink airtime in the last hour, as far as the server knows (01 section 5)."""
        for seq in [s for s, (at, _) in self.airtime.items() if now_ms - at >= HOUR_MS]:
            del self.airtime[seq]
        return sum(ms for _, ms in self.airtime.values())

    def can_afford(self, airtime: float, now_ms: int) -> bool:
        """Within its airtime budget, and not recently out of it."""
        if self.duty_cycle_until_ms is not None and now_ms < self.duty_cycle_until_ms:
            return False
        return self.airtime_used_ms(now_ms) + airtime <= PROXY_AIRTIME_MS_PER_HOUR


class Device:
    """A sensor added by pairing code, in any pairing state."""

    def __init__(
        self, subentry_id: str, name: str, data: Mapping[str, Any], root_fcnt_down: int
    ) -> None:
        self.subentry_id = subentry_id
        self.name = name
        # Kept for a device that is leaving after its subentry was removed.
        self.data = dict(data)
        self.dev_eui = bytes.fromhex(data[CONF_DEV_EUI])
        self.eui = data[CONF_DEV_EUI]
        self.profile: str = data[CONF_PROFILE]
        self.profile_def: Profile | None = get_profile(self.profile)
        self.heartbeat_minutes: int = data.get(CONF_HEARTBEAT) or default_heartbeat(self.profile)
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
        # Monotonic time of the last valid uplink in this run: silence is timed with
        # it, so a wall-clock jump (NTP after boot) does not make devices unreachable.
        self.last_heard_ms: int | None = None
        self.rssi: float | None = None
        self.snr: float | None = None
        self.stalled = False
        # Supervision (07 section 8). stale: readings not heard since the device was
        # last unreachable, shown as unavailable until an uplink carries them again.
        self.unreachable = False
        self.stale: set[str] = set()
        # Proxy subentry_id -> when that proxy last heard the device (07 section 9).
        self.heard_by: dict[str, datetime] = {}
        self.single_path_since: datetime | None = None
        # Event readings seen, by reading key: each has an event entity.
        self.events: set[str] = set()
        # The first event of a new key, for its entity to show once it is added.
        self.first_events: dict[str, str] = {}
        # Command queue (07 section 7), and the commands of the last downlink that
        # carried any: (its reply_to, their ids), until the next uplink settles them.
        self.commands: list[Command] = []
        self.in_flight: tuple[int, list[int]] | None = None
        self.next_command_id = 1
        self.last_command: dict[str, str] | None = None
        # Set while LEAVE is outstanding after the subentry was removed (04 section 6.1).
        self.leaving_since: datetime | None = None
        self.lock = asyncio.Lock()

    @classmethod
    def from_subentry(cls, subentry: ConfigSubentry, root_fcnt_down: int) -> Device:
        """The device a subentry describes."""
        return cls(subentry.subentry_id, subentry.title, subentry.data, root_fcnt_down)

    def entity_keys(self) -> list[str]:
        """Reading keys with an entity: the profile's, then those heard (03 section 9.3)."""
        keys = [c.key for c in self.profile_def.channels] if self.profile_def else []
        for key in (*self.readings, *sorted(self.events)):
            if key not in keys:
                keys.append(key)
        return keys

    @property
    def last_fcnt(self) -> int:
        """Highest accepted uplink counter."""
        return int(self.uplinks.counter.last)

    @property
    def supervised(self) -> bool:
        """Only paired devices send heartbeats."""
        return self.state == STATE_PAIRED

    @property
    def unreachable_after(self) -> timedelta:
        """Silence after which the device is unreachable (07 section 8.1)."""
        return timedelta(minutes=3 * self.heartbeat_minutes + UNREACHABLE_GRACE_MIN)

    def recent_proxies(self, now: datetime) -> list[str]:
        """Subentry ids of the proxies that heard the device in the last 24 hours."""
        return [sid for sid, t in self.heard_by.items() if now - t < SINGLE_PATH_WINDOW]

    @property
    def proxy_count(self) -> int:
        """Proxies that heard the device in the last 24 hours (07 section 9)."""
        return len(self.recent_proxies(_utcnow()))

    @property
    def single_path(self) -> bool:
        """Heard by exactly one proxy over the last 24 hours (07 section 8.2)."""
        return self.supervised and self.proxy_count == 1

    def reading_available(self, key: str) -> bool:
        """A reading is shown unless the device is unreachable or has not resent it."""
        return not self.unreachable and key not in self.stale

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
            "unreachable": self.unreachable,
            "stale": sorted(self.stale),
            "heard_by": {sid: t.isoformat() for sid, t in self.heard_by.items()},
            "single_path_since": self.single_path_since.isoformat()
            if self.single_path_since
            else None,
            "events": sorted(self.events),
            "commands": [c.to_store() for c in self.commands],
            "in_flight": list(self.in_flight) if self.in_flight else None,
            "next_command_id": self.next_command_id,
            "last_command": self.last_command,
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
        self.last_seen = _parse_time(d["last_seen"])
        self.rssi = d["rssi"]
        self.snr = d["snr"]
        # Added with supervision; older records lack them.
        self.unreachable = d.get("unreachable", False)
        self.stale = set(d.get("stale", []))
        self.heard_by = {
            sid: t for sid, v in d.get("heard_by", {}).items() if (t := _parse_time(v)) is not None
        }
        self.single_path_since = _parse_time(d.get("single_path_since"))
        # Added with commands and events.
        self.events = set(d.get("events", []))
        self.commands = [Command.from_store(c) for c in d.get("commands", [])]
        if (flight := d.get("in_flight")) is not None:
            self.in_flight = (flight[0], list(flight[1]))
        self.next_command_id = d.get("next_command_id", 1)
        self.last_command = d.get("last_command")


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
        # Devices whose subentry was removed, until they acknowledge LEAVE (04 6.1).
        self.leaving: dict[str, Device] = {}
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
        # Repair issues raised by this server, with their placeholders.
        self._issues: dict[str, dict[str, str]] = {}
        # Silence is counted from here at the earliest: nothing was heard while stopped.
        self._started = _utcnow()
        self._started_ms = _now_ms()
        self.foreign_frames = 0

    # ------------------------------------------------------------------ lifecycle

    async def async_start(self) -> None:
        """Load state and open the UDP endpoint. Raises OSError if the port is taken."""
        self._root_counters = await self._root_store.async_load() or {}
        stored = await self._state_store.async_load() or {}
        self._proxy_ids = stored.get("proxies", {})
        self._stored_devices = stored.get("devices", {})
        subentry_euis = {
            s.data[CONF_DEV_EUI]
            for s in self.entry.subentries.values()
            if s.subentry_type == SUBENTRY_DEVICE
        }
        for eui, rec in stored.get("leaving", {}).items():
            # Added again while Home Assistant was stopped: the new device wins.
            if eui in subentry_euis:
                continue
            device = Device(
                rec["subentry_id"], rec["name"], rec["data"], self._root_counters.get(eui, 0)
            )
            device.load_store(rec["record"])
            device.leaving_since = _parse_time(rec["since"])
            self.leaving[eui] = device
        self.async_sync_subentries(initial=True)
        self._rebuild_tags()
        # Repair issues are not kept across restarts: raise the current ones again.
        self._supervise(self._started)
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
            self._set_issue("proxy_mismatch", sid, None)
            for device in self.devices.values():
                if device.heard_by.pop(sid, None) is not None:
                    async_dispatcher_send(self.hass, signal_device_update(device.eui))
        removed = [eui for eui, d in self.devices.items() if d.subentry_id not in subentries]
        for eui in removed:
            removed_device = self.devices.pop(eui)
            self._stored_devices.pop(eui, None)
            for issue in DEVICE_ISSUES:
                self._set_issue(issue, eui, None)
            self._start_leave(removed_device)
        if removed:
            self._identified.clear()
            self._rebuild_tags()

        for sid, sub in subentries.items():
            if sub.subentry_type == SUBENTRY_PROXY:
                if sid in self.proxies:
                    known_proxy = self.proxies[sid]
                    if known_proxy.name != sub.title:
                        known_proxy.name = sub.title
                        if known_proxy.proxy_id is not None:
                            self._rename(known_proxy.proxy_id.hex(), sub.title)
                    continue
                pid = self._proxy_ids.get(sid)
                proxy = Proxy(sub, bytes.fromhex(pid) if pid else None)
                self.proxies[sid] = proxy
                if proxy.proxy_id is not None:
                    self._by_proxy_id[proxy.proxy_id] = proxy
            elif sub.subentry_type == SUBENTRY_DEVICE:
                eui = sub.data[CONF_DEV_EUI]
                if (known := self.devices.get(eui)) is not None:
                    if known.name != sub.title:
                        known.name = sub.title
                        self._rename(eui, sub.title)
                    known.heartbeat_minutes = sub.data.get(CONF_HEARTBEAT) or default_heartbeat(
                        known.profile
                    )
                    continue
                if (left := self.leaving.pop(eui, None)) is not None:
                    _LOGGER.info(
                        "%s added again before it acknowledged LEAVE; LEAVE dropped", left.name
                    )
                    self._identified.clear()
                    self._rebuild_tags()
                device = Device.from_subentry(sub, self._root_counters.get(eui, 0))
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
        leaving = {
            eui: {
                "subentry_id": d.subentry_id,
                "name": d.name,
                "data": d.data,
                "since": d.leaving_since.isoformat() if d.leaving_since else None,
                "record": d.to_store(),
            }
            for eui, d in self.leaving.items()
        }
        return {"proxies": self._proxy_ids, "devices": self._stored_devices, "leaving": leaving}

    def _root_data(self) -> dict[str, int]:
        for eui, d in self._all_devices():
            self._root_counters[eui] = int(d.join.root_fcnt_down)
        return self._root_counters

    def _all_devices(self) -> Iterator[tuple[str, Device]]:
        """Devices with a subentry, and those still leaving."""
        yield from self.devices.items()
        yield from self.leaving.items()

    @callback
    def _save_soon(self) -> None:
        """Persist now, without waiting for it (07 section 10: the command queue)."""
        if self._stopped:
            return
        task = self.hass.async_create_background_task(self._async_save_now(), f"{DOMAIN} save")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

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

    async def _async_save_now(self) -> None:
        await self._async_save()

    # ------------------------------------------------------------------ tags

    def _rebuild_tags(self) -> None:
        """The tag table for epochs e-1, e, e+1 (07 section 3)."""
        now = int(time.time())
        e = cs.epoch_of(now)
        table: dict[tuple[bytes, bytes], list[Device]] = {}
        for _, device in self._all_devices():
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
        self._supervise(_utcnow())

    # ------------------------------------------------------------------ supervision

    @callback
    def _supervise(self, now: datetime) -> None:
        """Unreachable and single-path devices (07 section 8), and their repair issues."""
        for device in list(self.leaving.values()):
            self._expire_commands(device, now)
        for device in self.devices.values():
            changed = self._expire_commands(device, now)
            for sid in [s for s, t in device.heard_by.items() if now - t >= SINGLE_PATH_WINDOW]:
                del device.heard_by[sid]
                changed = True
            if device.supervised and not device.unreachable:
                heard_ms = device.last_heard_ms or self._started_ms
                if _now_ms() - heard_ms > device.unreachable_after.total_seconds() * 1000:
                    device.unreachable = True
                    device.stale = set(device.readings)
                    changed = True
                    _LOGGER.info(
                        "%s is unreachable: nothing heard for %s minutes",
                        device.name,
                        3 * device.heartbeat_minutes + UNREACHABLE_GRACE_MIN,
                    )
                    self._fire(EVENT_DEVICE_UNREACHABLE, device)
            single = device.single_path
            if single and device.single_path_since is None:
                device.single_path_since = now
                changed = True
            elif not single and device.single_path_since is not None:
                device.single_path_since = None
                changed = True
            self._device_issues(device, now)
            if changed:
                self._schedule_save()
                async_dispatcher_send(self.hass, signal_device_update(device.eui))

    @callback
    def _device_issues(self, device: Device, now: datetime) -> None:
        """Raise or clear the device's repair issues to match its state."""
        eui, name = device.eui, device.name
        self._set_issue(
            "pairing_stalled",
            eui,
            {"name": name} if device.stalled and device.state != STATE_PAIRED else None,
        )
        unreachable = None
        if device.unreachable:
            seen = device.last_seen
            unreachable = {
                "name": name,
                "last_seen": dt_util.as_local(seen).strftime("%Y-%m-%d %H:%M") if seen else "-",
                "heartbeat": str(device.heartbeat_minutes),
            }
        self._set_issue("device_unreachable", eui, unreachable)
        single_path = None
        since = device.single_path_since
        if since is not None and now - since >= SINGLE_PATH_REPAIR:
            sids = device.recent_proxies(now)
            proxy = self.proxies.get(sids[0]) if sids else None
            single_path = {"name": name, "proxy": proxy.name if proxy else "-"}
        self._set_issue("single_path", eui, single_path)

    @callback
    def _set_issue(self, key: str, subject: str, placeholders: dict[str, str] | None) -> None:
        """Raise issue `key` for a device or proxy with these placeholders, or clear it."""
        issue_id = f"{key}_{subject}"
        if placeholders is None:
            self._issues.pop(issue_id, None)
            # Also clears one left by an earlier instance of the server (a reload).
            ir.async_delete_issue(self.hass, DOMAIN, issue_id)
            return
        if self._issues.get(issue_id) == placeholders:
            return
        self._issues[issue_id] = placeholders
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key=key,
            translation_placeholders=placeholders,
        )

    def _reachable_again(self, device: Device) -> None:
        """Any valid uplink clears unreachable (07 section 8.1)."""
        if device.unreachable:
            device.unreachable = False
            _LOGGER.info("%s is reachable again", device.name)
            self._set_issue("device_unreachable", device.eui, None)
            self._fire(EVENT_DEVICE_REACHABLE, device)

    @callback
    def _fire(self, bus_event: str, device: Device, **extra: Any) -> None:
        """Fire a bus event about a device (07 section 11.3)."""
        registry = dr.async_get(self.hass)
        entry = registry.async_get_device_by_identifier((DOMAIN, device.eui), self.entry.entry_id)
        self.hass.bus.async_fire(
            bus_event,
            {
                "device_id": entry.id if entry else None,
                "dev_eui": device.eui,
                "name": device.name,
                **extra,
            },
        )

    def device_for(self, device_id: str) -> Device | None:
        """The sensor behind a device registry id."""
        entry = dr.async_get(self.hass).async_get(device_id)
        if entry is None or self.entry.entry_id not in entry.config_entries:
            return None
        for domain, identifier in entry.identifiers:
            if domain == DOMAIN and identifier in self.devices:
                return self.devices[identifier]
        return None

    # ------------------------------------------------------------------ commands

    async def async_queue(
        self, device: Device, name: str, slot: str, cmd: int, args: bytes = b""
    ) -> bool:
        """Queue a command, replacing a queued one for the same slot (07 section 7).

        Returns False if the queue is full. The queue is on disk when this returns.
        """
        kept = [c for c in device.commands if c.slot != slot]
        if len(kept) >= COMMAND_QUEUE_MAX:
            return False
        device.commands = [
            *kept,
            Command(device.next_command_id, name, slot, cmd, args, _utcnow()),
        ]
        device.next_command_id += 1
        _LOGGER.info("%s: %s queued", device.name, name)
        async_dispatcher_send(self.hass, signal_device_update(device.eui))
        await self._async_save()
        return True

    @callback
    def _start_leave(self, device: Device) -> None:
        """The device's subentry was removed (04 section 6.1).

        A reachable paired device is sent LEAVE and kept until it acknowledges it or 48
        hours pass. Any other device is forgotten now: it cannot be reached.
        """
        if not device.supervised or device.unreachable:
            return
        now = _utcnow()
        device.leaving_since = now
        device.commands = [
            Command(device.next_command_id, "leave", "leave", cs.CMD_LEAVE, b"", now)
        ]
        device.next_command_id += 1
        device.in_flight = None
        self.leaving[device.eui] = device
        _LOGGER.info("%s removed: LEAVE queued", device.name)
        self._save_soon()

    @callback
    def _forget(self, device: Device) -> None:
        """Delete a leaving device's record. Its root counter stays (02 section 8.4)."""
        self._root_data()
        if self.leaving.pop(device.eui, None) is not None:
            self._identified.clear()
            self._rebuild_tags()
            self._save_soon()

    def _take_commands(self, device: Device) -> tuple[bytes, list[Command], bool]:
        """Queued commands that fit one downlink, in queue order, and whether more remain
        (07 section 5)."""
        out = b""
        taken: list[Command] = []
        for c in device.commands:
            if len(out) + len(c.encoded) > COMMANDS_MAX_BYTES:
                break
            out += c.encoded
            taken.append(c)
        return out, taken, len(taken) < len(device.commands)

    @callback
    def _settle(self, device: Device, fcnt: int, dl_ack: bool) -> None:
        """Settle the last downlink's commands on an uplink after it (07 section 7).

        DL_ACK does not say which downlink it acknowledges (chrisuthe/ConnectSensors#8).
        A sensor accepts only downlinks answering its latest uplink, so only an uplink
        with a higher fcnt than that downlink's reply_to was sent after it; a late or
        retransmitted earlier frame settles nothing.
        """
        if device.in_flight is None or fcnt <= device.in_flight[0]:
            return
        _, ids = device.in_flight
        device.in_flight = None
        sent = [c for c in device.commands if c.id in ids]
        if dl_ack:
            device.commands = [c for c in device.commands if c.id not in ids]
            for c in sent:
                self._delivered(device, c)
        else:
            self._give_up(device, [c for c in sent if c.sends >= COMMAND_MAX_SENDS])
        self._save_soon()
        async_dispatcher_send(self.hass, signal_device_update(device.eui))

    @callback
    def _delivered(self, device: Device, command: Command) -> None:
        _LOGGER.info("%s acknowledged %s", device.name, command.name)
        device.last_command = {
            "name": command.name,
            "result": "delivered",
            "at": _utcnow().isoformat(),
        }
        if command.cmd == cs.CMD_SET_HEARTBEAT:
            # The sensor uses it from now on; so do supervision and the Settings step.
            minutes = int.from_bytes(command.args, "big")
            device.heartbeat_minutes = minutes
            subentry = self.entry.subentries.get(device.subentry_id)
            if subentry is not None and device.leaving_since is None:
                self.hass.config_entries.async_update_subentry(
                    self.entry, subentry, data={**subentry.data, CONF_HEARTBEAT: minutes}
                )
        elif command.cmd == cs.CMD_LEAVE:
            _LOGGER.info("%s left the network", device.name)
            self._forget(device)

    @callback
    def _expire_commands(self, device: Device, now: datetime) -> bool:
        """Drop commands not delivered in time (07 section 7). True if any were."""
        expired = [
            c
            for c in device.commands
            if now - c.queued_at >= (LEAVE_EXPIRY if c.cmd == cs.CMD_LEAVE else COMMAND_EXPIRY)
        ]
        if expired:
            self._give_up(device, expired)
        return bool(expired)

    @callback
    def _give_up(self, device: Device, commands: list[Command]) -> None:
        """Drop undelivered commands, and say so (07 section 7)."""
        if not commands:
            return
        ids = {c.id for c in commands}
        device.commands = [c for c in device.commands if c.id not in ids]
        for c in commands:
            _LOGGER.warning(
                "%s did not acknowledge %s, queued %s and sent %s times; given up",
                device.name,
                c.name,
                dt_util.as_local(c.queued_at).strftime("%Y-%m-%d %H:%M"),
                c.sends,
            )
            device.last_command = {"name": c.name, "result": "failed", "at": _utcnow().isoformat()}
            if c.cmd == cs.CMD_LEAVE:
                self._forget(device)
        self._save_soon()
        async_dispatcher_send(self.hass, signal_device_update(device.eui))

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
        proxy.last_hello = _utcnow()
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
        self._set_issue(
            "proxy_mismatch",
            proxy.subentry_id,
            {
                "name": proxy.name,
                "proxy_settings": _radio(
                    hello["region"], hello["frequency_hz"], hello["sf"], hello["bw_khz"]
                ),
                "network_settings": _radio(self.region, freq, sf, bw),
                "region": self.region,
            }
            if mismatch
            else None,
        )
        if proxy.proxy_id is not None and (previous is None or previous["fw"] != hello["fw"]):
            self._set_sw_version(proxy.proxy_id.hex(), hello["fw"])
        now = int(time.time())
        self._send(proxy, cs.M_SERVER_STATUS, cs.server_status_body(now, self.net_tags(), m["seq"]))
        async_dispatcher_send(self.hass, signal_proxy_update(proxy.subentry_id))

    def _tx_result(self, proxy: Proxy, body: bytes) -> None:
        t = cs.parse_tx_result(body)
        seq = t["downlink_seq"]
        result = TX_RESULTS.get(t["result"], str(t["result"]))
        proxy.tx_results.append((result, t["offset_us"]))
        if result != "sent":
            proxy.airtime.pop(seq, None)
        if result == "duty_cycle":
            proxy.duty_cycle_until_ms = _now_ms() + DUTY_CYCLE_BACKOFF_S * 1000
        pending = proxy.pending_downlinks.pop(seq, None)
        if pending is not None and pending.result is not None and not pending.result.done():
            pending.result.set_result(result)
        _LOGGER.debug(
            "%s: downlink %s to %s: %s, %.1f ms after target",
            proxy.name,
            seq,
            pending.eui if pending else None,
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
        if got.get("counter") != "too_old":  # copies have no counter verdict
            new_path = proxy.subentry_id not in device.heard_by
            device.heard_by[proxy.subentry_id] = _utcnow()
            if new_path:
                async_dispatcher_send(self.hass, signal_device_update(device.eui))
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
            candidates = [(d, keyset) for _, d in self._all_devices()]
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
            await self._deliver(tx, downlink)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("Error processing an uplink from %s", tx.device.name)
        finally:
            # A retransmission of the frame may have started its own collection.
            if self._collecting.get(tx.frame) is tx:
                del self._collecting[tx.frame]

    async def _act(self, tx: Transmission) -> bytes | None:
        """07 section 4, steps 6 and 7. Returns the downlink to send, if any."""
        device, h = tx.device, tx.header
        fresh = tx.counter in ("accept", "accept_late")
        if fresh:
            best = max(tx.receptions, key=lambda r: r.snr)
            device.last_seen = _utcnow()
            device.last_heard_ms = _now_ms()
            device.rssi, device.snr = best.rssi, best.snr
            self._reachable_again(device)

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

        queued = device.state == STATE_PAIRED
        if fresh and queued:
            self._settle(device, h.fcnt, bool(uflags & cs.UF_DL_ACK))
        leaving = device.leaving_since is not None
        if leaving and device.eui not in self.leaving:
            return None  # LEAVE acknowledged or given up: the record is gone
        if fresh and not leaving:
            self._apply_readings(device, tx.plaintext[1:])
            self._schedule_save()
            async_dispatcher_send(self.hass, signal_device_update(device.eui))

        # Undelivered commands ride on any answer, and are a reason to answer (07 5).
        # Not on an answer to a late arrival: the sensor accepts only a downlink that
        # answers its latest uplink (02 section 10.2).
        dflags = 0
        taken: list[Command] = []
        latest = h.fcnt >= device.last_fcnt
        if queued and device.commands and latest and (fresh or tx.answer):
            commands, taken, more = self._take_commands(device)
            dflags = cs.DF_PENDING if more else 0
        if not (tx.answer or taken or (fresh and uflags & cs.UF_TIME_UNSURE)):
            return None
        # The device record must be on disk before SET_NETWORK goes out (04 section 4.6).
        if commands and not taken and not await self._async_save():
            return None
        assert device.session_keys is not None
        fcnt = await self._next_fcnt_down(device)
        if fcnt is None:
            return None
        if taken:
            for c in taken:
                c.sends += 1
            device.in_flight = (h.fcnt, [c.id for c in taken])
            self._schedule_save()
            _LOGGER.debug(
                "%s: sending %s with frame %s",
                device.name,
                ", ".join(c.name for c in taken),
                h.fcnt,
            )
        return cs.seal(
            device.session_keys,
            cs.T_DOWNLINK,
            h.net_tag,
            h.dev_tag,
            fcnt,
            cs.downlink_payload(h.fcnt, int(time.time()), dflags, commands),
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
                self._device_issues(device, _utcnow())
        async_dispatcher_send(self.hass, signal_device_update(device.eui))
        frame: bytes = r["frame"]
        return frame

    def _rename(self, identifier: str, name: str) -> None:
        """Follow a subentry rename in the device registry."""
        registry = dr.async_get(self.hass)
        entry = registry.async_get_device_by_identifier((DOMAIN, identifier), self.entry.entry_id)
        if entry is not None:
            registry.async_update_device(entry.id, name=name)

    @callback
    def async_clear_issues(self) -> None:
        """Withdraw this server's repair issues: nothing watches them once it stops."""
        for issue_id in list(self._issues):
            ir.async_delete_issue(self.hass, DOMAIN, issue_id)
        self._issues.clear()

    def _set_sw_version(self, identifier: str, version: str) -> None:
        """Keep the device registry's software version current."""
        registry = dr.async_get(self.hass)
        entry = registry.async_get_device_by_identifier((DOMAIN, identifier), self.entry.entry_id)
        if entry is not None:
            registry.async_update_device(entry.id, sw_version=version)

    def _clear_stall(self, device: Device) -> None:
        if device.stalled:
            device.stalled = False
            self._set_issue("pairing_stalled", device.eui, None)

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
            if name == "button":
                self._event(device, r["channel"], name, value)
                continue
            if name in ("unknown", "raw", "text"):
                continue
            key = reading_key(r["channel"], name)
            new = key not in device.entity_keys()
            device.readings[key] = value
            device.stale.discard(key)
            if new:
                async_dispatcher_send(self.hass, SIGNAL_NEW_READING, device, key)

    def _event(self, device: Device, channel: int, kind: str, value: int) -> None:
        """An event reading: fires the event entity and a bus event, never a state
        (03 section 5, 07 section 11.3)."""
        event_type = BUTTON_EVENTS.get(value)
        if event_type is None:
            _LOGGER.debug("%s: %s value %s is not an event type", device.name, kind, value)
            return
        key = reading_key(channel, kind)
        if key not in device.entity_keys():
            device.events.add(key)
            device.first_events[key] = event_type
            async_dispatcher_send(self.hass, SIGNAL_NEW_READING, device, key)
        else:
            device.events.add(key)
            async_dispatcher_send(self.hass, signal_device_event(device.eui), key, event_type)
        self._fire(EVENT_READING, device, channel=channel, kind=kind, event_type=event_type)

    def _candidates(self, tx: Transmission, airtime: float) -> list[Reception]:
        """Receptions whose proxy may answer (07 section 6.1), within its airtime budget."""
        now_ms = _now_ms()
        return [
            r
            for r in tx.receptions
            # A proxy removed while the uplink was collected is no longer asked.
            if self.proxies.get(r.proxy.subentry_id) is r.proxy
            and r.proxy.can_downlink
            and r.boot_id == _boot_id(r.proxy.link)
            and r.proxy.can_afford(airtime, now_ms)
        ]

    async def _deliver(self, tx: Transmission, downlink: bytes) -> None:
        """Answer in RX1 through the best proxy, falling back to RX2 (07 section 6).

        At most one proxy is asked per window, and RX2 only after RX1 failed. A sensor
        opens RX2 only after a confirmed uplink (08 section 7).
        """
        rx2 = tx.header.ftype in CONFIRMED_TYPES
        _, sf, bw = REGIONS[self.region]
        airtime = airtime_ms(len(downlink), sf, bw * 1000)
        candidates = self._candidates(tx, airtime)
        rx1: list[Reception] = []
        if _now_ms() - tx.started_ms <= RX1_DEADLINE_MS:
            rx1 = [r for r in candidates if not r.flags & cs.UPF_TS_UNCERTAIN and r.proxy.timing_ok]
        if rx1:
            first = _best(rx1)
            result = await self._transmit(tx, first, RX1_OFFSET_US, downlink, airtime, wait=True)
            if result == "sent":
                return
            # The next best candidate, or the same proxy if it is the only one (6.3).
            candidates = self._candidates(tx, airtime)
            candidates = [r for r in candidates if r.proxy is not first.proxy] or candidates
        if not rx2:
            if not rx1:
                _LOGGER.debug(
                    "%s: no proxy can answer frame %s in RX1", tx.device.name, tx.header.fcnt
                )
            return
        if not candidates:
            _LOGGER.debug(
                "%s: no proxy can answer frame %s%s",
                tx.device.name,
                tx.header.fcnt,
                " in RX2" if rx1 else "",
            )
            return
        await self._transmit(tx, _best(candidates), RX2_OFFSET_US, downlink, airtime, wait=False)

    async def _transmit(
        self,
        tx: Transmission,
        reception: Reception,
        offset: int,
        downlink: bytes,
        airtime: float,
        *,
        wait: bool,
    ) -> str | None:
        """Ask one proxy to transmit; with wait, return its TX_RESULT (None if late)."""
        proxy = reception.proxy
        seq = self._send(
            proxy, cs.M_DOWNLINK, cs.downlink_msg_body(reception.rx_time_us, offset, downlink)
        )
        proxy.downlinks_sent += 1
        proxy.airtime[seq] = (_now_ms(), airtime)
        future: asyncio.Future[str] | None = self.hass.loop.create_future() if wait else None
        proxy.pending_downlinks[seq] = PendingDownlink(tx.device.eui, future)
        while len(proxy.pending_downlinks) > TX_HISTORY:
            del proxy.pending_downlinks[next(iter(proxy.pending_downlinks))]
        window = "RX1" if offset == RX1_OFFSET_US else "RX2"
        _LOGGER.debug(
            "%s: answered frame %s from %s in %s",
            proxy.name,
            tx.header.fcnt,
            tx.device.name,
            window,
        )
        if future is None:
            return None
        try:
            deadline = tx.started_ms + offset // 1000 + airtime + TX_RESULT_MARGIN_MS
            async with asyncio.timeout((deadline - _now_ms()) / 1000):
                return await future
        except TimeoutError:
            proxy.tx_timeouts += 1
            # Counts as a failure when ranking proxies; a late result is still recorded.
            proxy.tx_results.append(("no_result", 0))
            _LOGGER.debug("%s: no TX_RESULT for the %s downlink in time", proxy.name, window)
            return None

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
                    "tx_timeouts": p.tx_timeouts,
                    "timing_ok": p.timing_ok,
                    "airtime_last_hour_ms": round(p.airtime_used_ms(_now_ms()), 1),
                }
                for p in self.proxies.values()
            },
            "devices": {
                eui: d.to_store()
                | {
                    "profile": d.profile,
                    "known_profile": d.profile_def is not None,
                    "heartbeat_minutes": d.heartbeat_minutes,
                    "single_path": d.single_path,
                }
                for eui, d in self.devices.items()
            },
            "leaving": {
                eui: d.to_store() | {"since": d.leaving_since} for eui, d in self.leaving.items()
            },
            "root_counters": dict(self._root_data()),
        }


def _best(receptions: list[Reception]) -> Reception:
    """Highest SNR; within 1 dB of it, higher RSSI, then fewer recent failures (6.2)."""
    top = max(r.snr for r in receptions)
    tied = [r for r in receptions if r.snr >= top - SNR_TIE_DB]
    return max(tied, key=lambda r: (r.rssi, -r.proxy.failures))
