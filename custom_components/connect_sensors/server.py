"""The ConnectSensors network server (spec 07), on Home Assistant's event loop.

Protocol work (frames, envelopes, proxy sessions, dedupe, JOIN answers) is done by the
connectsensors package. This module keeps the state around it: which proxies and
devices exist, what is persisted when, and which proxy answers which uplink.

Pairing codes, keys and decrypted payloads are never logged (07 section 11.6).
"""

from __future__ import annotations

import asyncio
from collections import Counter, deque
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
import secrets
import struct
import time
from typing import Any

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
    BUTTON_EVENTS,
    COLLECTION_WINDOW_MS,
    COMMAND_EXPIRY_H,
    COMMAND_MAX_SENDS,
    COMMAND_QUEUE_MAX,
    COMMANDS_MAX_BYTES,
    CONF_DEV_EUI,
    CONF_HEARTBEAT,
    CONF_IGNORE_FEDERATED_EVENTS,
    CONF_IGNORE_FEDERATED_READINGS,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    DUTY_CYCLE_BACKOFF_S,
    EVENT_DEVICE_REACHABLE,
    EVENT_DEVICE_UNREACHABLE,
    EVENT_READING,
    EXCHANGED_TIMEOUT_S,
    LEAVE_EXPIRY_H,
    PROXY_TIMEOUT_S,
    REGIONS,
    RESERVE_BLOCK,
    RX1_DEADLINE_MS,
    RX1_MAX_LATE_OR_BUSY,
    RX1_MAX_OFFSET_US,
    RX1_OFFSET_US,
    RX2_OFFSET_US,
    SAVE_DELAY_S,
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
    signal_device_added,
    signal_device_event,
    signal_device_update,
    signal_new_reading,
    signal_proxy_bound,
    signal_proxy_update,
)
from .federation import Federation, FedReception, Heard
from .profiles import Profile, default_heartbeat, get_profile
from .readings import reading_key

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
# Root downlink counters outlive devices and the config entry (02 section 8.4).
ROOT_STORAGE_KEY = f"{DOMAIN}.root_counters"
STATE_STORAGE_KEY = f"{DOMAIN}.state"
# 2: connectsensors 0.4.0. Sessions from 0.3.x are gone (see _migrate_state).
STATE_STORAGE_VERSION = 2

CONFIRMED_TYPES = frozenset({cs.T_DATA_CONFIRMED, cs.T_HEARTBEAT_CONFIRMED, cs.T_JOIN, cs.T_SYNC})
TICK = timedelta(seconds=5)
AGE_CAP_MS = 1500
TX_HISTORY = 20
HOUR_MS = 3_600_000
SINGLE_PATH_WINDOW = timedelta(hours=SINGLE_PATH_WINDOW_H)
SINGLE_PATH_REPAIR = timedelta(hours=SINGLE_PATH_REPAIR_H)
DEVICE_ISSUES = (
    "pairing_stalled",
    "device_unreachable",
    "single_path",
    "single_path_federated",
    "re_pair",
)
PROXY_ISSUES = ("proxy_mismatch", "shared_key")
COMMAND_EXPIRY = timedelta(hours=COMMAND_EXPIRY_H)
LEAVE_EXPIRY = timedelta(hours=LEAVE_EXPIRY_H)
EXCHANGED_TIMEOUT = timedelta(seconds=EXCHANGED_TIMEOUT_S)


def _now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def _unix() -> int:
    """Unix time: epochs, tags and server_time (02 section 6.1)."""
    return int(time.time())


def _utcnow() -> datetime:
    """Wall-clock time, for last-seen times and supervision."""
    return dt_util.utcnow()


def _new_nonce() -> int:
    return secrets.randbits(32) or 1


def _radio(region: str, freq: int | None, sf: int, bw: int) -> str:
    mhz = f" {freq / 1e6:g} MHz" if freq is not None else ""
    return f"{region}{mhz} SF{sf} {bw} kHz"


def _parse_time(value: str | None) -> datetime | None:
    return dt_util.parse_datetime(value) if value else None


@dataclass(slots=True)
class PendingDownlink:
    """A DOWNLINK waiting for its TX_RESULT; result is set if someone waits for it."""

    eui: str
    frame: bytes
    result: asyncio.Future[str] | None
    # A neighbour's federated downlink: told the result and offset (09 section 7.5).
    on_result: Callable[[str, int], None] | None = None


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
    # Copies delivered through mailboxes (07 section 4.4).
    fed: list[FedReception] = field(default_factory=list)
    # Queued commands the answer carries; each counts a send once a proxy is asked.
    commands: list[Command] = field(default_factory=list)
    # The answer carries a command, or NOP (09 section 9).
    carries: bool = False


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
        # Another proxy holds the same key, so either could forge its DOWNLINKs.
        self.shared = False
        self.uplinks = 0
        self.downlinks_sent = 0
        self.tx_results: deque[tuple[str, int]] = deque(maxlen=TX_HISTORY)
        self.tx_timeouts = 0
        self.pending_downlinks: dict[int, PendingDownlink] = {}
        # Airtime of downlinks sent, by DOWNLINK seq: (sent at ms, airtime ms).
        self.airtime: dict[int, tuple[int, float]] = {}
        self.duty_cycle_until_ms: int | None = None
        # Our own downlinks on the proxy's clock: (boot_id, start us, end us). A
        # neighbour's federated downlink may not displace one (09 section 7.4).
        self.own_tx: deque[tuple[int, int, int]] = deque(maxlen=TX_HISTORY)

    def rekey(self, key: bytes) -> None:
        """Take a new key. The proxy_id stays bound (05 section 4.2): messages under the
        old key are refused, and the proxy is offline until it connects with the new one."""
        self.key = key
        self.link = cs.ServerLink(_new_nonce)
        self.addr = None
        self.hello = None
        self.last_hello_ms = None

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
            and not self.shared
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
        return self.airtime_used_ms(now_ms) + airtime <= cs.PROXY_AIRTIME_MS_PER_HOUR


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
        # The pairing stall counts JOINs only after a KEY_EXCHANGE was SENT (07 5.1).
        self.join = cs.JoinResponder(k_root, self.dev_eui, root_fcnt_down, count_after_sent=True)
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
        # When the device last entered EXCHANGED, for its 2-minute timeout (04 4.2).
        self.exchanged_at: datetime | None = None
        # Paired before connectsensors 0.4.0: the sensor lost that pairing when its
        # firmware was upgraded, and has to be paired again.
        self.re_pair = False
        # Supervision (07 section 8). stale: readings not heard since the device was
        # last unreachable, shown as unavailable until an uplink carries them again.
        self.unreachable = False
        self.stale: set[str] = set()
        # Proxy subentry_id -> when that proxy last heard the device (07 section 9).
        self.heard_by: dict[str, datetime] = {}
        # Federated path (server and publisher pseudonym) -> when it last delivered
        # the device (07 section 8.2), kept apart from the proxies.
        self.fed_heard_by: dict[str, datetime] = {}
        # Reading keys whose value came from a federation-only frame (07 section 4.4).
        self.fed_only: set[str] = set()
        self.single_path_since: datetime | None = None
        # Event readings seen, by reading key: each has an event entity.
        self.events: set[str] = set()
        # The first event of a new key, for its entity to show once it is added.
        self.first_events: dict[str, str] = {}
        # Command queue (07 section 7), and the ids of the commands awaiting
        # acknowledgement, bound to their downlink's reply_to.
        self.commands: list[Command] = []
        self.acks = cs.CommandAcks[int]()
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
    def supervised_minutes(self) -> int:
        """The heartbeat interval supervision allows for.

        A sensor applies SET_HEARTBEAT on receipt and acknowledges it on the uplink
        after, one new interval later, so a longer one counts from when it is queued.
        """
        queued = [
            int.from_bytes(c.args, "big") for c in self.commands if c.cmd == cs.CMD_SET_HEARTBEAT
        ]
        return max([self.heartbeat_minutes, *queued])

    @property
    def unreachable_after(self) -> timedelta:
        """Silence after which the device is unreachable (07 section 8.1)."""
        return timedelta(minutes=3 * self.supervised_minutes + UNREACHABLE_GRACE_MIN)

    def recent_proxies(self, now: datetime) -> list[str]:
        """Subentry ids of the proxies that heard the device in the last 24 hours."""
        return [sid for sid, t in self.heard_by.items() if now - t < SINGLE_PATH_WINDOW]

    @property
    def proxy_count(self) -> int:
        """Proxies that heard the device in the last 24 hours (07 section 9)."""
        return len(self.recent_proxies(_utcnow()))

    def recent_federated(self, now: datetime) -> list[str]:
        """Federated paths that delivered the device in the last 24 hours."""
        return [p for p, t in self.fed_heard_by.items() if now - t < SINGLE_PATH_WINDOW]

    @property
    def federated_paths(self) -> list[str]:
        """Federated paths that delivered the device in the last 24 hours."""
        return sorted(self.recent_federated(_utcnow()))

    @property
    def federated_count(self) -> int:
        """How many federated paths delivered the device in the last 24 hours."""
        return len(self.federated_paths)

    @property
    def single_path(self) -> bool:
        """Heard by exactly one proxy of our own, or only through federated paths, over
        the last 24 hours. Federated paths never clear it (07 section 8.2)."""
        own = self.proxy_count
        return self.supervised and (own == 1 or (own == 0 and self.federated_count > 0))

    def reading_available(self, key: str) -> bool:
        """A reading is shown unless the device is unreachable or has not resent it."""
        return not self.unreachable and key not in self.stale

    def set_session(self, k_session: bytes) -> None:
        """Start using a new k_session; its downlink counter starts at 1 (02 section 8.4)."""
        self.k_session = k_session
        self.session_keys = cs.keys_from(k_session, self.dev_eui)
        self.fcnt_down = 0
        self.fcnt_down_reserved = 0

    def clear_session(self) -> None:
        """Forget the session keys. A pending exchange stays with the JoinResponder."""
        self.k_session = None
        self.session_keys = None
        self.fcnt_down = 0
        self.fcnt_down_reserved = 0

    def to_store(self) -> dict[str, Any]:
        """The persisted record (07 section 2.2)."""
        pending = self.join.pending
        return {
            "state": self.state,
            "k_session": self.k_session.hex() if self.k_session else None,
            "fcnt_down_reserved": self.fcnt_down_reserved,
            "last_fcnt": self.last_fcnt,
            "seen": sorted(self.uplinks.counter.seen),
            "pending": pending.to_dict() if pending is not None else None,
            "stalled": self.stalled,
            "exchanged_at": self.exchanged_at.isoformat() if self.exchanged_at else None,
            "re_pair": self.re_pair,
            "readings": self.readings,
            "sw_version": self.sw_version,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "rssi": self.rssi,
            "snr": self.snr,
            "unreachable": self.unreachable,
            "stale": sorted(self.stale),
            "heard_by": {sid: t.isoformat() for sid, t in self.heard_by.items()},
            "fed_heard_by": {path: t.isoformat() for path, t in self.fed_heard_by.items()},
            "fed_only": sorted(self.fed_only),
            "single_path_since": self.single_path_since.isoformat()
            if self.single_path_since
            else None,
            "events": sorted(self.events),
            "commands": [c.to_store() for c in self.commands],
            # The awaiting-acknowledgement record, under its milestone 2 name.
            "in_flight": [self.acks.reply_to, self.acks.awaiting]
            if self.acks.reply_to is not None
            else None,
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
        self.uplinks = cs.UplinkCollector(d["last_fcnt"], d["seen"])
        pending = cs.PendingExchange.from_dict(p) if (p := d["pending"]) is not None else None
        self.join = cs.JoinResponder(
            self.join.k_root,
            self.dev_eui,
            self.join.root_fcnt_down,
            pending,
            count_after_sent=True,
        )
        self.stalled = d["stalled"]
        self.exchanged_at = _parse_time(d.get("exchanged_at"))
        self.re_pair = d.get("re_pair", False)
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
        self.fed_heard_by = {
            path: t
            for path, v in d.get("fed_heard_by", {}).items()
            if (t := _parse_time(v)) is not None
        }
        self.fed_only = set(d.get("fed_only", []))
        self.single_path_since = _parse_time(d.get("single_path_since"))
        # Added with commands and events.
        self.events = set(d.get("events", []))
        self.commands = [Command.from_store(c) for c in d.get("commands", [])]
        flight = d.get("in_flight")
        self.acks = cs.CommandAcks[int](
            flight[0] if flight else None, flight[1] if flight else (), d["last_fcnt"]
        )
        self.next_command_id = d.get("next_command_id", 1)
        self.last_command = d.get("last_command")


def _migrate_state(data: dict[str, Any]) -> dict[str, Any]:
    """State stored by connectsensors 0.3.x (storage version 1).

    0.4.0 changed tags and pairing, and the sensor firmware built on it drops the
    pairing it stored. So every session here is void: each device goes back to
    SCANNED, and one that was paired is flagged for re-pairing. Its uplink counter
    restarts, so its JOIN is accepted whatever counter the new firmware resumes
    from. Root downlink counters live in their own store and are kept (02 8.4).
    Leaving devices are dropped: their LEAVE could only go out under the old keys.
    """
    devices = {
        eui: rec
        | {
            "state": STATE_SCANNED,
            "k_session": None,
            "fcnt_down_reserved": 0,
            "last_fcnt": 0,
            "seen": [],
            "pending": None,
            "stalled": False,
            "exchanged_at": None,
            "re_pair": rec.get("state") == STATE_PAIRED,
            "unreachable": False,
            # Shown as unavailable until the device is paired and heard again.
            "stale": sorted(rec.get("readings", {})),
            "single_path_since": None,
            "commands": [],
            "in_flight": None,
        }
        for eui, rec in data.get("devices", {}).items()
    }
    return {"proxies": data.get("proxies", {}), "devices": devices, "leaving": {}}


class _StateStore(Store[dict[str, Any]]):
    """The state store, migrated on load."""

    async def _async_migrate_func(
        self, old_major_version: int, old_minor_version: int, old_data: dict[str, Any]
    ) -> dict[str, Any]:
        if old_major_version < 2:
            return _migrate_state(old_data)
        return old_data


class NetworkServer(asyncio.DatagramProtocol):
    """One ConnectSensors network: its UDP endpoint, proxies and devices."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self.entry = entry
        self.port: int = entry.data[CONF_PORT]
        self.region: str = entry.data[CONF_REGION]
        self.proxies: dict[str, Proxy] = {}
        self.devices: dict[str, Device] = {}
        # Devices whose subentry was removed, until they acknowledge LEAVE (04 6.1).
        self.leaving: dict[str, Device] = {}
        self._by_proxy_id: dict[bytes, Proxy] = {}
        # Atomic writes (fsync and rename): a counter lost to a power cut would be reused.
        self._root_store: Store[dict[str, int]] = Store(
            hass, STORAGE_VERSION, ROOT_STORAGE_KEY, atomic_writes=True
        )
        self._state_store = _StateStore(
            hass, STATE_STORAGE_VERSION, STATE_STORAGE_KEY, atomic_writes=True
        )
        self._root_counters: dict[str, int] = {}
        self._proxy_ids: dict[str, str] = {}
        self._stored_devices: dict[str, Any] = {}
        self._transport: asyncio.DatagramTransport | None = None
        self._tags = cs.TagTable()
        self._tag_epoch: int | None = None
        self._tag_time: int = 0
        # The tag block of each device's last_fcnt in the table (07 section 3).
        self._tag_blocks: dict[str, bytes] = {}
        self._collecting: dict[bytes, Transmission] = {}
        # Confirmed transmissions none of our proxies could answer, with their answer:
        # a neighbour's copy may still arrive in time for RX2 (07 section 4.4).
        self._unanswered: dict[bytes, tuple[Transmission, bytes]] = {}
        self._identified: dict[bytes, tuple[Device, int]] = {}
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
        # Bootstrap frames identified by trial decryption (02 section 11.3).
        self.trial_decryptions = 0
        self.options: dict[str, Any] = dict(entry.options)
        self.federation: Federation | None = None

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
        federation = Federation(self.hass, self, self.options)
        if federation.enabled:
            self.federation = federation
            await federation.async_start()

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
        if self.federation is not None:
            await self.federation.async_stop()
        self._unanswered.clear()
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
            for issue in PROXY_ISSUES:
                self._set_issue(issue, sid, None)
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
                    if known_proxy.key != (key := bytes.fromhex(sub.data[CONF_KEY])):
                        known_proxy.rekey(key)
                        _LOGGER.info("Proxy %s has a new key", sub.title)
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
                device = Device.from_subentry(sub, self._root_counters.get(eui, 0))
                left = None
                if (record := self._stored_devices.get(eui)) is not None:
                    device.load_store(record)
                elif (left := self.leaving.pop(eui, None)) is not None:
                    # The sensor has not left, so it is still paired under these keys.
                    device.join = left.join
                    device.load_store(left.to_store())
                    device.commands = []
                    device.acks = cs.CommandAcks[int](last_fcnt=device.last_fcnt)
                    _LOGGER.info(
                        "%s added again before it acknowledged LEAVE; LEAVE dropped", left.name
                    )
                self.devices[eui] = device
                if left is not None:
                    self._identified.clear()
                    self._rebuild_tags()
                if not initial:
                    async_dispatcher_send(
                        self.hass, signal_device_added(self.entry.entry_id), device
                    )
        self._check_keys()
        if not initial:
            self._schedule_save()

    @callback
    def _check_keys(self) -> None:
        """No downlinks through a proxy whose key another proxy holds (05 section 4.2,
        07 section 6.1), and a repair for each that would accept them (07 11.5)."""
        holders = Counter(p.key for p in self.proxies.values())
        for proxy in self.proxies.values():
            shared = holders[proxy.key] > 1
            if shared and not proxy.shared:
                _LOGGER.warning(
                    "Proxy %s shares its key with another proxy; no downlinks go through it",
                    proxy.name,
                )
            proxy.shared = shared
            downlink = proxy.hello is not None and bool(proxy.hello["caps"] & cs.CAP_DOWNLINK)
            self._set_issue(
                "shared_key",
                proxy.subentry_id,
                {"name": proxy.name} if shared and downlink else None,
                fix={"entry_id": self.entry.entry_id, "subentry_id": proxy.subentry_id},
            )

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
        """The whole tag table, for the current epoch (07 section 3)."""
        now = _unix()
        self._tags = cs.TagTable()
        self._tag_blocks.clear()
        self._tag_epoch = cs.epoch_of(now)
        self._tag_time = now
        for _, device in self._all_devices():
            self._update_tags(device)

    def _update_tags(self, device: Device) -> None:
        """One device's entries: the normal tags of epochs e-1 to e+1 and the SYNC tags
        around its last_fcnt, under its session keys while EXCHANGED or PAIRED."""
        if device.session_keys is None or device.state not in (STATE_EXCHANGED, STATE_PAIRED):
            self._tags.remove(device.eui)
            self._tag_blocks.pop(device.eui, None)
            if self.federation is not None:
                self.federation.devices_changed()
            return
        assert self._tag_epoch is not None
        self._tags.update(
            device.eui, device.session_keys.k_dev_tag, self._tag_epoch, device.last_fcnt
        )
        self._tag_blocks[device.eui] = _tag_block(device.last_fcnt)
        if self.federation is not None:
            self.federation.devices_changed()

    @callback
    def _tick(self, _now: datetime) -> None:
        now = _unix()
        e = cs.epoch_of(now)
        jumped = abs(now - self._tag_time - TICK.total_seconds()) > 60
        if e != self._tag_epoch or jumped:
            # Proxies hold no tags, so nothing is sent at the boundary (05 5.3).
            self._rebuild_tags()
        else:
            self._tag_time = now
        for proxy in self.proxies.values():
            async_dispatcher_send(self.hass, signal_proxy_update(proxy.subentry_id))
        self._supervise(_utcnow())
        if self.federation is not None:
            self.federation.tick(_utcnow())

    # ------------------------------------------------------------------ supervision

    @callback
    def _supervise(self, now: datetime) -> None:
        """Unreachable and single-path devices (07 section 8), and their repair issues."""
        for device in list(self.leaving.values()):
            self._expire_commands(device, now)
        for device in self.devices.values():
            changed = self._expire_commands(device, now)
            if (
                device.state == STATE_EXCHANGED
                and device.exchanged_at is not None
                and now - device.exchanged_at >= EXCHANGED_TIMEOUT
            ):
                self._exchange_timed_out(device)
                self._save_soon()  # a state change (07 section 10)
                changed = True
            for sid in [s for s, t in device.heard_by.items() if now - t >= SINGLE_PATH_WINDOW]:
                del device.heard_by[sid]
                changed = True
            for path in [
                p for p, t in device.fed_heard_by.items() if now - t >= SINGLE_PATH_WINDOW
            ]:
                del device.fed_heard_by[path]
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
                        3 * device.supervised_minutes + UNREACHABLE_GRACE_MIN,
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
    def _exchange_timed_out(self, device: Device) -> None:
        """No uplink under the pending k_session verified in 2 minutes: back to SCANNED,
        and the pending k_session is no longer recognized (04 section 4.2). The pending
        exchange is kept, so a retransmitted JOIN still gets the same answer."""
        device.state = STATE_SCANNED
        device.exchanged_at = None
        device.clear_session()
        self._update_tags(device)
        _LOGGER.info("%s: pairing timed out; press its button again", device.name)

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
                "heartbeat": str(device.supervised_minutes),
            }
        self._set_issue("device_unreachable", eui, unreachable)
        single_path = single_path_federated = None
        since = device.single_path_since
        if since is not None and now - since >= SINGLE_PATH_REPAIR:
            sids = device.recent_proxies(now)
            if sids:
                proxy = self.proxies.get(sids[0])
                single_path = {"name": name, "proxy": proxy.name if proxy else "-"}
            else:
                single_path_federated = {"name": name}
        self._set_issue("single_path", eui, single_path)
        self._set_issue("single_path_federated", eui, single_path_federated)
        self._set_issue(
            "re_pair",
            eui,
            {"name": name} if device.re_pair and device.state != STATE_PAIRED else None,
            fix={"entry_id": self.entry.entry_id, "dev_eui": eui},
        )

    @callback
    def _set_issue(
        self,
        key: str,
        subject: str,
        placeholders: dict[str, str] | None,
        fix: dict[str, str | int | float | None] | None = None,
    ) -> None:
        """Raise issue `key` for a device or proxy with these placeholders, or clear it.
        With fix, the issue is fixable and its repair flow gets fix as its data."""
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
            is_fixable=fix is not None,
            data=fix,
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
        if entry is None or entry.config_entry_id != self.entry.entry_id:
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
        if device.proxy_count == 0 and device.federated_count > 0:
            # 07 section 7: only RX2 answers to confirmed uplinks reach such a device.
            _LOGGER.info(
                "%s is heard only through neighbours: %s goes out with its next "
                "confirmed uplink, within its heartbeat interval",
                device.name,
                name,
            )
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
        device.acks = cs.CommandAcks[int](last_fcnt=device.last_fcnt)
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
    def _settle(self, device: Device, fcnt: int, counter: str, dl_ack: bool) -> None:
        """Settle the commands awaiting acknowledgement on an uplink (07 section 7)."""
        reply_to = device.acks.reply_to
        delivered = device.acks.uplink(fcnt, counter, dl_ack)
        if not delivered:
            if reply_to is not None and counter == "accept" and fcnt > reply_to:
                # Not acknowledged yet. The record stays, since a late arrival may
                # still carry the DL_ACK, but a command sent too often is given up.
                # LEAVE waits its 48 hours (04 section 6.1), however often it was sent.
                self._give_up(
                    device,
                    [
                        c
                        for c in device.commands
                        if c.id in device.acks.awaiting
                        and c.sends >= COMMAND_MAX_SENDS
                        and c.cmd != cs.CMD_LEAVE
                    ],
                )
            return
        done = [c for c in device.commands if c.id in delivered]
        device.commands = [c for c in device.commands if c.id not in delivered]
        for c in done:
            self._delivered(device, c)
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
        async_dispatcher_send(self.hass, signal_proxy_bound(self.entry.entry_id), proxy)

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
        boot_id = proxy.link.boot_id
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
        self._check_keys()
        if proxy.proxy_id is not None and (previous is None or previous["fw"] != hello["fw"]):
            self._set_sw_version(proxy.proxy_id.hex(), hello["fw"])
        now = _unix()
        self._send(proxy, cs.M_SERVER_STATUS, cs.server_status_body(now, m["seq"]))
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
        if pending is not None and pending.on_result is not None:
            pending.on_result(result, t["offset_us"])
        if (
            pending is not None
            and result == "sent"
            and (device := self.devices.get(pending.eui)) is not None
            and device.join.sent(pending.frame)
        ):
            self._schedule_save()
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
            # No device of ours verified it: a neighbour's, perhaps (09 section 6.1).
            if self.federation is not None and not proxy.radio_mismatch:
                self.federation.heard(
                    Heard(
                        proxy,
                        boot_id,
                        u["rx_time_us"],
                        u["rssi"],
                        u["snr"],
                        u["flags"],
                        now_ms,
                    ),
                    frame,
                    h,
                )
            return
        device, keys, plaintext = identified
        got = device.uplinks.receive(frame, now_ms)
        block = self._tag_blocks.get(device.eui)
        if block is not None and block != _tag_block(device.last_fcnt):
            # last_fcnt moved into a new block: move the SYNC tags along (07 3).
            self._update_tags(device)
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
        self._start(tx)

    def _start(self, tx: Transmission) -> None:
        """Collect a new transmission, act on it and answer it (07 section 4)."""
        self._collecting[tx.frame] = tx
        task = self.hass.async_create_background_task(
            self._process(tx), f"{DOMAIN} uplink {tx.device.eui}"
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def federated_uplink(
        self, frame: bytes, h: cs.ParsedHeader, fed: list[FedReception], arrived_ms: int
    ) -> None:
        """A frame delivered by a mailbox: processed like a copy from a proxy, from step
        2 of 07 section 4, and identified by table lookup only (07 section 4.4)."""
        try:
            identified = self._identify(frame, h, arrived_ms, local=False)
        except Exception:
            _LOGGER.exception("Error identifying a federated frame")
            return
        if identified is None:
            return
        device, keys, plaintext = identified
        # Received age_ms before its delivery arrived (07 section 4.3).
        # The age is the publisher's claim: capped as the server caps it (09 7.2), so a
        # retry cannot be passed off as a copy.
        age = min(AGE_CAP_MS, max(r.age_ms for r in fed))
        got = device.uplinks.receive(frame, arrived_ms, age)
        block = self._tag_blocks.get(device.eui)
        if block is not None and block != _tag_block(device.last_fcnt):
            self._update_tags(device)
        if got.get("counter") != "too_old":
            new_path = any(r.path not in device.fed_heard_by for r in fed)
            now = _utcnow()
            for r in fed:
                device.fed_heard_by[r.path] = now
            if new_path:
                async_dispatcher_send(self.hass, signal_device_update(device.eui))
        if got["result"] == "copy":
            if (tx := self._collecting.get(frame)) is not None:
                tx.fed.extend(fed)
            elif (
                (waiting := self._unanswered.get(frame)) is not None
                and arrived_ms - waiting[0].started_ms < RX2_OFFSET_US // 1000
                and self.federation is not None
            ):
                tx, downlink = waiting
                tx.fed.extend(fed)
                task = self.hass.async_create_background_task(
                    self._request_late(tx, downlink), f"{DOMAIN} federated request {device.eui}"
                )
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
            return
        if got["counter"] == "too_old":
            return
        tx = Transmission(
            frame, h, device, keys, plaintext, got["counter"], got["answer"], arrived_ms, [], fed
        )
        self._start(tx)

    async def _request_late(self, tx: Transmission, downlink: bytes) -> None:
        if self.federation is not None and await self.federation.request(tx, downlink, tx.carries):
            self._unanswered.pop(tx.frame, None)

    def _identify(
        self, frame: bytes, h: cs.ParsedHeader, now_ms: int, local: bool = True
    ) -> tuple[Device, cs.DeviceKeys, bytes] | None:
        """07 section 4, step 4. Copies reuse the first copy's identification. A frame
        that is not local (a federated one) is identified by lookup only (4.4)."""
        self._identified = {
            f: v for f, v in self._identified.items() if now_ms - v[1] < cs.FRAME_MEMORY_MS
        }
        known = self._identified.get(frame)
        if known is not None and (opened := self._try(known[0], frame, h.ftype)) is not None:
            return opened
        if not local and h.ftype == cs.T_JOIN:
            return None  # pairing is never federated (09 section 8)

        route, euis = cs.identify_route(self._tags, frame, local=local)
        if not local and self.federation is not None:
            # The registered SYNC window reaches past the table's (09 section 5.5).
            extra = [e for e in self.federation.lookup(h.tag) if e not in euis]
            if extra:
                route, euis = "table", [*euis, *extra]
        tried: list[Device] = []
        for eui in euis:
            if (device := self._device(eui)) is None:
                continue
            tried.append(device)
            if (opened := self._try(device, frame, h.ftype)) is not None:
                self._identified[frame] = (device, now_ms)
                return opened
        # A bootstrap frame that missed the table, or whose candidates all failed the
        # MIC, is tried against every device (02 section 11.3).
        if local and cs.is_bootstrap(h.ftype) and route != "drop":
            if not self._bootstrap_allowed(now_ms):
                return None  # dropped unprocessed (07 section 4, step 4)
            for _, device in self._all_devices():
                if device in tried:
                    continue
                if (opened := self._try(device, frame, h.ftype)) is not None:
                    self.trial_decryptions += 1
                    _LOGGER.debug("%s: frame %s found by trial decryption", device.name, h.fcnt)
                    self._identified[frame] = (device, now_ms)
                    return opened
        self.foreign_frames += 1
        return None

    def _bootstrap_allowed(self, now_ms: int) -> bool:
        """At most 50 bootstrap frames a minute are trial-decrypted (02 section 11.3)."""
        while self._bootstrap and now_ms - self._bootstrap[0] > 60_000:
            self._bootstrap.popleft()
        if len(self._bootstrap) >= BOOTSTRAP_PER_MINUTE:
            return False
        self._bootstrap.append(now_ms)
        return True

    def _device(self, eui: str) -> Device | None:
        return self.devices.get(eui) or self.leaving.get(eui)

    @staticmethod
    def _keys(device: Device, ftype: int) -> cs.DeviceKeys | None:
        """The key set a frame type may use for this device now: the root keys for a
        JOIN while waiting to be paired, else the session keys while EXCHANGED or
        PAIRED (02 section 11.3)."""
        if ftype == cs.T_JOIN:
            return device.root_keys if device.state in (STATE_SCANNED, STATE_EXCHANGED) else None
        if device.state in (STATE_EXCHANGED, STATE_PAIRED):
            return device.session_keys
        return None

    @classmethod
    def _try(
        cls, device: Device, frame: bytes, ftype: int
    ) -> tuple[Device, cs.DeviceKeys, bytes] | None:
        """Open the frame with the key set its type may use for this device."""
        if (keys := cls._keys(device, ftype)) is None:
            return None
        try:
            _, plaintext = cs.open_frame(keys, frame)
        except Exception:  # InvalidTag or ValueError: not this device
            return None
        return device, keys, plaintext

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
        if self._keys(device, h.ftype) is not tx.keys:
            # The keys it verified under were dropped while it waited for the lock:
            # the exchange timed out or was replaced.
            _LOGGER.debug("%s: frame %s is under keys no longer in use", device.name, h.fcnt)
            return None
        fresh = tx.counter in ("accept", "accept_late")
        if fresh:
            device.last_seen = _utcnow()
            device.last_heard_ms = _now_ms()
            if tx.receptions:  # signal figures are our own proxies' (07 section 9)
                best = max(tx.receptions, key=lambda r: r.snr)
                device.rssi, device.snr = best.rssi, best.snr
            self._reachable_again(device)

        if h.ftype == cs.T_JOIN:
            return await self._join(tx) if tx.answer else None

        uflags = tx.plaintext[0]
        commands = b""
        if device.state == STATE_EXCHANGED and fresh:
            # 04 section 4.4, step 6: the first uplink under the pending k_session
            # verified. Its DL_ACK, if set, acknowledges KEY_EXCHANGE (07 section 7),
            # which is no queued command. Answering it completes pairing on the sensor.
            device.state = STATE_PAIRED
            device.join.paired()
            device.exchanged_at = None
            device.re_pair = False
            self._clear_stall(device)
            self._set_issue("re_pair", device.eui, None)
            self._update_tags(device)
            _LOGGER.info("%s paired", device.name)
            # PAIRED and k_session are on disk before the answer (04 section 4.6).
            if not await self._async_save():
                return None

        queued = device.state == STATE_PAIRED
        self._settle(device, h.fcnt, tx.counter, bool(uflags & cs.UF_DL_ACK))
        if self.federation is not None:
            self.federation.uplink(device, h.fcnt, tx.counter, bool(uflags & cs.UF_DL_ACK))
        leaving = device.leaving_since is not None
        if leaving and device.eui not in self.leaving:
            return None  # LEAVE acknowledged or given up: the record is gone
        if fresh and not leaving:
            # Heard so far only through mailboxes: federation-only (07 section 4.4).
            # Our proxies' copies arrive first, so a later one is rare.
            self._apply_readings(device, tx.plaintext[1:], federation_only=not tx.receptions)
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
        if (
            not commands
            and not tx.receptions
            and queued
            and latest
            and self.federation is not None
            and self.federation.owner
        ):
            # A federated answer carries NOP, so the sensor's DL_ACK can credit the
            # publisher (09 section 7.7).
            commands = cs.command(cs.CMD_NOP, b"")
        tx.carries = bool(commands)
        assert device.session_keys is not None
        fcnt = await self._next_fcnt_down(device)
        if fcnt is None:
            return None
        if taken:
            tx.commands = taken
            # Two answers to one uplink keep only the commands both carry (07 7).
            device.acks.downlink(h.fcnt, [c.id for c in taken])
            self._save_soon()
            _LOGGER.debug(
                "%s: sending %s with frame %s",
                device.name,
                ", ".join(c.name for c in taken),
                h.fcnt,
            )
        # The answer carries the uplink's tag (02 section 6.1).
        return cs.seal(
            device.session_keys,
            cs.T_DOWNLINK,
            h.tag,
            fcnt,
            cs.downlink_payload(h.fcnt, _unix(), dflags, commands),
        )

    async def _join(self, tx: Transmission) -> bytes | None:
        """Answer a JOIN (07 section 5.1)."""
        device = tx.device
        r = device.join.join(tx.frame, secrets.token_bytes(32), _unix())
        if r["action"] == "new_exchange":
            device.set_session(r["k_session"])
            device.state = STATE_EXCHANGED
            device.exchanged_at = _utcnow()
            self._clear_stall(device)
            # The first session uplink is found by its tags (07 section 3).
            self._update_tags(device)
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

    def _apply_readings(self, device: Device, data: bytes, federation_only: bool = False) -> None:
        """Update reading values (03 section 6). An invalid payload applies nothing.

        Readings and events of a federation-only frame are marked, or ignored if the
        user chose to (07 section 4.4).
        """
        if not data:
            return
        try:
            readings = cs.decode_readings(data)
        except ValueError as err:
            _LOGGER.warning("%s sent invalid readings: %s", device.name, err)
            return
        skip_readings = federation_only and self.options.get(CONF_IGNORE_FEDERATED_READINGS)
        skip_events = federation_only and self.options.get(CONF_IGNORE_FEDERATED_EVENTS)
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
                if not skip_events:
                    self._event(device, r["channel"], name, value, federation_only)
                continue
            if name in ("unknown", "raw", "text") or skip_readings:
                continue
            key = reading_key(r["channel"], name)
            new = key not in device.entity_keys()
            device.readings[key] = value
            device.stale.discard(key)
            if federation_only:
                device.fed_only.add(key)
            else:
                device.fed_only.discard(key)
            if new:
                async_dispatcher_send(
                    self.hass, signal_new_reading(self.entry.entry_id), device, key
                )

    @callback
    def _event(
        self, device: Device, channel: int, kind: str, value: int, federation_only: bool = False
    ) -> None:
        """An event reading: fires the event entity and a bus event, never a state
        (03 section 5, 07 section 11.3)."""
        event_type = BUTTON_EVENTS.get(value)
        if event_type is None:
            _LOGGER.debug("%s: %s value %s is not an event type", device.name, kind, value)
            return
        key = reading_key(channel, kind)
        if federation_only:
            device.fed_only.add(key)
        else:
            device.fed_only.discard(key)
        if key not in device.entity_keys():
            device.events.add(key)
            device.first_events[key] = event_type
            async_dispatcher_send(self.hass, signal_new_reading(self.entry.entry_id), device, key)
        else:
            device.events.add(key)
            async_dispatcher_send(self.hass, signal_device_event(device.eui), key, event_type)
        self._fire(
            EVENT_READING,
            device,
            channel=channel,
            kind=kind,
            event_type=event_type,
            federation_only=federation_only,
        )

    def _candidates(self, tx: Transmission, airtime: float) -> list[Reception]:
        """Receptions whose proxy may answer (07 section 6.1), within its airtime budget."""
        now_ms = _now_ms()
        return [
            r
            for r in tx.receptions
            # A proxy removed while the uplink was collected is no longer asked.
            if self.proxies.get(r.proxy.subentry_id) is r.proxy
            and r.proxy.can_downlink
            and r.boot_id == r.proxy.link.boot_id
            and r.proxy.can_afford(airtime, now_ms)
        ]

    async def _deliver(self, tx: Transmission, downlink: bytes) -> None:
        """Answer in RX1 through the best proxy, falling back to RX2 (07 section 6).

        At most one proxy is asked per window, and RX2 only after RX1 failed. A sensor
        opens RX2 only after a confirmed uplink (08 section 7).
        """
        rx2 = tx.header.ftype in CONFIRMED_TYPES
        airtime = cs.region_airtime_ms(self.region, len(downlink))
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
        if not candidates and not rx1 and self.federation is not None and self.federation.owner:
            # None of our proxies can answer: one federated RX2 request, never both
            # (07 sections 4.4 and 6.3). Copies that come later may still make it.
            if not (tx.fed and await self.federation.request(tx, downlink, tx.carries)) and (
                # A copy that came while the first try was ranked gets one more.
                not tx.fed or not await self.federation.request(tx, downlink, tx.carries)
            ):
                now_ms = _now_ms()
                self._unanswered = {
                    f: v
                    for f, v in self._unanswered.items()
                    if now_ms - v[0].started_ms < RX2_OFFSET_US // 1000
                }
                self._unanswered[tx.frame] = (tx, downlink)
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
        self.federated_request_sent(tx)
        start = reception.rx_time_us + offset
        proxy.own_tx.append((reception.boot_id, start, start + int(airtime * 1000)))
        if self.federation is not None:
            self.federation.answered_locally(tx)
        proxy.airtime[seq] = (_now_ms(), airtime)
        future: asyncio.Future[str] | None = self.hass.loop.create_future() if wait else None
        proxy.pending_downlinks[seq] = PendingDownlink(tx.device.eui, downlink, future)
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

    # ------------------------------------------------------------------ federation

    def clock(self) -> int:
        """Monotonic milliseconds, as the rest of the server counts them."""
        return _now_ms()

    def unix(self) -> int:
        """Unix seconds (tags, registration expiry)."""
        return _unix()

    def utcnow(self) -> datetime:
        """Wall-clock time (repairs)."""
        return _utcnow()

    @callback
    def set_issue(self, key: str, subject: str, placeholders: dict[str, str] | None) -> None:
        """Raise or clear a repair issue on behalf of the federation client."""
        self._set_issue(key, subject, placeholders)

    def federated_devices(self) -> Iterator[tuple[str, bytes, int, int, int]]:
        """Devices whose tags an owner registers: (dev_eui, k_dev_tag, last_fcnt, seconds
        since last heard, heartbeat minutes). Fresh keys count at once (09 5.3)."""
        now = _utcnow()
        for eui, device in self._all_devices():
            if device.session_keys is None or device.state not in (STATE_EXCHANGED, STATE_PAIRED):
                continue
            seen = device.last_seen
            silence = max(0, int((now - seen).total_seconds())) if seen else 0
            yield (
                eui,
                device.session_keys.k_dev_tag,
                device.last_fcnt,
                silence,
                device.supervised_minutes,
            )

    def federated_request_sent(self, tx: Transmission) -> None:
        """A downlink went out for the transmission: one send of its commands (an RX2
        resend after RX1 is the same frame)."""
        for c in tx.commands:
            c.sends += 1
        tx.commands = []

    def proxy_current(self, proxy: Proxy) -> bool:
        """The proxy has not been removed."""
        return self.proxies.get(proxy.subentry_id) is proxy

    def own_downlink_overlaps(self, proxy: Proxy, start_us: int, airtime: float) -> bool:
        """Whether a transmission at start_us on the proxy's clock would overlap one of
        our own downlinks there (09 section 7.4, check 6)."""
        end = start_us + int(airtime * 1000)
        boot = proxy.link.boot_id
        return any(b == boot and a < end and start_us < z for b, a, z in proxy.own_tx)

    def transmit_for_neighbour(
        self,
        proxy: Proxy,
        rx_time_us: int,
        downlink: bytes,
        airtime: float,
        on_result: Callable[[str, int], None],
    ) -> int | None:
        """Send a neighbour's RX2 answer through one of our proxies (09 section 7.4).
        Returns the DOWNLINK seq, or None if the proxy cannot take it now."""
        if not self.proxy_current(proxy) or not proxy.can_downlink:
            return None
        seq = self._send(
            proxy, cs.M_DOWNLINK, cs.downlink_msg_body(rx_time_us, RX2_OFFSET_US, downlink)
        )
        proxy.downlinks_sent += 1
        proxy.airtime[seq] = (_now_ms(), airtime)
        proxy.pending_downlinks[seq] = PendingDownlink("", downlink, None, on_result)
        while len(proxy.pending_downlinks) > TX_HISTORY:
            del proxy.pending_downlinks[next(iter(proxy.pending_downlinks))]
        return seq

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
                    "shared_key": p.shared,
                    "boot_id": p.link.boot_id,
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
                    "federated_paths": d.federated_count,
                }
                for eui, d in self.devices.items()
            },
            "leaving": {
                eui: d.to_store() | {"since": d.leaving_since} for eui, d in self.leaving.items()
            },
            "root_counters": dict(self._root_data()),
            "federation": self.federation.diagnostics() if self.federation else None,
        }


def _tag_block(fcnt: int) -> bytes:
    """The bootstrap tag block holding an uplink counter value (02 section 6.1). A
    device with no uplink yet counts as block 0, as in the tag table."""
    return cs.block_index(max(fcnt, 1))


def _best(receptions: list[Reception]) -> Reception:
    """Highest SNR; within 1 dB of it, higher RSSI, then fewer recent failures (6.2)."""
    top = max(r.snr for r in receptions)
    tied = [r for r in receptions if r.snr >= top - SNR_TIE_DB]
    return max(tied, key=lambda r: (r.rssi, -r.proxy.failures))
