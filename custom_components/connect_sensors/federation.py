"""Federation through mailbox servers (spec 09): the client side of one home.

Three opt-ins, each off by default:

- owner: register the tags of this home's devices, take their frames from other homes'
  proxies into the uplink path, and request RX2 answers through those proxies;
- publisher: publish frames this home's proxies heard that no device of its own
  verified, never JOIN;
- federated downlinks: transmit the RX2 answers that neighbours request through this
  home's proxies.

The protocol rules (CBOR, message schemas, signatures, proofs, the SYNC window and the
RX2 timing rule) are the spec's reference module. This module is the connection and
the state around it. Owner keys are never logged.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import logging
import math
import random
import secrets
import ssl
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import aiohttp
import connectsensors as cs

# The 09 helpers are not in the package's public surface yet; they are the same
# reference module, which cs-mailbox imports the same way.
from connectsensors import _csframe as csf
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.storage import Store
from homeassistant.util.ssl import create_client_context

from .const import (
    COLLECTION_WINDOW_MS,
    CONF_FEDERATION_DOWNLINKS,
    CONF_FEDERATION_OWNER,
    CONF_FEDERATION_PUBLISH,
    CONF_MAILBOX_CERTIFICATE,
    CONF_MAILBOX_SERVERS,
    DOMAIN,
    FED_BOOTSTRAP_PER_MINUTE,
    FED_BUDGET_MS_PER_HOUR,
    FED_DOWNLINKS_PER_MINUTE,
    MAILBOX_UNREACHABLE_REPAIR_H,
    PUBLISH_PER_SECOND,
    REGIONS,
    RX2_OFFSET_US,
    TX_RESULTS,
    signal_mailbox_update,
)

if TYPE_CHECKING:
    from .server import Device, NetworkServer, Proxy, Transmission

_LOGGER = logging.getLogger(__name__)

STORAGE_KEY = f"{DOMAIN}.mailbox"
STORAGE_VERSION = 1

SUBPROTOCOL = "cs-mailbox.1"
CONNECT_TIMEOUT_S = 15
PING_INTERVAL_S = 30  # 09 section 12.5
REFRESH_INTERVAL_S = 30
BACKOFF_MIN_S = 1.0
BACKOFF_MAX_S = 300.0
# SYNC registrations last a day and are renewed while their block stays in the
# window (09 section 5.3 allows up to 7 days).
SYNC_REGISTRATION_S = 86_400
RENEW_BEFORE_S = cs.EPOCH_SECONDS
REGISTER_CHUNK = 32  # 09 section 12.7
WITHDRAW_CHUNK = 256
MAX_RECEPTIONS = 8
# A forwarded DOWNLINK whose TX_RESULT never comes still gets its one result (09 7.5).
TX_RESULT_WAIT_S = 5
SOFT_FAILURE_S = 5  # 09 section 9: no result within 5 seconds of the request
QUOTA_HALF_LIFE_MS = 3_600_000
RECORD_HALF_LIFE_S = 7 * 86_400
RECORD_FLOOR = 0.01  # decayed weight below which a credit or failure is forgotten
HOUR_MS = 3_600_000
MINUTE_MS = 60_000
# Plain ws:// is for the reference server's --insecure-dev mode in tests only:
# 09 section 12.1 requires TLS.
ALLOW_INSECURE = False

CODE = {name: i for i, name in enumerate(csf.MB_ERRORS)}
RESULT_CODE = {name: code for code, name in TX_RESULTS.items()}
CONFIRMED = frozenset({cs.T_DATA_CONFIRMED, cs.T_HEARTBEAT_CONFIRMED, cs.T_SYNC})
REFUSALS = ("BAD_PROOF", "NOT_PERMITTED")


def error_name(code: int) -> str:
    """The 09 section 12.6 name of an error code."""
    return csf.MB_ERRORS[code] if code < len(csf.MB_ERRORS) else str(code)


def server_key(url: str) -> str:
    """A short stable name for a mailbox URL, for device and issue ids."""
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def url_host(url: str) -> str:
    """The host a greeting must be signed for (09 section 12.3): lowercase, A-label,
    without a port."""
    host = urlsplit(url).hostname or ""
    return host.encode("idna").decode("ascii").lower()


def build_ssl_context(certificate: str | None) -> ssl.SSLContext:
    """Home Assistant's client context, with TLS 1.3 (09 section 12.1) and the user's
    pinned certificate trusted as well, if any. Loads files: run it in an executor."""
    context = create_client_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    if certificate:
        context.load_verify_locations(cadata=certificate)
    return context


class MailboxRefused(Exception):
    """The server refused the handshake, with its 09 section 12.6 error name."""

    def __init__(self, code: str) -> None:
        super().__init__(f"refused: {code}")
        self.code = code


@dataclass(slots=True)
class FedReception:
    """One reception of a delivered frame: a federated path (07 section 4.4)."""

    server: MailboxServer
    publisher: bytes
    rx_ref: bytes
    rssi: float
    snr: float
    age_ms: int
    flags: int
    rtt_pub: int
    arrived_ms: int

    @property
    def path(self) -> str:
        """A federated path is one publisher pseudonym on one server (09 section 9)."""
        return f"{self.server.key}:{self.publisher.hex()}"


@dataclass(slots=True)
class Heard:
    """One of this home's proxies' copies of a frame it will publish."""

    proxy: Proxy
    boot_id: int
    rx_time_us: int
    rssi: float
    snr: float
    flags: int
    at_ms: int


@dataclass(slots=True)
class Publishing:
    """A foreign transmission being collected, then published (09 section 6.1)."""

    frame: bytes
    ftype: int
    first_ms: int
    heard: list[Heard] = field(default_factory=list)
    published: bool = False
    # One answer per transmission, through any server (09 section 7.4, check 4).
    answered: bool = False


@dataclass(slots=True)
class Issued:
    """An rx_ref this home published, remembered 5 seconds (09 section 6.2)."""

    heard: Heard
    txn: Publishing
    downlink: bool
    at_ms: int
    used: bool = False


@dataclass(slots=True)
class OwnerRequest:
    """The one federated request of a transmission, across all servers (09 7.3)."""

    tx: Transmission
    downlink: bytes
    carries: bool
    tried: set[tuple[str, bytes]] = field(default_factory=set)
    current: FedReception | None = None
    seq: int = 0
    sent_ms: int = 0
    result: str | None = None
    soft_timer: asyncio.TimerHandle | None = None


@dataclass(slots=True)
class Exchange:
    """A federated answer awaiting the sensor's next uplink, for 09 section 9."""

    reply_to: int
    path: str
    reporters: set[str]
    answers: int = 1
    sent: bool = False


class PublisherRecord:
    """Credits and failures of one publisher on one server (09 section 9)."""

    def __init__(
        self, credits: list[float] | None = None, failures: list[list[float]] | None = None
    ):
        self.credits = credits or []
        self.failures = [(t, w) for t, w in failures or []]

    def scores(self, now: float) -> tuple[float, float]:
        """C and F, each decayed with a half-life of 7 days."""
        c = sum(2 ** (-(now - t) / RECORD_HALF_LIFE_S) for t in self.credits)
        f = sum(w * 2 ** (-(now - t) / RECORD_HALF_LIFE_S) for t, w in self.failures)
        return c, f

    def prune(self, now: float) -> None:
        """Forget what has decayed to almost nothing."""
        keep = RECORD_HALF_LIFE_S * math.log2(1 / RECORD_FLOOR)
        self.credits = [t for t in self.credits if now - t < keep]
        self.failures = [(t, w) for t, w in self.failures if now - t < keep]

    def rank(self, now: float) -> tuple[int, float]:
        """Class (0 proven, 1 unknown, 2 failed) and, for failed, the score to sort by."""
        c, f = self.scores(now)
        if c >= 1 and f < c / 2:
            return 0, 0.0
        if f < 0.5:
            return 1, 0.0
        return 2, -(c - f)

    def to_store(self) -> dict[str, Any]:
        return {"c": self.credits, "f": [list(x) for x in self.failures]}


class MailboxLink:
    """One authenticated connection to a mailbox server: the handshake of 09 section
    12.3, signed messages with seq (12.4), and replies to ping_s."""

    def __init__(self, url: str, seed: bytes) -> None:
        parts = urlsplit(url)
        if parts.scheme not in ("wss", "ws") or not parts.hostname:
            raise ValueError("expected wss://host[:port]/...")
        if parts.scheme == "ws" and not ALLOW_INSECURE:
            raise ValueError("plain ws:// is not allowed: 09 section 12.1 requires TLS")
        self.url = url
        self.secure = parts.scheme == "wss"
        self.host = url_host(url)
        self._seed = seed
        self.pub = cs.ed25519_pub(seed)
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.server_id = b""
        self.generation = 0
        self.nonce_s = b""
        self.pseudonym = b""
        self.seq = 0
        self.rtt_ms: int | None = None
        self._pings: dict[int, int] = {}
        self._clock: Callable[[], int] = lambda: 0

    async def connect(
        self,
        session: aiohttp.ClientSession,
        context: ssl.SSLContext | None,
        clock: Callable[[], int],
        verify: Callable[[bytes, bytes, bytes], Coroutine[Any, Any, str | None]],
    ) -> None:
        """Open the WebSocket and run the handshake. Raises on any failure."""
        self._clock = clock
        async with asyncio.timeout(CONNECT_TIMEOUT_S):
            self.ws = await session.ws_connect(
                self.url,
                protocols=(SUBPROTOCOL,),
                ssl=(context or True) if self.secure else False,
                max_msg_size=csf.MB_MAX_MESSAGE,
                autoping=True,
                heartbeat=None,
                compress=0,
            )
            if self.ws.protocol != SUBPROTOCOL:
                raise ConnectionError("the server did not accept the cs-mailbox.1 subprotocol")
            nonce_c = secrets.token_bytes(16)
            await self.ws.send_bytes(csf.cbor([csf.MB_HELLO, csf.MB_VERSION, nonce_c]))
            g = csf.mb_decode(await self._recv_bytes())
            if g[0] == csf.MB_ERROR:
                raise MailboxRefused(error_name(g[2]))
            if g[0] != csf.MB_GREETING:
                raise ConnectionError("expected a greeting")
            _, version, server_pub, server_id, generation, nonce_s, sig = g
            msg = csf.greeting_msg(self.host, nonce_c, nonce_s, generation)
            if (
                version != csf.MB_VERSION
                or server_id != csf.server_id_of(server_pub)
                or await verify(server_pub, msg, sig)
            ):
                raise ConnectionError("the greeting failed its checks")
            self.server_id, self.generation, self.nonce_s = server_id, generation, nonce_s
            sig = csf.ed_sign(self._seed, csf.auth_msg(server_id, nonce_c, nonce_s, self.pub))
            await self.ws.send_bytes(csf.cbor([csf.MB_AUTH, self.pub, sig]))
            w = csf.mb_decode(await self._recv_bytes())
            if w[0] == csf.MB_ERROR:
                raise MailboxRefused(error_name(w[2]))
            if w[0] != csf.MB_WELCOME:
                raise ConnectionError("expected welcome")
            self.pseudonym = w[1]
            self.seq = 0

    async def _recv_bytes(self) -> bytes:
        assert self.ws is not None
        msg = await self.ws.receive()
        if msg.type == aiohttp.WSMsgType.BINARY:
            data: bytes = msg.data
            return data
        if msg.type == aiohttp.WSMsgType.TEXT:
            raise ConnectionError("a text message (09 section 12.1)")
        code = self.ws.close_code
        raise ConnectionError(f"closed ({code})" if code else "closed")

    async def send(self, m: list[Any]) -> int:
        """Sign and send m (its type and fields, without seq or sig); returns its seq."""
        assert self.ws is not None
        self.seq += 1
        await self.ws.send_bytes(
            csf.signed(self._seed, self.server_id, self.nonce_s, [m[0], self.seq, *m[1:]])
        )
        return self.seq

    async def ping(self) -> None:
        """Measure rtt_owner (09 section 12.5)."""
        self._pings[self.seq + 1] = self._clock()
        await self.send([csf.MB_PING])

    async def recv(self) -> list[Any]:
        """The next server message other than ping_s and pong_s, which are handled
        here. Messages of unknown type are skipped (09 section 12.5)."""
        while True:
            data = await self._recv_bytes()
            try:
                m = csf.mb_decode(data)
            except ValueError:
                try:
                    v = csf.uncbor(data)
                except ValueError:
                    v = None
                if isinstance(v, list) and v and type(v[0]) is int and v[0] not in csf.MB_SCHEMA:
                    continue
                raise ConnectionError("a malformed message") from None
            if m[0] == csf.MB_PING_S:
                await self.send([csf.MB_PONG, m[1]])
            elif m[0] == csf.MB_PONG_S:
                sent = self._pings.pop(m[1], None)
                if sent is not None:
                    self.rtt_ms = max(0, self._clock() - sent)
            else:
                return m

    async def close(self) -> None:
        if self.ws is not None and not self.ws.closed:
            await self.ws.close()


class MailboxServer:
    """This home on one mailbox server: owner and publisher on one key."""

    def __init__(self, url: str, seed: bytes, down_since: datetime) -> None:
        self.url = url
        self.key = server_key(url)
        self.host = url_host(url)
        self.seed = seed
        self.link: MailboxLink | None = None
        self.up = False
        self.down_since: datetime | None = down_since
        self.last_error: str | None = None
        self.refused: str | None = None
        self.connects = 0
        # tag -> (device, key fingerprint, expires)
        self.registered: dict[bytes, tuple[str, bytes, int]] = {}
        self.pending: dict[int, tuple[str, Any]] = {}
        self.issued: dict[bytes, Issued] = {}
        self.published: deque[int] = deque()
        self.quota: dict[bytes, tuple[float, int]] = {}  # requester -> (airtime ms, at ms)
        self.deliveries = 0
        self.publishes = 0
        self.forwards = 0

    @property
    def rtt_ms(self) -> int | None:
        return self.link.rtt_ms if self.link is not None and self.up else None

    def diagnostics(self) -> dict[str, Any]:
        link = self.link
        return {
            "url": self.url,
            "connected": self.up,
            "down_since": self.down_since.isoformat() if self.down_since else None,
            "last_error": self.last_error,
            "refused": self.refused,
            "connects": self.connects,
            "server_id": link.server_id.hex() if link and link.server_id else None,
            "generation": link.generation if link else None,
            "pseudonym": link.pseudonym.hex() if link and link.pseudonym else None,
            "rtt_ms": self.rtt_ms,
            "registered_tags": len(self.registered),
            "deliveries": self.deliveries,
            "publishes": self.publishes,
            "forwards": self.forwards,
        }


class Federation:
    """The three opt-ins of 09 for one network server."""

    def __init__(self, hass: HomeAssistant, server: NetworkServer, options: dict[str, Any]) -> None:
        self.hass = hass
        self.server = server
        self.owner: bool = bool(options.get(CONF_FEDERATION_OWNER))
        self.publish: bool = bool(options.get(CONF_FEDERATION_PUBLISH))
        self.downlinks: bool = bool(options.get(CONF_FEDERATION_DOWNLINKS))
        self.urls: list[str] = list(dict.fromkeys(options.get(CONF_MAILBOX_SERVERS) or []))
        self.certificate: str | None = options.get(CONF_MAILBOX_CERTIFICATE) or None
        self.servers: dict[str, MailboxServer] = {}
        self._store: Store[dict[str, Any]] = Store(
            hass, STORAGE_VERSION, STORAGE_KEY, atomic_writes=True
        )
        self._stored: dict[str, Any] = {}
        self._context: ssl.SSLContext | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._timers: set[asyncio.TimerHandle] = set()
        self._stopped = False
        self._refresh_pending = False
        # Owner: tag -> devices, for every tag this home keeps registered; and the
        # Ed25519 work behind them, by (k_dev_tag, index).
        self.tag_devices: dict[bytes, list[str]] = {}
        self._tag_cache: dict[tuple[bytes, bytes], tuple[bytes, bytes]] = {}
        self._fed_bootstrap: deque[int] = deque()
        self._requests: dict[int, OwnerRequest] = {}  # id(tx) -> request
        self._last_publisher: dict[str, str] = {}  # device -> path of its last request
        self._exchanges: dict[str, Exchange] = {}
        self.records: dict[str, PublisherRecord] = {}
        # Publisher.
        self._collecting: dict[bytes, Publishing] = {}
        self._recent: dict[bytes, Publishing] = {}
        self._fed_downlinks: deque[int] = deque()
        self._fed_airtime: dict[str, list[tuple[int, float]]] = {}  # proxy -> (at ms, airtime)

    @property
    def enabled(self) -> bool:
        """Federation does anything at all only with an opt-in and a server."""
        return bool(self.urls) and (self.owner or self.publish or self.downlinks)

    # ------------------------------------------------------------------ lifecycle

    async def async_start(self) -> None:
        """Load the owner keys and connect to every server."""
        self._stored = await self._store.async_load() or {}
        keys: dict[str, dict[str, Any]] = self._stored.setdefault("servers", {})
        # A key is kept for as long as the home uses its server (09 section 2.3).
        for url in [u for u in keys if u not in self.urls]:
            del keys[url]
        for url in self.urls:
            k = keys.setdefault(url, {})
            if "client_seed" not in k:
                k["client_seed"] = secrets.token_bytes(32).hex()
                _LOGGER.info("Generated an owner key for mailbox %s", url)
        now = self.server.unix()
        self.records = {
            path: PublisherRecord(r.get("c"), r.get("f"))
            for path, r in self._stored.get("records", {}).items()
        }
        for record in self.records.values():
            record.prune(now)
        await self._store.async_save(self._store_data())
        self._context = await self.hass.async_add_executor_job(build_ssl_context, self.certificate)
        started = self.server.utcnow()
        for url in self.urls:
            self.servers[url] = MailboxServer(url, bytes.fromhex(keys[url]["client_seed"]), started)
        for s in self.servers.values():
            self._spawn(self._keep(s), f"{DOMAIN} mailbox {s.host}")
        self._spawn(self._refresher(), f"{DOMAIN} mailbox refresh")

    async def async_stop(self) -> None:
        """Close every connection and keep the records."""
        self._stopped = True
        for timer in self._timers:
            timer.cancel()
        self._timers.clear()
        for request in self._requests.values():
            if request.soft_timer is not None:
                request.soft_timer.cancel()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        for s in self.servers.values():
            if s.link is not None:
                await s.link.close()
            s.up = False
        await self._store.async_save(self._store_data())

    def _store_data(self) -> dict[str, Any]:
        return {
            "servers": self._stored.get("servers", {}),
            "records": {
                p: r.to_store() for p, r in self.records.items() if r.credits or r.failures
            },
        }

    def _spawn(self, coro: Coroutine[Any, Any, Any], name: str) -> None:
        if self._stopped:
            coro.close()
            return
        task = self.hass.async_create_background_task(coro, name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _later(self, delay_s: float, fn: Callable[[], None]) -> asyncio.TimerHandle:
        def run() -> None:
            self._timers.discard(handle)
            fn()

        handle = self.hass.loop.call_later(delay_s, run)
        self._timers.add(handle)
        return handle

    def _signal(self) -> None:
        async_dispatcher_send(self.hass, signal_mailbox_update(self.server.entry.entry_id))

    async def _verify(self, pub: bytes, msg: bytes, sig: bytes) -> str | None:
        """Strict Ed25519 (09 section 5.2); pure Python, so off the event loop."""
        return await self.hass.async_add_executor_job(csf.ed_verify_error, pub, msg, sig)

    async def _keep(self, s: MailboxServer) -> None:
        """Stay connected, reconnecting with backoff."""
        try:
            MailboxLink(s.url, s.seed)
        except ValueError as err:
            s.last_error = str(err)
            _LOGGER.error("Mailbox %s cannot be used: %s", s.url, err)
            return
        backoff = BACKOFF_MIN_S
        session = async_get_clientsession(self.hass)
        while True:
            try:
                s.link = MailboxLink(s.url, s.seed)
                await s.link.connect(session, self._context, self.server.clock, self._verify)
                backoff = BACKOFF_MIN_S
                await self._connected(s)
                pinger = self.hass.async_create_background_task(
                    self._pinger(s), f"{DOMAIN} mailbox ping {s.host}"
                )
                try:
                    while True:
                        await self._on_message(s, await s.link.recv())
                finally:
                    pinger.cancel()
            except asyncio.CancelledError:
                raise
            except MailboxRefused as err:
                s.last_error = err.code
                if err.code == "NOT_PERMITTED":
                    self._set_refused(s, err.code)
                self._disconnected(s, f"refused the connection: {err.code}")
            except Exception as err:  # any failure: reconnect
                s.last_error = f"{type(err).__name__}: {err}"
                self._disconnected(s, s.last_error)
            finally:
                if s.link is not None:
                    await s.link.close()
            await asyncio.sleep(backoff * random.uniform(0.8, 1.2))
            backoff = min(BACKOFF_MAX_S, backoff * 2)

    def _disconnected(self, s: MailboxServer, why: str) -> None:
        if s.up:
            _LOGGER.info("Mailbox %s disconnected: %s", s.url, why)
        elif s.connects == 0 and s.last_error != why:
            _LOGGER.debug("Mailbox %s: %s", s.url, why)
        s.up = False
        if s.down_since is None:
            s.down_since = self.server.utcnow()
        s.issued.clear()
        s.pending.clear()
        self._signal()

    async def _connected(self, s: MailboxServer) -> None:
        link = s.link
        assert link is not None
        k = self._stored["servers"][s.url]
        if k.get("server_id") not in (None, link.server_id.hex()):
            _LOGGER.warning("Mailbox %s has a new server key: treating it as a new server", s.url)
        elif k.get("generation") not in (None, link.generation):
            _LOGGER.info("Mailbox %s lost its registrations: registering again", s.url)
        if (k.get("server_id"), k.get("generation")) != (link.server_id.hex(), link.generation):
            k["server_id"], k["generation"] = link.server_id.hex(), link.generation
            await self._store.async_save(self._store_data())
        s.up = True
        s.connects += 1
        s.down_since = None
        s.last_error = None
        if s.refused == "NOT_PERMITTED":
            self._set_refused(s, None)
        _LOGGER.info("Mailbox %s connected", s.url)
        # Every live tag again on each connection: cheap, and it covers a new
        # generation or server key (09 sections 5.3 and 12.3).
        s.registered.clear()
        await link.ping()
        if self.owner:
            await self._register(s)
        self._signal()

    async def _pinger(self, s: MailboxServer) -> None:
        while True:
            await asyncio.sleep(PING_INTERVAL_S)
            if s.link is not None and s.up:
                await s.link.ping()

    async def _refresher(self) -> None:
        """Top up registrations each epoch and as SYNC windows move (09 5.3, 5.5)."""
        while True:
            await asyncio.sleep(REFRESH_INTERVAL_S)
            await self._refresh()

    async def _refresh(self) -> None:
        self._refresh_pending = False
        if not self.owner:
            return
        for s in self.servers.values():
            if s.up:
                try:
                    await self._register(s)
                except (ConnectionError, aiohttp.ClientError, RuntimeError) as err:
                    _LOGGER.debug("Mailbox %s: registering failed: %s", s.url, err)

    @callback
    def devices_changed(self) -> None:
        """A device's keys or SYNC block changed: register at once (09 section 5.3,
        fresh keys), and withdraw the tags of keys no longer in use."""
        if self.owner and not self._refresh_pending and not self._stopped:
            self._refresh_pending = True
            self._spawn(self._refresh(), f"{DOMAIN} mailbox register")

    @callback
    def tick(self, now: datetime) -> None:
        """Repairs for servers unreachable for an hour."""
        for s in self.servers.values():
            down = s.down_since
            outage = down is not None and now - down >= timedelta(
                hours=MAILBOX_UNREACHABLE_REPAIR_H
            )
            self.server.set_issue(
                "mailbox_unreachable",
                s.key,
                {"url": s.url, "error": s.last_error or "-"} if outage else None,
            )

    def _set_refused(self, s: MailboxServer, code: str | None) -> None:
        s.refused = code
        self.server.set_issue(
            "mailbox_refused", s.key, {"url": s.url, "code": code} if code else None
        )

    # ------------------------------------------------------------------ owner: registration

    def _tag(self, k_dev_tag: bytes, index: bytes) -> tuple[bytes, bytes]:
        """(seed, tag) for one index, cached: each is an Ed25519 key generation."""
        key = (k_dev_tag, index)
        hit = self._tag_cache.get(key)
        if hit is None:
            seed = cs.tag_seed(k_dev_tag, index)
            hit = self._tag_cache[key] = (seed, cs.tag_of_pub(cs.ed25519_pub(seed)))
        return hit

    def wanted(self, now: int) -> dict[bytes, tuple[bytes, int, str, bytes]]:
        """tag -> (seed, expires, device, key fingerprint) for every tag this home keeps
        registered: normal tags of epochs e-1 to e+2 and each device's SYNC window
        (09 sections 5.3 and 5.5). JOIN tags never (09 section 8)."""
        out: dict[bytes, tuple[bytes, int, str, bytes]] = {}
        used: set[tuple[bytes, bytes]] = set()
        e = cs.epoch_of(now)
        for eui, k_dev_tag, last_fcnt, silence, heartbeat in self.server.federated_devices():
            fp = hashlib.sha256(k_dev_tag).digest()[:8]
            for x in range(e - 1, e + 3):
                index = cs.epoch_index(x)
                used.add((k_dev_tag, index))
                seed, tag = self._tag(k_dev_tag, index)
                # No later than the end of epoch x + 1 (09 section 5.3).
                out[tag] = (seed, cs.EPOCH_SECONDS * (x + 2), eui, fp)
            first, last = csf.sync_window(max(last_fcnt, 1), silence, heartbeat)
            for n in range(first, last + 1):
                index = cs.block_index(n * cs.TAG_BLOCK + 1)
                used.add((k_dev_tag, index))
                seed, tag = self._tag(k_dev_tag, index)
                out[tag] = (seed, now + SYNC_REGISTRATION_S, eui, fp)
        self._tag_cache = {k: v for k, v in self._tag_cache.items() if k in used}
        devices: dict[bytes, list[str]] = {}
        for tag, (_, _, eui, _) in out.items():
            devices.setdefault(tag, []).append(eui)
        self.tag_devices = devices
        return out

    async def _register(self, s: MailboxServer) -> None:
        link = s.link
        assert link is not None
        now = self.server.unix()
        want = self.wanted(now)
        for t in [t for t, (_, _, exp) in s.registered.items() if exp <= now]:
            del s.registered[t]
        fingerprints = {eui: fp for _, _, eui, fp in want.values()}
        stale = [
            t
            for t, (eui, fp, _) in s.registered.items()
            if t not in want and fingerprints.get(eui) != fp
        ]
        if stale:  # a removed or re-paired device (09 section 5.3)
            for i in range(0, len(stale), WITHDRAW_CHUNK):
                await link.send([csf.MB_WITHDRAW, stale[i : i + WITHDRAW_CHUNK]])
            for t in stale:
                del s.registered[t]
            _LOGGER.debug("Mailbox %s: withdrew %s tags", s.url, len(stale))
        todo = [
            t
            for t, (_, exp, _, _) in want.items()
            if t not in s.registered
            or (s.registered[t][2] - now < RENEW_BEFORE_S and exp > s.registered[t][2])
        ]
        for i in range(0, len(todo), REGISTER_CHUNK):
            chunk = todo[i : i + REGISTER_CHUNK]
            entries = [
                csf.reg_entry(want[t][0], link.server_id, link.pub, want[t][1]) for t in chunk
            ]
            seq = await link.send([csf.MB_REGISTER, entries])
            s.pending[seq] = ("register", chunk)
            for t in chunk:
                _, exp, eui, fp = want[t]
                s.registered[t] = (eui, fp, exp)

    def lookup(self, tag: bytes) -> list[str]:
        """Devices that registered this tag."""
        return list(self.tag_devices.get(tag, ()))

    # ------------------------------------------------------------------ messages

    async def _on_message(self, s: MailboxServer, m: list[Any]) -> None:
        kind = m[0]
        if kind == csf.MB_REGISTERED:
            self._on_registered(s, m[1], m[2])
        elif kind == csf.MB_DELIVER:
            if self.owner:
                self._on_deliver(s, m)
        elif kind == csf.MB_FORWARD:
            await self._on_forward(s, m[1], m[2], m[3])
        elif kind == csf.MB_RESULT_S:
            await self._on_result(s, m[1], m[2], m[3])
        elif kind == csf.MB_ERROR:
            await self._on_error(s, m[1], error_name(m[2]))
        # Other types (handshake messages after welcome) are ignored.

    def _on_registered(self, s: MailboxServer, seq: int, statuses: list[int]) -> None:
        what = s.pending.pop(seq, None)
        if what is None or what[0] != "register":
            return
        names = [error_name(c) for c in statuses]
        refused = None
        for tag, name in zip(what[1], names, strict=False):
            if name != "OK":
                s.registered.pop(tag, None)
                if name in REFUSALS:
                    refused = name
        if refused is not None:
            _LOGGER.warning("Mailbox %s refused registrations: %s", s.url, refused)
            self._set_refused(s, refused)
        elif s.refused == "BAD_PROOF":
            self._set_refused(s, None)
        if any(n not in ("OK", *REFUSALS) for n in names):
            _LOGGER.debug(
                "Mailbox %s: registered %s",
                s.url,
                ", ".join(f"{n} x{names.count(n)}" for n in sorted(set(names))),
            )

    async def _on_error(self, s: MailboxServer, seq: int, name: str) -> None:
        what = s.pending.pop(seq, None)
        _LOGGER.debug("Mailbox %s: error %s for seq %s", s.url, name, seq)
        if name == "NOT_PERMITTED":
            self._set_refused(s, name)
        if what is None:
            return
        if what[0] == "register":
            for tag in what[1]:
                s.registered.pop(tag, None)
        elif what[0] == "request":
            await self._request_failed(what[1], name)

    # ------------------------------------------------------------------ owner: deliveries

    def _on_deliver(self, s: MailboxServer, m: list[Any]) -> None:
        _, frame, receptions, publisher, rtt_pub = m
        arrived = self.server.clock()
        s.deliveries += 1
        h = cs.parse_header(frame)
        if h is None or h.ftype == cs.T_JOIN or cs.is_downlink(h.ftype):
            _LOGGER.debug("Mailbox %s: dropped a delivered JOIN or invalid frame", s.url)
            return
        if h.ftype == cs.T_SYNC:
            # Separate from the local limiter (07 section 4.4).
            while self._fed_bootstrap and arrived - self._fed_bootstrap[0] > MINUTE_MS:
                self._fed_bootstrap.popleft()
            if len(self._fed_bootstrap) >= FED_BOOTSTRAP_PER_MINUTE:
                _LOGGER.debug("Mailbox %s: federated SYNC over the limit, dropped", s.url)
                return
            self._fed_bootstrap.append(arrived)
        fed = [
            FedReception(s, publisher, r[0], r[1] / 10, r[2] / 10, r[3], r[4], rtt_pub, arrived)
            for r in receptions
        ]
        self.server.federated_uplink(frame, h, fed, arrived)

    # ------------------------------------------------------------------ owner: requests

    async def request(self, tx: Transmission, downlink: bytes, carries: bool) -> bool:
        """Ask for an RX2 answer through one federated reception (09 section 7).
        Returns whether a request went out."""
        if not self.owner or id(tx) in self._requests:
            return False
        req = OwnerRequest(tx, downlink, carries)
        self._requests[id(tx)] = req
        if await self._choose(req):
            return True
        self._requests.pop(id(tx), None)
        return False

    def _floor_snr(self) -> float:
        _, sf, _ = REGIONS[self.server.region]
        return -2.5 * (sf - 4)

    def _ranked(self, req: OwnerRequest) -> list[FedReception]:
        """Receptions that qualify, in the order of 09 sections 9 and 10."""
        now = self.server.clock()
        unix = self.server.unix()
        ok = [
            r
            for r in req.tx.fed
            if r.flags & csf.RXF_DOWNLINK
            and r.server.up
            and (r.server.key, r.rx_ref) not in req.tried
            and csf.federated_request_ok(r.age_ms, r.rtt_pub, r.server.rtt_ms, now - r.arrived_ms)
        ]
        floor = self._floor_snr()
        last = self._last_publisher.get(req.tx.device.eui)
        rotate = len({r.path for r in ok}) > 1
        shuffle = {r.path: random.random() for r in ok}

        def key(r: FedReception) -> tuple[bool, int, float, int]:
            record = self.records.get(r.path)
            cls, failed = record.rank(unix) if record else (1, 0.0)
            snr = r.snr
            if snr < floor - 5 or snr > 20 or r.rssi > -20:
                snr = floor  # an implausible signal ranks as the floor
            order = -snr if cls == 0 else shuffle[r.path] if cls == 1 else failed
            rtt = (r.server.rtt_ms or 0) + r.rtt_pub
            return (rotate and r.path == last, cls, order, rtt)

        return sorted(ok, key=key)

    async def _choose(self, req: OwnerRequest) -> bool:
        ranked = self._ranked(req)
        if not ranked:
            _LOGGER.debug(
                "%s: no federated reception of frame %s can be answered in RX2",
                req.tx.device.name,
                req.tx.header.fcnt,
            )
            return False
        r = ranked[0]
        link = r.server.link
        assert link is not None
        req.current = r
        req.result = None
        req.tried.add((r.server.key, r.rx_ref))
        req.sent_ms = self.server.clock()
        self._last_publisher[req.tx.device.eui] = r.path
        try:
            req.seq = await link.send([csf.MB_REQUEST, r.rx_ref, req.downlink])
        except ConnectionError, aiohttp.ClientError, RuntimeError:
            return await self._choose(req)
        r.server.pending[req.seq] = ("request", req)
        _LOGGER.debug(
            "%s: requested an RX2 answer to frame %s through mailbox %s (age %s ms, "
            "rtt_pub %s ms, rtt_owner %s ms)",
            req.tx.device.name,
            req.tx.header.fcnt,
            r.server.host,
            r.age_ms,
            r.rtt_pub,
            r.server.rtt_ms,
        )
        self.server.federated_request_sent(req.tx)
        rtt = r.server.rtt_ms or 0
        tx_id = id(req.tx)

        def expire() -> None:
            # Not re-requested before the server's own timeout (09 section 7.5).
            if self._requests.get(tx_id) is req and req.current is r:
                self._requests.pop(tx_id, None)

        self._later((csf.FORWARD_TIMEOUT_MS + rtt) / 1000, expire)
        if req.soft_timer is not None:
            req.soft_timer.cancel()
        req.soft_timer = self._later(SOFT_FAILURE_S, lambda: self._soft_timeout(req, r))
        return True

    def _soft_timeout(self, req: OwnerRequest, r: FedReception) -> None:
        if req.current is r and req.result is None:
            self._failure(r.path, 0.5)

    async def _request_failed(self, req: OwnerRequest, name: str) -> None:
        """The server refused a request: try another reception if the timing allows,
        except after ALREADY_ANSWERED (09 section 7.5)."""
        if self._requests.get(id(req.tx)) is not req or req.current is None:
            return
        _LOGGER.debug("%s: federated request refused: %s", req.tx.device.name, name)
        if req.soft_timer is not None:
            req.soft_timer.cancel()
        req.current = None
        if name == "ALREADY_ANSWERED" or not await self._choose(req):
            self._requests.pop(id(req.tx), None)

    async def _on_result(self, s: MailboxServer, rx_ref: bytes, code: int, offset_us: int) -> None:
        req = next(
            (
                q
                for q in self._requests.values()
                if q.current is not None and q.current.server is s and q.current.rx_ref == rx_ref
            ),
            None,
        )
        name = TX_RESULTS.get(code, str(code))
        if req is None or req.current is None:
            return
        r = req.current
        req.result = name
        if req.soft_timer is not None:
            req.soft_timer.cancel()
        _LOGGER.debug(
            "%s: federated answer to frame %s: %s, %.1f ms after target",
            req.tx.device.name,
            req.tx.header.fcnt,
            name,
            offset_us / 1000,
        )
        if name == "sent":
            self._answered(req.tx, r.path, req.carries, sent=True)
            return
        self._failure(r.path, 0.5)
        # A non-SENT result releases the transmission (09 section 7.5).
        req.current = None
        if not await self._choose(req):
            self._requests.pop(id(req.tx), None)

    # ------------------------------------------------------------------ owner: records

    def _failure(self, path: str, weight: float) -> None:
        self.records.setdefault(path, PublisherRecord()).failures.append(
            (float(self.server.unix()), weight)
        )

    def _answered(self, tx: Transmission, path: str | None, carries: bool, sent: bool) -> None:
        """A downlink answered a transmission, through a publisher or locally."""
        eui, fcnt = tx.device.eui, tx.header.fcnt
        reporters = {r.path for r in tx.fed}
        ex = self._exchanges.get(eui)
        if ex is not None and ex.reply_to == fcnt:
            # Phantom retries: a retransmission the original's publisher did not
            # report is ignored for credit and failure (09 section 9).
            if tx.receptions or ex.reporters & reporters:
                ex.answers += 1
            return
        if path is None or not carries:
            self._exchanges.pop(eui, None)
            return
        self._exchanges[eui] = Exchange(fcnt, path, reporters, sent=sent)

    @callback
    def answered_locally(self, tx: Transmission) -> None:
        """A downlink of our own answered a transmission (counts for 09 section 9)."""
        if self._exchanges.get(tx.device.eui) is not None:
            self._answered(tx, None, False, sent=True)

    @callback
    def uplink(self, device: Device, fcnt: int, counter: str, dl_ack: bool) -> None:
        """Settle a federated exchange on the sensor's next new uplink (09 section 9)."""
        ex = self._exchanges.get(device.eui)
        if ex is None or counter != "accept" or fcnt <= ex.reply_to:
            return
        del self._exchanges[device.eui]
        if ex.answers != 1 or not ex.sent:
            return
        if dl_ack:
            self.records.setdefault(ex.path, PublisherRecord()).credits.append(
                float(self.server.unix())
            )
        elif fcnt == ex.reply_to + 1:
            self._failure(ex.path, 1.0)

    # ------------------------------------------------------------------ publisher

    @callback
    def heard(self, heard: Heard, frame: bytes, header: cs.ParsedHeader) -> None:
        """A frame from our proxies that no device of ours verified (09 section 6.1).
        The caller has checked the proxy's radio profile."""
        if not self.publish or header.ftype == cs.T_JOIN or cs.is_downlink(header.ftype):
            return
        key = csf.frame_key(frame)
        now = heard.at_ms
        txn = self._collecting.get(key)
        if txn is not None and now - txn.first_ms <= cs.MERGE_WINDOW_MS:
            txn.heard.append(heard)
            return
        prev = self._recent.get(key)
        if prev is not None and now - prev.first_ms <= cs.MERGE_WINDOW_MS:
            return  # a late copy of a transmission already published
        txn = Publishing(frame, header.ftype, now, [heard])
        self._collecting[key] = txn
        self._later(
            COLLECTION_WINDOW_MS / 1000,
            lambda: self._spawn(self._flush(key, txn), f"{DOMAIN} mailbox publish"),
        )

    def _candidate(self, heard: Heard) -> bool:
        """A proxy that may transmit a federated answer (07 section 6.1)."""
        proxy = heard.proxy
        return (
            self.server.proxy_current(proxy)
            and proxy.can_downlink
            and (proxy.link.boot_id == heard.boot_id)
        )

    async def _flush(self, key: bytes, txn: Publishing) -> None:
        """Publish one transmission to every server, at most once (09 section 6.1)."""
        if self._collecting.get(key) is txn:
            del self._collecting[key]
        now = self.server.clock()
        self._recent = {k: v for k, v in self._recent.items() if now - v.first_ms < MINUTE_MS}
        self._recent[key] = txn
        heard = sorted(txn.heard, key=lambda r: -r.snr)[:MAX_RECEPTIONS]
        for s in self.servers.values():
            if not s.up or s.link is None:
                continue
            while s.published and now - s.published[0] >= 1000:
                s.published.popleft()
            if len(s.published) >= PUBLISH_PER_SECOND:
                continue  # the newest beyond 5 a second is dropped
            s.published.append(now)
            s.issued = {r: v for r, v in s.issued.items() if now - v.at_ms < csf.RX_REF_MS}
            receptions = []
            for r in heard:
                rx_ref = secrets.token_bytes(8)  # fresh per reception and server (6.2)
                dl = self.downlinks and txn.ftype in CONFIRMED and self._candidate(r)
                s.issued[rx_ref] = Issued(r, txn, dl, now)
                flags = (csf.RXF_TS_UNCERTAIN if r.flags & cs.UPF_TS_UNCERTAIN else 0) | (
                    csf.RXF_DOWNLINK if dl else 0
                )
                receptions.append(
                    [
                        rx_ref,
                        max(-32768, min(32767, round(r.rssi * 10))),
                        max(-32768, min(32767, round(r.snr * 10))),
                        max(0, min(65535, now - r.at_ms)),
                        flags,
                    ]
                )
            try:
                await s.link.send([csf.MB_PUBLISH, txn.frame, receptions])
            except ConnectionError, aiohttp.ClientError, RuntimeError:
                continue
            s.publishes += 1
        txn.published = True

    # ------------------------------------------------------------------ federated downlinks

    async def _on_forward(
        self, s: MailboxServer, rx_ref: bytes, downlink: bytes, requester: bytes
    ) -> None:
        """09 section 7.4: the publisher's checks in order, then transmit."""
        now = self.server.clock()
        iss = s.issued.get(rx_ref)
        code, why, airtime = "", "", 0.0
        if iss is None or now - iss.at_ms > csf.RX_REF_MS or iss.used:
            code, why = "invalid", "rx_ref unknown, expired or used"
        elif not iss.downlink or not self.downlinks or not self._candidate(iss.heard):
            code, why = "not_capable", "no DOWNLINK on that reception"
        else:
            hd = cs.parse_header(downlink)
            if hd is None or not cs.is_downlink(hd.ftype) or downlink[2:10] != iss.txn.frame[2:10]:
                code, why = "invalid", "not a downlink for that frame"
            elif iss.txn.answered:
                code, why = "busy", "transmission already answered"
            else:
                airtime = cs.region_airtime_ms(self.server.region, len(downlink))
                why = self._over_budget(s, iss.heard.proxy, requester, airtime, now)
                if why:
                    code = "duty_cycle"
                elif self.server.own_downlink_overlaps(
                    iss.heard.proxy, iss.heard.rx_time_us + RX2_OFFSET_US, airtime
                ):
                    code, why = "busy", "own downlink on that proxy"
        if iss is not None:
            iss.used = True
        s.forwards += 1
        if code:
            _LOGGER.debug("Mailbox %s: refused a forwarded request: %s (%s)", s.url, code, why)
            await self._result(s, rx_ref, RESULT_CODE[code], 0)
            return
        assert iss is not None
        done = False

        def on_result(result: str, offset_us: int) -> None:
            nonlocal done
            if not done:
                done = True
                self._spawn(
                    self._result(s, rx_ref, RESULT_CODE.get(result, 6), offset_us),
                    f"{DOMAIN} mailbox result",
                )

        seq = self.server.transmit_for_neighbour(
            iss.heard.proxy, iss.heard.rx_time_us, downlink, airtime, on_result
        )
        if seq is None:
            await self._result(s, rx_ref, RESULT_CODE["not_capable"], 0)
            return
        iss.txn.answered = True
        self._fed_downlinks.append(now)
        self._fed_airtime.setdefault(iss.heard.proxy.subentry_id, []).append((now, airtime))
        amount, at = s.quota.get(requester, (0.0, now))
        s.quota[requester] = (amount * 0.5 ** ((now - at) / QUOTA_HALF_LIFE_MS) + airtime, now)
        # Exactly one result per forward (09 section 7.5), even if the proxy never
        # reports: by then the server's forward timeout has passed anyway.
        self._later(TX_RESULT_WAIT_S, lambda: on_result("radio_error", 0))
        _LOGGER.debug(
            "Mailbox %s: %s transmits a neighbour's RX2 answer", s.url, iss.heard.proxy.name
        )

    def _over_budget(
        self, s: MailboxServer, proxy: Proxy, requester: bytes, airtime: float, now: int
    ) -> str:
        """The federated airtime limits of 09 section 7.4, check 5; "" if within them."""
        hist = [x for x in self._fed_airtime.get(proxy.subentry_id, []) if now - x[0] < HOUR_MS]
        self._fed_airtime[proxy.subentry_id] = hist
        used = sum(a for _, a in hist)
        if used + airtime > FED_BUDGET_MS_PER_HOUR or not proxy.can_afford(airtime, now):
            return "proxy's federated budget"
        while self._fed_downlinks and now - self._fed_downlinks[0] >= MINUTE_MS:
            self._fed_downlinks.popleft()
        if len(self._fed_downlinks) >= FED_DOWNLINKS_PER_MINUTE:
            return f"{FED_DOWNLINKS_PER_MINUTE} federated downlinks a minute"
        if used > FED_BUDGET_MS_PER_HOUR / 2:
            active = {r for r, (_, at) in s.quota.items() if now - at < HOUR_MS} | {requester}
            amount, at = s.quota.get(requester, (0.0, now))
            if amount * 0.5 ** ((now - at) / QUOTA_HALF_LIFE_MS) > FED_BUDGET_MS_PER_HOUR / len(
                active
            ):
                return "requester's share"
        return ""

    async def _result(self, s: MailboxServer, rx_ref: bytes, code: int, offset_us: int) -> None:
        if s.up and s.link is not None:
            try:
                await s.link.send([csf.MB_RESULT, rx_ref, code, offset_us])
            except (ConnectionError, aiohttp.ClientError, RuntimeError) as err:
                _LOGGER.debug("Mailbox %s: result not sent: %s", s.url, err)

    # ------------------------------------------------------------------ diagnostics

    def diagnostics(self) -> dict[str, Any]:
        """Connection state; owner keys never appear here."""
        now = self.server.unix()
        return {
            "owner": self.owner,
            "publish": self.publish,
            "downlinks": self.downlinks,
            "pinned_certificate": self.certificate is not None,
            "servers": [s.diagnostics() for s in self.servers.values()],
            "registered_tags": len(self.tag_devices),
            "publishers": {
                path: dict(zip(("credits", "failures"), r.scores(now), strict=True))
                for path, r in self.records.items()
            },
        }
