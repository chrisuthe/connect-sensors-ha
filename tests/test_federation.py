"""Federation (spec 09) between two homes through the reference mailbox server.

Home A owns the sensor and pairs it through its own proxy (pairing is never federated).
Afterwards only home B's proxy hears the sensor: B publishes its frames, the mailbox
delivers them to A, and A's RX2 answers go out through B's proxy.

Both homes are config entries in one Home Assistant. The integration allows one entry
per instance through its flows only; B's storage keys are moved aside at setup.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
import ipaddress
import logging
from pathlib import Path
import socket
import subprocess
import sys
import time
from typing import Any
from unittest.mock import patch

import connectsensors as cs
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from homeassistant.const import CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import issue_registry as ir
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.components.diagnostics import (
    get_diagnostics_for_config_entry,
)

from custom_components.connect_sensors import federation as fed_module, server as server_module
from custom_components.connect_sensors.const import (
    CONF_DEV_EUI,
    CONF_FEDERATION_DOWNLINKS,
    CONF_FEDERATION_OWNER,
    CONF_FEDERATION_PUBLISH,
    CONF_IGNORE_FEDERATED_EVENTS,
    CONF_IGNORE_FEDERATED_READINGS,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_MAILBOX_CERTIFICATE,
    CONF_MAILBOX_SERVERS,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
)
from custom_components.connect_sensors.federation import Federation, MailboxServer
from custom_components.connect_sensors.server import NetworkServer

from .common import full_state, pair, parse_commands
from .conftest import DEV_EUI, K_ROOT, PROXY_ID, PROXY_KEY, assert_no_secrets
from .sim import SimProxy, SimSensor, downlink_body, free_udp_port

B_PROXY_KEY = "b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7"
B_PROXY_ID = bytes.fromhex("348518fffe0000b2")
DOOR = "binary_sensor.front_door_door"
OWNER = {CONF_FEDERATION_OWNER: True}
NEIGHBOUR = {CONF_FEDERATION_PUBLISH: True, CONF_FEDERATION_DOWNLINKS: True}
WAIT_S = 15


@pytest.fixture(autouse=True)
def insecure_dev() -> Generator[None]:
    """The reference server runs with --insecure-dev: plain ws:// on 127.0.0.1."""
    with patch.object(fed_module, "ALLOW_INSECURE", True):
        yield


def free_tcp_port() -> int:
    """A TCP port nothing listens on right now."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


class Mailbox:
    """cs-mailbox in a subprocess, as an operator runs it."""

    def __init__(self, tmp: Path, *extra: str, host: str = "127.0.0.1") -> None:
        self.port = free_tcp_port()
        self.host = host
        self.log = tmp / f"mailbox-{self.port}.log"
        self.key_file = tmp / f"mailbox-{self.port}.key"
        self.extra = extra
        self.proc: subprocess.Popen[bytes] | None = None
        scheme = "ws" if "--insecure-dev" in extra else "wss"
        self.url = f"{scheme}://{host}:{self.port}/"

    def start(self) -> None:
        """Start it and wait until it listens."""
        with self.log.open("ab") as out:
            self.proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "cs_mailbox",
                    "--host",
                    self.host,
                    "--listen",
                    "127.0.0.1",
                    "--port",
                    str(self.port),
                    "--key-file",
                    str(self.key_file),
                    *self.extra,
                ],
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with socket.socket() as s:
                if s.connect_ex(("127.0.0.1", self.port)) == 0:
                    return
            time.sleep(0.05)
        raise RuntimeError(f"cs-mailbox did not start: {self.text()}")

    def stop(self) -> None:
        """Stop it, as a crash would: its registrations are gone (it keeps nothing but
        its key). A graceful stop would wait for clients that cannot answer while the
        test blocks the event loop."""
        if self.proc is not None:
            self.proc.kill()
            self.proc.wait(10)
            self.proc = None

    def text(self) -> str:
        """Everything it logged."""
        return self.log.read_text() if self.log.exists() else ""


@pytest.fixture
def mailbox(tmp_path: Path) -> Iterator[Mailbox]:
    """The reference mailbox server, plain ws:// for tests only."""
    m = Mailbox(tmp_path, "--insecure-dev")
    m.start()
    yield m
    m.stop()


def home_entry(
    port: int, proxy_key: str, options: dict[str, Any], *, device: bool, title: str
) -> MockConfigEntry:
    """A network with one proxy and, for home A, the sensor."""
    subentries: list[dict[str, Any]] = [
        {
            "data": {CONF_KEY: proxy_key},
            "subentry_type": SUBENTRY_PROXY,
            "title": f"{title} proxy",
            "subentry_id": f"proxy_{title}",
            "unique_id": None,
        }
    ]
    if device:
        subentries.append(
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
            }
        )
    return MockConfigEntry(
        domain=DOMAIN,
        version=2,
        title=title,
        data={CONF_REGION: "US915", CONF_PORT: port},
        options=options,
        subentries_data=subentries,
    )


@contextmanager
def storage_of(name: str) -> Iterator[None]:
    """Store keys of a second home, so the two homes do not share state."""
    with (
        patch.object(server_module, "STATE_STORAGE_KEY", f"{DOMAIN}.state_{name}"),
        patch.object(server_module, "ROOT_STORAGE_KEY", f"{DOMAIN}.root_counters_{name}"),
        patch.object(fed_module, "STORAGE_KEY", f"{DOMAIN}.mailbox_{name}"),
    ):
        yield


async def setup(hass: HomeAssistant, entry: MockConfigEntry, name: str | None = None) -> None:
    """Set an entry up, under its own store keys if named."""
    entry.add_to_hass(hass)
    if name is None:
        assert await hass.config_entries.async_setup(entry.entry_id)
    else:
        with storage_of(name):
            assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()


async def until(cond: Callable[[], bool], what: str, within: float = WAIT_S) -> None:
    """Wait for a condition."""
    deadline = time.monotonic() + within
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.05)


def server_of(entry: MockConfigEntry) -> NetworkServer:
    """The entry's network server."""
    server: NetworkServer = entry.runtime_data
    return server


def federation_of(entry: MockConfigEntry) -> Federation:
    """The entry's federation client, which must be running."""
    federation = server_of(entry).federation
    assert federation is not None
    return federation


def mailbox_of(entry: MockConfigEntry) -> MailboxServer:
    """The entry's one mailbox server."""
    (s,) = federation_of(entry).servers.values()
    return s


def registered(entry: MockConfigEntry) -> bool:
    """Connected, with the sensor's tags registered and acknowledged."""
    s = mailbox_of(entry)
    return (
        s.up
        and s.rtt_ms is not None
        and any(eui == DEV_EUI for eui, _, _ in s.registered.values())
        and not any(kind == "register" for kind, _ in s.pending.values())
    )


class Homes:
    """Home A (owner of the sensor) and home B (owner of the proxy that hears it)."""

    def __init__(
        self,
        hass: HomeAssistant,
        a: MockConfigEntry,
        b: MockConfigEntry,
        proxy_a: SimProxy,
        proxy_b: SimProxy,
        sensor: SimSensor,
    ) -> None:
        self.hass, self.a, self.b = hass, a, b
        self.proxy_a, self.proxy_b, self.sensor = proxy_a, proxy_b, sensor

    @property
    def device(self) -> server_module.Device:
        return server_of(self.a).devices[DEV_EUI]

    def via_b(self, ftype: int, uflags: int = 0, readings: bytes | None = None) -> int:
        """The sensor sends a frame only B's proxy hears; returns its rx_time_us."""
        frame = self.sensor.uplink(ftype, full_state(0) if readings is None else readings, uflags)
        return self.proxy_b.uplink(frame, rssi=-98.5, snr=-3.25)

    async def rx2_via_b(self, rx: int) -> dict[str, Any]:
        """A's answer, transmitted in RX2 by B's proxy; the sensor accepts it."""
        m = await self.proxy_b.recv(cs.M_DOWNLINK, within=5)
        assert m["verdict"] == "accepted"
        dl = downlink_body(m)
        assert dl["ref_rx_time_us"] == rx
        assert dl["tx_offset_us"] == 3_000_000  # RX2 only (09 section 7)
        answer = self.sensor.downlink(dl["frame"])
        assert answer["reply_to"] == self.sensor.fcnt
        answer["parsed"] = parse_commands(answer["commands"])
        return answer


@asynccontextmanager
async def make_homes(
    hass: HomeAssistant,
    mailbox: Mailbox,
    a_options: dict[str, Any] | None = None,
    b_options: dict[str, Any] | None = None,
) -> AsyncGenerator[Homes]:
    servers = {CONF_MAILBOX_SERVERS: [mailbox.url]}
    port_a, port_b = free_udp_port(), free_udp_port()
    a = home_entry(port_a, PROXY_KEY, servers | (a_options or OWNER), device=True, title="A")
    b = home_entry(port_b, B_PROXY_KEY, servers | (b_options or NEIGHBOUR), device=False, title="B")
    await setup(hass, a)
    await setup(hass, b, "b")
    proxy_a = SimProxy(bytes.fromhex(PROXY_KEY), PROXY_ID, port_a)
    proxy_b = SimProxy(bytes.fromhex(B_PROXY_KEY), B_PROXY_ID, port_b)
    await proxy_a.start()
    await proxy_b.start()
    try:
        await proxy_a.connect()
        await proxy_b.connect()
        sensor = SimSensor(bytes.fromhex(K_ROOT), bytes.fromhex(DEV_EUI))
        # Pairing is local: through A's own proxy (09 section 8).
        await pair(hass, proxy_a, sensor)
        homes = Homes(hass, a, b, proxy_a, proxy_b, sensor)
        if server_of(a).federation is not None and federation_of(a).owner:
            await until(lambda: registered(a), "A's registrations")
        if server_of(b).federation is not None:
            await until(lambda: mailbox_of(b).up, "B's connection")
        yield homes
    finally:
        proxy_a.close()
        proxy_b.close()


@pytest.fixture
async def homes(hass: HomeAssistant, mailbox: Mailbox) -> AsyncGenerator[Homes]:
    """Both homes with the default opt-ins: A owner, B publisher with federated
    downlinks."""
    async with make_homes(hass, mailbox) as h:
        yield h


async def test_uplink_and_rx2_answer_through_a_neighbour(
    hass: HomeAssistant, homes: Homes, mailbox: Mailbox
) -> None:
    """A confirmed uplink heard only by B reaches A, marked federated, and A's RX2
    answer is transmitted by B's proxy (07 section 4.4, 09 section 7)."""
    rx = homes.via_b(cs.T_DATA_CONFIRMED, readings=cs.encode_reading(0, 0x01, 1))
    answer = await homes.rx2_via_b(rx)
    # No command queued: the answer carries NOP so the publisher can be credited.
    assert answer["parsed"] == [(cs.CMD_NOP, b"")]
    await homes.proxy_a.nothing()  # A's own proxy never heard it, so it never answers

    device = homes.device
    await until(lambda: device.readings.get("0:contact") == 1, "the reading at A")
    assert hass.states.get(DOOR) is not None
    assert hass.states.get(DOOR).state == "on"  # type: ignore[union-attr]
    # Heard only through the mailbox: marked federation-only (07 section 4.4).
    assert hass.states.get(DOOR).attributes["federation_only"] is True  # type: ignore[union-attr]
    assert device.federated_count == 1

    # The sensor's next uplink acknowledges the answer: one credit for B.
    homes.via_b(cs.T_HEARTBEAT, cs.UF_DL_ACK)
    federation = federation_of(homes.a)
    await until(lambda: any(r.credits for r in federation.records.values()), "a credit")

    # Both servers saw the whole exchange.
    log = mailbox.text()
    assert "forwarded to" in log
    assert "result" in log


async def test_queued_command_rides_on_a_federated_answer(
    hass: HomeAssistant, homes: Homes
) -> None:
    """A device heard only through federation gets its commands in RX2 answers to its
    confirmed uplinks (09 section 7.7), and its DL_ACK settles them."""
    server = server_of(homes.a)
    device = homes.device
    assert await server.async_queue(
        device, "set_heartbeat", "heartbeat", cs.CMD_SET_HEARTBEAT, b"\x00\x1e"
    )
    rx = homes.via_b(cs.T_HEARTBEAT_CONFIRMED)
    answer = await homes.rx2_via_b(rx)
    assert answer["parsed"] == [(cs.CMD_SET_HEARTBEAT, b"\x00\x1e")]
    homes.via_b(cs.T_HEARTBEAT, cs.UF_DL_ACK)
    await until(lambda: not device.commands, "the command acknowledged")
    assert device.last_command is not None
    assert device.last_command["result"] == "delivered"
    assert device.heartbeat_minutes == 30


async def test_federated_paths_never_clear_single_path(hass: HomeAssistant, homes: Homes) -> None:
    """A device heard by one proxy of its own and any number of federated paths is
    single-path; heard only through federation, it is single-path too (07 8.2)."""
    rx = homes.via_b(cs.T_HEARTBEAT_CONFIRMED)
    await homes.rx2_via_b(rx)
    device = homes.device
    await until(lambda: device.federated_count == 1, "the federated path")
    assert device.proxy_count == 1  # A's proxy, from pairing
    assert device.single_path

    device.heard_by.clear()
    assert device.proxy_count == 0
    assert device.single_path

    federated = hass.states.get("sensor.front_door_federated_paths")
    assert federated is not None
    proxies = hass.states.get("sensor.front_door_proxies_hearing_it")
    assert proxies is not None
    assert proxies.state == "1"  # its own proxies only


async def test_publishing_off(hass: HomeAssistant, mailbox: Mailbox) -> None:
    """With publishing off, B shares nothing: A never hears the sensor."""
    async with make_homes(hass, mailbox, b_options={CONF_FEDERATION_DOWNLINKS: True}) as homes:
        homes.via_b(cs.T_HEARTBEAT_CONFIRMED)
        await homes.proxy_b.nothing(1.0)
        assert homes.device.federated_count == 0
        assert " publish tag " not in mailbox.text()


async def test_federated_downlinks_off(hass: HomeAssistant, mailbox: Mailbox) -> None:
    """With federated downlinks off, B publishes without DOWNLINK on its receptions, so
    A cannot request an answer and B's proxy transmits nothing."""
    async with make_homes(hass, mailbox, b_options={CONF_FEDERATION_PUBLISH: True}) as homes:
        homes.via_b(cs.T_HEARTBEAT_CONFIRMED)
        device = homes.device
        await until(lambda: device.federated_count == 1, "the delivery at A")
        await homes.proxy_b.nothing(1.0)
        assert "forwarded to" not in mailbox.text()


async def test_owner_off(hass: HomeAssistant, mailbox: Mailbox) -> None:
    """Without the owner opt-in, A registers nothing, so nothing is delivered to it."""
    async with make_homes(hass, mailbox, a_options={CONF_FEDERATION_PUBLISH: True}) as homes:
        assert not mailbox_of(homes.a).registered
        homes.via_b(cs.T_HEARTBEAT_CONFIRMED)
        await homes.proxy_b.nothing(1.0)
        assert homes.device.federated_count == 0
        assert " register " not in mailbox.text()


async def test_nothing_happens_with_every_opt_in_off(hass: HomeAssistant, mailbox: Mailbox) -> None:
    """Servers listed but every opt-in off: no connection at all."""
    entry = home_entry(
        free_udp_port(),
        PROXY_KEY,
        {CONF_MAILBOX_SERVERS: [mailbox.url]},
        device=True,
        title="A",
    )
    await setup(hass, entry)
    assert server_of(entry).federation is None
    await asyncio.sleep(0.5)
    assert "welcome" not in mailbox.text()
    assert not [e for e in hass.states.async_entity_ids() if "mailbox" in e]


async def test_reconnect_registers_again(
    hass: HomeAssistant, homes: Homes, mailbox: Mailbox
) -> None:
    """A restarted mailbox has a new generation and an empty table: both homes
    reconnect, A registers again, and federation works as before (09 section 5.3)."""
    s = mailbox_of(homes.a)
    generation = s.link.generation if s.link else None
    mailbox.stop()
    await until(lambda: not s.up, "the disconnection")
    mailbox.start()
    await until(lambda: registered(homes.a), "A's registrations again")
    await until(lambda: mailbox_of(homes.b).up, "B's connection again")
    assert s.link is not None
    assert s.link.generation != generation
    assert s.connects == 2
    rx = homes.via_b(cs.T_HEARTBEAT_CONFIRMED)
    await homes.rx2_via_b(rx)


async def test_join_is_never_published(hass: HomeAssistant, homes: Homes, mailbox: Mailbox) -> None:
    """A JOIN B's proxy hears goes nowhere (09 section 8)."""
    homes.proxy_b.uplink(homes.sensor.join())
    await homes.proxy_b.nothing(1.0)
    assert " publish tag " not in mailbox.text()


async def test_ignore_federation_only_readings_and_events(
    hass: HomeAssistant, mailbox: Mailbox
) -> None:
    """The 07 section 4.4 options: frames heard only through federation keep their
    effect on supervision, but readings and events from them are ignored."""
    options = OWNER | {CONF_IGNORE_FEDERATED_READINGS: True, CONF_IGNORE_FEDERATED_EVENTS: True}
    async with make_homes(hass, mailbox, a_options=options) as homes:
        events: list[Any] = []
        hass.bus.async_listen(f"{DOMAIN}_event", events.append)
        before = homes.device.readings.get("0:contact")
        rx = homes.via_b(
            cs.T_DATA_CONFIRMED,
            readings=cs.encode_reading(0, 0x01, 1) + cs.encode_reading(1, 0x0B, 1),
        )
        await homes.rx2_via_b(rx)
        await hass.async_block_till_done()
        assert homes.device.federated_count == 1
        assert homes.device.readings.get("0:contact") == before
        assert events == []


async def test_owner_key_redacted(
    hass: HomeAssistant,
    hass_client: Callable[[], Awaitable[Any]],
    homes: Homes,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The owner key is never in diagnostics or the logs (07 section 11.6)."""
    caplog.set_level(logging.DEBUG, logger="custom_components.connect_sensors")
    rx = homes.via_b(cs.T_HEARTBEAT_CONFIRMED)
    await homes.rx2_via_b(rx)
    seeds = tuple(
        s.seed.hex() for e in (homes.a, homes.b) for s in federation_of(e).servers.values()
    )
    diag = await get_diagnostics_for_config_entry(hass, hass_client, homes.a)
    text = str(diag)
    assert_no_secrets(text, seeds)
    # The test harness's storage mock logs what it writes; only our own logs count.
    ours = "\n".join(
        r.getMessage() for r in caplog.records if r.name.startswith("custom_components")
    )
    assert_no_secrets(ours, seeds)
    servers = diag["server"]["federation"]["servers"]
    assert servers[0]["connected"] is True
    assert servers[0]["registered_tags"] > 0


async def test_unreachable_server_repair(hass: HomeAssistant, tmp_path: Path) -> None:
    """A server unreachable for an hour gets a repair issue."""
    dead = f"ws://127.0.0.1:{free_tcp_port()}/"
    entry = home_entry(
        free_udp_port(), PROXY_KEY, {CONF_MAILBOX_SERVERS: [dead]} | OWNER, device=True, title="A"
    )
    await setup(hass, entry)
    s = mailbox_of(entry)
    await until(lambda: s.last_error is not None, "a failed connection")
    assert not s.up
    issues = ir.async_get(hass)
    federation_of(entry).tick(datetime.now(UTC) + timedelta(minutes=30))
    assert issues.async_get_issue(DOMAIN, f"mailbox_unreachable_{s.key}") is None
    federation_of(entry).tick(datetime.now(UTC) + timedelta(hours=2))
    issue = issues.async_get_issue(DOMAIN, f"mailbox_unreachable_{s.key}")
    assert issue is not None
    assert issue.translation_placeholders is not None
    assert issue.translation_placeholders["url"] == dead
    connected = hass.states.get("binary_sensor.mailbox_127_0_0_1_connectivity")
    assert connected is not None
    assert connected.state == "off"


async def test_refused_registration_repair(hass: HomeAssistant, tmp_path: Path) -> None:
    """A members-only server refuses an unknown key: a repair (09 section 12.6)."""
    members = tmp_path / "members"
    members.write_text("00" * 32 + "\n")
    m = Mailbox(tmp_path, "--insecure-dev", "--members", str(members))
    m.start()
    try:
        entry = home_entry(
            free_udp_port(),
            PROXY_KEY,
            {CONF_MAILBOX_SERVERS: [m.url]} | OWNER,
            device=True,
            title="A",
        )
        await setup(hass, entry)
        s = mailbox_of(entry)
        await until(lambda: s.refused == "NOT_PERMITTED", "the refusal")
        assert ir.async_get(hass).async_get_issue(DOMAIN, f"mailbox_refused_{s.key}") is not None
    finally:
        m.stop()


def self_signed(tmp: Path, host: str) -> tuple[Path, Path, str]:
    """A certificate for host, its key, and the certificate as PEM text."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(host))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    cert_file, key_file = tmp / "cert.pem", tmp / "key.pem"
    cert_file.write_text(pem)
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_file, key_file, pem


async def test_tls_with_a_pinned_certificate(hass: HomeAssistant, tmp_path: Path) -> None:
    """wss:// with a self-signed certificate the user pasted (09 section 12.1); without
    it the certificate is refused."""
    # An address, not a name: the test harness's resolver cannot look names up.
    cert, key, pem = self_signed(tmp_path, "127.0.0.1")
    m = Mailbox(tmp_path, "--tls-cert", str(cert), "--tls-key", str(key))
    m.start()
    try:
        with patch.object(fed_module, "ALLOW_INSECURE", False):
            pinned = home_entry(
                free_udp_port(),
                PROXY_KEY,
                {CONF_MAILBOX_SERVERS: [m.url], CONF_MAILBOX_CERTIFICATE: pem} | OWNER,
                device=True,
                title="A",
            )
            await setup(hass, pinned)
            s = mailbox_of(pinned)
            await until(lambda: s.up, "the TLS connection")
            assert await hass.config_entries.async_unload(pinned.entry_id)

            unpinned = home_entry(
                free_udp_port(),
                PROXY_KEY,
                {CONF_MAILBOX_SERVERS: [m.url]} | OWNER,
                device=False,
                title="C",
            )
            await setup(hass, unpinned, "c")
            s = mailbox_of(unpinned)
            await until(lambda: s.last_error is not None, "the refused certificate")
            assert not s.up
            assert "certificate" in (s.last_error or "").lower()
    finally:
        m.stop()


async def test_registration_tops_up_and_withdraws(hass: HomeAssistant, homes: Homes) -> None:
    """Normal tags of e-1 to e+2 and the SYNC window are registered; the next epoch
    adds e+3, and a re-paired device's old tags are withdrawn (09 section 5.3)."""
    federation = federation_of(homes.a)
    s = mailbox_of(homes.a)
    assert s.link is not None
    await federation._register(s)
    await until(lambda: not s.pending, "the registrations acknowledged")
    now = int(time.time())
    want = federation.wanted(now)
    k_dev_tag = homes.sensor.session.k_dev_tag
    e = cs.epoch_of(now)
    normal = {
        cs.tag_of_pub(cs.ed25519_pub(cs.tag_seed(k_dev_tag, cs.epoch_index(x))))
        for x in range(e - 1, e + 3)
    }
    assert normal <= set(want)
    assert set(want) <= set(s.registered)
    for x in range(e - 1, e + 3):
        tag = cs.tag_of_pub(cs.ed25519_pub(cs.tag_seed(k_dev_tag, cs.epoch_index(x))))
        assert want[tag][1] == cs.EPOCH_SECONDS * (x + 2)  # end of epoch x + 1

    sent: list[list[Any]] = []
    real_send = s.link.send

    async def capture(m: list[Any]) -> int:
        sent.append(m)
        return await real_send(m)

    s.link.send = capture  # type: ignore[method-assign]
    later = now + cs.EPOCH_SECONDS
    with patch.object(server_module, "_unix", return_value=later):
        await federation._register(s)
    new_tag = cs.tag_of_pub(cs.ed25519_pub(cs.tag_seed(k_dev_tag, cs.epoch_index(e + 3))))
    assert any(m[0] == cs.MB_REGISTER for m in sent)
    assert new_tag in s.registered

    # A new session (re-pairing): the old session's tags are withdrawn.
    # (The pass a epoch ahead dropped epoch e - 1's registration as expired.)
    sent.clear()
    old = {t for t in normal if t in s.registered}
    assert len(old) == 3
    device = homes.device
    device.set_session(b"\x42" * 16)
    await federation._register(s)
    withdrawn = {t for m in sent if m[0] == cs.MB_WITHDRAW for t in m[1]}
    assert old <= withdrawn
    assert not old & set(s.registered)


async def test_options_flow(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """Everything is off by default; URLs must be wss://; an opt-in needs a server."""
    result = await hass.config_entries.options.async_init(loaded.entry_id)
    assert result["type"] is FlowResultType.FORM
    schema = result["data_schema"]
    assert schema is not None
    defaults = {str(k): k.default() for k in schema.schema if callable(getattr(k, "default", None))}
    assert defaults[CONF_FEDERATION_OWNER] is False
    assert defaults[CONF_FEDERATION_PUBLISH] is False
    assert defaults[CONF_FEDERATION_DOWNLINKS] is False

    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {CONF_MAILBOX_SERVERS: ["ws://mailbox.example/"]}
    )
    assert result["errors"] == {CONF_MAILBOX_SERVERS: "invalid_url"}
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {CONF_MAILBOX_SERVERS: [], CONF_FEDERATION_OWNER: True}
    )
    assert result["errors"] == {CONF_MAILBOX_SERVERS: "no_servers"}
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {CONF_MAILBOX_SERVERS: ["wss://mailbox.example/"], CONF_MAILBOX_CERTIFICATE: "nope"},
    )
    assert result["errors"] == {CONF_MAILBOX_CERTIFICATE: "invalid_certificate"}
    with patch.object(fed_module.Federation, "async_start"):
        result = await hass.config_entries.options.async_configure(
            result["flow_id"],
            {
                CONF_MAILBOX_SERVERS: ["wss://mailbox.example/"],
                CONF_FEDERATION_OWNER: False,
                CONF_FEDERATION_PUBLISH: True,
            },
        )
        await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert loaded.options[CONF_MAILBOX_SERVERS] == ["wss://mailbox.example/"]
    assert loaded.options[CONF_FEDERATION_PUBLISH] is True
    assert loaded.options[CONF_FEDERATION_OWNER] is False


async def test_downlinks_alone_do_not_connect(hass: HomeAssistant, mailbox: Mailbox) -> None:
    """Federated downlinks answer only what was published: alone, they connect nothing."""
    entry = home_entry(
        free_udp_port(),
        PROXY_KEY,
        {CONF_MAILBOX_SERVERS: [mailbox.url], CONF_FEDERATION_DOWNLINKS: True},
        device=True,
        title="A",
    )
    await setup(hass, entry)
    assert server_of(entry).federation is None
    await asyncio.sleep(0.5)
    assert "welcome" not in mailbox.text()


async def test_local_copy_answers_locally(
    hass: HomeAssistant, homes: Homes, mailbox: Mailbox
) -> None:
    """Heard by A's own proxy too: A answers through it, and requests nothing through
    federation, so there is one downlink for the window (07 section 6.3)."""
    frame = homes.sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0))
    rx_a = homes.proxy_a.uplink(frame)
    homes.proxy_b.uplink(frame, rssi=-98.5, snr=-3.25)
    m = await homes.proxy_a.recv(cs.M_DOWNLINK)
    assert downlink_body(m)["ref_rx_time_us"] == rx_a
    device = homes.device
    await until(lambda: device.federated_count == 1, "B's copy at A")
    await homes.proxy_b.nothing(1.0)
    assert "forwarded to" not in mailbox.text()


async def test_late_federated_copy_is_answered(hass: HomeAssistant, homes: Homes) -> None:
    """A's proxy heard the frame but cannot transmit: B's copy, arriving after A's
    collection window, still gets the RX2 answer through B (07 section 4.4)."""
    homes.proxy_a.hello(caps=0)
    await until(
        lambda: not server_of(homes.a).proxies["proxy_A"].can_downlink, "A's proxy receive-only"
    )
    frame = homes.sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0))
    homes.proxy_a.uplink(frame)
    await asyncio.sleep(0.3)  # past A's collection window
    rx_b = homes.proxy_b.uplink(frame, rssi=-98.5, snr=-3.25)
    await homes.rx2_via_b(rx_b)


async def test_non_sent_result_releases_and_counts(hass: HomeAssistant, homes: Homes) -> None:
    """B's proxy answers BUSY: the result releases the transmission and counts as a soft
    failure for B (09 sections 7.5 and 9); there is no other reception to try."""
    homes.proxy_b.tx_result = (2, 0)
    homes.via_b(cs.T_HEARTBEAT_CONFIRMED)
    await homes.proxy_b.recv(cs.M_DOWNLINK, within=5)
    federation = federation_of(homes.a)
    await until(lambda: any(r.failures for r in federation.records.values()), "the failure")
    (record,) = federation.records.values()
    assert record.failures[0][1] == 0.5
    assert not record.credits
    await homes.proxy_b.nothing(1.0)


async def test_clock_correction_rides_only_on_confirmed_uplinks(
    hass: HomeAssistant, homes: Homes
) -> None:
    """Through federation a clock correction goes out only in RX2 answers to confirmed
    uplinks (09 section 7.7): an unconfirmed TIME_UNSURE uplink gets no request."""
    device = homes.device
    fcnt_down = device.fcnt_down
    homes.via_b(cs.T_HEARTBEAT, cs.UF_TIME_UNSURE)
    await until(lambda: device.last_fcnt == homes.sensor.fcnt, "the uplink at A")
    await homes.proxy_b.nothing(1.0)
    # No answer was even built for it.
    assert device.fcnt_down == fcnt_down

    rx = homes.via_b(cs.T_HEARTBEAT_CONFIRMED, cs.UF_TIME_UNSURE)
    await homes.rx2_via_b(rx)
