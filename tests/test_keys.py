"""A downlink-capable proxy has a key of its own (05 section 4.2, 07 sections 6.1, 11.5)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
import re
from types import MappingProxyType
from unittest.mock import AsyncMock, patch

import connectsensors as cs
from homeassistant.components.repairs import repairs_flow_manager
from homeassistant.config_entries import SOURCE_USER, ConfigSubentry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import issue_registry as ir
from homeassistant.setup import async_setup_component
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors.const import CONF_KEY, DOMAIN, SUBENTRY_PROXY

from .common import answer, pair
from .conftest import PROXY_KEY
from .sim import SimProxy, SimSensor

SHARED_KEY = "5b" * 16
SHARED = {"s1": "348518fffe0000b1", "s2": "348518fffe0000b2"}


@pytest.fixture(autouse=True)
def source_ip() -> Generator[AsyncMock]:
    """Home Assistant's LAN address, for the cs_proxy snippet."""
    with patch(
        "custom_components.connect_sensors.config_flow.async_get_source_ip",
        AsyncMock(return_value="192.168.1.20"),
    ) as mock:
        yield mock


@pytest.fixture
def entry(entry: MockConfigEntry) -> MockConfigEntry:
    """The network, with two more proxies that hold one key between them."""
    subentries = dict(entry.subentries)
    for sid in SHARED:
        subentries[sid] = ConfigSubentry(
            data=MappingProxyType({CONF_KEY: SHARED_KEY}),
            subentry_type=SUBENTRY_PROXY,
            title=f"Shared {sid}",
            subentry_id=sid,
            unique_id=None,
        )
    entry.subentries = MappingProxyType(subentries)
    return entry


@pytest.fixture
async def shared(port: int) -> AsyncGenerator[dict[str, SimProxy]]:
    """Simulated proxies on the shared key."""
    sims = {
        sid: SimProxy(bytes.fromhex(SHARED_KEY), bytes.fromhex(pid), port, rx_time_us=base)
        for (sid, pid), base in zip(SHARED.items(), (1_000_000_000, 2_000_000_000), strict=True)
    }
    for sim in sims.values():
        await sim.start()
    yield sims
    for sim in sims.values():
        sim.close()


def issue(hass: HomeAssistant, sid: str) -> ir.IssueEntry | None:
    """The shared-key repair issue of a proxy, if raised."""
    return ir.async_get(hass).async_get_issue(DOMAIN, f"shared_key_{sid}")


async def test_shared_key_gets_no_downlinks(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    shared: dict[str, SimProxy],
    sensor: SimSensor,
) -> None:
    """Proxies on one key are never asked to transmit and raise a fixable repair, which
    gives the proxy a key of its own."""
    await proxy.connect()
    for sim in shared.values():
        await sim.connect()
    await hass.async_block_till_done()
    for sid in SHARED:
        found = issue(hass, sid)
        assert found is not None and found.is_fixable
    await pair(hass, proxy, sensor)

    # Heard only through the shared key: nobody answers, in either window.
    frame = sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(0, 0x01, 1))
    shared["s1"].uplink(frame, snr=10.0)
    shared["s2"].uplink(frame, snr=9.0)
    for sim in (*shared.values(), proxy):
        await sim.nothing(0.2)

    # Also heard by a proxy with its own key: that one answers, despite a lower SNR.
    frame = sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(0, 0x01, 0))
    shared["s1"].uplink(frame, snr=10.0)
    rx = proxy.uplink(frame, snr=1.0)
    sensor.downlink(await answer(proxy, rx))
    await shared["s1"].nothing(0.2)

    # The repair shows the new key and replaces the shared one on submit.
    assert await async_setup_component(hass, "repairs", {})
    manager = repairs_flow_manager(hass)
    assert manager is not None
    result = await manager.async_init(DOMAIN, context={"issue_id": "shared_key_s2"})
    assert result["type"] is FlowResultType.FORM
    placeholders = result["description_placeholders"]
    assert placeholders is not None
    match = re.search(r'key: "([0-9a-f]{32})"', placeholders["snippet"])
    assert match is not None and match.group(1) not in (SHARED_KEY, PROXY_KEY)
    result = await manager.async_configure(result["flow_id"], {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    await hass.async_block_till_done()
    assert loaded.subentries["s2"].data[CONF_KEY] == match.group(1)
    assert issue(hass, "s1") is None and issue(hass, "s2") is None
    assert not any(p.shared for p in loaded.runtime_data.proxies.values())

    # The old key no longer reaches s2; the new one does, from the same proxy_id.
    shared["s2"].hello()
    await shared["s2"].nothing()
    rekeyed = SimProxy(
        bytes.fromhex(match.group(1)), bytes.fromhex(SHARED["s2"]), shared["s2"].server_port
    )
    await rekeyed.start()
    try:
        await rekeyed.connect()
    finally:
        rekeyed.close()


async def test_receive_only_proxies_may_share(
    hass: HomeAssistant, loaded: MockConfigEntry, shared: dict[str, SimProxy]
) -> None:
    """A shared key is allowed for proxies that do not accept downlinks: no repair."""
    for sim in shared.values():
        await sim.connect(caps=0)
    await hass.async_block_till_done()
    assert issue(hass, "s1") is None and issue(hass, "s2") is None
    assert all(loaded.runtime_data.proxies[sid].shared for sid in SHARED)


async def test_new_proxy_key_is_unique(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """The config flow never hands out a key another proxy holds."""
    with patch(
        "custom_components.connect_sensors.config_flow.secrets.token_hex",
        side_effect=[SHARED_KEY, PROXY_KEY, "7c" * 16],
    ):
        result = await hass.config_entries.subentries.async_init(
            (loaded.entry_id, SUBENTRY_PROXY), context={"source": SOURCE_USER}
        )
    placeholders = result["description_placeholders"]
    assert placeholders is not None
    assert f'key: "{"7c" * 16}"' in placeholders["snippet"]


async def test_duplicate_key_refused(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """A proxy whose key another proxy got meanwhile is not added."""
    result = await hass.config_entries.subentries.async_init(
        (loaded.entry_id, SUBENTRY_PROXY), context={"source": SOURCE_USER}
    )
    placeholders = result["description_placeholders"]
    assert placeholders is not None
    match = re.search(r'key: "([0-9a-f]{32})"', placeholders["snippet"])
    assert match is not None
    hass.config_entries.async_add_subentry(
        loaded,
        ConfigSubentry(
            data=MappingProxyType({CONF_KEY: match.group(1)}),
            subentry_type=SUBENTRY_PROXY,
            title="Other proxy",
            unique_id=None,
        ),
    )
    result = await hass.config_entries.subentries.async_configure(
        result["flow_id"], {CONF_NAME: "Attic proxy"}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "duplicate_key"
    assert [
        s.title for s in loaded.subentries.values() if s.data.get(CONF_KEY) == match.group(1)
    ] == ["Other proxy"]
