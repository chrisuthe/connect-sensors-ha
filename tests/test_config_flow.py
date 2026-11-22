"""Config flow: the network, proxies (key and snippet) and devices (pairing code)."""

from __future__ import annotations

from collections.abc import Generator
import re
from unittest.mock import AsyncMock, patch

import connectsensors as cs
from homeassistant.config_entries import SOURCE_RECONFIGURE, SOURCE_USER
from homeassistant.const import CONF_NAME, CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors.const import (
    CONF_DEV_EUI,
    CONF_HEARTBEAT,
    CONF_K_NET,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_PAIRING_CODE,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
)

from .conftest import DEV_EUI, K_ROOT, PAIRING_CODE, state_of
from .sim import free_udp_port

HOST = "192.168.1.20"
OTHER_EUI = bytes.fromhex("c5c5000000000002")
OTHER_ROOT = bytes(range(16, 32))


@pytest.fixture(autouse=True)
def source_ip() -> Generator[AsyncMock]:
    """Home Assistant's LAN address."""
    with patch(
        "custom_components.connect_sensors.config_flow.async_get_source_ip",
        AsyncMock(return_value=HOST),
    ) as mock:
        yield mock


async def test_network(hass: HomeAssistant) -> None:
    """The network is created with a region, a port and a generated key."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
    assert result["type"] is FlowResultType.FORM
    with patch("custom_components.connect_sensors.async_setup_entry", return_value=True):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_REGION: "us915", CONF_PORT: 47801}
        )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    data = result["data"]
    assert data[CONF_REGION] == "US915"
    assert data[CONF_PORT] == 47801
    assert re.fullmatch("[0-9a-f]{32}", data[CONF_K_NET])

    # One network per Home Assistant.
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "single_instance_allowed"


async def test_reconfigure_port(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """A new port reloads the server on it; the network key stays."""
    k_net = loaded.data[CONF_K_NET]
    new_port = free_udp_port()
    result = await loaded.start_reconfigure_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_PORT: new_port}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    await hass.async_block_till_done()
    assert loaded.data[CONF_K_NET] == k_net
    assert loaded.runtime_data.port == new_port


async def test_add_proxy(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """Adding a proxy generates a key and shows a ready-to-paste snippet."""
    result = await hass.config_entries.subentries.async_init(
        (loaded.entry_id, SUBENTRY_PROXY), context={"source": SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    placeholders = result["description_placeholders"]
    assert placeholders is not None
    snippet = placeholders["snippet"]
    match = re.search(r'key: "([0-9a-f]{32})"', snippet)
    assert match is not None
    assert f"host: {HOST}" in snippet
    assert f"port: {loaded.data[CONF_PORT]}" in snippet
    assert "region: US915" in snippet

    result = await hass.config_entries.subentries.async_configure(
        result["flow_id"], {CONF_NAME: "Attic proxy"}
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    subentry = next(s for s in loaded.subentries.values() if s.title == "Attic proxy")
    assert subentry.data[CONF_KEY] == match.group(1)
    await hass.async_block_till_done()
    assert subentry.subentry_id in loaded.runtime_data.proxies

    # The snippet can be shown again later.
    result = await hass.config_entries.subentries.async_init(
        (loaded.entry_id, SUBENTRY_PROXY),
        context={"source": SOURCE_RECONFIGURE, "subentry_id": subentry.subentry_id},
    )
    placeholders = result["description_placeholders"]
    assert placeholders is not None
    assert match.group(1) in placeholders["snippet"]
    result = await hass.config_entries.subentries.async_configure(
        result["flow_id"], {CONF_NAME: "Loft proxy"}
    )
    assert result["type"] is FlowResultType.ABORT
    assert loaded.subentries[subentry.subentry_id].title == "Loft proxy"


async def _device_flow(hass: HomeAssistant, entry: MockConfigEntry, code: str) -> dict:  # type: ignore[type-arg]
    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, SUBENTRY_DEVICE), context={"source": SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    return dict(
        await hass.config_entries.subentries.async_configure(
            result["flow_id"], {CONF_PAIRING_CODE: code}
        )
    )


async def test_add_device(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """A valid pairing code adds the device, waiting for its JOIN."""
    code = cs.qr_encode(OTHER_EUI, OTHER_ROOT, "US915", "CS.CONTACT-1")
    result = await _device_flow(hass, loaded, f"  {code.lower()} ")
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "CS.CONTACT-1 0002"
    assert result["data"] == {
        CONF_DEV_EUI: OTHER_EUI.hex(),
        CONF_K_ROOT: OTHER_ROOT.hex(),
        CONF_PROFILE: "CS.CONTACT-1",
        CONF_REGION: "US915",
    }
    await hass.async_block_till_done()
    assert OTHER_EUI.hex() in loaded.runtime_data.devices
    assert state_of(hass, "sensor.cs_contact_1_0002_pairing").state == "scanned"


@pytest.mark.parametrize(
    ("code", "error"),
    [
        (PAIRING_CODE[:-1] + ("0" if PAIRING_CODE[-1] != "0" else "1"), "checksum_mismatch"),
        ("CS:1:not-a-code", "invalid_code"),
        ("hello", "invalid_code"),
    ],
)
async def test_bad_pairing_code(
    hass: HomeAssistant, loaded: MockConfigEntry, code: str, error: str
) -> None:
    """Typos are caught by the checksum; other input is rejected."""
    result = await _device_flow(hass, loaded, code)
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_PAIRING_CODE: error}


async def test_device_already_added(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """The same dev_eui cannot be added twice."""
    result = await _device_flow(hass, loaded, PAIRING_CODE)
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert DEV_EUI in loaded.runtime_data.devices


async def test_device_other_region(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """A device built for another region needs confirmation (04 section 3.2)."""
    code = cs.qr_encode(OTHER_EUI, OTHER_ROOT, "EU868", "CS.CONTACT-1")
    result = await _device_flow(hass, loaded, code)
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "region_mismatch"
    assert result["description_placeholders"] == {
        "device_region": "EU868",
        "network_region": "US915",
    }
    result = dict(await hass.config_entries.subentries.async_configure(result["flow_id"], {}))
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_REGION] == "EU868"


async def test_device_settings(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """The device's settings rename it and set the heartbeat supervision expects."""
    result = await hass.config_entries.subentries.async_init(
        (loaded.entry_id, SUBENTRY_DEVICE),
        context={"source": SOURCE_RECONFIGURE, "subentry_id": "device1"},
    )
    assert result["type"] is FlowResultType.FORM
    schema = result["data_schema"]
    assert schema is not None
    assert schema({CONF_NAME: "x"})[CONF_HEARTBEAT] == 30  # the profile's default

    result = await hass.config_entries.subentries.async_configure(
        result["flow_id"], {CONF_NAME: "Back door", CONF_HEARTBEAT: 5}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    subentry = loaded.subentries["device1"]
    assert subentry.title == "Back door"
    assert subentry.data[CONF_HEARTBEAT] == 5
    assert subentry.data[CONF_K_ROOT] == K_ROOT
    await hass.async_block_till_done()
    device = loaded.runtime_data.devices[DEV_EUI]
    assert (device.name, device.heartbeat_minutes) == ("Back door", 5)
