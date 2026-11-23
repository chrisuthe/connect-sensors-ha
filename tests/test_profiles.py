"""Device profiles (03 section 9) and the proxy's ESPHome device (07 section 11.2)."""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta

from homeassistant.const import CONF_PORT, STATE_OFF, STATE_ON, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr, entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors.const import (
    CONF_DEV_EUI,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
)
from custom_components.connect_sensors.profiles import get_profile

from .common import pair, parse_commands, uplink_answered
from .conftest import DEV_EUI, K_ROOT, PROXY_ID, PROXY_KEY, WallClock, state_of
from .sim import SimProxy, SimSensor

UNKNOWN_PROFILE = "ACME.THING-7"

# Spec 03 section 9.2, as published.
CLIMATE_1 = {
    "profile": "CS.CLIMATE-1",
    "name": "ConnectSensors climate sensor",
    "manufacturer": "ConnectSensors reference design",
    "model": "Climate v1",
    "power": "battery",
    "heartbeat_minutes": 30,
    "channels": [
        {"channel": 0, "kind": "temperature"},
        {"channel": 0, "kind": "humidity"},
        {"channel": 255, "kind": "battery_percent"},
    ],
}


def entry_with_profile(port: int, profile: str) -> MockConfigEntry:
    """A network with one proxy and one device of the given profile."""
    return MockConfigEntry(
        domain=DOMAIN,
        version=2,
        title="ConnectSensors",
        data={CONF_REGION: "US915", CONF_PORT: port},
        subentries_data=[
            {
                "data": {CONF_KEY: PROXY_KEY},
                "subentry_type": SUBENTRY_PROXY,
                "title": "Garage proxy",
                "subentry_id": "proxy1",
                "unique_id": None,
            },
            {
                "data": {
                    CONF_DEV_EUI: DEV_EUI,
                    CONF_K_ROOT: K_ROOT,
                    CONF_PROFILE: profile,
                    CONF_REGION: "US915",
                },
                "subentry_type": SUBENTRY_DEVICE,
                "title": "Thing",
                "subentry_id": "device1",
                "unique_id": DEV_EUI,
            },
        ],
    )


def sensor_entities(hass: HomeAssistant, entry: MockConfigEntry) -> set[str]:
    """Entity ids of the entry's non-diagnostic sensor entities."""
    return {
        e.entity_id
        for e in er.async_entries_for_config_entry(er.async_get(hass), entry.entry_id)
        if e.entity_category is None and e.unique_id.startswith(DEV_EUI)
    }


async def test_known_profile(hass: HomeAssistant, loaded: MockConfigEntry) -> None:
    """CS.CONTACT-1's entities exist before the first frame, named by the profile."""
    assert state_of(hass, "binary_sensor.front_door_door").state == STATE_UNKNOWN
    assert state_of(hass, "binary_sensor.front_door_door").attributes["device_class"] == "door"
    assert state_of(hass, "binary_sensor.front_door_tamper").state == STATE_UNKNOWN
    assert state_of(hass, "sensor.front_door_battery").state == STATE_UNKNOWN
    device = dr.async_get(hass).async_get_device_by_identifier((DOMAIN, DEV_EUI), loaded.entry_id)
    assert device is not None
    assert device.manufacturer == "ConnectSensors reference design"
    assert device.model == "Contact v1"
    assert device.model_id == "CS.CONTACT-1"
    assert loaded.runtime_data.devices[DEV_EUI].heartbeat_minutes == 30


def test_climate_profile_matches_spec() -> None:
    """CS.CLIMATE-1 is the published profile, field for field."""
    profile = get_profile("CS.CLIMATE-1")
    assert profile is not None
    fields = asdict(profile)
    del fields["any_writable"]
    # Unset optional fields (no name, not writable) are absent from the JSON.
    channels = [
        {k: v for k, v in c.items() if v is not None and v is not False} for c in fields["channels"]
    ]
    assert {**fields, "channels": channels} == CLIMATE_1


async def test_climate_supervision(
    hass: HomeAssistant, port: int, proxy: SimProxy, sensor: SimSensor, wall: WallClock
) -> None:
    """CS.CLIMATE-1 heartbeats every 30 minutes: unreachable after 3 x 30 + 5."""
    entry = entry_with_profile(port, "CS.CLIMATE-1")
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.runtime_data.devices[DEV_EUI].heartbeat_minutes == 30
    await proxy.connect()
    await pair(hass, proxy, sensor)
    assert state_of(hass, "binary_sensor.thing_reachable").state == STATE_ON

    await wall.advance(timedelta(minutes=94))
    assert state_of(hass, "binary_sensor.thing_reachable").state == STATE_ON
    await wall.advance(timedelta(minutes=2))
    assert state_of(hass, "binary_sensor.thing_reachable").state == STATE_OFF


async def test_unknown_profile(
    hass: HomeAssistant, port: int, proxy: SimProxy, sensor: SimSensor
) -> None:
    """An unknown profile gets entities from the readings it sends (03 section 9.3)."""
    entry = entry_with_profile(port, UNKNOWN_PROFILE)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert sensor_entities(hass, entry) == set()
    device = dr.async_get(hass).async_get_device_by_identifier((DOMAIN, DEV_EUI), entry.entry_id)
    assert device is not None
    assert device.model == UNKNOWN_PROFILE
    assert entry.runtime_data.devices[DEV_EUI].heartbeat_minutes == 60

    await proxy.connect()
    await pair(hass, proxy, sensor)
    await hass.async_block_till_done()
    assert sensor_entities(hass, entry) == {
        "binary_sensor.thing_opening",
        "sensor.thing_temperature",
    }
    assert state_of(hass, "sensor.thing_temperature").state == "21.53"

    # Nothing says which channels are writable, so set_value is sent as asked.
    await hass.services.async_call(
        DOMAIN,
        "set_value",
        {"device_id": device.id, "channel": 2, "kind": "temperature", "value": 21.5},
        blocking=True,
    )
    dl = await uplink_answered(hass, proxy, sensor)
    assert dl["parsed"] == [(0x40, bytes([2, 0x40, 0x08, 0x66]))]
    assert parse_commands(dl["commands"]) == dl["parsed"]


async def test_esphome_profile(hass: HomeAssistant, port: int) -> None:
    """ESPHOME: reading-driven entities, cs_sensor's default heartbeat."""
    entry = entry_with_profile(port, "ESPHOME")
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert sensor_entities(hass, entry) == set()
    assert entry.runtime_data.devices[DEV_EUI].heartbeat_minutes == 60
    device = dr.async_get(hass).async_get_device_by_identifier((DOMAIN, DEV_EUI), entry.entry_id)
    assert device is not None
    assert device.manufacturer == "ESPHome"


async def test_proxy_linked_to_esphome_device(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy
) -> None:
    """The proxy's MAC connection, from its EUI-64 (05 section 4.1), links its device
    to the ESPHome device of the same board.

    Since Home Assistant 2026.8 a device belongs to one config entry, so the two are
    linked devices sharing the connection rather than one merged device.
    """
    esphome = MockConfigEntry(domain="esphome", title="lora-proxy")
    esphome.add_to_hass(hass)
    registry = dr.async_get(hass)
    # ESPHome registers format_mac() of the MAC the node reports: lowercase, colons.
    board = registry.async_get_or_create(
        config_entry_id=esphome.entry_id,
        connections={(dr.CONNECTION_NETWORK_MAC, "34:85:18:00:00:01")},
        name="lora-proxy",
    )

    await proxy.connect()
    await hass.async_block_till_done()
    assert PROXY_ID.hex() == "348518fffe000001"
    ours = registry.async_get_device_by_identifier((DOMAIN, PROXY_ID.hex()), loaded.entry_id)
    assert ours is not None
    assert ours.connections == {(dr.CONNECTION_NETWORK_MAC, "34:85:18:00:00:01")}
    linked = registry.async_get_devices(identifiers=ours.identifiers, connections=ours.connections)
    assert {d.id for d in linked} == {ours.id, board.id}
