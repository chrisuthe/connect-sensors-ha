"""Event readings and supervision events (03 section 5, 07 section 11.3)."""

from __future__ import annotations

from datetime import timedelta

import connectsensors as cs
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_capture_events

from custom_components.connect_sensors.const import DOMAIN

from .common import pair, uplink_answered
from .conftest import DEV_EUI, Clock, WallClock, state_of
from .sim import SimProxy, SimSensor
from .test_supervision import SILENCE, heartbeat

BUTTON = "event.front_door_button"


async def test_button_events(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    clock: Clock,
) -> None:
    """A button reading fires the event entity and a bus event, and is not a state."""
    events = async_capture_events(hass, "connect_sensors_event")
    await proxy.connect()
    await pair(hass, proxy, sensor)
    assert hass.states.get(BUTTON) is None
    device = dr.async_get(hass).async_get_device_by_identifier((DOMAIN, DEV_EUI), loaded.entry_id)
    assert device is not None

    await uplink_answered(
        hass, proxy, sensor, cs.T_DATA_CONFIRMED, readings=cs.encode_reading(1, 0x08, 1)
    )
    await hass.async_block_till_done()
    state = state_of(hass, BUTTON)
    assert state.attributes["event_type"] == "press"
    assert state.attributes["device_class"] == "button"
    first = state.state

    await uplink_answered(
        hass, proxy, sensor, cs.T_DATA_CONFIRMED, readings=cs.encode_reading(1, 0x08, 3)
    )
    state = state_of(hass, BUTTON)
    assert state.attributes["event_type"] == "long_press"
    assert state.state != first

    assert [e.data for e in events] == [
        {
            "device_id": device.id,
            "dev_eui": DEV_EUI,
            "name": "Front door",
            "channel": 1,
            "kind": "button",
            "event_type": event_type,
            "federation_only": False,
        }
        for event_type in ("press", "long_press")
    ]
    # Not a reading with a state.
    assert "1:button" not in loaded.runtime_data.devices[DEV_EUI].readings

    # The retransmission of a press is the same frame: it fires nothing (03 section 5).
    frame = sensor.uplink(cs.T_DATA_CONFIRMED, cs.encode_reading(1, 0x08, 2))
    proxy.uplink(frame)
    await proxy.recv(cs.M_DOWNLINK)
    clock.advance(3000)  # after the merge window: a retransmission, answered again
    proxy.uplink(frame)
    await proxy.recv(cs.M_DOWNLINK)
    await hass.async_block_till_done()
    assert len(events) == 3


async def test_button_entity_after_restart(
    hass: HomeAssistant, loaded: MockConfigEntry, proxy: SimProxy, sensor: SimSensor
) -> None:
    """The event entity stays once the device has sent an event."""
    await proxy.connect()
    await pair(hass, proxy, sensor)
    await uplink_answered(
        hass, proxy, sensor, cs.T_DATA_CONFIRMED, readings=cs.encode_reading(1, 0x08, 1)
    )
    assert await hass.config_entries.async_reload(loaded.entry_id)
    await hass.async_block_till_done()
    assert hass.states.get(BUTTON) is not None


async def test_reachability_events(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    wall: WallClock,
) -> None:
    """Supervision changes fire bus events for automations (07 section 11.3)."""
    unreachable = async_capture_events(hass, "connect_sensors_device_unreachable")
    reachable = async_capture_events(hass, "connect_sensors_device_reachable")
    await proxy.connect()
    await pair(hass, proxy, sensor)
    device = dr.async_get(hass).async_get_device_by_identifier((DOMAIN, DEV_EUI), loaded.entry_id)
    assert device is not None
    expected = {"device_id": device.id, "dev_eui": DEV_EUI, "name": "Front door"}

    await wall.advance(SILENCE + timedelta(minutes=1))
    assert [e.data for e in unreachable] == [expected]
    assert reachable == []
    # Still unreachable: no second event.
    await wall.advance(timedelta(minutes=10))
    assert len(unreachable) == 1

    await heartbeat(hass, proxy, sensor)
    assert [e.data for e in reachable] == [expected]
    await heartbeat(hass, proxy, sensor)
    assert len(reachable) == 1
