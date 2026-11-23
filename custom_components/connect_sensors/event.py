"""Event entities: event readings such as button presses (03 section 7, 07 section 11.3)."""

from __future__ import annotations

from homeassistant.components.event import EventDeviceClass, EventEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import ConnectSensorsConfigEntry
from .const import (
    BUTTON_EVENTS,
    signal_device_added,
    signal_device_event,
    signal_new_reading,
)
from .entity import ReadingEntity
from .readings import EVENT_KINDS, split_reading_key
from .server import Device

PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConnectSensorsConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Add an event entity per event reading a device has, or its profile lists."""
    server = entry.runtime_data

    @callback
    def add_readings(device: Device, keys: list[str]) -> None:
        entities = [
            ReadingEvent(device, key) for key in keys if split_reading_key(key)[1] in EVENT_KINDS
        ]
        if entities:
            async_add_entities(entities, config_subentry_id=device.subentry_id)

    @callback
    def add_device(device: Device) -> None:
        add_readings(device, device.entity_keys())

    @callback
    def add_reading(device: Device, key: str) -> None:
        add_readings(device, [key])

    for device in server.devices.values():
        add_device(device)
    entry.async_on_unload(
        async_dispatcher_connect(hass, signal_device_added(entry.entry_id), add_device)
    )
    entry.async_on_unload(
        async_dispatcher_connect(hass, signal_new_reading(entry.entry_id), add_reading)
    )


class ReadingEvent(ReadingEntity, EventEntity):
    """A `button` reading: press, double press, long press, release."""

    _attr_device_class = EventDeviceClass.BUTTON
    _attr_event_types = list(BUTTON_EVENTS.values())
    _attr_translation_key = "button"

    @property
    def available(self) -> bool:
        """Events are not states: only an unreachable device makes them unavailable."""
        return not self.device.unreachable

    async def async_added_to_hass(self) -> None:
        """Fire on the device's events; also the one that made this entity."""
        await super().async_added_to_hass()
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, signal_device_event(self.device.eui), self._async_event
            )
        )
        if (first := self.device.first_events.pop(self._key, None)) is not None:
            self._async_event(self._key, first)

    @callback
    def _async_event(self, key: str, event_type: str) -> None:
        if key == self._key:
            self._trigger_event(event_type)
            self.async_write_ha_state()
