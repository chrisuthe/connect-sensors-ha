"""Buttons: start pairing a device again (04 section 4.2)."""

from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import ConnectSensorsConfigEntry
from .const import DOMAIN, signal_device_added
from .entity import DeviceEntity
from .server import Device, NetworkServer

PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConnectSensorsConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """A Start pairing button for each device."""
    server = entry.runtime_data

    @callback
    def add_device(device: Device) -> None:
        async_add_entities(
            [StartPairingButton(server, device)], config_subentry_id=device.subentry_id
        )

    for device in server.devices.values():
        add_device(device)
    entry.async_on_unload(
        async_dispatcher_connect(hass, signal_device_added(entry.entry_id), add_device)
    )


class StartPairingButton(DeviceEntity, ButtonEntity):
    """Gives the user another 10 minutes to press the device's button."""

    _attr_translation_key = "start_pairing"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, server: NetworkServer, device: Device) -> None:
        super().__init__(device, "start_pairing")
        self._server = server

    async def async_press(self) -> None:
        """Re-arm the pairing deadline."""
        if not self._server.async_start_pairing(self.device):
            raise ServiceValidationError(
                translation_domain=DOMAIN,
                translation_key="already_paired",
                translation_placeholders={"name": self.device.name},
            )
