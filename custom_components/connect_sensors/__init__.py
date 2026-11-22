"""The ConnectSensors integration: the network server for ConnectSensors LoRa sensors."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PORT, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.storage import Store

from .const import DOMAIN
from .server import STATE_STORAGE_KEY, STORAGE_VERSION, NetworkServer

PLATFORMS = [Platform.BINARY_SENSOR, Platform.SENSOR]

type ConnectSensorsConfigEntry = ConfigEntry[NetworkServer]


async def async_setup_entry(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> bool:
    """Start the network server and its entities."""
    server = NetworkServer(hass, entry)
    try:
        await server.async_start()
    except OSError as err:
        await server.async_stop()
        raise ConfigEntryNotReady(
            translation_domain=DOMAIN,
            translation_key="port_unavailable",
            translation_placeholders={"port": str(server.port), "error": str(err)},
        ) from err
    entry.runtime_data = server
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> None:
    """Subentries added or removed are picked up live; a new port needs a reload."""
    server = entry.runtime_data
    if entry.data[CONF_PORT] != server.port:
        hass.config_entries.async_schedule_reload(entry.entry_id)
        return
    server.async_sync_subentries()


async def async_unload_entry(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> bool:
    """Stop the server, flushing its state."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.async_stop()
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> None:
    """Forget devices and proxies. Root downlink counters are kept (02 section 8.4)."""
    await Store[dict[str, object]](hass, STORAGE_VERSION, STATE_STORAGE_KEY).async_remove()
