"""The ConnectSensors integration: the network server for ConnectSensors LoRa sensors."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PORT, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import (
    config_validation as cv,
    device_registry as dr,
    issue_registry as ir,
)
from homeassistant.helpers.storage import Store
from homeassistant.helpers.typing import ConfigType

from .const import DOMAIN
from .entity import mailbox_device_id
from .server import STATE_STORAGE_KEY, STATE_STORAGE_VERSION, NetworkServer
from .services import async_setup_services

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.BINARY_SENSOR, Platform.EVENT, Platform.SENSOR]

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

type ConnectSensorsConfigEntry = ConfigEntry[NetworkServer]


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register the actions, which work while a network is loaded."""
    async_setup_services(hass)
    return True


async def async_migrate_entry(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> bool:
    """Version 2 (connectsensors 0.4.0) has no network key (02 section 5.2). The
    device records are migrated with the server's state store."""
    if entry.version > 2:
        return False
    if entry.version == 1:
        data = {k: v for k, v in entry.data.items() if k != "k_net"}
        hass.config_entries.async_update_entry(entry, data=data, version=2)
        _LOGGER.info("Migrated the network to version 2: the network key is gone")
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> bool:
    """Start the network server and its entities."""
    server = NetworkServer(hass, entry)
    try:
        await server.async_start()
    except OSError as err:
        await server.async_stop()
        server.async_clear_issues()
        raise ConfigEntryNotReady(
            translation_domain=DOMAIN,
            translation_key="port_unavailable",
            translation_placeholders={"port": str(server.port), "error": str(err)},
        ) from err
    entry.runtime_data = server
    _remove_old_mailboxes(hass, entry)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


def _remove_old_mailboxes(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> None:
    """Drop the devices of mailbox servers no longer in use."""
    federation = entry.runtime_data.federation
    servers = federation.servers.values() if federation else ()
    keep = {mailbox_device_id(entry.entry_id, s) for s in servers}
    registry = dr.async_get(hass)
    for device in dr.async_entries_for_config_entry(registry, entry.entry_id):
        ids = {i for d, i in device.identifiers if d == DOMAIN}
        if any(i.startswith("mailbox_") for i in ids) and not ids & keep:
            registry.async_update_device(device.id, remove_config_entry_id=entry.entry_id)


async def _async_update_listener(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> None:
    """Subentries added or removed are picked up live; a new port or new federation
    options need a reload."""
    server = entry.runtime_data
    if entry.data[CONF_PORT] != server.port or dict(entry.options) != server.options:
        hass.config_entries.async_schedule_reload(entry.entry_id)
        return
    server.async_sync_subentries()


async def async_unload_entry(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> bool:
    """Stop the server, flushing its state."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.async_stop()
        entry.runtime_data.async_clear_issues()
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: ConnectSensorsConfigEntry) -> None:
    """Forget devices and proxies. Root downlink counters are kept (02 section 8.4)."""
    registry = ir.async_get(hass)
    for domain, issue_id in list(registry.issues):
        if domain == DOMAIN:
            ir.async_delete_issue(hass, DOMAIN, issue_id)
    await Store[dict[str, object]](hass, STATE_STORAGE_VERSION, STATE_STORAGE_KEY).async_remove()
