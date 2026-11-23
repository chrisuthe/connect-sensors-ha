"""Binary sensors: state readings (contact, leak, ...) and proxy connectivity."""

from __future__ import annotations

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.components.binary_sensor.const import BinarySensorDeviceClass
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import ConnectSensorsConfigEntry
from .const import signal_device_added, signal_new_reading, signal_proxy_bound
from .entity import DeviceEntity, MailboxEntity, ProxyEntity, ReadingEntity
from .federation import MailboxServer
from .readings import BINARY_KINDS, BINARY_UNKNOWN, split_reading_key
from .server import Device, Proxy

PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConnectSensorsConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Add entities for known devices and proxies, and for new ones as they appear."""
    server = entry.runtime_data

    @callback
    def add_readings(device: Device, keys: list[str]) -> None:
        entities = [
            ReadingBinarySensor(device, key)
            for key in keys
            if split_reading_key(key)[1] in BINARY_KINDS
        ]
        if entities:
            async_add_entities(entities, config_subentry_id=device.subentry_id)

    @callback
    def add_device(device: Device) -> None:
        async_add_entities([DeviceReachable(device)], config_subentry_id=device.subentry_id)
        add_readings(device, device.entity_keys())

    @callback
    def add_reading(device: Device, key: str) -> None:
        add_readings(device, [key])

    @callback
    def add_proxy(proxy: Proxy) -> None:
        async_add_entities([ProxyConnected(proxy)], config_subentry_id=proxy.subentry_id)

    if server.federation is not None:
        async_add_entities(
            MailboxConnected(entry.entry_id, s) for s in server.federation.servers.values()
        )
    for device in server.devices.values():
        add_device(device)
    for proxy in server.proxies.values():
        if proxy.proxy_id is not None:
            add_proxy(proxy)
    entry.async_on_unload(
        async_dispatcher_connect(hass, signal_device_added(entry.entry_id), add_device)
    )
    entry.async_on_unload(
        async_dispatcher_connect(hass, signal_new_reading(entry.entry_id), add_reading)
    )
    entry.async_on_unload(
        async_dispatcher_connect(hass, signal_proxy_bound(entry.entry_id), add_proxy)
    )


class ReadingBinarySensor(ReadingEntity, BinarySensorEntity):
    """A one-byte state reading (03 section 4.1)."""

    def __init__(self, device: Device, key: str) -> None:
        super().__init__(device, key)
        _, kind = split_reading_key(key)
        spec = BINARY_KINDS[kind]
        self._attr_device_class = spec.device_class
        if self.channel is not None and self.channel.binary_device_class is not None:
            self._attr_device_class = self.channel.binary_device_class
        if self._attr_device_class is None:
            self._attr_translation_key = kind
        if spec.diagnostic:
            self._attr_entity_category = EntityCategory.DIAGNOSTIC

    @property
    def is_on(self) -> bool | None:
        """Unknown (0xFF) shows as unknown, never as off (03 section 6)."""
        value = self.device.readings.get(self._key)
        if value is None or value == BINARY_UNKNOWN:
            return None
        return bool(value)


class DeviceReachable(DeviceEntity, BinarySensorEntity):
    """Heard within three heartbeat intervals and 5 minutes (07 sections 8.1 and 11.2)."""

    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_translation_key = "reachable"

    def __init__(self, device: Device) -> None:
        super().__init__(device, "reachable")

    @property
    def is_on(self) -> bool | None:
        """Unknown until the device is paired and supervised."""
        return not self.device.unreachable if self.device.supervised else None


class ProxyConnected(ProxyEntity, BinarySensorEntity):
    """A HELLO within the last 180 seconds (07 section 8.2)."""

    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, proxy: Proxy) -> None:
        super().__init__(proxy, "connected")

    @property
    def is_on(self) -> bool:
        """Connected."""
        return self.proxy.connected


class MailboxConnected(MailboxEntity, BinarySensorEntity):
    """Connected and authenticated to a mailbox server (09 section 12.3)."""

    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, entry_id: str, server: MailboxServer) -> None:
        super().__init__(entry_id, server, "connected")

    @property
    def is_on(self) -> bool:
        """Connected."""
        return self.mailbox.up

    @property
    def extra_state_attributes(self) -> dict[str, str | int | None]:
        """Round-trip time and why the last attempt failed."""
        return {"rtt_ms": self.mailbox.rtt_ms, "last_error": self.mailbox.last_error}
