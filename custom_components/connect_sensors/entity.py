"""Base entities for sensors (by dev_eui) and proxies (by proxy_id)."""

from __future__ import annotations

from homeassistant.helpers.device_registry import CONNECTION_NETWORK_MAC, DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import Entity

from .const import DOMAIN, signal_device_update, signal_proxy_update
from .server import Device, Proxy


def proxy_mac(proxy_id: bytes) -> str | None:
    """The MAC a proxy_id was derived from (05 section 4.1), so HA can merge the proxy
    with its ESPHome device."""
    if proxy_id[3:5] != b"\xff\xfe":
        return None
    return ":".join(f"{b:02x}" for b in proxy_id[:3] + proxy_id[5:])


class DeviceEntity(Entity):
    """An entity of one paired (or pairing) sensor."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, device: Device, suffix: str) -> None:
        self.device = device
        self._attr_unique_id = f"{device.eui}_{suffix}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, device.eui)},
            name=device.name,
            manufacturer="ConnectSensors" if device.profile.startswith("CS.") else None,
            model=device.profile,
            sw_version=device.sw_version,
            serial_number=device.eui.upper(),
        )

    async def async_added_to_hass(self) -> None:
        """Update on the device's signal."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, signal_device_update(self.device.eui), self.async_write_ha_state
            )
        )


class ReadingEntity(DeviceEntity):
    """An entity showing one of the device's readings."""

    def __init__(self, device: Device, key: str) -> None:
        super().__init__(device, key.replace(":", "_"))
        self._key = key

    @property
    def available(self) -> bool:
        """Silence is never shown as a value (07 section 8.1)."""
        return self.device.reading_available(self._key)


class ProxyEntity(Entity):
    """An entity of one proxy, once its proxy_id is known."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, proxy: Proxy, suffix: str) -> None:
        assert proxy.proxy_id is not None
        self.proxy = proxy
        pid = proxy.proxy_id.hex()
        self._attr_unique_id = f"{pid}_{suffix}"
        mac = proxy_mac(proxy.proxy_id)
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, pid)},
            connections={(CONNECTION_NETWORK_MAC, mac)} if mac else set(),
            name=proxy.name,
            model="cs_proxy",
            sw_version=proxy.hello["fw"] if proxy.hello else None,
            serial_number=pid.upper(),
        )

    async def async_added_to_hass(self) -> None:
        """Update on the proxy's signal."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, signal_proxy_update(self.proxy.subentry_id), self.async_write_ha_state
            )
        )
