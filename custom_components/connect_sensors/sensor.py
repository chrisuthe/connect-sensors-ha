"""Sensors: numeric readings, device diagnostics and proxy counters."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import (
    SIGNAL_STRENGTH_DECIBELS,
    SIGNAL_STRENGTH_DECIBELS_MILLIWATT,
    EntityCategory,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import ConnectSensorsConfigEntry
from .const import (
    SIGNAL_DEVICE_ADDED,
    SIGNAL_NEW_READING,
    SIGNAL_PROXY_BOUND,
    STATE_EXCHANGED,
    STATE_PAIRED,
    STATE_SCANNED,
)
from .entity import DeviceEntity, ProxyEntity, ReadingEntity
from .readings import SENSOR_KINDS, SENSOR_STATE_CLASS, split_reading_key
from .server import Device, Proxy

PARALLEL_UPDATES = 0


@dataclass(frozen=True, kw_only=True)
class DeviceSensorDescription(SensorEntityDescription):
    """A diagnostic of a sensor device."""

    value_fn: Callable[[Device], str | int | float | datetime | None]
    attributes_fn: Callable[[Device], dict[str, Any]] | None = None


@dataclass(frozen=True, kw_only=True)
class ProxySensorDescription(SensorEntityDescription):
    """A diagnostic of a proxy."""

    value_fn: Callable[[Proxy], int | None]


def _pairing(device: Device) -> str:
    return "stalled" if device.stalled and device.state != STATE_PAIRED else device.state


def _command(device: Device) -> str | None:
    """Pending while any command is queued, else how the last one ended (07 section 7)."""
    if device.commands:
        return "pending"
    return device.last_command["result"] if device.last_command else None


def _command_attributes(device: Device) -> dict[str, Any]:
    last = device.last_command
    return {
        "queued": [c.name for c in device.commands],
        "last_command": last["name"] if last else None,
        "last_result_at": last["at"] if last else None,
    }


DEVICE_SENSORS: tuple[DeviceSensorDescription, ...] = (
    DeviceSensorDescription(
        key="pairing",
        translation_key="pairing",
        device_class=SensorDeviceClass.ENUM,
        options=[STATE_SCANNED, STATE_EXCHANGED, STATE_PAIRED, "stalled"],
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=_pairing,
    ),
    DeviceSensorDescription(
        key="last_seen",
        translation_key="last_seen",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.last_seen,
    ),
    DeviceSensorDescription(
        key="rssi",
        device_class=SensorDeviceClass.SIGNAL_STRENGTH,
        native_unit_of_measurement=SIGNAL_STRENGTH_DECIBELS_MILLIWATT,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.rssi,
    ),
    DeviceSensorDescription(
        key="snr",
        translation_key="snr",
        native_unit_of_measurement=SIGNAL_STRENGTH_DECIBELS,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.snr,
    ),
    DeviceSensorDescription(
        key="proxies",
        translation_key="proxies",
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.proxy_count if d.supervised else None,
        attributes_fn=lambda d: {"single_path": d.single_path},
    ),
    DeviceSensorDescription(
        key="command",
        translation_key="command",
        device_class=SensorDeviceClass.ENUM,
        options=["pending", "delivered", "failed"],
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=_command,
        attributes_fn=_command_attributes,
    ),
    DeviceSensorDescription(
        key="uplink_counter",
        translation_key="uplink_counter",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.last_fcnt or None,
    ),
)

# HELLO counters, in the order of 05 section 5.2.
_HELLO_COUNTERS = (
    "heard",
    "forwarded",
    "duplicates",
    "invalid",
    "rate_limited",
    "tx_ok",
    "tx_failed",
)


def _hello_counter(index: int) -> Callable[[Proxy], int | None]:
    def value(proxy: Proxy) -> int | None:
        return int(proxy.hello["counters"][index]) if proxy.hello else None

    return value


PROXY_SENSORS: tuple[ProxySensorDescription, ...] = tuple(
    ProxySensorDescription(
        key=name,
        translation_key=name,
        state_class=SensorStateClass.TOTAL_INCREASING,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=_hello_counter(i),
    )
    for i, name in enumerate(_HELLO_COUNTERS)
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConnectSensorsConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Add entities for known devices and proxies, and for new ones as they appear."""
    server = entry.runtime_data

    @callback
    def add_device(device: Device) -> None:
        entities: list[SensorEntity] = [DeviceSensor(device, d) for d in DEVICE_SENSORS]
        entities.extend(_readings(device, device.entity_keys()))
        async_add_entities(entities, config_subentry_id=device.subentry_id)

    @callback
    def add_reading(device: Device, key: str) -> None:
        if entities := _readings(device, [key]):
            async_add_entities(entities, config_subentry_id=device.subentry_id)

    @callback
    def add_proxy(proxy: Proxy) -> None:
        async_add_entities(
            [ProxySensor(proxy, d) for d in PROXY_SENSORS], config_subentry_id=proxy.subentry_id
        )

    for device in server.devices.values():
        add_device(device)
    for proxy in server.proxies.values():
        if proxy.proxy_id is not None:
            add_proxy(proxy)
    entry.async_on_unload(async_dispatcher_connect(hass, SIGNAL_DEVICE_ADDED, add_device))
    entry.async_on_unload(async_dispatcher_connect(hass, SIGNAL_NEW_READING, add_reading))
    entry.async_on_unload(async_dispatcher_connect(hass, SIGNAL_PROXY_BOUND, add_proxy))


def _readings(device: Device, keys: list[str]) -> list[SensorEntity]:
    return [ReadingSensor(device, key) for key in keys if split_reading_key(key)[1] in SENSOR_KINDS]


class ReadingSensor(ReadingEntity, SensorEntity):
    """A numeric reading (03 sections 4.1 and 4.2)."""

    _attr_state_class = SENSOR_STATE_CLASS

    def __init__(self, device: Device, key: str) -> None:
        super().__init__(device, key)
        _, kind = split_reading_key(key)
        self._kind = SENSOR_KINDS[kind]
        self._attr_device_class = self._kind.device_class
        self._attr_native_unit_of_measurement = self._kind.unit
        if self._kind.device_class is None:
            self._attr_translation_key = kind
        if self._kind.diagnostic:
            self._attr_entity_category = EntityCategory.DIAGNOSTIC

    @property
    def native_value(self) -> float | None:
        """Scaled to real units; the unknown sentinel shows as unknown (03 section 6)."""
        value = self.device.readings.get(self._key)
        if value is None or value == self._kind.unknown:
            return None
        if self._kind.scale == 1.0:
            return float(value)
        return round(float(value) * self._kind.scale, 2)


class DeviceSensor(DeviceEntity, SensorEntity):
    """A diagnostic of a sensor device."""

    entity_description: DeviceSensorDescription

    def __init__(self, device: Device, description: DeviceSensorDescription) -> None:
        super().__init__(device, description.key)
        self.entity_description = description

    @property
    def native_value(self) -> str | int | float | datetime | None:
        """Current value."""
        return self.entity_description.value_fn(self.device)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Extra diagnostics, for descriptions that have them."""
        fn = self.entity_description.attributes_fn
        return fn(self.device) if fn else None


class ProxySensor(ProxyEntity, SensorEntity):
    """A counter from the proxy's last HELLO."""

    entity_description: ProxySensorDescription

    def __init__(self, proxy: Proxy, description: ProxySensorDescription) -> None:
        super().__init__(proxy, description.key)
        self.entity_description = description

    @property
    def native_value(self) -> int | None:
        """Current value."""
        return self.entity_description.value_fn(self.proxy)
