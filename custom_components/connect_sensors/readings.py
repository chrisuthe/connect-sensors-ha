"""How readings (spec 03) become Home Assistant entities: the default mapping of
spec 03 section 7, for the kinds milestone 1 supports."""

from dataclasses import dataclass

from homeassistant.components.binary_sensor.const import BinarySensorDeviceClass
from homeassistant.components.sensor import SensorDeviceClass, SensorStateClass
from homeassistant.const import (
    CONCENTRATION_PARTS_PER_MILLION,
    LIGHT_LUX,
    PERCENTAGE,
    UnitOfElectricPotential,
    UnitOfPressure,
    UnitOfTemperature,
)


@dataclass(frozen=True, kw_only=True)
class BinaryKind:
    """A one-byte state kind shown as a binary_sensor."""

    device_class: BinarySensorDeviceClass | None
    diagnostic: bool = False


@dataclass(frozen=True, kw_only=True)
class SensorKind:
    """A numeric kind shown as a sensor."""

    device_class: SensorDeviceClass | None
    unit: str | None
    scale: float = 1.0
    unknown: int | None = None
    diagnostic: bool = False


BINARY_KINDS: dict[str, BinaryKind] = {
    "contact": BinaryKind(device_class=BinarySensorDeviceClass.OPENING),
    "motion": BinaryKind(device_class=BinarySensorDeviceClass.MOTION),
    "leak": BinaryKind(device_class=BinarySensorDeviceClass.MOISTURE),
    "tamper": BinaryKind(device_class=BinarySensorDeviceClass.TAMPER, diagnostic=True),
    "vibration": BinaryKind(device_class=BinarySensorDeviceClass.VIBRATION),
    "binary": BinaryKind(device_class=None),
}

# Binary kinds send 0xFF when the value is unknown (03 section 4.1).
BINARY_UNKNOWN = 0xFF

SENSOR_KINDS: dict[str, SensorKind] = {
    "battery_percent": SensorKind(
        device_class=SensorDeviceClass.BATTERY, unit=PERCENTAGE, unknown=0xFF, diagnostic=True
    ),
    "percent": SensorKind(device_class=None, unit=PERCENTAGE, unknown=0xFF),
    "temperature": SensorKind(
        device_class=SensorDeviceClass.TEMPERATURE,
        unit=UnitOfTemperature.CELSIUS,
        scale=0.01,
        unknown=-0x8000,
    ),
    "humidity": SensorKind(
        device_class=SensorDeviceClass.HUMIDITY, unit=PERCENTAGE, scale=0.01, unknown=0xFFFF
    ),
    "battery_voltage": SensorKind(
        device_class=SensorDeviceClass.VOLTAGE,
        unit=UnitOfElectricPotential.MILLIVOLT,
        unknown=0xFFFF,
        diagnostic=True,
    ),
    "illuminance": SensorKind(
        device_class=SensorDeviceClass.ILLUMINANCE, unit=LIGHT_LUX, unknown=0xFFFF
    ),
    "pressure": SensorKind(
        device_class=SensorDeviceClass.ATMOSPHERIC_PRESSURE,
        unit=UnitOfPressure.HPA,
        scale=0.1,
        unknown=0xFFFF,
    ),
    "co2": SensorKind(
        device_class=SensorDeviceClass.CO2, unit=CONCENTRATION_PARTS_PER_MILLION, unknown=0xFFFF
    ),
    "voltage": SensorKind(
        device_class=SensorDeviceClass.VOLTAGE,
        unit=UnitOfElectricPotential.MILLIVOLT,
        unknown=0xFFFF,
    ),
}

SENSOR_STATE_CLASS = SensorStateClass.MEASUREMENT

# Profile overrides of the default mapping (03 section 9), for the reference profiles.
# The contact profile's channel 0 is a door.
PROFILE_BINARY_OVERRIDES: dict[tuple[str, int, str], BinarySensorDeviceClass] = {
    ("CS.CONTACT-1", 0, "contact"): BinarySensorDeviceClass.DOOR,
}


def reading_key(channel: int, kind: str) -> str:
    """Key of a (channel, kind) pair in a device's stored readings."""
    return f"{channel}:{kind}"


def split_reading_key(key: str) -> tuple[int, str]:
    """Inverse of reading_key."""
    channel, kind = key.split(":", 1)
    return int(channel), kind


def is_supported(kind: str) -> bool:
    """Whether milestone 1 creates an entity for this kind."""
    return kind in BINARY_KINDS or kind in SENSOR_KINDS
