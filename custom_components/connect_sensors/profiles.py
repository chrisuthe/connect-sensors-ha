"""Built-in device profiles (spec 03 section 9), keyed by the pairing code's profile id.

Control-plane profile distribution (spec 10) is not implemented: a device whose
profile is not listed here gets entities from the readings it sends (03 section 9.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from homeassistant.components.binary_sensor.const import BinarySensorDeviceClass

from .readings import reading_key

# cs_sensor's default heartbeat (its `heartbeat:` option), for profiles that do not
# give one.
DEFAULT_HEARTBEAT_MINUTES = 60


@dataclass(frozen=True, kw_only=True)
class Channel:
    """One (channel, kind) the device sends (03 section 9.2, `channels`)."""

    channel: int
    kind: str
    name: str | None = None
    binary_device_class: BinarySensorDeviceClass | None = None
    writable: bool = False

    @property
    def key(self) -> str:
        """Its key in a device's readings."""
        return reading_key(self.channel, self.kind)


@dataclass(frozen=True, kw_only=True)
class Profile:
    """One device model (03 section 9.2)."""

    profile: str
    name: str
    manufacturer: str | None
    model: str
    power: Literal["battery", "mains"] | None = None
    heartbeat_minutes: int
    channels: tuple[Channel, ...] = ()
    # Firmware built from YAML (cs_sensor) decides its own channels, so any channel
    # may accept SET_VALUE; a fixed design lists its writable ones.
    any_writable: bool = False

    def channel(self, key: str) -> Channel | None:
        """The profile's entry for a reading key, if it lists one."""
        return next((c for c in self.channels if c.key == key), None)

    def writable(self, channel: int, kind: str) -> bool:
        """Whether SET_VALUE (0x40) may target this channel (03 section 8)."""
        if self.any_writable:
            return True
        entry = self.channel(reading_key(channel, kind))
        return entry is not None and entry.writable


PROFILES: dict[str, Profile] = {
    p.profile: p
    for p in (
        # spec 03 section 9.2.
        Profile(
            profile="CS.CONTACT-1",
            name="ConnectSensors contact sensor",
            manufacturer="ConnectSensors reference design",
            model="Contact v1",
            power="battery",
            heartbeat_minutes=30,
            channels=(
                Channel(
                    channel=0,
                    kind="contact",
                    name="Door",
                    binary_device_class=BinarySensorDeviceClass.DOOR,
                ),
                Channel(channel=0, kind="tamper", name="Tamper"),
                Channel(channel=255, kind="battery_percent"),
            ),
        ),
        # spec 03 section 9.2.
        Profile(
            profile="CS.CLIMATE-1",
            name="ConnectSensors climate sensor",
            manufacturer="ConnectSensors reference design",
            model="Climate v1",
            power="battery",
            heartbeat_minutes=30,
            channels=(
                Channel(channel=0, kind="temperature"),
                Channel(channel=0, kind="humidity"),
                Channel(channel=255, kind="battery_percent"),
            ),
        ),
        # cs_sensor's default profile id: channels come from its YAML, so from readings.
        Profile(
            profile="ESPHOME",
            name="ESPHome sensor",
            manufacturer="ESPHome",
            model="cs_sensor",
            heartbeat_minutes=DEFAULT_HEARTBEAT_MINUTES,
            any_writable=True,
        ),
    )
}


def get_profile(profile_id: str) -> Profile | None:
    """The built-in profile for an id, or None for an unknown one."""
    return PROFILES.get(profile_id)


def default_heartbeat(profile_id: str) -> int:
    """The heartbeat interval in minutes a profile starts with (08 section 4.2)."""
    profile = PROFILES.get(profile_id)
    return profile.heartbeat_minutes if profile else DEFAULT_HEARTBEAT_MINUTES
