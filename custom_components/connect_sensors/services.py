"""Actions that queue commands for a sensor (07 section 11.4)."""

from __future__ import annotations

import struct

import connectsensors as cs
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import ATTR_DEVICE_ID
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import config_validation as cv
import probatio

from .const import CMD_SET_VALUE, DOMAIN, HEARTBEAT_MAX, HEARTBEAT_MIN, STATE_PAIRED
from .readings import BINARY_KINDS, SENSOR_KINDS
from .server import Device, NetworkServer

SERVICE_SET_HEARTBEAT = "set_heartbeat"
SERVICE_SET_VALUE = "set_value"
ATTR_MINUTES = "minutes"
ATTR_CHANNEL = "channel"
ATTR_KIND = "kind"
ATTR_VALUE = "value"

KIND_IDS = {name: kind for kind, (name, _) in cs.KINDS.items()}
# Kinds a value can be set for: states, in the units their entities show.
SETTABLE_KINDS = sorted({*BINARY_KINDS, *SENSOR_KINDS, "enum", "int16", "int32"})

SET_HEARTBEAT_SCHEMA = probatio.Schema(
    {
        probatio.Required(ATTR_DEVICE_ID): cv.string,
        probatio.Required(ATTR_MINUTES): probatio.All(
            probatio.Coerce(int), probatio.Range(min=HEARTBEAT_MIN, max=HEARTBEAT_MAX)
        ),
    }
)
SET_VALUE_SCHEMA = probatio.Schema(
    {
        probatio.Required(ATTR_DEVICE_ID): cv.string,
        probatio.Required(ATTR_CHANNEL): probatio.All(
            probatio.Coerce(int), probatio.Range(min=0, max=255)
        ),
        probatio.Required(ATTR_KIND): probatio.In(SETTABLE_KINDS),
        probatio.Required(ATTR_VALUE): probatio.Coerce(float),
    }
)


@callback
def async_setup_services(hass: HomeAssistant) -> None:
    """Register the actions."""
    hass.services.async_register(
        DOMAIN, SERVICE_SET_HEARTBEAT, _async_set_heartbeat, schema=SET_HEARTBEAT_SCHEMA
    )
    hass.services.async_register(
        DOMAIN, SERVICE_SET_VALUE, _async_set_value, schema=SET_VALUE_SCHEMA
    )


def _target(call: ServiceCall) -> tuple[NetworkServer, Device]:
    """The loaded network and paired sensor an action targets."""
    device_id: str = call.data[ATTR_DEVICE_ID]
    for entry in call.hass.config_entries.async_entries(DOMAIN):
        if entry.state is not ConfigEntryState.LOADED:
            continue
        server: NetworkServer = entry.runtime_data
        if (device := server.device_for(device_id)) is not None:
            if device.state != STATE_PAIRED:
                raise ServiceValidationError(
                    translation_domain=DOMAIN,
                    translation_key="not_paired",
                    translation_placeholders={"name": device.name},
                )
            return server, device
    raise ServiceValidationError(
        translation_domain=DOMAIN,
        translation_key="unknown_device",
        translation_placeholders={"device_id": device_id},
    )


async def _queue(
    server: NetworkServer, device: Device, name: str, slot: str, cmd: int, args: bytes
) -> None:
    if not await server.async_queue(device, name, slot, cmd, args):
        raise ServiceValidationError(
            translation_domain=DOMAIN,
            translation_key="queue_full",
            translation_placeholders={"name": device.name},
        )


async def _async_set_heartbeat(call: ServiceCall) -> None:
    """Queue SET_HEARTBEAT (02 section 10.3)."""
    server, device = _target(call)
    minutes: int = call.data[ATTR_MINUTES]
    await _queue(
        server,
        device,
        "set_heartbeat",
        "heartbeat",
        cs.CMD_SET_HEARTBEAT,
        minutes.to_bytes(2, "big"),
    )


async def _async_set_value(call: ServiceCall) -> None:
    """Queue command 0x40 for a writable channel (03 section 8)."""
    server, device = _target(call)
    channel: int = call.data[ATTR_CHANNEL]
    kind: str = call.data[ATTR_KIND]
    profile = device.profile_def
    if profile is not None and not profile.writable(channel, kind):
        raise ServiceValidationError(
            translation_domain=DOMAIN,
            translation_key="not_writable",
            translation_placeholders={"name": device.name, "channel": str(channel), "kind": kind},
        )
    value: float = call.data[ATTR_VALUE]
    if (sensor := SENSOR_KINDS.get(kind)) is not None:
        value /= sensor.scale
    try:
        args = cs.encode_reading(channel, KIND_IDS[kind], round(value))
    except struct.error as err:
        raise ServiceValidationError(
            translation_domain=DOMAIN,
            translation_key="value_out_of_range",
            translation_placeholders={"kind": kind},
        ) from err
    await _queue(server, device, "set_value", f"value_{channel}", CMD_SET_VALUE, args)
