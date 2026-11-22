"""Diagnostics download, with every key redacted (07 section 11.6)."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from . import ConnectSensorsConfigEntry
from .const import CONF_K_NET, CONF_K_ROOT, CONF_KEY

TO_REDACT = {
    CONF_K_NET,
    CONF_K_ROOT,
    CONF_KEY,
    "k_session",
    "sensor_pub",
    "frame",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConnectSensorsConfigEntry
) -> dict[str, Any]:
    """The entry, its subentries and the server's runtime state."""
    return {
        "entry": async_redact_data(entry.as_dict(), TO_REDACT),
        "server": async_redact_data(entry.runtime_data.diagnostics(), TO_REDACT),
    }
