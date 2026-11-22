"""Repair flows (spec 07 section 11.5)."""

from __future__ import annotations

from homeassistant.components.repairs import RepairsFlow, RepairsFlowResult
from homeassistant.core import HomeAssistant
import probatio

from .config_flow import async_proxy_snippet, new_proxy_key
from .const import CONF_KEY


async def async_create_fix_flow(
    hass: HomeAssistant, issue_id: str, data: dict[str, str | int | float | None] | None
) -> RepairsFlow:
    """The flow that fixes an issue; only shared_key is fixable."""
    assert data is not None
    return SharedKeyRepairFlow(str(data["entry_id"]), str(data["subentry_id"]))


class SharedKeyRepairFlow(RepairsFlow):
    """Give a proxy on a shared key a key of its own (05 section 4.2)."""

    def __init__(self, entry_id: str, subentry_id: str) -> None:
        self._entry_id = entry_id
        self._subentry_id = subentry_id
        self._key: str | None = None

    async def async_step_init(self, user_input: dict[str, str] | None = None) -> RepairsFlowResult:
        """Start; user_input here carries only the issue_id."""
        return await self.async_step_confirm()

    async def async_step_confirm(
        self, user_input: dict[str, str] | None = None
    ) -> RepairsFlowResult:
        """Show the new key's snippet; it replaces the old key on submit."""
        entry = self.hass.config_entries.async_get_entry(self._entry_id)
        subentry = entry.subentries.get(self._subentry_id) if entry else None
        if entry is None or subentry is None:
            return self.async_abort(reason="proxy_removed")
        if self._key is None:
            self._key = new_proxy_key(entry)
        if user_input is not None:
            self.hass.config_entries.async_update_subentry(
                entry, subentry, data={**subentry.data, CONF_KEY: self._key}
            )
            return self.async_create_entry(data={})
        return self.async_show_form(
            step_id="confirm",
            data_schema=probatio.Schema({}),
            description_placeholders={
                "name": subentry.title,
                "snippet": await async_proxy_snippet(self.hass, entry, self._key),
            },
        )
