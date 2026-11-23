"""Config flow: the network, then proxies and devices as subentries (spec 07 section 11.1)."""

from __future__ import annotations

import secrets
import ssl
from typing import Any
from urllib.parse import urlsplit

import connectsensors as cs
from homeassistant.components.network import async_get_source_ip
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    ConfigSubentryFlow,
    OptionsFlow,
    SubentryFlowResult,
)
from homeassistant.const import CONF_NAME, CONF_PORT
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.selector import (
    BooleanSelector,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)
import probatio

from .const import (
    CONF_DEV_EUI,
    CONF_FEDERATION_DOWNLINKS,
    CONF_FEDERATION_OWNER,
    CONF_FEDERATION_PUBLISH,
    CONF_HEARTBEAT,
    CONF_IGNORE_FEDERATED_EVENTS,
    CONF_IGNORE_FEDERATED_READINGS,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_MAILBOX_CERTIFICATE,
    CONF_MAILBOX_SERVERS,
    CONF_PAIRING_CODE,
    CONF_PROFILE,
    CONF_REGION,
    DEFAULT_PORT,
    DOMAIN,
    HEARTBEAT_MAX,
    HEARTBEAT_MIN,
    REGIONS,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
)
from .profiles import default_heartbeat

PORT_SELECTOR = NumberSelector(
    NumberSelectorConfig(min=1, max=65535, step=1, mode=NumberSelectorMode.BOX)
)
HEARTBEAT_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=HEARTBEAT_MIN,
        max=HEARTBEAT_MAX,
        step=1,
        mode=NumberSelectorMode.BOX,
        unit_of_measurement="min",
    )
)


async def async_proxy_snippet(hass: HomeAssistant, entry: ConfigEntry, key: str) -> str:
    """The cs_proxy configuration for a proxy with this key (06)."""
    host = await async_get_source_ip(hass)
    return (
        "cs_proxy:\n"
        "  radio_id: lora\n"
        f"  region: {entry.data[CONF_REGION]}\n"
        "  server:\n"
        f"    host: {host}\n"
        f"    port: {entry.data[CONF_PORT]}\n"
        f'    key: "{key}"\n'
    )


def proxy_keys(entry: ConfigEntry) -> set[str]:
    """The keys the network's proxies hold."""
    return {
        s.data[CONF_KEY].lower()
        for s in entry.subentries.values()
        if s.subentry_type == SUBENTRY_PROXY
    }


def new_proxy_key(entry: ConfigEntry) -> str:
    """A key no proxy holds: a proxy that accepts downlinks needs its own (05 4.2)."""
    used = proxy_keys(entry)
    while (key := secrets.token_hex(16)) in used:
        pass
    return key


class ConnectSensorsConfigFlow(ConfigFlow, domain=DOMAIN):
    """Create the network."""

    VERSION = 2

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Choose the region profile and port."""
        if user_input is not None:
            return self.async_create_entry(
                title="ConnectSensors",
                data={
                    CONF_REGION: user_input[CONF_REGION].upper(),
                    CONF_PORT: int(user_input[CONF_PORT]),
                },
            )
        return self.async_show_form(
            step_id="user",
            data_schema=probatio.Schema(
                {
                    probatio.Required(CONF_REGION, default="us915"): SelectSelector(
                        SelectSelectorConfig(
                            options=[region.lower() for region in REGIONS],
                            mode=SelectSelectorMode.DROPDOWN,
                            translation_key=CONF_REGION,
                        )
                    ),
                    probatio.Required(CONF_PORT, default=DEFAULT_PORT): PORT_SELECTOR,
                }
            ),
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Change the UDP port. The region stays."""
        entry = self._get_reconfigure_entry()
        if user_input is not None:
            return self.async_update_and_abort(
                entry, data_updates={CONF_PORT: int(user_input[CONF_PORT])}
            )
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=probatio.Schema(
                {probatio.Required(CONF_PORT, default=entry.data[CONF_PORT]): PORT_SELECTOR}
            ),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> FederationOptionsFlow:
        """Federation through mailbox servers (spec 09)."""
        return FederationOptionsFlow()

    @classmethod
    @callback
    def async_get_supported_subentry_types(
        cls, config_entry: ConfigEntry
    ) -> dict[str, type[ConfigSubentryFlow]]:
        """Proxies and devices."""
        return {SUBENTRY_PROXY: ProxySubentryFlow, SUBENTRY_DEVICE: DeviceSubentryFlow}


OPT_INS = (CONF_FEDERATION_OWNER, CONF_FEDERATION_PUBLISH, CONF_FEDERATION_DOWNLINKS)
FEDERATION_FLAGS = (*OPT_INS, CONF_IGNORE_FEDERATED_EVENTS, CONF_IGNORE_FEDERATED_READINGS)


def mailbox_url_error(url: str) -> str | None:
    """Why a mailbox URL cannot be used: 09 section 12.1 requires wss://."""
    try:
        parts = urlsplit(url)
        parts.port  # noqa: B018 - raises for a bad port
    except ValueError:
        return "invalid_url"
    if parts.scheme != "wss" or not parts.hostname:
        return "invalid_url"
    return None


def certificate_error(pem: str) -> str | None:
    """Whether a pasted certificate can be trusted for the mailbox servers."""
    try:
        ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).load_verify_locations(cadata=pem)
    except ssl.SSLError, ValueError:
        return "invalid_certificate"
    return None


class FederationOptionsFlow(OptionsFlow):
    """Mailbox servers and the three federation opt-ins, each off by default (09)."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """One form: servers, an optional pinned certificate and the opt-ins."""
        errors: dict[str, str] = {}
        if user_input is not None:
            urls = [u.strip() for u in user_input.get(CONF_MAILBOX_SERVERS, []) if u.strip()]
            pem = (user_input.get(CONF_MAILBOX_CERTIFICATE) or "").strip()
            if any(mailbox_url_error(u) for u in urls):
                errors[CONF_MAILBOX_SERVERS] = "invalid_url"
            elif not urls and any(user_input.get(k) for k in OPT_INS):
                errors[CONF_MAILBOX_SERVERS] = "no_servers"
            if pem and certificate_error(pem):
                errors[CONF_MAILBOX_CERTIFICATE] = "invalid_certificate"
            if not errors:
                options: dict[str, Any] = {
                    CONF_MAILBOX_SERVERS: list(dict.fromkeys(urls)),
                    **{k: bool(user_input.get(k)) for k in FEDERATION_FLAGS},
                }
                if pem:
                    options[CONF_MAILBOX_CERTIFICATE] = pem
                return self.async_create_entry(data=options)
        current = user_input or self.config_entry.options
        return self.async_show_form(
            step_id="init",
            data_schema=probatio.Schema(
                {
                    probatio.Optional(
                        CONF_MAILBOX_SERVERS, default=list(current.get(CONF_MAILBOX_SERVERS, []))
                    ): TextSelector(TextSelectorConfig(type=TextSelectorType.URL, multiple=True)),
                    **{
                        probatio.Optional(k, default=bool(current.get(k, False))): BooleanSelector()
                        for k in FEDERATION_FLAGS
                    },
                    probatio.Optional(
                        CONF_MAILBOX_CERTIFICATE,
                        description={"suggested_value": current.get(CONF_MAILBOX_CERTIFICATE, "")},
                    ): TextSelector(TextSelectorConfig(multiline=True)),
                }
            ),
            errors=errors,
        )


class ProxySubentryFlow(ConfigSubentryFlow):
    """Add a proxy: generate its key and show the cs_proxy snippet (05 section 4.2, 06)."""

    _key: str | None = None

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        """Show the snippet; the proxy is added on submit."""
        entry = self._get_entry()
        if self._key is None:
            self._key = new_proxy_key(entry)
        if user_input is not None:
            # Another flow may have added a proxy with this key meanwhile.
            if self._key in proxy_keys(entry):
                return self.async_abort(reason="duplicate_key")
            return self.async_create_entry(title=user_input[CONF_NAME], data={CONF_KEY: self._key})
        count = sum(1 for s in entry.subentries.values() if s.subentry_type == SUBENTRY_PROXY)
        return self.async_show_form(
            step_id="user",
            data_schema=probatio.Schema(
                {probatio.Required(CONF_NAME, default=f"Proxy {count + 1}"): str}
            ),
            description_placeholders={
                "snippet": await async_proxy_snippet(self.hass, entry, self._key)
            },
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Show the snippet again and allow a rename."""
        subentry = self._get_reconfigure_subentry()
        if user_input is not None:
            return self.async_update_and_abort(
                self._get_entry(), subentry, title=user_input[CONF_NAME]
            )
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=probatio.Schema(
                {probatio.Required(CONF_NAME, default=subentry.title): str}
            ),
            description_placeholders={
                "snippet": await async_proxy_snippet(
                    self.hass, self._get_entry(), subentry.data[CONF_KEY]
                )
            },
        )


class DeviceSubentryFlow(ConfigSubentryFlow):
    """Add a device by pairing code (04 section 3.2 and 4.4)."""

    _code: dict[str, str]
    _name: str

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        """Enter the pairing code and a name."""
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                code = cs.qr_parse(user_input[CONF_PAIRING_CODE])
            except ValueError as err:
                errors[CONF_PAIRING_CODE] = (
                    "checksum_mismatch" if "checksum" in str(err) else "invalid_code"
                )
            else:
                entry = self._get_entry()
                if any(s.unique_id == code["dev_eui"] for s in entry.subentries.values()):
                    return self.async_abort(reason="already_configured")
                self._code = code
                self._name = (user_input.get(CONF_NAME) or "").strip() or (
                    f"{code['profile']} {code['dev_eui'][-4:].upper()}"
                )
                if code["region"] != entry.data[CONF_REGION]:
                    return await self.async_step_region_mismatch()
                return self._create()
        return self.async_show_form(
            step_id="user",
            data_schema=probatio.Schema(
                {
                    probatio.Required(CONF_PAIRING_CODE): TextSelector(),
                    probatio.Optional(CONF_NAME): TextSelector(),
                }
            ),
            errors=errors,
        )

    async def async_step_region_mismatch(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Warn and ask for confirmation (04 section 3.2, rule 6)."""
        if user_input is not None:
            return self._create()
        return self.async_show_form(
            step_id="region_mismatch",
            description_placeholders={
                "device_region": self._code["region"],
                "network_region": self._get_entry().data[CONF_REGION],
            },
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Rename the device, or tell Home Assistant its heartbeat interval."""
        subentry = self._get_reconfigure_subentry()
        if user_input is not None:
            return self.async_update_and_abort(
                self._get_entry(),
                subentry,
                title=user_input[CONF_NAME],
                data={**subentry.data, CONF_HEARTBEAT: int(user_input[CONF_HEARTBEAT])},
            )
        heartbeat = subentry.data.get(CONF_HEARTBEAT) or default_heartbeat(
            subentry.data[CONF_PROFILE]
        )
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=probatio.Schema(
                {
                    probatio.Required(CONF_NAME, default=subentry.title): TextSelector(),
                    probatio.Required(CONF_HEARTBEAT, default=heartbeat): HEARTBEAT_SELECTOR,
                }
            ),
        )

    def _create(self) -> SubentryFlowResult:
        return self.async_create_entry(
            title=self._name,
            data={
                CONF_DEV_EUI: self._code["dev_eui"],
                CONF_K_ROOT: self._code["k_root"],
                CONF_PROFILE: self._code["profile"],
                CONF_REGION: self._code["region"],
            },
            unique_id=self._code["dev_eui"],
        )
