"""A proxy's downlink airtime and its budget come from connectsensors (01 section 5)."""

from __future__ import annotations

from unittest.mock import patch

import connectsensors as cs
from homeassistant.const import CONF_PORT
from homeassistant.core import HomeAssistant
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.connect_sensors import server
from custom_components.connect_sensors.const import (
    CONF_DEV_EUI,
    CONF_K_NET,
    CONF_K_ROOT,
    CONF_KEY,
    CONF_PROFILE,
    CONF_REGION,
    DOMAIN,
    REGIONS,
    SUBENTRY_DEVICE,
    SUBENTRY_PROXY,
)

from .common import answer, full_state, pair
from .conftest import DEV_EUI, K_NET, K_ROOT, PROXY_KEY
from .sim import SimProxy, SimSensor


@pytest.fixture(params=["US915", "EU868"])
def region(request: pytest.FixtureRequest) -> str:
    """The network's region profile."""
    value: str = request.param
    return value


@pytest.fixture
def entry(port: int, region: str) -> MockConfigEntry:
    """The network in this region, with one proxy and one device."""
    return MockConfigEntry(
        domain=DOMAIN,
        title="ConnectSensors",
        data={CONF_REGION: region, CONF_PORT: port, CONF_K_NET: K_NET},
        subentries_data=[
            {
                "data": {CONF_KEY: PROXY_KEY},
                "subentry_type": SUBENTRY_PROXY,
                "title": "Garage proxy",
                "subentry_id": "proxy1",
                "unique_id": None,
            },
            {
                "data": {
                    CONF_DEV_EUI: DEV_EUI,
                    CONF_K_ROOT: K_ROOT,
                    CONF_PROFILE: "CS.CONTACT-1",
                    CONF_REGION: region,
                },
                "subentry_type": SUBENTRY_DEVICE,
                "title": "Front door",
                "subentry_id": "device1",
                "unique_id": DEV_EUI,
            },
        ],
    )


def test_region_radio_matches_package(region: str) -> None:
    """The HELLO check and the airtime estimate use the same SF and bandwidth."""
    _, sf, bw_khz = REGIONS[region]
    assert cs.REGIONS[region] == (sf, bw_khz * 1000)


async def test_airtime_from_package(
    hass: HomeAssistant,
    loaded: MockConfigEntry,
    proxy: SimProxy,
    sensor: SimSensor,
    region: str,
) -> None:
    """Each downlink counts the region's airtime, against the package's budget."""
    await proxy.connect(region=region)
    await pair(hass, proxy, sensor)
    state = loaded.runtime_data.proxies["proxy1"]

    rx = proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0)))
    frame = await answer(proxy, rx)
    sensor.downlink(frame)
    airtime = cs.region_airtime_ms(region, len(frame))
    assert state.airtime[max(state.airtime)][1] == airtime

    # One acknowledgement short of the budget: the next confirmed uplink goes unanswered.
    used = state.airtime_used_ms(server._now_ms())
    with patch.object(cs, "PROXY_AIRTIME_MS_PER_HOUR", used + airtime - 1):
        proxy.uplink(sensor.uplink(cs.T_HEARTBEAT_CONFIRMED, full_state(0)))
        await proxy.nothing(wait=1.0)
