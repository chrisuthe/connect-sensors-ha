"""Constants for the ConnectSensors integration."""

from typing import Final

DOMAIN: Final = "connect_sensors"

CONF_REGION: Final = "region"
CONF_K_NET: Final = "k_net"
CONF_KEY: Final = "key"
CONF_DEV_EUI: Final = "dev_eui"
CONF_K_ROOT: Final = "k_root"
CONF_PROFILE: Final = "profile"
CONF_PAIRING_CODE: Final = "pairing_code"
CONF_HEARTBEAT: Final = "heartbeat_minutes"

SUBENTRY_PROXY: Final = "proxy"
SUBENTRY_DEVICE: Final = "device"

DEFAULT_PORT: Final = 47800


# Region profiles (spec 01 section 3): name -> (frequency in Hz or None while
# provisional, spreading factor, bandwidth in kHz).
REGIONS: Final[dict[str, tuple[int | None, int, int]]] = {
    "US915": (918_500_000, 9, 500),
    "EU868": (None, 9, 125),
}

# Pairing states (spec 04 section 4.2).
STATE_SCANNED: Final = "scanned"
STATE_EXCHANGED: Final = "exchanged"
STATE_PAIRED: Final = "paired"

# Spec 07 section 4.1 and 4.2.
COLLECTION_WINDOW_MS: Final = 150
RX1_DEADLINE_MS: Final = 800
RX1_OFFSET_US: Final = 1_000_000
RX2_OFFSET_US: Final = 3_000_000

# Spec 07 section 6.1 and 8.2: a proxy is offline after 180 s without HELLO.
PROXY_TIMEOUT_S: Final = 180

# Spec 02 section 11.3: bootstrap frames processed per minute.
BOOTSTRAP_PER_MINUTE: Final = 50

# Spec 02 section 8.2: counter reservation block size.
RESERVE_BLOCK: Final = 64

# Spec 07 section 10: uplink counters are persisted within 10 s of an accepted frame.
SAVE_DELAY_S: Final = 10

# Spec 07 section 6.2: a proxy's timing record disqualifies it from RX1.
RX1_MAX_OFFSET_US: Final = 80_000
RX1_MAX_LATE_OR_BUSY: Final = 2
SNR_TIE_DB: Final = 1.0

# How long after the first copy of an uplink the RX1 proxy's TX_RESULT may take.
# RX1 transmits 1 s after the uplink, so this leaves about 0.4 s for the result to
# arrive and still 1.5 s for the RX2 DOWNLINK to reach the next proxy.
TX_RESULT_WAIT_MS: Final = 1_500

# Spec 01 section 5: a proxy transmits at most 72 s in any rolling hour.
PROXY_AIRTIME_MS_PER_HOUR: Final = 72_000
# After DUTY_CYCLE a proxy's budget frees up as its transmissions age out, at a
# time the server cannot see; leave it alone for a while.
DUTY_CYCLE_BACKOFF_S: Final = 600

# Spec 08 section 4.2: heartbeat interval range, and the profile defaults this
# integration knows. Unknown profiles get cs_sensor's default.
HEARTBEAT_MIN: Final = 1
HEARTBEAT_MAX: Final = 1440
PROFILE_HEARTBEAT_MINUTES: Final = {"CS.CONTACT-1": 30}
DEFAULT_HEARTBEAT_MINUTES: Final = 60

# Spec 07 section 8.1: unreachable after 3 heartbeat intervals plus 5 minutes.
UNREACHABLE_GRACE_MIN: Final = 5

# Spec 07 section 8.2: single-path over the last 24 hours. The repair issue waits
# until that has held for another 24 hours, so a newly paired device, a proxy
# reboot or a second proxy still being placed does not raise one.
SINGLE_PATH_WINDOW_H: Final = 24
SINGLE_PATH_REPAIR_H: Final = 24

TX_RESULTS: Final = {
    0: "sent",
    1: "too_late",
    2: "busy",
    3: "duty_cycle",
    4: "not_capable",
    5: "invalid",
    6: "radio_error",
}

SIGNAL_DEVICE_ADDED: Final = f"{DOMAIN}_device_added"
SIGNAL_PROXY_BOUND: Final = f"{DOMAIN}_proxy_bound"
SIGNAL_NEW_READING: Final = f"{DOMAIN}_new_reading"


def default_heartbeat(profile: str) -> int:
    """The heartbeat interval in minutes a profile starts with (08 section 4.2)."""
    return PROFILE_HEARTBEAT_MINUTES.get(profile, DEFAULT_HEARTBEAT_MINUTES)


def signal_device_update(dev_eui: str) -> str:
    """Dispatcher signal for changes to one sensor."""
    return f"{DOMAIN}_device_update_{dev_eui}"


def signal_proxy_update(subentry_id: str) -> str:
    """Dispatcher signal for changes to one proxy."""
    return f"{DOMAIN}_proxy_update_{subentry_id}"
