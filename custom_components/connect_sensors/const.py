"""Constants for the ConnectSensors integration."""

from typing import Final

DOMAIN: Final = "connect_sensors"

CONF_REGION: Final = "region"
CONF_KEY: Final = "key"
CONF_DEV_EUI: Final = "dev_eui"
CONF_K_ROOT: Final = "k_root"
CONF_PROFILE: Final = "profile"
CONF_PAIRING_CODE: Final = "pairing_code"
CONF_HEARTBEAT: Final = "heartbeat_minutes"

# Federation options (spec 09), each off by default.
CONF_MAILBOX_SERVERS: Final = "mailbox_servers"
CONF_MAILBOX_CERTIFICATE: Final = "mailbox_certificate"
CONF_FEDERATION_OWNER: Final = "federation_owner"
CONF_FEDERATION_PUBLISH: Final = "federation_publish"
CONF_FEDERATION_DOWNLINKS: Final = "federation_downlinks"
# Spec 07 section 4.4: ignore events, and separately readings, of federation-only frames.
CONF_IGNORE_FEDERATED_EVENTS: Final = "ignore_federated_events"
CONF_IGNORE_FEDERATED_READINGS: Final = "ignore_federated_readings"

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
# Spec 04 section 4.2: EXCHANGED falls back to SCANNED when no uplink under the pending
# k_session verifies within 2 minutes.
EXCHANGED_TIMEOUT_S: Final = 120
# Spec 04 section 4.2: SCANNED ends 10 minutes after the pairing code was entered. The
# device is not discarded but expires, until the user starts pairing again.
PAIRING_DEADLINE_S: Final = 600
# Pairing sensor value of an expired device.
PAIRING_EXPIRED: Final = "expired"

# Spec 07 section 4, step 7: a neighbouring epoch's tag is skew only this far from a
# boundary.
SKEW_MARGIN_S: Final = 300
# A sensor whose clock stays wrong gets a correction on an otherwise unanswered uplink
# at most this often (07 section 5).
CORRECTION_INTERVAL_S: Final = 600

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
RX1_MAX_OFFSET_US: Final = 30_000
RX1_MAX_LATE_OR_BUSY: Final = 2
SNR_TIE_DB: Final = 1.0

# How long after the RX1 transmission (1 s after the uplink, plus the frame's
# airtime, in case a proxy reports only once done) its TX_RESULT may take. Even
# for the longest EU868 frame the RX2 DOWNLINK still goes out 1 s before RX2.
TX_RESULT_MARGIN_MS: Final = 500

# After DUTY_CYCLE a proxy's budget frees up as its transmissions age out, at a
# time the server cannot see; leave it alone for a while.
DUTY_CYCLE_BACKOFF_S: Final = 600

# Spec 08 section 4.2: heartbeat interval range. Profile defaults are in profiles.py.
HEARTBEAT_MIN: Final = 1
HEARTBEAT_MAX: Final = 1440

# Spec 07 section 8.1: unreachable after 3 heartbeat intervals plus 5 minutes.
UNREACHABLE_GRACE_MIN: Final = 5

# Spec 07 section 8.2: single-path over the last 24 hours. The repair issue waits
# until the device has been single-path for 24 hours straight, so a newly paired
# device or a second proxy still being placed does not raise one.
SINGLE_PATH_WINDOW_H: Final = 24
SINGLE_PATH_REPAIR_H: Final = 24

# Spec 09 section 6.1: publishes per second per mailbox server.
PUBLISH_PER_SECOND: Final = 5
# Spec 09 section 7.4 and 11 (mode 4): federated airtime per proxy, 0.5% of a rolling
# hour, and federated downlinks per minute in total.
FED_BUDGET_MS_PER_HOUR: Final = 18_000
FED_DOWNLINKS_PER_MINUTE: Final = 10
# Spec 07 section 4.4: federated SYNC frames per minute, across all mailbox servers.
FED_BOOTSTRAP_PER_MINUTE: Final = 50
# A mailbox server unreachable this long gets a repair issue.
MAILBOX_UNREACHABLE_REPAIR_H: Final = 1

TX_RESULTS: Final = {
    0: "sent",
    1: "too_late",
    2: "busy",
    3: "duty_cycle",
    4: "not_capable",
    5: "invalid",
    6: "radio_error",
}


# Spec 07 section 11.3: bus events.
EVENT_DEVICE_UNREACHABLE: Final = f"{DOMAIN}_device_unreachable"
EVENT_DEVICE_REACHABLE: Final = f"{DOMAIN}_device_reachable"
# An event reading (03 section 4.1, `button`), with its event type.
EVENT_READING: Final = f"{DOMAIN}_event"

# Event types of a `button` reading by value (03 section 7).
BUTTON_EVENTS: Final = {1: "press", 2: "double_press", 3: "long_press", 4: "release"}

# Spec 03 section 8: the set-value application command.
CMD_SET_VALUE: Final = 0x40

# Spec 07 section 7: undelivered commands are dropped after 7 days, LEAVE after 48
# hours (04 section 6.1). Commands are also given up after this many downlinks
# without DL_ACK, so a sensor that cannot hear them does not cost airtime for a week.
COMMAND_EXPIRY_H: Final = 7 * 24
LEAVE_EXPIRY_H: Final = 48
COMMAND_MAX_SENDS: Final = 30
# Queued commands per device. Each kind replaces its earlier one (07 section 7), so
# only SET_VALUE on many channels can fill it.
COMMAND_QUEUE_MAX: Final = 16
# Spec 02 section 10.1: command bytes in one downlink.
COMMANDS_MAX_BYTES: Final = 55


def signal_device_update(dev_eui: str) -> str:
    """Dispatcher signal for changes to one sensor."""
    return f"{DOMAIN}_device_update_{dev_eui}"


def signal_device_event(dev_eui: str) -> str:
    """Dispatcher signal for an event reading of one sensor: (key, event type)."""
    return f"{DOMAIN}_device_event_{dev_eui}"


def signal_proxy_update(subentry_id: str) -> str:
    """Dispatcher signal for changes to one proxy."""
    return f"{DOMAIN}_proxy_update_{subentry_id}"


def signal_device_added(entry_id: str) -> str:
    """Dispatcher signal for a device added to a network: (device)."""
    return f"{DOMAIN}_device_added_{entry_id}"


def signal_proxy_bound(entry_id: str) -> str:
    """Dispatcher signal for a proxy that said its first HELLO: (proxy)."""
    return f"{DOMAIN}_proxy_bound_{entry_id}"


def signal_new_reading(entry_id: str) -> str:
    """Dispatcher signal for a reading key first heard: (device, key)."""
    return f"{DOMAIN}_new_reading_{entry_id}"


def signal_mailbox_update(entry_id: str) -> str:
    """Dispatcher signal for changes to a network's mailbox connections."""
    return f"{DOMAIN}_mailbox_update_{entry_id}"
