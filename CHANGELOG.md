# Changelog

## Unreleased

- **Pairing deadline** (spec 04 section 4.2): a device must be paired within 10 minutes
  of entering its code. After that its *Pairing* entity shows *Expired*, its root key
  is no longer tried, and a button press is logged once and ignored. The new *Start
  pairing* button, and opening the re-pair repair, give another 10 minutes. Devices
  added before this version start with a fresh 10 minutes.
- **Clock correction** (spec 07 sections 4 and 5): a sensor whose tag belongs to the
  previous or next hour, more than 5 minutes from an hour boundary, has a wrong clock.
  Like a `TIME_UNSURE` uplink, its unconfirmed uplinks are answered in RX1 to correct
  it, at most every 10 minutes; through federation only confirmed uplinks are. The
  last skew found is in the device's diagnostics (`clock_skew`).

## 0.2.1

- Require **connectsensors 0.4.2**, and take the federation (spec 09) helpers from its
  public API instead of the private `connectsensors._csframe` module. No behaviour change.

## 0.2.0

- **Federation** through mailbox servers (ConnectSensors spec 09). There are three
  opt-ins, all off by default: receive your sensors through neighbours, share your
  coverage, and let neighbours use your proxies for replies. The options also take a
  pinned certificate for self-signed servers. There is a connectivity entity per
  server, *Federated paths* per device, `federation_only` markers, and repairs for an
  unreachable or refusing server. Pairing is never federated.
- **connectsensors 0.4.x:** key-fingerprint tags and the shorter pairing (see below).
  Existing devices must be re-paired once, and a repair issue says so.
- The spec fixes of connectsensors 0.3.0 (see below).

Not yet: mDNS discovery, profile distribution (spec 10).

## 0.1.0

First release: milestones 1, 2a and 2b.

## Details by milestone

### Milestone 1

- UDP proxy sessions (`HELLO_REQUEST`, nonce echo, `SERVER_STATUS`), per-proxy keys.
- Uplink collection and dedupe, tag lookup and trial decryption, counter checks.
- Acks for confirmed uplinks in RX1 through the proxy with the best SNR that heard them.
- Pairing: `JOIN` → `KEY_EXCHANGE` → the first session uplink → paired, with a root
  downlink counter that is never reset (kept even after the device is removed).
- Entities: contact, motion, leak and other binary readings; temperature, humidity,
  battery and other numeric readings; diagnostics for pairing state, last seen, RSSI,
  SNR and uplink counter. Proxies: connectivity and their `HELLO` counters.
- State in Home Assistant's storage; keys and pairing codes never logged, and redacted
  from diagnostics.

### Milestone 2a

- Proxy choice per spec 07 section 6: RX1 through the best-SNR copy among connected,
  downlink-capable proxies on the network's radio profile with a healthy timing
  record. For a confirmed uplink only, RX2 through the next best when RX1 fails or its
  `TX_RESULT` does not come back in time (07 section 6.3). One proxy per window, and
  each proxy's airtime budget is respected.
- Supervision (above): unreachable devices, single-path devices.
- Repair issues, each cleared automatically: a stalled pairing, a proxy on other radio
  settings, an unreachable device, a single-path device.

### Milestone 2b

- A per-device command queue (spec 07 section 7), persisted. Commands ride on the
  answer to the device's next uplink, which is answered for that reason alone, and are
  sent again until a newer uplink acknowledges them with `DL_ACK`. A command not acknowledged
  after 30 sends or 7 days is dropped and logged; the device's *Command* entity shows
  pending, delivered or failed.
- Actions: `connect_sensors.set_heartbeat` and `connect_sensors.set_value` (command
  `0x40`, for channels the profile lists as writable), and `LEAVE` on removal.
- Events: a `button` reading fires the device's event entity (*press*, *double press*,
  *long press*, *release*) and a `connect_sensors_event` bus event. It is never a state.
- Device profiles, by the pairing code's profile id: `CS.CONTACT-1`, `CS.CLIMATE-1` and
  `ESPHOME`. A profile sets the device's manufacturer, model, default heartbeat and
  expected entities. Other profiles get entities from the readings they send.
- A proxy whose `proxy_id` is derived from its MAC carries that MAC as a connection, so
  Home Assistant links it to the ESPHome device of the same board.

### connectsensors 0.3.0 spec fixes

- `DL_ACK` is bound to the downlink it acknowledges (07 section 7): only an uplink
  newer than that downlink delivers its commands, a late uplink can still carry the
  acknowledgement, and when two answers to one uplink carry different commands only
  those both carried count as delivered. Answers to late arrivals carry no commands.
- A proxy is trusted for RX1 only if none of its last 20 `SENT` results was more than
  30 ms late (07 section 6.2).
- A pairing stalls after 3 repeated `JOIN`s counted from the first `KEY_EXCHANGE` a
  proxy reported sent (07 section 5.1), and a sensor restarted mid-pairing gets a new
  exchange (04 section 4.2).
- Each proxy has a key of its own (05 section 4.2). Proxies that share a key get no
  downlinks, and one that would accept them raises a repair issue that gives it a new
  key.

### connectsensors 0.4.0

- Frames carry one 8-byte tag, a key fingerprint derived from the device's own keys
  (02 section 6). There is no network key and no `SET_NETWORK`. Home Assistant keeps a
  tag table: each device's normal tags for the previous, current and next hour, and its
  `SYNC` tags for the counter blocks around the last frame it sent (07 section 3). The
  table moves on at each hour.
- `JOIN`, and a `SYNC` far past the last counter seen, are found by trial decryption,
  at most 50 a minute (02 section 11.3).
- Pairing ends on the first session uplink: it marks the device paired, and its answer
  completes pairing on the sensor (04 section 4). If no such uplink arrives within 2
  minutes of `KEY_EXCHANGE`, the device waits for the button again.
- `SERVER_STATUS` carries no tags (05 section 5.3).
- **Upgrading:** sensors on the 0.4.0 firmware come back unpaired. Each device that
  was paired shows *Waiting for button press*, its readings show as unavailable, and a
  repair issue asks you to press its button near one of your proxies. No pairing code
  or factory reset is needed.

