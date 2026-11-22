# connect-sensors-ha

Home Assistant integration (HACS) for [ConnectSensors](https://github.com/chrisuthe/ConnectSensors):
the network server for an open LoRa sensor network.

The integration is the network server of
[spec 07](https://github.com/chrisuthe/ConnectSensors/blob/main/spec/07-network-server.md).
It listens for `cs_proxy` nodes on UDP, pairs sensors by their pairing code, and turns
their readings into entities. The protocol itself comes from the
[`connectsensors`](https://pypi.org/project/connectsensors/) package.

**Status:** milestone 2: delivery, supervision, commands, events and device profiles on top of the milestone 1 bench MVP. The protocol and this integration are drafts.

## Install with HACS

1. In Home Assistant, open **HACS**, then the menu (⋮) → **Custom repositories**.
2. Add `https://github.com/chrisuthe/connect-sensors-ha` with type **Integration**.
3. Find **ConnectSensors** in HACS, **Download** it, and restart Home Assistant.
4. Go to **Settings → Devices & services → Add integration → ConnectSensors**.

Home Assistant 2026.10 or later is required.

Without HACS, copy `custom_components/connect_sensors` into your configuration's
`custom_components` folder and restart.

## Set up

1. **Network.** Adding the integration creates the network: choose the region profile
   (`US915`; `EU868` is provisional) and the UDP port (default 47800). A network key
   is generated and stays in Home Assistant.
2. **Proxies.** On the integration's page, **Add proxy**. Paste the `cs_proxy:` snippet
   it shows (with the generated key and Home Assistant's address) into the proxy's
   ESPHome configuration and install it. The proxy appears as a device when its first
   `HELLO` arrives. **Show configuration** on the proxy shows the snippet again.
3. **Devices.** **Add device**, type the `CS:1:...` pairing code from inside the
   battery compartment, then press the device's button. The device's *Pairing*
   entity goes from *Waiting for button press* to *Exchanging keys* to *Paired*, and
   its entities appear: those its profile lists straight away, others with the first
   readings that carry them.
4. **Removing a device** sends it `LEAVE` on its next uplink, if it is reachable, so it
   returns to unpaired. Home Assistant forgets it once it acknowledges, or after 48
   hours. Adding it again later just needs its pairing code and a button press.

A device that was paired with another server, including `cs_testserver.py`, must be
factory reset first: hold its button for 10 seconds. If you forget, pairing stalls and
Home Assistant raises a repair issue saying so.

## Supervision

A paired device is *unreachable* when nothing has been heard from it for three
heartbeat intervals and 5 minutes. Its readings then show as unavailable, never as
their last value, and a repair issue is raised. The next uplink makes it reachable
again, and each reading comes back when an uplink carries it.

Home Assistant cannot read a device's heartbeat interval from the device, so it
assumes the profile's (30 minutes for `CS.CONTACT-1`, otherwise 60, `cs_sensor`'s
default). If yours is configured differently, set it in the device's **Settings**, or
change the device itself with the `connect_sensors.set_heartbeat` action: once the
device acknowledges it, the setting follows.

Supervision changes fire `connect_sensors_device_unreachable` and
`connect_sensors_device_reachable` events, with `device_id`, `dev_eui` and `name`.

Each device's *Proxies hearing it* entity counts the proxies that heard it in the last
24 hours, with a `single_path` attribute when only one did. If that lasts another day,
a repair issue suggests adding a proxy within range.

## What milestone 1 covers

- UDP proxy sessions (`HELLO_REQUEST`, nonce echo, `SERVER_STATUS`), per-proxy keys.
- Uplink collection and dedupe, tag lookup and trial decryption, counter checks.
- Acks for confirmed uplinks in RX1 through the proxy with the best SNR that heard them.
- Pairing: `JOIN` → `KEY_EXCHANGE` → `SYNC` → `SET_NETWORK` → paired, with a root
  downlink counter that is never reset (kept even after the device is removed).
- Entities: contact, motion, leak and other binary readings; temperature, humidity,
  battery and other numeric readings; diagnostics for pairing state, last seen, RSSI,
  SNR and uplink counter. Proxies: connectivity and their `HELLO` counters.
- State in Home Assistant's storage; keys and pairing codes never logged, and redacted
  from diagnostics.

## What milestone 2a adds

- Proxy choice per spec 07 section 6: RX1 through the best-SNR copy among connected,
  downlink-capable proxies on the network's radio profile with a healthy timing
  record. For a confirmed uplink only, RX2 through the next best when RX1 fails or its
  `TX_RESULT` does not come back in time (07 section 6.3). One proxy per window, and
  each proxy's airtime budget is respected.
- Supervision (above): unreachable devices, single-path devices.
- Repair issues, each cleared automatically: a stalled pairing, a proxy on other radio
  settings, an unreachable device, a single-path device.

## What milestone 2b adds

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

## What the connectsensors 0.3.0 spec fixes change

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

Not yet: mDNS, federation, key rotation, profile distribution (spec 10).

## Development

Requires [uv](https://docs.astral.sh/uv/). Python 3.14 is installed by uv if needed.

```
bash script/verify
```

runs everything CI runs except HACS validation: ruff, mypy, a translations check,
pytest (with `pytest-homeassistant-custom-component`) and, if podman or docker is
available, `hassfest`. The tests drive the integration with a simulated proxy over UDP
and a simulated sensor.
