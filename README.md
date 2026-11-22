# connect-sensors-ha

Home Assistant integration (HACS) for [ConnectSensors](https://github.com/chrisuthe/ConnectSensors):
the network server for an open LoRa sensor network.

The integration is the network server of
[spec 07](https://github.com/chrisuthe/ConnectSensors/blob/main/spec/07-network-server.md).
It listens for `cs_proxy` nodes on UDP, pairs sensors by their pairing code, and turns
their readings into entities. The protocol itself comes from the
[`connectsensors`](https://pypi.org/project/connectsensors/) package.

**Status:** milestone 2a: delivery and supervision on top of the milestone 1 bench MVP. The protocol and this integration are drafts.

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
   its entities appear with its first readings.

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
default). If yours is configured differently, set it in the device's **Settings**.

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
  record; RX2 through the next best when RX1 fails or its `TX_RESULT` does not come
  back in time. One proxy per window, and each proxy's airtime budget is respected.
- Supervision (above): unreachable devices, single-path devices.
- Repair issues, each cleared automatically: a stalled pairing, a proxy on other radio
  settings, an unreachable device, a single-path device.

Not yet: command queue with resend, `SET_HEARTBEAT`, device profiles, events, mDNS,
federation.

## Development

Requires [uv](https://docs.astral.sh/uv/). Python 3.14 is installed by uv if needed.

```
bash script/verify
```

runs everything CI runs except HACS validation: ruff, mypy, a translations check,
pytest (with `pytest-homeassistant-custom-component`) and, if podman or docker is
available, `hassfest`. The tests drive the integration with a simulated proxy over UDP
and a simulated sensor.
