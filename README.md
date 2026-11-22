# connect-sensors-ha

Home Assistant integration (HACS) for [ConnectSensors](https://github.com/chrisuthe/ConnectSensors):
the network server for an open LoRa sensor network.

The integration is the network server of
[spec 07](https://github.com/chrisuthe/ConnectSensors/blob/main/spec/07-network-server.md).
It listens for `cs_proxy` nodes on UDP, pairs sensors by their pairing code, and turns
their readings into entities. The protocol itself comes from the
[`connectsensors`](https://pypi.org/project/connectsensors/) package.

**Status:** milestone 1, a bench MVP. The protocol and this integration are drafts.

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

Not yet: command queue with resend, supervision (unavailable after missed heartbeats),
RX2 fallback after a failed `TX_RESULT`, device profiles, repairs beyond a stalled
pairing, mDNS, federation.

## Development

Requires [uv](https://docs.astral.sh/uv/). Python 3.14 is installed by uv if needed.

```
bash script/verify
```

runs everything CI runs except HACS validation: ruff, mypy, a translations check,
pytest (with `pytest-homeassistant-custom-component`) and, if podman or docker is
available, `hassfest`. The tests drive the integration with a simulated proxy over UDP
and a simulated sensor.
