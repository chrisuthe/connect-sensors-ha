# connect-sensors-ha

Home Assistant integration (HACS) for [ConnectSensors](https://github.com/chrisuthe/ConnectSensors):
the network server for an open LoRa sensor network.

The integration is the network server of
[spec 07](https://github.com/chrisuthe/ConnectSensors/blob/main/spec/07-network-server.md).
It listens for `cs_proxy` nodes on UDP, pairs sensors by their pairing code, and turns
their readings into entities. The protocol itself comes from the
[`connectsensors`](https://pypi.org/project/connectsensors/) package.

**Status:** 0.2.3. Federation through mailbox servers arrived in 0.2.0; 0.2.2 added the pairing deadline and clock correction; 0.2.3 needs connectsensors 0.4.3. Changes by version are in [CHANGELOG.md](CHANGELOG.md). The protocol and this integration are drafts.

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
   (`US915`; `EU868` is provisional) and the UDP port (default 47800).
2. **Proxies.** On the integration's page, **Add proxy**. Paste the `cs_proxy:` snippet
   it shows (with the generated key and Home Assistant's address) into the proxy's
   ESPHome configuration and install it. The proxy appears as a device when its first
   `HELLO` arrives. **Show configuration** on the proxy shows the snippet again.
3. **Devices.** **Add device**, type the `CS:1:...` pairing code from inside the
   battery compartment, then press the device's button. The device's *Pairing*
   entity goes from *Waiting for button press* to *Exchanging keys* to *Paired*, and
   its entities appear: those its profile lists straight away, others with the first
   readings that carry them. Press the button within 10 minutes of adding the device;
   after that the pairing shows *Expired* and the button press is ignored. The
   device's *Start pairing* button gives another 10 minutes.
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

## Federation

Federation lets homes carry each other's sensors through mailbox servers (ConnectSensors
[spec 09](https://github.com/chrisuthe/ConnectSensors/blob/main/spec/09-federation-relay.md)).
A sensor that only a neighbour's proxy hears still reaches you, and your proxies can
carry a neighbour's sensors. Readings stay encrypted end to end: neither the mailbox
server nor the neighbour can read them.

**Pairing is never federated.** Pair a new device near one of your own proxies; after
that it works anywhere a federating neighbour's proxy hears it.

On the integration's page, **Configure**:

- **Mailbox servers:** the `wss://` address of each server to use, for example one run
  by your neighbourhood with the [cs-mailbox app](https://github.com/chrisuthe/HAOS-cs-mailbox).
  For a server with a self-signed certificate, paste its certificate into **Trusted
  certificate**.
- **Receive my sensors through neighbours:** register your devices with the servers
  and accept their frames from neighbours' proxies. Acknowledgements and queued
  commands go back through those proxies in the second receive window.
- **Share my coverage with neighbours:** pass frames your proxies hear that belong to
  none of your devices to the servers, which deliver them only to their owners.
- **Let neighbours' sensors use my proxies to receive replies:** transmit replies for
  neighbours, with your own devices first and at most 0.5% of each proxy's airtime.

Everything is off until you turn it on. A connectivity entity shows each server's
state, each device's *Federated paths* entity counts the neighbours that carried it,
and readings or events that arrived only through federation carry
`federation_only: true` (two options can ignore them).

## Development

Requires [uv](https://docs.astral.sh/uv/). Python 3.14 is installed by uv if needed.

```
bash script/verify
```

runs everything CI runs except HACS validation: ruff, mypy, a translations check,
pytest (with `pytest-homeassistant-custom-component`) and, if podman or docker is
available, `hassfest`. The tests drive the integration with a simulated proxy over UDP
and a simulated sensor.
