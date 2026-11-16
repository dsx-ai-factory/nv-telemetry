# nv-telemetry-fleet

An example daemon that acquires a fleet of Redfish and gNMI endpoints, the
way a hardware-health service would, and prints what it collects. It shows
acquisition only: there is no export.

```sh
FLEET_BMC_PASSWORD=… FLEET_SWITCH_PASSWORD=… \
  cargo run -p nv-telemetry-fleet -- --config examples/fleet/fleet.example.toml [--verbose]
```

Ctrl-C stops it. Reports already in flight are still delivered, and each
endpoint prints `retired` when it is done.

## What it does

1. It reads the fleet file once. [`fleet.example.toml`](fleet.example.toml)
   documents every key.
2. It **discovers** each Redfish endpoint by following links from
   `/redfish/v1`:
   - each chassis, and the sensors in each chassis's `Sensors` collection;
   - the log services under Managers, Systems and Chassis;
   - the update service;
   - the event service's SSE URI.

   Discovery reads links only. A document the BMC refuses is skipped. An
   unreachable BMC is retried with backoff, and it joins the collector once
   discovery succeeds.
3. It **plans and binds** units:
   - Redfish: one poll per sensor, chassis, log service and update
     service, plus the event stream.
   - gNMI: one Subscribe call for all of the endpoint's `subscribe` leaves,
     plus one Get poll per `get` entry.
4. It hands every endpoint to **one collector** and prints each report as
   it arrives:

```
03:12:24.521 mock-dell redfish.sensor.odata/sensor-read succeeded items=3 1.7ms
    reading sensor/System.Embedded.1/Current_1 = 18.9 A
    state sensor/System.Embedded.1/Current_1 health = "OK"
```

With `--verbose`, each report is followed by one line per observation and
one per projection issue.

## Limits

- **Discovery runs once per endpoint.** A sensor or log service that
  appears later is not read until the daemon restarts.
- **gNMI connects lazily.** A switch that is down shows up as failed
  acquisitions, and the subscription reconnects with backoff. TLS uses the
  system's roots, and there is no option to skip certificate verification.
- **The rate policy limits each endpoint.** At the library default (burst
  4, 1/s), an endpoint with many sensors reads them more slowly than their
  interval asks. The example file raises it.

## Against bmc-mock

The example file expects two BMCs from `../bare-metal-manager-core`:

```sh
M=../bare-metal-manager-core
$M/target/debug/bmc-mock --port 18266 --state-backend internal \
  --hardware-profile dell_poweredge_r750 --cert-path $M/crates/bmc-mock &
$M/target/debug/bmc-mock --port 18267 --state-backend internal \
  --instance-index 1 --cert-path $M/crates/bmc-mock &
```

The mock accepts any password, for example `FLEET_BMC_PASSWORD=dummy`.
There is no gNMI mock, so `switch-a` reports connectivity failures and
keeps retrying.
