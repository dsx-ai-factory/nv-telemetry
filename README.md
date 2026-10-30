# nv-telemetry

[![CI](https://github.com/dsx-ai-factory/nv-telemetry/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dsx-ai-factory/nv-telemetry/actions/workflows/ci.yml)

Rust libraries for bare-metal telemetry collection.

Use this workspace when building a service that reads BMC telemetry and needs a
shared observation model for readings, states, inventory, and logs. The
embedding service owns endpoint inventory, credentials, dispatch, timers, and
consumer policy. The [probe example](examples/probe/src/main.rs) shows that
integration with mock and HTTP transports.

## Current capabilities

| Component | Available now |
| --- | --- |
| Observation model | Versioned protobuf schema, generated validated Rust types, canonical ordering, content hashing, and encode/decode support. |
| Redfish | Sensor and chassis reads, log-service and firmware-inventory walks, and server-sent events projected into observation batches. |
| gNMI | Configured subscriptions that map numeric, string, and boolean leaves to readings and states. |
| Orchestration | Deterministic poll/stream planning, endpoint admission, circuit-breaker and rate policies, report assembly, and stream reconnect policy. |
| Probe | A demo embedder with in-process mock and live HTTP modes; tagged batches, statuses, and projection issues on stdout. |

The project is under active development, and `nv.telemetry.v1` is not yet a
compatibility promise. Shared export machinery and OTLP and Prometheus exporters
are unfinished. Redfish catalog discovery, bulk
`TelemetryService` collection, session handling, and vendor leniency are also
unfinished. Supply explicit Redfish resource targets to the probe.

## Try the mock probe

Install Git, [Rust with rustup](https://www.rust-lang.org/tools/install), and
the native linker/build tools for your platform. The checked-in
[rust-toolchain.toml](rust-toolchain.toml) selects Rust **1.95.0**. CI runs on
Linux, macOS, and Windows.

Run from a terminal:

```sh
git clone https://github.com/dsx-ai-factory/nv-telemetry.git
cd nv-telemetry
cargo run --locked -p nv-telemetry-probe -- --mode mock --endpoint-id demo --sensor /redfish/v1/Chassis/1U/Sensors/CPU1Temp --count 1 --strict
```

This uses the checked-in sensor fixture and needs no BMC, credentials, or
external service. The first build needs network access to download dependencies.
A successful run prints two `batch:` lines (readings and states), one
`status:` line with a successful outcome, and exits with status 0. The fixture
reports CPU temperature **47.5 Cel**. An `issues:` line identifies projection
issues; `--strict` makes any acquisition failure or projection issue produce
exit status 1. Argument-parse errors produce exit status 2; runtime errors,
including a malformed `--base-url`, produce exit status 1.

For live collection, the probe's HTTP mode requires a reachable Redfish
endpoint, its resource paths, `PROBE_USERNAME`, and, when needed,
`PROBE_PASSWORD`. See the [probe usage and driver](examples/probe/src/main.rs) for
`--base-url`, polling options, and HTTP-only `--event-stream`. Keep TLS
certificate verification enabled; `--insecure` disables certificate checks.

## Build and test

The mock probe and ordinary Cargo builds do not need `buf` or `protoc`.
To build API documentation and run the probe checks:

```sh
cargo doc --locked --workspace --no-deps
cargo test --locked -p nv-telemetry-probe --test cli --test e2e_mock
```

The full contribution gate also needs GNU Make, Bash, **buf 1.72.0**, and
**nightly-2026-06-16** with rustfmt. Install buf from its
[installation guide](https://buf.build/docs/cli/installation), then run:

```sh
make rust-install
make all
```

`make all` checks formatting, protobuf lint, generated output, clippy, builds,
tests, and API documentation. CI uses `make ci`,
which installs the formatting toolchain before the same checks. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the contribution and DCO process.

# Overview

`nv-telemetry` collects telemetry from bare-metal endpoints and emits immutable
observations. It accepts endpoint inventory and collection intent from an
embedding service, plans how to obtain the requested data, executes collection
through an embedder-owned dispatcher, and reports what it observed. Sources
only read; health verdicts, thresholds, and remediation are consumer policy.

The observation model is a versioned, annotated protobuf schema. Wire types,
validators, canonical form, and protocol projections are all generated from
that schema by a compiler this project owns, so the contract consumers hold is
the schema rather than a Rust crate: a batch is the same object to an
in-process consumer, a service in another language, and stored history.

See [docs/DATA-MODEL.md](docs/DATA-MODEL.md) for how the observation model
works, with real Redfish and gNMI payloads mapped onto it, and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design and the reasoning
behind it.

# Project structure

```
schema/               nv-telemetry-schema         .proto contract + descriptor set
codegen/              nv-telemetry-codegen        schema compiler (build-time only)
model/                nv-telemetry-model          data plane, generated
source/               nv-telemetry-source         acquisition contract
sources/
  redfish/            nv-telemetry-redfish        HTTP/JSON, OData, DMTF schemas
  gnmi/               nv-telemetry-gnmi           configured gNMI subscriptions
orchestration/        nv-telemetry-orchestration  planner, providers, recipes
examples/
  probe/              nv-telemetry-probe          demo embedder (not published)
export/               nv-telemetry-export         planned shared batch traversal
exporters/
  otlp/               nv-telemetry-otlp           planned
  prometheus/         nv-telemetry-prometheus     planned
docs/                 architecture and design notes
```

`schema` holds the `.proto` files and exports the compiled descriptor set, so
one artifact serves the compiler, reflection consumers, and builds in other
languages. `codegen` is the compiler that reads it; it is a build-time tool and
never a runtime dependency of anything downstream. `model` is the data plane:
validated wrappers that own the invariants, canonical ordering, and content
hashing, with no protocol, I/O, or async dependencies. The prost structs
generated from the schema are crate-internal — the decode target, not the
public face — so a caller receives a validated type or nothing.

`source` defines what every protocol implements — source and stage traits,
capability probes, endpoint access, request classification — so `orchestration`
can plan against the contract and protocol crates can implement it without
either depending on the other. Schema-backed source crates are meant to hold the
same three things: transport, projection manifests, and generated projection
code, of which only the transport is hand-written. A REST API is a transport
rather than a source, so each vendor API gets its own crate and manifest rather
than sharing one.

`schema`, `codegen`, `model`, `source`, `sources/redfish`, `sources/gnmi`,
`orchestration`, and `examples/probe` carry code today. The Redfish providers
cover sensor, chassis, log, firmware, and event acquisition; fixture tests
assert batches and issue lists together. Orchestration plans polls and streams
and builds the dispatcher admission stack; the probe owns the runtime and
driving loop.
The export crates remain placeholders whose doc comments describe
the intended boundaries.

`sources/gnmi` collects numeric readings and states with string or boolean
values. Applications supply the transport, paths, and mappings. The
`gnmi.configured` provider supports exact leaf mappings and collection mappings
that derive subjects from list keys. It checks values at runtime and keeps strings
as received. It does not validate against YANG or traverse JSON subtrees. See the
crate's API documentation for a construction example.

Exporters are separate crates because their dependency trees are disjoint;
`export` holds only what they share, such as joining readings to descriptors
and flattening subjects into labels or attributes.

Dependencies form a tree rooted at `model`: `source` and `export` depend on it,
protocol crates depend on `source`, exporters depend on `export`, and
`orchestration` depends on `source` without knowing any protocol crate exists.
`codegen` hangs off to the side, depending on `schema`, and nothing that
consumes the data plane depends on either of them. Nothing in the data plane
knows a protocol exists, and nothing below the acquisition plane knows Redfish
exists.

## Schema and generated code

Proto packages are versioned independently of crate names:

| Package                   | Contents                                     |
| ------------------------- | -------------------------------------------- |
| `nv.telemetry.v1`         | observation contract                          |
| `nv.telemetry.options.v1` | annotation vocabulary                         |
| `nv.telemetry.mapping.v1` | manifest schema for projections               |

Extension numbers for the annotation vocabulary are registered in
[docs/EXTENSIONS.md](docs/EXTENSIONS.md).

Generated output is checked in, so a codegen change is reviewable as a
schema-shaped diff and no downstream build needs a protobuf compiler. That
includes `schema/contract.lock`, a sorted record of every declaration in the
contract, and gNMI bindings generated from the vendored OpenConfig schemas.
Regenerate and verify with:

```
make codegen                  # rewrite generated output and the contract lock
make check-codegen            # fail if the generated output is stale
```

`make check-codegen` runs as part of `make ci`, ahead of the build steps, so a
schema edit without regeneration fails immediately rather than surfacing later
as a compile error.

The schema itself is checked by [buf](https://buf.build), configured in
`buf.yaml` and run by the same pipeline:

```
make proto-lint      # style and naming, including enum value prefixes
make fmt             # formats Rust and .proto together
```

The library and schema are under active development. Breaking refactors are
allowed; `nv.telemetry.v1` is not yet a compatibility promise. Checks validate
the current schema and its generated output, not historical schemas or saved
wire fixtures. `contract.lock` remains a readable schema snapshot, and model
tests enforce validation, canonicalization, and encode/decode round trips.

buf is a checker only. The descriptor set is built by protox from
`schema/build.rs`, so `cargo build` needs no external binary and a consumer of
the data plane pulls only `prost`. Contributors do need the buf binary for
`make all`; see [its install docs](https://buf.build/docs/cli/installation).

`Cargo.lock` is committed, which is not the usual choice for a library. It is
not for consumers — their resolution ignores it entirely — but for the
generated tree: the formatter that decides those bytes reaches this workspace
only transitively, so no manifest entry can pin it and only the lockfile can.
The cost is that ordinary builds see one resolution, which the scheduled
`Latest dependencies` workflow exists to offset by building against a fresh
one every week.

## Support

For bugs and feature requests, open a [GitHub issue](https://github.com/dsx-ai-factory/nv-telemetry/issues).
Include the revision, command or API call, expected result, and a sanitized error
or output sample. For design and integration context, start with
[the architecture](docs/ARCHITECTURE.md) and [the data model](docs/DATA-MODEL.md).
Use the private process in [SECURITY.md](SECURITY.md) for vulnerabilities.

# Contribution Guidelines
- Start here: [CONTRIBUTING.md](CONTRIBUTING.md)
- Code of Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## Security
- Vulnerability disclosure: [SECURITY.md](SECURITY.md)
- Do not file public issues for security reports.

# License
This project is licensed under the Apache-2 License - see the LICENSE file for details
- License: [LICENSE](LICENSE)
