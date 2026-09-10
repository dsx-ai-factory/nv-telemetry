# nv-telemetry
Libraries for bare-metal telemetry collection

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
  gnmi/               nv-telemetry-gnmi           gRPC subscribe, YANG paths
orchestration/        nv-telemetry-orchestration  planner, providers, recipes
examples/
  probe/              nv-telemetry-probe          demo embedder (not published)
export/               nv-telemetry-export         shared batch traversal
exporters/
  otlp/               nv-telemetry-otlp
  prometheus/         nv-telemetry-prometheus
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
either depending on the other. Each crate under `sources/` is meant to hold the
same three things: transport, projection manifests, and generated projection
code, of which only the transport is hand-written. A REST API is a transport
rather than a source, so each vendor API gets its own crate and manifest rather
than sharing one.

`schema`, `codegen`, `model`, `source`, and `sources/redfish` carry code
today; the remaining crates are placeholders whose doc comments state what
belongs in them. The contract's messages exist, every gate runs over them,
`model` is the generated validated data plane — canonically ordered and
content-hashable — `source` is the acquisition contract, and the Redfish
crate's first provider reads one sensor into readings and states batches,
pinned by a fixture corpus that asserts batches and issue lists together.

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
contract. Regenerate and verify with:

```
make codegen                  # rewrite generated output and the contract lock
make check-codegen            # fail if the generated output is stale
make check-redfish-features  # test every supported Redfish feature row
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

# Contribution Guidelines
- Start here: [CONTRIBUTING.md](CONTRIBUTING.md)
- Code of Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## Security
- Vulnerability disclosure: [SECURITY.md](SECURITY.md)
- Do not file public issues for security reports.

# License
This project is licensed under the Apache-2 License - see the LICENSE file for details
- License: [LICENSE](LICENSE)
