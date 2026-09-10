# Architecture

## Purpose

`nv-telemetry` is a read-only telemetry collection library for bare-metal
systems. It accepts endpoint inventory and collection intent, plans how to
obtain the requested data, executes collection through an embedder-owned
dispatcher, and emits immutable observations.

The observation model is a versioned, annotated protobuf schema. Everything
that depends on the model's shape — wire types, validators, in-process
wrappers, protocol projections — is generated from that schema by a compiler
the library owns. The contract consumers hold is the schema, not a Rust crate:
a batch is the same object to an in-process consumer, a sibling service in
another language, and stored history.

The library reports what it observed. It does not know why the data is needed
or what a larger system intends to do with it.

## Scope

The following decisions define the library boundary:

- **Sources only observe.** They may read sensors, logs, state, and inventory,
  but they never mutate a device.
- **Health is consumer policy.** Threshold classification, health verdicts,
  staleness decisions, rack aggregation, and remediation are outside the
  library.
- **Endpoint inventory comes from the embedder.** The library reacts to
  endpoint additions, changes, and removals. It does not discover endpoints.
- **Exporters are optional consumers.** OTLP and Prometheus live in their own
  crates, but they consume the same output available to any embedder.

## Ownership boundaries

The design has four planes:

```
┌─ Integration plane: embedding service ───────────────────────────────┐
│ endpoint inventory, collection intent, configuration, reconciliation │
│ loop, final dispatcher graph/runtime, consumers and output routing   │
├─ Orchestration plane: nv-telemetry ──────────────────────────────────┤
│ data needs, provider planning, capability probing, dispatcher task   │
│ vocabulary, typed policy, and standard subtree recipes               │
├─ Acquisition plane: one crate per protocol, over a shared contract ──┤
│ Redfish, gNMI, and vendor-API sources that construct requests and    │
│ parse responses; no scheduling, sink, health, or mutation policy     │
├─ Data plane: nv-telemetry schema ────────────────────────────────────┤
│ protobuf observation contract, generated model, validators,          │
│ canonical form, shared batches                                       │
└───────────────────────────────────────────────────────────────────────┘
```

The embedding service owns the application. The library provides reusable
planning, acquisition, and data-model components without hiding an application
runtime inside itself.

One component exists at build time rather than run time: the schema compiler,
which turns the data plane's contract into code. It is part of the library's
delivery, with an owner, tests, and a release discipline of its own.

## End-to-end flow

```
Endpoint inventory ─┐
                    ├─> embedder reconciliation loop
Generic data needs ─┘             │
                                  v
    ┌─────────────────> library provider planner
    │                             │
    │                             v
    │                 dispatcher subtree recipes
    │         (acquisition tasks and capability probes)
    │                             │
    │                             v
    │              embedder-owned dispatcher graph
    │                             │
    │                             v
    │                  read-only source tasks
    │                             │
    │            ┌────────────────┼────────────────┐
    │            v                v                v
    │       observation      acquisition      capability
    │         batches          status          results
    │            └───────┬────────┘                │
    │                    v                         │
    │            consumer adapters                 │
    └──────────────────────────────────────────────┘
                 replan when capability is learned
```

On an endpoint or policy change, the embedder invokes the planner and recipes
to update only the affected portion of its dispatcher graph. Endpoint removal,
task cancellation, draining, and final graph lifecycle remain under embedder
control.

The loop on the left is why planning is not a single pass: a capability probe
is itself dispatched work, so an unknown capability resolves only after its
probe has been admitted and run.

## Inputs

The library accepts three kinds of generic input.

### Endpoint events

An endpoint event adds, changes, or removes an endpoint. An endpoint definition
contains:

- a stable endpoint identity;
- one or more supported access methods;
- an opaque credential reference or credential provider;
- static attributes such as rack, serial number, and device class;
- a generation or equivalent revision used to detect changes.

Secrets must not enter observation batches, diagnostics, or logs.

### Data needs

A data need describes what the deployment wants to observe, not which protocol
request to execute. Examples include:

- sensor readings every 30 seconds;
- firmware inventory every hour;
- device state observations;
- log records or an event subscription.

A need may carry cadence, freshness, timeout, and priority requirements. It
must not encode a specific provider unless the embedder intentionally applies a
provider override.

### Collection policy

Collection policy configures planning, dispatch, and delivery independently of
source implementation. Configuration changes are reconciled in the same way as
endpoint changes and do not require source-specific code.

## Needs, providers, and planning

A **data need** states what data is required. A **provider** states one way to
obtain it.

Three kinds of discovery remain distinct:

- **Endpoint inventory** is supplied by the embedder and determines which
  devices exist.
- **Capability probing** determines which providers an endpoint supports.
- **Resource cataloging** discovers protocol resources inside an endpoint,
  such as sensors, log services, or firmware components.

All three feed planning, but they differ in who produces them. Endpoint
inventory is a control input: it arrives from the embedder and the library
never derives it. A capability result is a control fact the library produces
itself by issuing a probe request. A resource catalog is an acquisition result
that can be reused by other acquisition stages and published as observation
data.

Collapsing any pair moves work into the wrong plane. If the library derived
endpoint inventory, it would decide which devices exist, which belongs to the
embedder. If capability probing and resource cataloging were one step, learning
whether a bulk provider exists would require walking every sensor, making
planning as expensive as collection and coupling plan stability to device data
churn; the two also age differently, since capability is near-static and
TTL-cached while a catalog changes whenever hardware does. If cataloging
happened inside planning, the planner would perform I/O and the catalog could
not also be emitted as an inventory batch.

For example, sensor readings could be provided by:

- Redfish TelemetryService with one bulk request;
- Redfish Sensor OData with multiple widely-supported requests;
- a gNMI subscription against a YANG path;
- a switch-specific vendor API.

Each provider declares:

- the need it can satisfy;
- its provider identity and request class;
- whether it is polled or streamed;
- its estimated request cost;
- how to probe endpoint capability;
- how to construct requests and parse responses.

The planner evaluates providers per endpoint. It caches capability results with
a configurable TTL, walks a deterministic preference list, and selects the
first supported provider. Persistent provider-local failure may demote that
provider and cause replanning.

This is intentionally not a scoring engine. Selection must remain explainable:
the resolved plan records the selected provider and why alternatives were
rejected or demoted.

### Probing is dispatched work

A capability probe is a request against a device, so it is subject to the same
admission control as any acquisition: it runs as a dispatcher task under the
endpoint's concurrency limits, rate budget, and breakers, and carries its own
request class. Nothing in the library issues an unscheduled request. Without
this, a fleet-wide restart or a wave of TTL expiries would probe every endpoint
at once, which is exactly the burst the dispatcher exists to smooth, and probes
would evade the breaker that an unreachable endpoint has already tripped.

Planning is therefore asynchronous rather than a single blocking pass. When a
need has no cached capability result, the planner emits a probe task and leaves
that need unresolved; the probe result lands as a control fact and triggers a
replan for the affected endpoint. A resolved plan is consequently allowed to be
partial, and the embedder reconciles its dispatcher graph incrementally as
capability becomes known.

An endpoint whose capability is still unknown produces no observations and no
fabricated ones. Probe failures are reported as acquisition status like any
other request failure.

### Composable acquisition stages

A provider recipe expands a data need into a directed acyclic graph of
acquisition stages. A stage declares its prerequisites and may produce:

- a private, protocol-specific artifact for downstream stages;
- one or more public observation batches, but only for requested data needs.

Equivalent stages for the same endpoint are deduplicated. Their artifacts are
cached by generation or TTL and invalidated when endpoint access changes or
the provider reports that they are stale.

For Redfish sensors, a `SensorCatalog` stage discovers resources once and
retains `SensorLink` handles privately. The same catalog can then:

- project sensor metadata into an inventory batch when sensor inventory was
  explicitly requested;
- expand into per-sensor OData reading tasks;
- provide metadata needed to interpret bulk TelemetryService readings.

If both inventory and readings are requested, they share one catalog fetch. If
only readings are requested, the catalog remains an internal prerequisite and
does not publish unsolicited inventory.

Redfish is the worked example throughout this document because it is the
messiest, not because it is the shape every source takes. A gNMI source has the
same stages with different contents: `Capabilities` is its capability probe,
and a subscription replaces the catalog-then-poll expansion entirely.

Logs, firmware, and other domains use their own catalog or probe stages. They
share only prerequisites they explicitly declare, such as a Redfish
`ServiceRoot` stage:

```
ServiceRoot
  ├─ SensorCatalog ──> sensor inventory and/or reading tasks
  ├─ LogServiceCatalog ──> periodic log tasks
  ├─ EventServiceProbe ──> SSE subscription
  └─ FirmwareCatalog ──> firmware inventory
```

The embedder chooses data needs. Protocol modules declare recipes. The planner
resolves provider preferences and deduplicates stages. Every stage that
performs network I/O is admitted by the dispatcher.

Private artifacts such as `SensorLink` never enter the data plane. Public
inventory items and readings instead share a stable, protocol-neutral subject
identity so consumers can correlate them without understanding Redfish.

## Dispatcher placement

The dispatcher is the execution mechanism between planning and acquisition:

- the planner decides **what** should be collected and **which provider**
  should supply it;
- library recipes translate that plan into dispatcher subtrees;
- the dispatcher decides **when** polled work may run;
- the admitted source task constructs the request and parses the response.

Capability probes are dispatched on the same terms. Planning emits them as
tasks rather than issuing them directly, so the question the planner needs
answered is subject to the same limits, breakers, and request-class policy as
the collection it enables.

The library exposes dispatcher-compatible task types, typed tuning policy, and
standard subtree recipes. The embedding service owns the final graph, graph
reconciliation, node lifecycle, and the `Runtime` driving loop. This allows
telemetry work to participate in a larger application dispatcher graph.

Sources do not implement their own scheduling, retries, backoff, rate limiting,
or sinks. Every task carries a request-class identity so those behaviors can be
configured consistently outside protocol code.

### Standard dispatcher recipe

The standard recipe is opinionated but not mandatory:

```
global concurrency
  └─ endpoint admission
      └─ endpoint connectivity breaker
          └─ request-class lane
              └─ request-class breaker
                  └─ rate/burst token bucket
                      └─ priority queue
                          └─ task leaves
```

The two breaker scopes serve different purposes:

- common connectivity, endpoint availability, or authentication failures may
  affect the endpoint breaker;
- unsupported or failing provider-specific requests affect only their request
  class.

The exact dispatcher node composition is an implementation contract with the
dispatcher library. The architecture requires the scopes and behavior above,
not a particular internal node representation.

## Configuration

Configuration is layered and deterministically resolved. Later, more-specific
values override earlier values:

1. library defaults;
2. deployment defaults;
3. protocol overrides;
4. endpoint-selector overrides, such as device class or rack;
5. individual endpoint overrides;
6. request-class overrides.

The resolved configuration and its contributing layers must be inspectable for
every planned task.

Policy is divided into four areas:

- **Collection intent:** enabled needs, cadence, freshness, timeout, and
  priority.
- **Provider policy:** preference order, capability-probe TTL, and failure
  demotion.
- **Dispatcher policy:** global and per-endpoint concurrency, token-bucket
  rate and burst, priority lane, endpoint and request-class breakers,
  retry/backoff, jitter, and shutdown behavior.
- **Delivery policy:** per-consumer queue bounds and overflow behavior.

The library supports three levels of customization:

1. use defaults and specify only data needs;
2. override typed policy values while using the standard recipes;
3. bypass recipes and compose raw dispatcher nodes.

The third level is the escape hatch for applications whose topology cannot be
expressed by the standard recipes. The library does not define a configuration
language for arbitrary graph construction.

Configuration validation rejects impossible rates, unbounded queues, unknown
request classes, invalid endpoint selectors, and retry policies that cannot
meet their collection freshness requirements.

## Acquisition contract

A source is deliberately small. It:

1. builds a request or subscription from endpoint access;
2. parses protocol data into the shared observation model;
3. classifies acquisition failures sufficiently for dispatcher and planner
   policy.

It does not choose cadence, retry itself, publish to a sink, derive health, or
update external state.

One acquisition may produce more than one homogeneous observation batch. For
example, a response containing readings and inventory may emit one batch for
each domain while retaining common endpoint, origin, and timing metadata.

Each source crate owns its transport decoding. Redfish JSON is decoded inside
the Redfish source with whatever machinery suits JSON; none of it reaches the
data plane, which has no JSON or protocol dependencies.

## Data contract

The data plane is defined by a versioned protobuf package, `nv.telemetry.v1`.
Observation batches, payload domains, the resource graph, subjects, signal
metadata, and acquisition status are protobuf messages, and the Rust model is
generated from them.

The schema is the source of truth; hand-written types never define the
contract.

Not every rule is expressible as an annotation. A reading's signal key must
resolve against a descriptor in the same batch, a relationship's source must
name a resource present in its graph, a range's minimum must not exceed its
maximum, a window's end must follow its start, and a failure class is present
exactly when the outcome is a failure. These are cross-field rules, carried by
the validated wrappers rather than by the vocabulary, and the schema marks
each one where it applies.

The model must preserve:

- endpoint identity without copying static context into every item;
- recursive typed properties, explicit nulls, references, units, timestamps,
  attributes, and provenance;
- stable resource identity and typed relationships across protocol sources;
- complete versus partial snapshot semantics;
- immutable sharing across multiple consumers;
- validated construction, with invariants that hold identically for built and
  decoded data.

### Schema and annotations

The schema carries two kinds of annotation. Invariant and semantic options
state what a valid message is — presence requirements, finite-float
constraints, size and depth bounds, non-emptiness, rejection of an enum's
unspecified value, element uniqueness within a collection, canonical ordering
— and how it is compared, which is where the collection-metadata exclusion
lives.
Where an established vocabulary exists the schema uses it rather than
inventing one. The exception is value constraints, where protovalidate would
otherwise be the obvious choice: the compiler is a component this project owns
and must own anyway, for wrappers, canonical form, hashing, and projections,
and adding a second constraint vocabulary with its own toolchain would split
the invariants across two systems that fail differently. The cost is that a
consumer in another language does not get these constraints enforced for free,
and must either trust the producer or re-state them.

Protocol knowledge is deliberately not an annotation on core fields. Source
mappings live in per-protocol manifests (see Projection), so the core schema
stays protocol-neutral and one field does not accumulate one option per
protocol.

Custom option numbers come from the range protobuf reserves for use inside a
single organization, and are registered in `docs/EXTENSIONS.md`. Numbers in
that range are explicitly not globally unique and cannot be publicly
registered, so the registry is a coordination document rather than a
guarantee; taking a globally registered number would mean renumbering, and the
number is baked into every descriptor set already shipped. Options are
identified on the wire by number alone, and they are namespaced per extendee,
so a claim that does not name the extendee cannot prevent the collision it is
written to prevent. An uncoordinated collision surfaces as misdecoded
annotations in whatever tool first loads both schemas, far from either owner.

### Presence

proto3 encodes an unset scalar as its zero value. For telemetry that default
fabricates data: a reading of 0.0 and an absent reading must never be the same
bytes. Every scalar field therefore either declares explicit presence
(`optional`) or carries an annotation stating that its zero value is
meaningful, and the compiler rejects any schema that violates the rule. This
is the schema-level form of the output rule that a failed request emits no
fabricated observation.

Presence is necessary and not sufficient. A field that is set may still carry
nothing: the empty string sets a string, and an enum's unspecified value sets
an enum, so both satisfy a presence requirement while saying nothing — and
both are what a half-failed read produces. Identity and naming fields therefore
also declare non-emptiness, and the enums a consumer branches on reject their
unspecified value. A subject with an empty identifier is the sharpest case,
because subjects are hashed: every subject an identity projection failed on
would hash equal and join to the same resource.

Rejection stops at the zero value. An enum value the build does not recognise
is a newer producer naming something real, so it decodes rather than
invalidating the message; deciding what to do with it belongs to the consumer.
Preserving the numeric token keeps an unrecognised answer distinct from an
unspecified value without requiring the consumer to understand its meaning.

### Foreign types

A contract field's message type must itself be declared in the contract. The
rules only see what they walk, so a type from another package contributes
scalars nothing has checked, and an empty instance of it decodes to their zero
values. `google.protobuf.DoubleValue` is the case that motivates the rule: it
is the conventional proto3 spelling of an optional double, and an empty one
reads back as zero rather than as absent.

The cost is real and deliberate. `google.protobuf.Timestamp`, `Duration`, and
`FieldMask` are excluded too, so the contract declares its own, and an exporter
that wants the well-known types converts at its boundary like any other
mapping. The alternative — trusting types the rules cannot inspect — is the
one thing the data plane is built to refuse.

### Values

Recursive properties use the library's own `Value` message: null, boolean,
signed and unsigned 64-bit integers, finite double, string, bytes, timestamp,
list, and key-sorted map. `google.protobuf.Struct` is not used because it
collapses every number to double, silently corrupting 64-bit identifiers, and
cannot represent bytes or timestamps. Depth is bounded by an annotation on
`Value` itself rather than on each field that holds one, because recursion is
a property of the type. The bound does not translate into a decoder setting —
runtimes fix their own recursion limit, and one logical level of a recursive
type costs several message levels — so it is chosen to fit underneath the
runtime's limit with margin, and the type carrying it records the arithmetic.
Lengths and element counts are bounded per field, where a decoder can be given
nothing to enforce and validators do the work.

One route stays open at the schema level: a map key. It is a scalar with
implicit presence that cannot be made `optional` and cannot carry an
annotation, so an entry encoded without its key decodes as the zero key, and no
schema rule can prevent it. Validation closes it instead — a decoded map entry
whose key is the type's zero value is rejected — which is why keys are chosen
so that their zero value is never a legitimate identifier.

The observation model admits only finite floats, so that observations remain
exactly comparable and survive a serialization round trip. A non-finite
source value is reported as an invalid field rather than a missing one: the
device answered, and the answer cannot be represented. Collapsing that into
absence would tell a consumer nothing was reported, hiding a device that
reports garbage behind one that stays quiet.

### Generated model and validated currency

Generated wire types are plain structs with public fields and no invariants;
nothing about them prevents a non-finite reading or an unsorted graph. The
model therefore distinguishes wire types from validated types.

A message annotated as validated gets a generated owned type with private
fields, constructed only through a generated builder or fallible conversion
from the wire type, both of which run the same checks. Owned rather than a
newtype over the wire struct, because ownership is what permits reshaping,
and reshaping is where the invariants become the type: a `required` field is
a plain value rather than an `Option`, a required oneof is an enum rather
than an `Option` of one, an enum field cannot carry the unspecified value,
and a recursive value is a real tree with a sorted, duplicate-free map. What
the schema forbids is unrepresentable after conversion instead of checked at
each use; encoding rebuilds the wire form from the validated one. The wire
types stay crate-internal and the validated types take the public names.

The in-process unit of flow is an `Arc` of a validated type. Fan-out shares by
reference and never re-encodes; encoding happens once per process boundary.
Everything downstream of validated ingress may assume the invariants hold.

Because a validated type's representation is private, it can change without
breaking the public contract; that is where storage-layout freedom lives. The
generated representation is the baseline layout, and alternatives are adopted
behind wrappers only where measurements show material benefit. Raw, unwrapped
types trade that freedom for zero-cost access deliberately.

Validation is symmetric between construction and decode: a decoded batch
passes the same validators as a built one, so the model cannot produce what it
cannot consume. Structural bounds are enforced twice — at decode by message
size and recursion limits, and by validators on logical size such as graph
resource counts and value nesting.

Symmetry does not follow from the per-field bounds alone, and this is the one
place it has to be arranged deliberately. Those bounds are local: a resource
count, a map's entry count, and a string's length are each modest, while their
product is not, so a batch can satisfy every one of them and still be far
larger than any decoder will accept. Nor does a recursion bound translate into
a decoder setting, because runtimes fix their own limit and one logical level
of a recursive type costs several message levels. The aggregate ceiling is
therefore a single limit the validated wrappers apply at construction, chosen
to match what decoding accepts, and recursive types carry a depth bound
selected to fit underneath the runtime's fixed limit rather than to look
round. A caller may tighten the bounds but not
loosen them, because decoding has no caller to take a bound from and applies
the default. An acquisition reading an endpoint incrementally owns its own
buffering ceiling, and collection policy owns request-level limits.

### Canonical form and content hashing

Where order is not semantic — graph resources, relationships, attribute sets —
validated construction sorts into canonical order, so equal content is
represented equally. Which fields those are is declared per field rather than
per message, because a message routinely mixes both kinds: a `Value` holding a
list whose order is data can sit beside an attribute set whose order is not,
and a message-level flag would either corrupt the first or leave the second
uncanonicalized.

Sorting repeated messages needs a total order over messages, which has to be
defined rather than assumed. The order compares hash-visible fields first, in
the hash's own traversal: fields in field-number order, comparing present
values pairwise, where an absent field sorts before a present one and each
value type compares within itself — integers numerically, strings and bytes
lexicographically, booleans false first, doubles numerically with only finite
values admitted, nested messages recursively. Collection-metadata fields are
compared only after every hash-visible field, as a final tiebreaker. The split
is what keeps both properties at once: elements that tie on hash-visible
fields contribute identical hash streams in either order, so the tiebreaker
cannot move the hash, while still making canonical bytes deterministic.
Encoded bytes are not used in the comparison, for the same reason they are not
hashed.

Content hashes are computed by a generated logical traversal of present known
fields, labeled by field number, skipping fields annotated as collection
metadata — an observation timestamp or an entity tag records how a fact was
collected rather than what was observed, and hashing it would report that an
unchanged device changed. The exclusion is transitive: the traversal inherits
it through nested messages whether or not they are hashable themselves, so a
resource inside a hashable graph still has its collection metadata skipped. Encoded bytes are never hashed: protobuf
encoding is not canonical across implementations, and unknown fields would
make equal graphs hash unequal. Absent fields contribute nothing, so a schema
revision that adds fields changes a hash only when those fields carry data —
an upgrade that observes nothing new reports no change. Field numbers label
meanings within the current schema. During development, a schema refactor may
change them, so content hashes from different revisions
must not be assumed comparable.

A field whose zero value is declared meaningful is the exception in the other
direction: it has no "present" to test, so it is hashed unconditionally.
Either kind of annotation change on a field a hashable message reaches —
declaring a zero meaningful, marking collection metadata, or removing either —
changes every existing hash. These changes are a review obligation; the
contract lock records annotations so that the diff makes them visible.
Hashing also requires validated construction, since equal content only hashes
equal once canonicalization has run.

The hash is a generated capability computed where comparison happens, not a
field carried on the wire, so it always reflects the comparer's own decoded
view. A convergence adapter depends on this: state comparison must be exact
and must not report spurious change.

### Evolution

The library and `nv.telemetry.v1` schema are under active development. The
package suffix identifies the current schema; it is not a stability promise.
Refactors may change fields, field numbers, validation rules, generated APIs,
and hashing semantics. Earlier serialized data is not required to remain
readable, and no historical-schema compatibility gate restricts development.
A release compatibility policy will be defined when the contract stabilizes.

The checked-in contract lock records every declaration: messages and fields
with their numbers, types, cardinality, presence, and annotations, enum values,
and oneofs with their members and invariants. It is a review surface and a
freshness check: a schema edit must arrive with matching generated output.

Current-schema guarantees remain enforced: explicit presence, bounds,
cross-field validation, canonicalization, encode/decode round trips, and
deterministic generation. Builders retain validated construction and private
representations; they do not imply a stable API during development.

## The schema compiler

The library owns the generator that turns the schema into code. It is a
build-time, descriptor-driven plugin that consumes:

- the compiled descriptor set of the core schema, including custom options;
- projection manifests (see Projection);
- schema indexes describing source fields, whether emitted by a source
  generator or read from a protobuf descriptor pool.

It emits:

- annotation validation and the invariant rules, and the contract lock that
  makes a schema edit without its generated output fail the build;
- wire types on the standard Rust protobuf substrate;
- invariant validators and canonicalization;
- validated wrappers, builders, and accessors;
- logical content hashing;
- projection code, the projection-issue vocabulary, and provenance tables
  mapping each core field to the source path it came from, so tooling can
  answer where a reading originated — the planner's explainability
  requirement, extended to data.

Annotation errors fail the build, attributed to the declaration that caused
them: a field, message, or extension by fully-qualified name, and a file by
path where the rule is about the file itself. A name identifies the
declaration uniquely, which is what a fix needs.

The descriptor set retains source information, which costs it about 44 KB. The
schema's comments are where the reasoning lives, and dropping them would mean
the generated types — the place a consumer actually reads — arrive with no
explanation of why a field exists or what its absence means. Retaining it also
leaves the door open to file-and-line attribution on schema errors. The compiler never emits silently
degraded code: an annotation it cannot honor is an error, not a warning.
Generated output is deterministic and unchanged output is not rewritten, so a
codegen change is reviewable as a schema-shaped diff and can be golden-tested
against one.

Generation covers the mechanical majority and is not a universal vocabulary.
A projection or validator the annotation vocabulary cannot express cleanly is
written by hand and registers through the same traits generated code
implements. Growing the vocabulary to swallow every special case is a failure
mode, not a goal.

## Projection

Protocol integrations map source data into the data plane through declared
projections compiled by the schema compiler. Projection declarations contain
source-specific field knowledge; the core observation model and dispatcher do
not depend on protocol schema types.

### Manifests

A projection manifest is a per-protocol data file — instances of a mapping
schema — that names the source schema path, the target core field, subject
derivation, unit handling, and the known-value set for open enumerations.
Manifests reference protocol schema paths such as `Sensor/Reading`, never the
source crate's Rust shapes, so the projection backend can change without
rewriting a declaration.

The compiler never resolves a path against a protocol directly. It resolves
against a schema index: a backend-neutral description of a source field — its
path, type, cardinality, presence, and the schema version range it exists in.
An index is either generated from a source's schema bundle, or read from a
protobuf descriptor pool where the payload really is proto-described. The seam
is what makes the second kind of source an added index rather than a rewritten
compiler.

Being proto-native is not the same as being its own index, and gNMI is the
case that shows the difference. Its descriptor pool describes the transport
envelope — `Notification`, `Update`, `Path`, `TypedValue` — not the data
model. A path such as `/interfaces/interface[name=eth0]/state/counters`
is defined by a YANG module, and the value's type comes from YANG rather than
from the proto, so gNMI needs a generated index exactly as Redfish does. The
descriptor-pool backend applies where the messages themselves are the model:
a subscription negotiated with PROTO encoding against generated types, or a
gRPC telemetry API whose responses are the schema. Only there is type checking
total.

What being proto-native does buy is real but narrower: decode comes from the
protobuf runtime rather than hand-written parsing, there is no vendor-leniency
layer, and capability probing is a native call rather than an inference from
which resources happen to exist.

The compiler validates every manifest against its index at build time. A
mistyped path or a type mismatch is a build error carrying the manifest
location and the schema versions checked. Redfish is the standing
example: the Redfish crate's generator, also owned, emits its index from the
DMTF schema bundle.

Validation is also the checking boundary for projection compilation. Every
declaration an *author* can get wrong is a lint rule with its own diagnostic,
and `compile` wraps a clean check into the receipt emission requires — so
emission cannot run on unchecked manifests. Emission then resolves against
the same index the lint checked and refuses, loudly and by declaration, what
the compiler has not implemented; it never silently degrades. The deliberate
consequence is that "manifest accepted" and "generated behavior" are one
claim: what the lint admits, emission can build, and what emission cannot
build, `make codegen` reports.

Requested-location templates are parsed once, by `LocationPattern`, which
admits one canonical absolute resource-path grammar and owns placeholder
segmentation. Lint validates through that parser and emission renders its
typed literal, wildcard, and capture segments, so URI normalization in a
source crate cannot make an accepted template unmatchable.

Compilation produces an expected set of destinations and bytes; generation
writes exactly those, byte-compared against the checked-in tree. Files under
a generated directory that no manifest produces anymore are reported as
orphans for the operator to delete — the compiler never removes files.

Target construction is therefore profile-based. The unary profiles are
`SignalDescriptor` and `Reading`, whose sole identity landing is `key`;
`StateObservation` and `InventoryItem`, whose sole identity landing is
`subject` (the item also carries compiler-filled provenance); and
`LogRecord`, which carries no identity at all — a record is a fact in its own
right, so its manifest declares no subject and emission derives none, while
its `subject` field (what the entry is about) stays reserved by type. A
profile is an explicit promise that the compiler knows how to complete that
target's builder and triage every device-driven invariant. Reflection
verifies the promise against the contract, but never invents a profile.
Messages with multiple identities, batch/payload shapes, and helper messages
are rejected as projection roots until their construction semantics are
designed and registered deliberately.

Profiles also carry payload-level obligations that no unary builder can prove.
For a source type that emits `Reading`, compilation requires exactly one
`SignalDescriptor` instance with the same derived key and proves that the
descriptor is unconditional whenever a sample can emit. More than one
descriptor would duplicate a key; a gated or absent descriptor would leave a
sample unresolved in `Readings`. These are compile errors, not provider
assembly failures. Each target profile also names the repeated payload field it
enters. Compilation reads that field's `max_items` annotation and rejects a
static instance count above it, so a device answering every gate cannot make
provider assembly discover a plan/model cardinality disagreement.

Source presence is a four-state semantic even though Rust uses only three
container shapes: required/non-null is `T`, optional/non-null is `Option<T>`,
required/nullable is `Option<T>` whose `None` means explicit null, and
optional/nullable is `Option<Option<T>>`. The compiled read plan preserves
those meanings through intermediate fields, and lint requires a null policy
when any segment of the path can be explicitly null rather than inspecting the
leaf alone. Emission applies that policy only after distinguishing absence from
explicit null. Raw protobuf enum numbers in a
manifest are validated before lowering, and static enum spellings must be
non-empty, so token emission receives only total typed choices.

Rust type checking still applies to the generated projection code, but the
load-bearing guarantees are schema validation at build time and the payload
corpus in tests. A projection is only as trustworthy as the device data it has
been shown, because deployed devices deviate from their schemas.

Open enumerations project to strings with known-value annotations for
validation, never to proto enums: Redfish enums are open string sets, and an
enum type cannot represent an unlisted value without lying about it.

### Signals

Signal metadata and samples are projected separately. Metadata produces an
immutable `SignalDescriptor` indexed by a canonical `SignalKey`. Sensor,
EnvironmentMetrics, and MetricReport samples project to the same key, so the
planner can change acquisition routes without changing public reading
identity. Routes spell the same signal differently — a metric report naming a
reading with a property fragment the sensor resource does not use, or a request
adding query options — so generated subject derivation reduces the requested
location before matching its manifest template. The captured scope and ID then
form the key. Reduction lives in the source crate's location-grammar hook used
by every generated matcher, rather than in provider call sites, so a route
cannot accidentally derive a different identity by skipping it.

On the wire, readings reference their descriptor by key, so units and bounds
are not repeated on every sample. In process, a `SignalCatalog` — a runtime
structure above the wire model, not a message — joins keys to shared
descriptors and owns metadata revisions. A refresh that reports an unchanged
definition keeps the existing descriptor, so revision counts real definition
changes rather than polls and sharing survives across refreshes.

A descriptor carries only what a signal *is*: its identity, kind, unit, and
the range the sensor can read. Device-settable configuration, alarm thresholds
being the obvious case, is observed as resource state instead. The line is
mutability. A threshold is writable on most implementations, which makes it a
convergence target rather than reading metadata, and carrying it in both
places would give one fact two representations that can disagree. Consumers
that classify readings join by subject against the resource graph. Threshold
units resolve through the exact `(StateObservation.subject,
StateObservation.facet)` signal key. An absent facet matches only an absent
`SignalKey.facet`; it does not select another signal on that resource.
Projections validate facet strings using the schema's non-empty and length
bounds, and state-series validation groups by `(subject, facet, name)`.

### Subjects

That join only holds if both halves derive the subject the same way, so the
convention is part of the contract rather than each projection's choice.
Derivation is declared in the manifest and enforced by generated code.

A subject names what a thing *is*, never where it was read from: the URI stays
the resource's source key. Where an identifier is unique only within a
collection, the subject carries enough of the containing scope to separate
them. A Redfish sensor is the standing example, since `Sensor.Id` repeats
across chassis, so its subject is `sensor` / `{chassis}/{id}`. A location that
does not yield the scope is reported as an invalid field rather than guessed
at, because a wrong subject silently joins to the wrong resource.

### Projection issues

Generated extraction distinguishes a field the device did not report from a
field it reported unusably. Missing and invalid are different facts, and both
are structured projection issues attached to the acquisition result rather
than log lines. The non-finite float rule above is one instance; unresolvable
subject scope is another. Issues never become fabricated observations.

## Output contract

The library exposes three generic output streams: observation batches,
acquisition status, and projection issues.

### Observation batches

The unit of flow is a validated, immutable observation batch: shared by
reference in process, encoded once per external boundary. A batch contains:

- shared endpoint context;
- origin and provider/request-class provenance;
- the observation time or window;
- completeness information;
- one homogeneous payload domain: readings, logs, state observations,
  inventory, or an observed resource graph.

Collection providers do not assemble that envelope independently. Their
`Acquire::perform` hook receives no collection timestamp and returns only
`AcquisitionParts`: validated coverage/payload pairs plus issues. The
non-overridable source-layer `acquire(unit, at)` wrapper captures the admitted
unit's endpoint and origin, polls the hook, and stamps every batch with that
identity and its caller's `at`. Every batch from one successful acquisition
therefore shares its endpoint, provider, request class, and start time;
orchestration owns using that same admitted identity and instant when it builds
the corresponding status.

Batches are safe to fan out by reference. Consumers may include exporters,
in-process channels, persistence, APIs, or external adapters.

Completeness is part of the observation, not an acquisition error:

- a complete snapshot may be used to infer that previously known inventory is
  absent;
- a partial snapshot must not silently delete unobserved inventory;
- a failed request emits no fabricated device observation.

### Observed resource graph

The resource graph is the structured snapshot domain for OOB device state. It
preserves canonical subjects, original source keys, source schemas and
versions, recursive properties, explicit nulls, unresolved references, and
typed directed relationships.

Graph snapshots may represent an entire endpoint or a subtree of one. Subject
scope on a graph means reachability from the scope subject rather than subject
equality, because a graph is a connected structure and the natural unit of
partial collection is a subtree such as one chassis and its contents. A
complete graph replaces the prior snapshot's resource and relationship sets
for the same endpoint, scope, and collection definition. Previously collected
members missing from the new snapshot are removed, even when they are no
longer reachable in the new graph. The provider must define the relation kinds
and external-target boundaries it enumerates. Reachability of included nodes
is a structural check, never proof of exhaustive collection. A walk with an
unresolved collection branch must remain partial. A partial graph updates
observed facts but cannot establish that omitted facts were removed.

Reachability follows relations from source to target. A subtree therefore has
to hang off its root by outgoing edges, which is a constraint on projections: a
walk that records only the inverse relation, each child naming its parent,
produces a graph its own root cannot reach.

Absence is also tracked per resource. A resource records whether its properties
are the device's full representation, so a consumer can tell a property the
device does not implement from one that was never requested. A convergence
consumer that conflates the two would read an uncollected property as unset and
attempt to write it.

Each resource maps to exactly one source location. Data from another URI becomes
its own resource joined by a relation, never a merge, because merging collapses
two observation times and two entity tags into one.

Resource targets may be outside the collected graph so partial Redfish and
switch topology walks can retain external links. Relationship sources must be
present because the source is the resource on which the relation was observed.
Cycles are valid: containment, management, fabric, and peer relationships do
not form one universal tree.

Both ends of a relationship are identities, so an edge is a resolved claim
rather than a collected one. Because targets may be external, stating one costs
no fetch, only knowledge of what the target is called; where a source names
things canonically that comes out of the link itself. A link a walk cannot yet
name stays an unresolved reference on the resource, carrying the location with
no identity attached, and becomes an edge when a later pass resolves it. The
distinction is what keeps a partial walk honest, since an identity invented to
force an edge is indistinguishable from a real external target.

Graph validation enforces canonical order and finite floats, and graph
comparison uses the logical content hash defined in the data contract, so a
whole graph can be compared exactly. Graph size and property nesting are
bounded as described there: the bound is on what the model accepts, at decode
and at validated construction alike, so no malformed endpoint response becomes
a stored graph that later exhausts a collector serving many endpoints.

The graph is suitable input for an embedder-owned adapter that materializes
convergence observed state. Desired state, drift calculation, operation
selection, and mutation remain outside `nv-telemetry`.

### Acquisition status

`AcquisitionStatus` reports operational outcomes such as success or failure,
retryability, duration, endpoint, provider, and request class. It allows the
embedder to operate the collection pipeline without confusing pipeline failure
with device state.

Operator-facing failure detail is derived from classified facts. Source
adapters do not copy raw response bodies or transport error displays into it:
those strings can contain credential-bearing URLs, query secrets, or arbitrary
device data. Protocol-specific evidence belongs in a separately designed,
redacted diagnostic channel rather than the generic status contract. The
in-process failure type also caps derived detail to the status schema's byte
bound, truncating at a UTF-8 boundary with a marker, so orchestration can copy
it without creating a second validation failure.

Observation absence, collection failure, and stale prior data are distinct.
The library preserves the information needed for consumers to apply their own
freshness and absence policies.

### Projection issues

`ProjectionIssues` carries what source fields failed to become — one issue
per failed field, missing and invalid distinct — under the same identity and
instant that stamp the sibling batches. Issues
ride beside batches, never inside one, and an acquisition with no issues
emits no envelope at all. Sources yield in-process issues only; orchestration
builds the envelope, exactly as it builds `AcquisitionStatus`.

### Delivery

The embedder owns fan-out and delivery. Consumer queues must be bounded and
their overflow behavior explicit. A slow consumer must not retain unbounded
history or silently impose application-wide backpressure.

The embedder fences endpoint and plan generations before delivery: an old
acquisition completing after replacement must not overwrite the new plan's
observations. It also defines provider authority when providers overlap.
These are local control facts, currently absent from the wire; an external
consumer needs the corresponding delivery contract.

Timestamp ordering applies only within a comparable clock domain and epoch.
Device `observed_at` and collector `window.start` must never be compared using
one as the other's fallback. Clock resets, equal or absent timestamps, and
provider changes can leave observations unordered; consumers preserve that
ambiguity instead of deriving order from canonical list position. No global
ordering across endpoints is implied.

## Optional integrations

OTLP and Prometheus exporters are separate crates, and consume observation
batches like any other adapter. Export mapping and serialization occur only at the exporter
boundary. Sharing protobuf as a technology does not make the schemas the same:
the exporter maps the core schema to OTLP's, and no OTLP concept leaks inward.

## Testing

Correctness rests on three tiers, because schema-driven generation moves the
guarantees from the type system into validation and tests.

1. **Compiler goldens.** Schema and manifests in, generated code out, diffed
   in CI. Deterministic generation makes a codegen change reviewable as a
   schema-shaped diff.
2. **Payload corpus.** Recorded real device payloads, per vendor, model, and
   firmware revision, replayed through decode and projection, asserting both
   the emitted batches and the issue lists. The corpus is the de facto
   projection contract and is curated as a first-class artifact; a vendor
   quirk fixed without a corpus entry is a regression waiting to recur.
3. **Model properties.** Any valid batch encodes, decodes, and revalidates
   equal, with an equal content hash; validation is idempotent; decode at
   validated ingress is fuzzed.

Schema hygiene sits outside these tiers and belongs to buf: style and naming
through `buf lint` and formatting through `buf format`. That is a build-time
concern nothing else in the stack covers, and keeping it separate from the invariant rules is deliberate — the rules
encode what the data must mean, which no general linter can know, while the
things buf checks are exactly the ones a general linter already knows better
than we would. The cost is one more binary a contributor installs.

The corpus differs from the other two in where it comes from. Goldens and
properties are derived from the schema, so they follow it automatically; the
corpus is collected from real devices, and no amount of schema work produces
it.

## Modularity

The rule is that heavy dependencies reach only what needs them. Cargo features
are the tool for cutting up one crate whose dependency tree is homogeneous;
separate crates are the tool when the trees are genuinely disjoint, which is
the case on both the acquisition and delivery sides. So:

- the schema, and the generated data-plane crate built from it — wire types,
  validators, wrappers, hashing — with no I/O, protocol, dispatcher, or
  exporter dependencies;
- the schema compiler as a build-time tool, never a runtime dependency of
  consumers, which is what checking generated output in buys;
- an acquisition contract crate stating what every protocol implements, so the
  planner and the protocol crates depend on it rather than on each other;
- one crate per protocol source, each owning its transport decode. A source is
  a schema plus a transport, not a transport alone: REST is how bytes arrive,
  so each vendor API is its own crate rather than a shared "REST" one;
- one crate per exporter over a shared traversal crate, because the OTLP and
  Prometheus client libraries have nothing to do with each other;
- orchestration vocabulary kept lightweight.

Every optional transport combination a source crate claims to support is a
named build-and-test matrix row. Default and all-feature workspace builds test
the aggregate endpoints; they do not prove isolated features, because Cargo
feature unification can otherwise hide unused code, missing dependencies, or a
test-only feature leak.

Every source crate has the same three parts — transport, projection manifests,
and generated projection code — and only the first is hand-written. Its size
tracks how messy the protocol is, which is why Redfish is the large one and a
proto-described source is small.

## Unconstrained by this design

### Streamed acquisition placement

Polled work maps directly to dispatcher tasks. Streamed sources admit two
placements, and the architecture constrains neither:

- a stream-read adapter inside the dispatcher graph, keeping more activity
  under common fairness control; or
- subscriptions beside the dispatcher, with connection and reconnection
  attempts admitted through it.

The source abstraction is expressive enough for both, and whichever is chosen
has to account for cancellation, reconnect backoff, fairness, shutdown, and
output backpressure. gNMI is the source that exercises the question, because a
`Subscribe` in STREAM mode is a long-lived gRPC stream rather than a request
and a response.

## Falsifiable claims

The design asserts properties that no amount of review establishes, because
they are claims about a running system rather than about a schema. Each is
falsifiable by one scenario:

1. a provider can be swapped without changing public reading identity — one
   polled need served by two providers, such as Redfish TelemetryService and
   Sensor OData, yielding the same signal keys;
2. streamed and polled acquisition share one source abstraction — one streamed
   provider alongside a polled one;
3. planning is incremental — an endpoint or policy change rebuilds only the
   affected dispatcher subtrees;
4. absence, failure, and staleness stay distinct — complete, partial, failed,
   stale, and slow-consumer scenarios each reaching consumers as a different
   fact;
5. projection survives real devices rather than their schemas — a payload
   corpus spanning vendors and firmware revisions replayed through it;
6. the contract is a wire contract, not a Rust one — a cross-process consumer
   decoding batches produced against the same current schema;
7. current-schema correctness is enforced — missing explicit presence, invalid
   annotations, and stale generated output are each rejected;
8. sharing by reference costs nothing until a boundary — allocation,
   memory-retention, and throughput measurements, with in-process fan-out
   measured against encode-at-boundary costs.
