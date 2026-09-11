# The data model

A working guide to `nv.telemetry.v1`: what the messages are, how identity
works, and what real Redfish and gNMI payloads look like once projected.

`docs/ARCHITECTURE.md` explains why the design is shaped this way. This file
explains how to use it. Where the two disagree, the schema itself is the
source of truth — every field name, number, and bound below is quoted from
`schema/proto/nv/telemetry/v1/`.

## The shape of an observation

Everything the library emits is an `ObservationBatch`. A batch is one
acquisition's worth of data from one endpoint, and it carries four required
pieces of context plus exactly one payload:

```
ObservationBatch
├── endpoint   EndpointContext   which device, and its static attributes
├── origin     Origin            which provider and request class produced this
├── window     ObservationWindow when it was observed
├── coverage   Coverage          how much of the endpoint this covers
└── payload    oneof, required   readings | logs | states | inventory | resources
```

The payload is a `oneof` and the schema requires a case to be set, so a batch
always carries a domain. One acquisition that yields two domains — a sensor
catalog fetch that produces both readings and inventory — becomes two batches
sharing the same endpoint, origin, and window. They are never mixed.

Endpoint context is carried once per batch rather than on every item. A
million readings from one BMC repeat the endpoint identity zero times.

### Coverage is the part people get wrong

```proto
message Coverage {
  optional Completeness completeness = 1;  // required
  optional Subject      scope        = 2;  // absent means the whole endpoint
}
```

`COMPLETENESS_COMPLETE` is a claim: *within this scope, what you see is
everything there is.* A consumer is entitled to conclude that anything it knew
about inside that scope and does not see here is gone. `COMPLETENESS_PARTIAL`
makes no such claim — it updates what it contains and says nothing about what
it omits.

Get this wrong in the safe direction. If a walk was truncated, a request
partially failed, or you are unsure, emit `PARTIAL`. A wrong `COMPLETE`
deletes real inventory from a consumer's view.

What "everything there is" means is decided per payload domain, because each
domain names its things differently, and a consumer reconciling a complete
batch applies exactly this rule and no other:

- **`readings`.** The descriptors are the population. A `SignalKey` known
  inside the scope and absent from a complete batch's descriptors is a signal
  that no longer exists. A descriptor present with no sample says the signal
  exists and has no value right now — a null `Reading`, a sensor offline. Sample
  absence is never a deletion; only descriptor absence is.
- **`states`.** The `(subject, facet, name)` facts are the population. A fact absent
  from a complete batch is a condition the resource no longer reports.
- **`inventory`.** The subjects are the population. An absent subject is a
  removed part.
- **`resources`.** A complete graph replaces the previously collected resource
  and relation sets for the same endpoint, scope, and collection definition.
  Deletions are old members absent from the new snapshot, even though those
  members are no longer reachable through its edges. Reachability validates
  the included graph; it does not prove exhaustive collection. A walk with an
  unresolved collection branch is `PARTIAL` and cannot delete any old member.
  Providers must define which relation kinds and external targets delimit the
  collection before emitting `COMPLETE`; consumers must not infer that
  boundary from the current graph alone.
- **`logs`.** Never `COMPLETE`. A log is a stream of which the device retains a
  window; a walk reports the entries it saw, and the device may have rotated
  others out or appended since. `Coverage.scope` names the service walked —
  the namespace of every record's `entry_id` — and completeness is `PARTIAL`.

## Identity: `Subject`

Every observed thing is named by a `Subject`, and the same physical thing gets
the same subject regardless of which protocol or route observed it. That is
what lets a reading from Redfish and a state observation from a vendor API
join.

```proto
message Subject {
  optional string kind  = 1;  // required, ≤128   what the thing IS
  repeated string scope = 2;  // ≤16 elements, ≤256 each, ORDER IS SEMANTIC
  optional string id    = 3;  // required, ≤256   identifier within that scope
}
```

Three rules:

**A subject names what a thing is, never where it was read from.** The URI
stays in `source_key` on the graph, or in `InventoryItem.source_key`. It is
provenance, not identity — because the same sensor read through two different
URIs is one sensor.

**Scope carries containment when an id is not globally unique.** Redfish
`Sensor.Id` repeats across chassis: `/Chassis/1U/Sensors/Inlet` and
`/Chassis/2U/Sensors/Inlet` are different sensors with the same `Id`. So the
subject is `kind: "sensor"`, `scope: ["1U"]`, `id: "Inlet"`.

**If the scope cannot be determined, that is an error, not a guess.** A sensor
whose containing chassis could not be resolved must be reported as an invalid
field, not emitted with an empty scope. An empty scope means "no containing
scope", and guessing produces a subject that silently joins to the wrong
resource.

`scope` is deliberately not marked `unordered`: `["1U", "PSU1"]` is a different
location from `["PSU1", "1U"]`, so canonicalization must not sort it. It is one
of only two repeated fields in the contract whose order is data — the other is
`Value.List.values`, which reports what the device reported in the order the
device reported it. Every other repeated field is `unordered`.

The elements are `non_empty` even though the list itself may be empty. An empty
list means "no containing scope", which a top-level chassis genuinely has; an
empty *element* means a scope the walk failed to name, which would place the
subject under a container that does not exist.

**What the shipped subjects are, and are not.** The projections that ship
derive a subject from the resource's own `Id` and the container named in the
location the collector requested — a *source representation* of identity. It
is stable across polls of one endpoint and unique within it, and it joins to
the rest of the fleet through `EndpointContext.endpoint_id`, so today's
subjects are endpoint-local: two endpoints reporting `{sensor, [1U], Inlet}`
are two sensors until something says otherwise. A *canonical* identity — one
subject for one physical thing seen from two endpoints, or from two routes on
one endpoint — is a resolution step this model has not performed, and the
contract does not pretend it has: the first rule above is the goal the
`Subject` shape is built to carry, not a property the shipped data has.
The gate before the resource graph ships is a corpus fixture that proves the
dual-route case one way or the other — a sensor reached through
`/Chassis/1U/Sensors/Inlet` and through `/Chassis/1U/Thermal#/Temperatures/0`
must yield one `SignalKey`, or two subjects joined by a relation, on purpose
and pinned. Until then the dual-route limitation below stands, and a consumer
keys its state on `(endpoint_id, subject)`.

## Signals: `SignalKey` and `SignalDescriptor`

Readings are split into *what a signal is* (metadata) and *what it read*
(samples), joined by a key.

```proto
message SignalKey {
  optional Subject subject = 1;  // required
  optional string  facet   = 2;  // ≤128, absent when the resource has one signal
}

message SignalDescriptor {
  optional SignalKey  key   = 1;  // required
  optional string     kind  = 2;  // ≤128,  what it measures
  optional string     unit  = 3;  // ≤64,   UCUM
  optional ValueRange range = 4;  //        what the sensor CAN read
}
```

`facet` exists for resources carrying more than one signal — a power supply
reporting both instantaneous watts and cumulative kilowatt-hours is one
subject, two facets.

The descriptor carries only what a signal **is**. It deliberately does not
carry thresholds. A Redfish `Threshold.Reading` is writable — the DMTF schema
marks it `readonly: false` — which makes it a convergence target, not reading
metadata. Thresholds are observed as `StateObservation`s instead. Carrying
them in both places would give one fact two representations that can disagree.

`unit` is UCUM, and its absence means **unknown** — the source stated no unit
— never dimensionless. A dimensionless signal carries UCUM's unity, `"1"`, the
way a count or a ratio does. The distinction is load-bearing for a consumer
doing arithmetic: it may combine two `"1"` signals and must not combine two
whose units it does not know, and an absent unit collapsing into "no unit" is
how a temperature gets averaged with a fan speed. A projection sets `"1"` only
when the source's type says so (a YANG counter is a count); it never guesses it
for a bare number.

A `Readings` payload carries its descriptors alongside its samples, so a batch
is self-describing: a consumer arriving mid-stream, or reading one batch out
of storage, needs no prior state to interpret it.

## Values: the recursive type

`Value` is the contract's own tagged union, used wherever a source hands over
schemaless or semi-structured data — resource properties, log attributes,
endpoint attributes.

```proto
Value = null | bool | sint64 | uint64 | double(finite)
      | string(≤4096) | bytes(≤4096) | Timestamp
      | List(≤1024 values) | Map(≤1024 entries, key-sorted)
```

Three things to know:

**It is not `google.protobuf.Struct`.** Struct collapses every number to a
double, which silently corrupts a 64-bit counter or serial number above 2⁵³,
and it cannot represent bytes or timestamps at all.

**`Map` is a sorted entry list, not a proto `map`.** A proto map has no wire
order to canonicalize, silently keeps the last duplicate key, and its
synthetic key/value fields cannot carry bounds. The entry list is `unordered`,
so canonicalization sorts it by key, which makes duplicates adjacent; the
`unique_by: ["key"]` annotation is what rejects them.

**`List` order is data; `Map` order is not.** `List.values` is one of the two
repeated fields in the contract that canonicalization must not sort — the other
is `Subject.scope`.

Depth is bounded at 16 logical levels. That number is derived, not chosen for
roundness: prost admits 100 nested message levels, one logical level of map
nesting costs three (`Value` → `Map` → `Entry`), and the deepest batch path
adds four before the first `Value`, so 16 costs 50 of 100 and leaves the same
again in margin. `codegen/tests/depth.rs` pins it.

## Which payload domain?

| The fact is… | Domain | Message |
| --- | --- | --- |
| a number a sensor measured | `readings` | `Reading` |
| a device-reported condition, status, or writable setting | `states` | `StateObservation` |
| a log or event record | `logs` | `LogRecord` |
| a flat "this exists" fact | `inventory` | `InventoryItem` |
| structured device state with relationships | `resources` | `ResourceGraph` |

The line between `readings` and `states` is whether it is a measured number.
A temperature is a reading. `Status.Health: "OK"` is a state. A threshold is a
state, because it is writable. A sensor that answered without a value is
reporting state, not a reading — `Reading.value` is required, so there is no
such thing as a reading without a number.

The line between `inventory` and `resources` is structure. Inventory answers
"what exists" and is flat. The graph answers "how is it arranged" and carries
typed relationships, source keys, entity tags, and per-resource completeness.

---

# Worked example 1 — a Redfish sensor

Source: `GET /redfish/v1/Chassis/1U/Sensors/CPU1Temp`

```json
{
  "@odata.id": "/redfish/v1/Chassis/1U/Sensors/CPU1Temp",
  "@odata.type": "#Sensor.v1_2_0.Sensor",
  "@odata.etag": "W/\"1A2B3C\"",
  "Id": "CPU1Temp",
  "Name": "CPU 1 Temperature",
  "ReadingType": "Temperature",
  "Reading": 47.5,
  "ReadingUnits": "Cel",
  "ReadingRangeMin": 0,
  "ReadingRangeMax": 105,
  "PhysicalContext": "CPU",
  "Status": { "State": "Enabled", "Health": "OK" },
  "Thresholds": {
    "UpperCritical": { "Reading": 95, "Activation": "Increasing" }
  }
}
```

This one document produces **two batches**, because it carries two domains.

### Batch 1 — readings

```
ObservationBatch
  endpoint:  { endpoint_id: "bmc-lab-07" }
  origin:    { provider: "redfish.sensor.odata", request_class: "sensor-read" }
  window:    { start: 2026-07-30T21:14:03Z }
  coverage:  { completeness: PARTIAL }        # one sensor, not a full walk
  readings:
    descriptors: [
      { key:   { subject: { kind: "sensor", scope: ["1U"], id: "CPU1Temp" } },
        kind:  "temperature",
        unit:  "Cel",
        range: { min: { double_value: 0 }, max: { double_value: 105 } } }
    ]
    samples: [
      { key:   { subject: { kind: "sensor", scope: ["1U"], id: "CPU1Temp" } },
        value: { double_value: 47.5 } }
    ]
```

Note what happened to identity: `@odata.id` did **not** become the subject.
The chassis segment `1U` became the scope, `Id` became the id, and the URI is
kept as provenance only on the graph route.

### Batch 2 — states

```
  coverage: { completeness: PARTIAL }
  states:
    observations: [
      { subject: { kind: "sensor", scope: ["1U"], id: "CPU1Temp" },
        name:    "state",
        value:   { string_value: "Enabled" } },
      { subject: { kind: "sensor", scope: ["1U"], id: "CPU1Temp" },
        name:    "health",
        value:   { string_value: "OK" } },
      { subject: { kind: "sensor", scope: ["1U"], id: "CPU1Temp" },
        name:    "threshold.upper-critical",
        value:   { map_value: { entries: [
                    { key: "activation", value: { string_value: "Increasing" } },
                    { key: "reading",    value: { double_value: 95 } } ] } } }
    ]
```

The threshold lands here rather than in the descriptor because it is writable.
A convergence consumer reads it as observed state and may drive it toward a
desired value; a classification consumer joins it to the reading by the exact
`(subject, facet)` signal key. `StateObservation.facet` uses the same optional,
non-empty, at-most-128-byte vocabulary as `SignalKey.facet`. Absence matches
only a key with no facet; it never selects an arbitrary descriptor of the
subject. The single-signal Sensor example omits facet on both sides, so this
threshold's `reading` uses `CPU1Temp`'s `Cel`. A resource exposing power and
energy instead uses distinct facets such as `"power"` and `"energy"` on its
descriptors and thresholds. A threshold carries no unit of its own; a consumer
that has not seen the descriptor for that exact key cannot interpret its
number yet. It must not borrow a unit from another facet.

**Repeated facets form a timestamped series.** `States.observations` is
`unordered`: canonical position is not transition order. When a `(subject,
facet, name)` occurs more than once in a batch, every observation must carry
`observed_at` and no two may share an instant. Validation rejects missing or
equal timestamps on repeated facets. A single observation may omit its time.

Device `observed_at` values can be ordered only within a comparable clock
domain and clock epoch. Collector `window.start` is a separate domain: never
substitute it into device-time ordering. Within a collector's uninterrupted
clock epoch, unstamped polls can be ordered by their admission times. A clock
reset or provider change requires an embedder decision about authority.
Before delivering results, the embedder must discard completions from obsolete
endpoint/plan generations. Generation fences and provider authority are local
control state, not fields currently carried on the wire; external consumers
need that delivery contract or must retain the ambiguity.

### When `Reading` is `null`

Redfish types `Sensor.Reading` as `["number", "null"]`, and a null reading is
routine — a PSU bay with no supply installed, a fan mid-spin-up, a sensor in
`UnavailableOffline`.

`Reading.value` is required and `NumericValue` has no null arm, so **no sample
is emitted**. Emit the `SignalDescriptor` anyway, so the signal is known to
exist, and put the condition in `states`. A complete readings batch may omit
the sample: it is the descriptor's absence, never the sample's, that tells a
consumer a signal is gone, and the descriptor is there. Mark the batch
`PARTIAL` unless you genuinely walked everything — for the usual reason, not
for this one.

---

# Worked example 2 — a Redfish chassis subtree

Source: a walk of `/redfish/v1/Chassis/1U` and its links.

```
ObservationBatch
  origin:   { provider: "redfish.graph", request_class: "chassis-walk" }
  coverage: { completeness: PARTIAL,
              scope: { kind: "chassis", id: "1U" } }
  resources:
    resources: [
      { subject:             { kind: "chassis", id: "1U" },
        source_key:          "/redfish/v1/Chassis/1U",
        source_schema:       "#Chassis.v1_25_0.Chassis",
        entity_tag:          "W/\"9F8E\"",
        observed_at:         2026-07-30T21:14:03Z,
        properties:          { entries: [
                                 { key: "manufacturer", value: { string_value: "NVIDIA" } },
                                 { key: "model",        value: { string_value: "HGX" } },
                                 { key: "serial_number",value: { string_value: "SN-4417" } } ] },
        properties_complete: true,
        unresolved:          [ { location: "/redfish/v1/Chassis/1U/Drives", property: "Drives" } ] },

      { subject:             { kind: "sensor", scope: ["1U"], id: "CPU1Temp" },
        source_key:          "/redfish/v1/Chassis/1U/Sensors/CPU1Temp",
        properties_complete: false }        # only identity was collected
    ]
    relations: [
      { source: { kind: "chassis", id: "1U" },
        target: { kind: "sensor", scope: ["1U"], id: "CPU1Temp" },
        kind:   "contains" }
    ]
```

Four things this example is demonstrating:

**Scope on a graph means reachability, not subject equality.** The batch is
partial for chassis `1U`: its `Drives` collection was not walked. It cannot
establish deletion of a previously observed drive. A fully enumerated snapshot
must include outgoing edges from its scope root to the collected nodes, but
that structural check alone never proves the walk was exhaustive.

**`properties_complete` is required and it matters.** `true` means "this is
the device's full representation" — a property absent here is one the device
does not implement. `false` means "I collected some properties" — absent tells
you nothing. A convergence consumer that conflated the two would read an
uncollected property as unset and try to write it.

**A link you cannot yet name stays unresolved.** The `Drives` collection was
not walked, so it is an `UnresolvedReference` carrying the location with no
identity attached — not a `ResourceRelation` with an invented target. That
distinction is what keeps a partial walk honest: an invented identity is
indistinguishable from a real external target.

**`entity_tag` and `observed_at` are excluded from the content hash.** They
carry `collection_metadata: true`. `ResourceGraph` is `hashable` so a
convergence adapter can compare two polls exactly; if the ETag and the read
time were hashed, an idle device would report a change on every single poll —
the one thing the hash exists to prevent.

---

# Worked example 3 — a gNMI subscription update

Source: a `SubscribeResponse` carrying a `Notification`.

```
timestamp: 1785621243000000000
prefix:    { target: "switch-3", elem: [ {name:"interfaces"},
                                         {name:"interface", key:{"name":"Ethernet1/1"}} ] }
update: [
  { path: {elem:[{name:"state"},{name:"counters"},{name:"in-octets"}]},
    val:  { uint_val: 91827364554433 } },
  { path: {elem:[{name:"state"},{name:"oper-status"}]},
    val:  { string_val: "UP" } }
]
```

One notification, two domains again — a counter and a status.

```
# Batch 1
  endpoint: { endpoint_id: "switch-3" }
  origin:   { provider: "gnmi.subscribe", request_class: "interface-counters" }
  window:   { start: 2026-07-30T21:14:03.000000000Z }   # Notification.timestamp
  coverage: { completeness: PARTIAL }                   # ON_CHANGE/SAMPLE stream
  readings:
    descriptors: [ { key: { subject: { kind: "interface", scope: ["switch-3"],
                                       id: "Ethernet1/1" },
                            facet: "state/counters/in-octets" },
                     kind: "counter" } ]
    samples:     [ { key: <same>, value: { uint_value: 91827364554433 } } ]

# Batch 2
  states:
    observations: [ { subject: { kind: "interface", scope: ["switch-3"],
                                 id: "Ethernet1/1" },
                      name:    "state/oper-status",
                      value:   { string_value: "UP" } } ]
```

**`uint_value`, not `double_value`.** This is exactly why `NumericValue` is a
union. 91827364554433 fits a double today, but interface counters run to 2⁶⁴
and a double loses integer precision above 2⁵³ — a silently wrong counter is a
fabricated observation with extra steps.

**Which arm is fixed by the source's declared type, not by the value.** YANG
says `in-octets` is a `uint64`, so it is always `uint_value`, even when the
value happens to be small. Choosing by value would move a signal between arms
as a reading crossed zero or lost its fraction, and every such move registers
as a content change to a consumer comparing hashes.

**No unit, or `"1"`.** OpenConfig rarely carries a `units` statement, so
`SignalDescriptor.unit` is absent for most leaves — *unknown*, and a consumer
treats it so. `in-octets` is the exception in the other direction: YANG types
it `counter64`, a count, so the projection may declare UCUM's `"1"` from the
type. It must not declare `"1"` for a `decimal64` it cannot place.

**Streams are `PARTIAL`.** A subscription update reports what changed; it
never asserts what else exists.

---

# Failures are not observations

A collection failure is not a batch. It is an `AcquisitionStatus`, on a
separate stream:

```
AcquisitionStatus
  endpoint_id:   "bmc-lab-07"
  provider:      "redfish.sensor.odata"
  request_class: "sensor-read"
  outcome:       OUTCOME_FAILED
  failure_class: FAILURE_CLASS_TIMEOUT
  retryable:     true
  started_at:    2026-07-30T21:14:03Z
  duration_nanos: 30000000000
```

A failed request emits **no batch at all** — never an empty one, never one
with zeroes. Three facts stay distinct and a consumer needs all three:
observation absence (a `COMPLETE` batch that omits something), collection
failure (this stream), and staleness (derived from batch timestamps by the
consumer's own policy).

`failure_class` drives dispatcher policy: connectivity and authentication
failures may trip the endpoint breaker, while unsupported and protocol
failures affect only their request class.

A fourth fact rides its own stream: what one source field failed to become.
`ProjectionIssues` carries the same identity and instant that stamp its
sibling batches, and one issue per failed field:

```
ProjectionIssue
  path:   "Chassis.Sensors[3].Reading"
  kind:   ISSUE_KIND_INVALID
  detail: "not a finite number"
```

Missing and invalid are different facts — silence versus an answer that
cannot be used — and issues ride beside batches, never inside one: a response
that answered and was wholly unusable would otherwise need a fabricated empty
batch just to carry them. An acquisition with no issues emits no
`ProjectionIssues` at all; an empty envelope would be the same fabrication.

---

# The annotation vocabulary

Rules live on the schema as custom options, and the compiler enforces them
before generating anything. See `docs/EXTENSIONS.md` for the numbering.

| Option | On | Means |
| --- | --- | --- |
| `zero_is_meaningful` | field | this scalar's zero is real data, so it needs no `optional` |
| `finite` | field | doubles must be finite; a NaN is an invalid field, not a missing one |
| `required` | field | validators reject a message where this is absent |
| `unordered` | field | this repeated field's order is not semantic, so canonicalization sorts it |
| `max_items` / `max_len` | field | bounds; `0` is a real bound, which is why they are `optional` |
| `collection_metadata` | field | records how a fact was collected, not what was observed — skipped by hashing |
| `non_empty` | field | a string or bytes value must carry something; per element on a repeated field |
| `reject_unspecified` | field | an enum field must not carry the zero value |
| `unique_by` | field | element fields that identify an element of this repeated field; duplicates are rejected |
| `validated` | message | emit a wrapper that owns the invariants |
| `hashable` | message | emit logical content hashing; requires `validated` |
| `max_depth` | message | recursion bound for a self-referential type |
| `required` | oneof | a case must be set |

The headline rule the compiler enforces: **every scalar the contract can reach
either declares `optional` or annotates `zero_is_meaningful`.** proto3 encodes
an unset scalar as its zero, so without this a reading of 0.0 and a reading
that was never taken are the same bytes. Nothing in `nv.telemetry.v1` currently
needs the exemption — every scalar has explicit presence.

The compiler also rejects the indirect routes to the same failure: a map with
a scalar value type (an entry carrying only its key decodes the value as
zero), a field whose message type is declared outside the contract (its
scalars are never checked — `google.protobuf.DoubleValue` is the trap), and
proto2 files (a `required` scalar reports as having presence but generates as
a bare value).

## Presence is not content

`required` proves a field was set, which is a weaker claim than it looks. The
empty string sets a string; `UNSPECIFIED` sets an enum. Both satisfy `required`
while carrying no information, and both are what a projection produces when a
read half-failed. `non_empty` and `reject_unspecified` close that gap on the
declarations where the difference matters — identity, naming, and the enums a
consumer branches on.

Two boundaries are deliberate. `non_empty` is absent from every field carrying
verbatim device text — `Value.string_value`, `Value.bytes_value`, and
`LogRecord.message`. A device that reported `"SerialNumber": ""` reported
something, and a Redfish `LogEntry` whose `Message` is empty because the text
lives in a registry under `MessageId` is ordinary; calling either invalid would
be fabrication pointing the other way, and would fail a whole batch over it.
The rule holds for identifiers, projected vocabulary tokens, and
library-generated values, where an empty string is only ever a failed read. And
`reject_unspecified` rejects the zero value only, never an unrecognised one —
an unrecognised numeric token remains distinct from an unspecified value.
Unknown values decode; what to do about one is the consumer's call. This
preservation behavior is not a cross-revision compatibility promise.

`unique_by` names the fields that identify an element rather than comparing
whole elements, because the contradictions worth catching are the ones where
the rest of the element differs: two `SignalDescriptor`s for one key with
different units are a batch a consumer cannot interpret, and comparing whole
elements would call them distinct. Every named key must be one the element
always carries — `required`, or `zero_is_meaningful` and so having no absent
state at all — because a key that can be absent would make two elements that
both omit it duplicates of each other.

It applies only where a repeat is genuinely incoherent rather than merely
surprising:

| Collection | Key | Why |
| --- | --- | --- |
| `Value.Map.entries` | `key` | a map with two values for one key has no reading |
| `Readings.descriptors` | `key` | two definitions of one signal, and no rule for choosing |
| `ResourceGraph.resources` | `subject` | a graph node is its identity |
| `ResourceGraph.relations` | `source`, `target`, `kind` | edges are a set; the same pair may hold several *kinds* of edge |
| `Inventory.items` | `subject` | inventory is the set of what exists |

Every other repeated field is deliberately excluded, and for a reason:

| Collection | Why not |
| --- | --- |
| `Readings.samples` | a metric report carries a series for one signal, separated by their own optional timestamps |
| `States.observations` | a gNMI `ON_CHANGE` window can carry an interface going down and back up |
| `Logs.records` | `entry_id` is optional — many sources do not stamp entries at all |
| `ObservedResource.unresolved` | two distinct properties may name the same URI, and adding `property` to the key names an optional field |
| `Value.List.values` | a list reports what the device reported; repeats are data |
| `Subject.scope` | a scalar list, so there are no element fields to key on |

The pattern is that a uniqueness key must be `required` on the element type. In
every excluded case the field that would separate the elements — a timestamp, a
source entry id, the property that held a link — is one the source may not
supply, and a key that can be absent would call two elements duplicates
precisely when the source was least informative.

## Adding a contract message

A new wire message touches these artifacts; the list exists so first passes
are complete rather than review-discovered:

1. The `.proto` under `schema/proto/nv/telemetry/v1/` — per-field
   annotations decided (`required`, `non_empty`, bounds,
   `reject_unspecified`; on repeated fields `unordered` and `unique_by`: is
   order data, and what is an element's identity?), `validated` on the
   message.
2. `make codegen` — regenerates wire/model/limits and moves
   `schema/contract.lock`; the lock diff is the review surface.
3. `model/src/rules.rs` — the cross-field rules the vocabulary cannot
   state, or a deliberate `Ok(())` recording that the schema states none
   (compiler-forced: the model does not build without an answer).
4. `model/src/tests/boundary.rs` — a boundary test per wrapper rule, on the
   builder and decode paths both, and the maximal round trip extended when
   the message carries `encode_to_vec`.
5. `model/src/tests/wire_properties.rs` — every new `optional` enum field
   joins the presence census.
6. A producer bridge in `source/` when sources originate the type, with its
   bounding and validity story stated.
7. Docs: this file (semantics, a worked example) and `ARCHITECTURE.md`'s
   Output contract section when the message is a stream.

---

# Known limitations

Honest list. These are catalogued, not hidden, and none is a bug in the
implementation — they are places the model does not yet reach.

**Rules that live in comments.** The vocabulary still cannot express
cross-field constraints, and those are stated in schema comments as "wrapper
rules": `ValueRange` needs at least one bound with min not exceeding max,
`ObservationWindow`'s end must follow its start, `AcquisitionStatus` carries a
`failure_class` exactly when it failed, `ProjectionIssue` quotes a `detail`
exactly when its kind is invalid, `ProjectionIssues` is never empty,
`Timestamp.nanos` is bounded below one second, every `SignalKey` a sample
references must resolve in the same batch, and a complete `ResourceGraph`
must be reachable from its scope subject.
Validators enforce them; the vocabulary cannot state them, because each
relates one field to another.

Two absolute bounds are also missing: a minimum item count, and a value range
for numbers. The first is why an empty payload with `COMPLETE` is a valid batch
asserting total absence — though that is partly semantic, since a genuinely
empty complete collection is a real observation.

**gNMI JSON payloads.** `json_ietf_val` is the dominant production encoding and
cannot currently be carried: `Value.bytes_value` caps at 4096 bytes and a
single `/interfaces` subtree is far larger. `Notification.delete[]` has no
representation either.

**Redfish `Power`/`Thermal` fragments.** One document containing separately
addressable array elements has no non-lossy encoding — split it and N
resources share one ETag and one fetch time; keep it whole and the elements
lose their subjects.

**Dual sensor routes.** The same physical sensor reached via `/Sensors` and via
`/Thermal#/Temperatures` produces different `SignalKey`s today, and subjects
are endpoint-local in general (see *Identity*). The resource graph does not
ship until a corpus fixture settles the dual-route case.

**Units on thresholds.** A threshold in `states` carries no unit; it is bound
to the unit of the `SignalDescriptor` for the exact `(subject, facet)` key, which
travels in a different batch. A consumer that has the threshold and not the
descriptor holds a number it cannot yet interpret.

**Logs are never complete, and a walk is bounded.** A log read ships the
entries that arrived since its last shipped walk: the read keeps a cursor —
the newest `occurred_at` shipped and the ids shipped at that instant — reads
newest first, and stops where the cursor was reached. The member and time
budget then caps how many *new* entries one poll carries, and a burst larger
than the budget loses its oldest entries from view, not its newest. A log
whose newest entry is older than the cursor (wiped and refilled, or a device
clock stepped back) is read again in full; a wipe refilled within the
cursor's own second under the same ids is invisible, and entries the device
does not stamp are shipped every poll. The cursor is not persisted, so a
restart replays one window. Records that share an `occurred_at` — devices
stamp to the second, and a burst lands many on one instant — carry no order
but `entry_id`, which is the device's own spelling and compares numerically
only when the device numbers its entries.

---

# Rules of thumb

- Never fabricate. No sample is better than a zero, and no batch is better
  than an empty one.
- Prefer `PARTIAL`. `COMPLETE` is a deletion instruction, and what it deletes
  is decided per domain: descriptors, facets, subjects, reachable resources —
  never samples, never log records.
- The subject is what the thing is. The URI is provenance. Until identity is
  resolved across routes, key state on `(endpoint_id, subject)`.
- An absent `unit` is unknown. Dimensionless is `"1"`.
- A repeated state facet requires distinct timestamps in one clock domain;
  list position never carries transition order.
- Pick the numeric arm from the source's declared type, once, and never vary
  it per poll.
- Thresholds and anything else writable are state, not metadata.
- If the scope cannot be derived, report an invalid field. Do not guess.
