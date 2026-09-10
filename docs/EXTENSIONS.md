# Protobuf extension numbers

`nv-telemetry` annotates its schema with custom options, which are extensions
of `google.protobuf.*Options`. This file is the registry for the numbers it
uses.

## Allocation

| Number | Extends          | Option                                     |
| ------ | ---------------- | ------------------------------------------ |
| 52001  | `FieldOptions`   | `nv.telemetry.options.v1.field_invariant`   |
| 52002  | `MessageOptions` | `nv.telemetry.options.v1.message_invariant` |
| 52003  | `OneofOptions`   | `nv.telemetry.options.v1.oneof_invariant`   |

`nv-telemetry` claims 52000–52099 for every extendee.

## Why these numbers are not, and cannot be, globally unique

Protobuf reserves 50000–99999 for use inside a single organization and
explicitly does not treat those numbers as globally unique; the public
extension registry allocates numbers outside that range. A number in the 52000
block therefore cannot be publicly registered, and no amount of documentation
makes it collision-proof outside NVIDIA.

This is the current internal allocation. The library is under development,
so the allocation may be refactored without preserving historical descriptor
sets. Before publishing a stable schema outside NVIDIA, choose globally
registered numbers and define a release compatibility policy.

## Coordination

Extension numbers are namespaced **per extendee**, not globally. `FieldOptions`
52001 and `MessageOptions` 52001 are different slots, so a claim that does not
name the extendee cannot prevent the collision it is written to prevent. The
table above names both.

Numbers must be coordinated with every other NVIDIA schema that could share a
descriptor pool with this one. `infra-controller` already uses `MethodOptions`
51000 and `FieldOptions` 51001 for its admin-CLI annotations, which is why this
project starts at 52000 rather than at the bottom of the range.

A collision does not fail where it is made. Options are identified on the wire
by number alone, so two schemas claiming one number produce either a
pool-construction failure or a silently misdecoded annotation in whichever tool
first loads both — typically a reflection client, a language binding, or a
service owned by neither team.

## Adding one

An option is defined exactly once, as a row of the option tables in
`codegen/src/options.rs` (`FIELD_OPTIONS`, `MESSAGE_OPTIONS`, `ONEOF_OPTIONS`).
The row carries the option's declared shape, the fields it may be written on,
its semantic stances, its contract-lock rendering, and what the canary must
read back — and the reader, the lint, the lock, and the canary all consume the
rows. The decisions an option demands are therefore columns the compiler will
not build without, not steps a review has to remember; this replaced a
seven-step checklist whose steps kept being found half-done.

1. Add a row to the allocation table above, naming the extendee — for a new
   extension number only.
2. Declare the field in
   `schema/proto/nv/telemetry/options/v1/annotations.proto`, and give the
   canary a field that uses it.
3. Add the struct field, the reader line, and the table row in
   `codegen/src/options.rs`.
4. Add the row to the vocabulary table in `docs/DATA-MODEL.md` — the one step
   nothing checks.

Every half-done state between 2 and 3 fails the build naming the option: a
declaration with no table row is `NotRead`, a row with no declaration is
`MissingField`, a shape the row does not match is `WrongType`, a missing
reader line does not compile, and a canary that does not exercise the option
fails its probe. Two conventions are enforced by the columns themselves:
numeric options are `optional uint32`, because a bare proto3 scalar cannot
distinguish a bound of zero from no bound; and where the option applies is the
`Applies` column, so writing it somewhere it means nothing is rejected without
a hand-written rule.

Only genuinely contextual rules live outside the table, in
`codegen/src/lint.rs`, each commented with why a column cannot carry it:
reachability from a hashable message, oneof membership, value-dependent
clashes such as `non_empty` with `max_len: 0`, recursion for `max_depth`, and
the `unique_by` key checks.

## Removing one

Delete the declaration, the canary field, the struct field, the reader line,
and the table row together; the compiler holds the halves to each other, in
both directions, with the same errors as above.

Once a stable contract is released, reserve both halves: `reserved 6;` **and**
`reserved "max_len";`. A number reservation alone leaves the name free, so a later field can take the old name
at a new number and collide with stored JSON and text-format data, which is
exactly where old and new tooling meet.

Those reservations apply once a stable contract has been released. The current
development schema has no historical compatibility requirement: removing or
renumbering an option is a coordinated edit of the declaration, compiler,
canary, and generated artifacts. No reservation is required solely to preserve
a development revision.
