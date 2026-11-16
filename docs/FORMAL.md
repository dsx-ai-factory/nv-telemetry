# Formal model

Some of what this library promises is about a running system: a poll is not
starved, a stream comes back after the device resets, a retired endpoint
goes quiet. Each depends on several components and on what the device does
in between, which review and unit tests establish poorly.

This is the register of those promises and of what they rest on: what we
assume about the dispatcher, the device, and the embedder, the state
machines we own, and the properties we claim. Read it before changing
scheduling, stream, or lifecycle code, and update it in the same change.

Nothing parses this file. What is enforced is `make spec` and the tests
named below; a status here is as accurate as its last edit.

## How it is checked

| Question | By | Where | Runs in |
|---|---|---|---|
| Does the design hold over time, whatever the device does? | Quint specs, every reachable state | `spec/*.qnt` | `make spec` |
| Does the code follow the design? | Traces of `endpoint.qnt` and `stream.qnt` replayed against the code | `spec/traces/`, `orchestration/tests/spec_replay.rs`, `sources/redfish/tests/event_stream_replay.rs` | `cargo test` |
| Does it hold at the numbers we ship? | The real collector at the default policies (P10) | `orchestration/tests/defaults.rs` | `cargo test` |
| Do the dispatcher's schedulers do what D1 to D9 say? | The dispatcher's own unit tests | `nv-redfish` | not here |

**A spec** is variables, an `init` action, and a `step` action; a property
is an invariant. Every spec is finite, so the checker visits every state.
A spec runs as several instances, the same model under different
parameters. An instance named `…_before_…` carries a defect that was
fixed and must fail its property.

**The replay.** `make spec` draws 40 traces from each replayed spec, from a
fixed seed, into `spec/traces/`, with a fingerprint of the spec. The
endpoint replay seats the endpoint in the real collector under a paused
clock, performs the device's acts and the ticks, and requires every poll
and connect to have been dispatched exactly as often as the spec
dispatched it. The stream replay plays the spec's device through the
transport and holds `EventStream` to the resume id of each open, the run
each event ships under, each gap, and the resume position after every
step. A spec edited since its traces were drawn fails `cargo test` until
`make spec` runs again.

A replay reaches the code through its public API. `logwalk.qnt` has none
for that reason: the public API does not set the walk's budget.

## Status of an entry

| Status | Meaning |
|---|---|
| assumed | Stated here; nothing in this repository checks it. |
| checked | An invariant of a spec in `spec/`, model-checked. |
| replayed | Checked, and spec traces are replayed against the code. |

## How specs are written

- **Time is whole ticks.** An instance says what a tick is.
- **"Eventually" is a bound.** A due poll is dispatched within `WINDOW`
  ticks. An instance carries the least bound that holds, and this register
  quotes it.
- **The device is an action** with no guard beyond its own state. The
  checker picks when.
- **The runtime does not idle.** Time advances only when nothing is
  admissible (D9).
- **Bound what grows.** Timers count down, counters stop one past their
  bound. A spec that is not finite does not terminate under TLC.
- **A defect stays as an instance** that must fail.
- **A failing spec is fixed in the design it models,** never in the
  property or the device.
- **A replayed instance has one reading.** `replay` and `replay_stopping`
  set `EAGER`, so the collector reacts to each event before the device
  acts again, and keep the bucket's balance off zero, where the spec lets
  a scan come or not.

## Assumptions

### Dispatcher (`nv-redfish-dispatcher` 0.22.1)

Re-read these when the dependency moves. The replay exercises D2 to D7 and
D9 as the collector uses them; they stay `assumed` all the same.

| Id | Assumption | Source | Status |
|---|---|---|---|
| D1 | The runtime refreshes readiness, pulls one item at a time, and reports the completion of every dispatched item exactly once. | `scheduler.rs` | assumed |
| D2 | The token bucket starts full and accrues continuously up to its capacity. It admits when the balance covers the child's cost hint, or one whole token when there is no hint, either held to the capacity. It charges the item's real cost, into debt if needed, and admits nothing while in debt. While in debt it does not ask its subtree for readiness, and reports the instant one token is back. A zero interval means always full. | `schedulers/token_bucket.rs` | assumed |
| D3 | Strict priority takes from the highest class that has work, and passes no cost hint upward. With D2, our bucket therefore gates on one token whatever the unit costs. | `schedulers/strict_priority.rs` | assumed |
| D4 | Within a class, the first child with work is taken, starting after the child taken last. | `schedulers/round_robin.rs` | assumed |
| D5 | The breaker opens when the failure fraction of its sample window passes the threshold, once it has the minimum samples. Open, it dispatches nothing until the cool-down has passed since the completion that opened it. It then admits one probe, and only after work dispatched earlier has drained. The probe is whatever its subtree hands it next. A successful probe closes it and clears the window; a failed one opens it again for a full cool-down. Open, it does not ask its subtree for readiness. | `schedulers/circuit_breaker.rs` | assumed |
| D6 | Bounded concurrency never has more than its cap in flight, and a slot frees only on completion. At its cap it does not ask its subtree for readiness. | `schedulers/bounded_concurrency.rs` | assumed |
| D7 | A periodic leaf is due at once when seated. Its next due instant is one interval after dispatch, not after completion, and it never bursts to catch up. | `schedulers/periodic_leaf.rs` | assumed |
| D8 | Fixed cost stamps the declared cost on every item its child yields. | `schedulers/fixed_cost.rs` | assumed |
| D9 | The runtime dispatches work until the scheduler has none, then sleeps to the earliest instant any node reported, and wakes on a queue event. It keeps the earliest instant it was told until that instant passes, whether or not the node that named it still waits for it. A graceful shutdown admits nothing new, lets work in flight complete, drains its outputs, and only then reports the shutdown. | `runtime.rs` | assumed |

### Device and transport

| Id | Assumption | Source | Status |
|---|---|---|---|
| E1 | An event id is unique only within one run of the device's ids, and the ids restart when the device does. bmcweb's is an in-memory counter that starts at one, and it is both the SSE id and the record's `EventId`. | `sources/redfish/src/stream.rs`; bmcweb `event_service_manager.hpp` | assumed |
| E2 | A device answers a resume position it does not hold in one of two ways. bmc-mock refuses it with HTTP 400; its ids carry a generation that changes on reset, so it never holds a position from before one. bmcweb sends `EventBufferExceeded` under a fresh id and goes on from there. Neither answer says whether the history aged out or the ids restarted. | bmc-mock `event_service.rs`; bmcweb `event_service_manager.hpp`; `sources/redfish/src/failure.rs` | assumed |
| E3 | A device may close an open stream at any moment, before or after delivering. | `sources/redfish/src/stream.rs` | assumed |
| E4 | A device may drop events it could not buffer. It says so with `EventBufferExceeded`; a silent drop cannot be detected. | `sources/redfish/src/stream.rs` | assumed |
| E5 | A device may stop answering, and a request then ends only if something bounds it. A Redfish walk has its own deadline of 30 seconds. A single Redfish read has the transport's timeout, two minutes by default in nv-redfish's client. A gNMI poll has the deadline the caller wraps it in. Opening an event stream has none of its own: nv-redfish's client sends that request with no timeout and checks idleness only once frames flow, and the gNMI source sets no connect timeout. The stream leaf's connect deadline, 30 seconds by default, bounds it (M2). | `sources/redfish/src/provider.rs`; nv-redfish-bmc-http `reqwest.rs`; `sources/gnmi/src/lib.rs`; `orchestration/src/` | assumed |
| E6 | A payload may fail to decode or be a `MetricReport`. Neither ends the stream. | `sources/redfish/src/stream.rs` | assumed |
| E7 | Any request may fail with any failure class. | `source/src/` | assumed |
| E8 | A device whose ids carry no generation finds a stale id once it has issued that id again after a restart, and resumes after it without a sign. The events before it are lost with no gap, and nothing in the stream reveals it. bmcweb compares the id alone. | bmcweb `event_service_manager.hpp` | assumed |
| E9 | A log service lists its entries oldest first or newest first, and says which only through member ids that number the entries, when they do. A full log drops its oldest entry. Entries written within one second share a stamp. | `sources/redfish/src/provider.rs` | assumed |
| E10 | A device's clock may step back: a BMC without a battery-backed clock restarts in the past until its time is set. What it writes then is stamped before older entries. | none here; no fixture has it | assumed |
| E11 | A log may be cleared, and may then number its entries from one again. | `sources/redfish/src/provider.rs` | assumed |
| E12 | A device that advertises `$filter` answers the members stamped at or after the instant asked, in the collection's order. | `sources/redfish/src/provider.rs` | assumed |
| E13 | A collection longer than a page is listed in pages, each linking to the next, under the collection's count. A device honours `$skip`, or answers its first page whatever the skip says. | `sources/redfish/src/provider.rs` | assumed |

### Embedder

| Id | Assumption | Source | Status |
|---|---|---|---|
| C1 | The collector runs inside a tokio runtime with timers. | `orchestration/src/collector.rs` | assumed |
| C2 | The consumer keeps taking reports. While the report stream is full, an endpoint's clock stands still, so every bound below is in the endpoint's time, not wall time. | `orchestration/src/collector.rs` | assumed |
| C3 | An embedder that restarts hands back the last position `resume_position` gave it. An older one is safe on a device that refuses stale ids. On bmcweb it is not: see P5. | `sources/redfish/src/stream.rs` | assumed |

## Machines we own

| Id | Machine | Source |
|---|---|---|
| M1 | **Endpoint subtree.** The bucket over strict priority: stream connects at priority 1, the polls at 0. The polls take turns under the in-flight bound (default 1) over the breaker. A connect holds no in-flight slot and passes no breaker: it pays the bucket and backs off on its own ladder. Each leaf carries its declared cost. `validate` refuses a zero refill, burst, cool-down, first retry, or connect deadline, so a spec may take them as positive. On an endpoint with polls no reconnect comes sooner than `per × ceil(2 × total connect cost / refill)`. | `orchestration/src/recipe.rs` |
| M2 | **Stream leaf.** `Due → Connecting → Streaming → Ended → Due`, or `Stopped`. A failed connect goes from `Connecting` to `Ended`. The delay starts at the first retry, doubles for each instance in a row that delivered nothing, is capped, and gains the endpoint's stagger; the floor of M1 applies on top. A failure that says retrying is pointless stops the stream. Dropping the reports stops it from any phase and aborts a connect in flight. A connect completes when the stream opens: an open stream holds no in-flight slot and no tokens. An ended instance is scheduled by the next readiness scan to reach its leaf, and the delay runs from that scan; a bucket in debt keeps the scan away (D2). A connect that has not opened the stream by the policy's deadline is aborted at such a scan and reported as a retryable timeout. | `orchestration/src/stream.rs` |
| M3 | **SSE cursor.** `Live`, `Resume(position)`, or `Refused(id)`. `Resume` reopens after its id in the same run; `Live` and `Refused` open without an id in a new run. Every payload that carries an id moves the cursor, decoded or not. A refused resume becomes `Refused` and the stream reopens live within the same connect attempt, its first item an issue at `@gap`. An `EventBufferExceeded` record is an issue at `@gap` and starts a new run for its payload and those after it. Any other failed open keeps the position. | `sources/redfish/src/stream.rs` |
| M4 | **Collector lifecycle.** One task and one runtime per endpoint, and one bounded report stream for all of them. A task holds the reports the stream has no room for, in order, and pulls no stream item while it holds any. Removal drops the streams, shuts the runtime down gracefully, delivers the reports held, and then delivers `Retired`; the endpoint's id stays taken until that retirement is in the stream, so an endpoint added again cannot report ahead of it. Shutting the collector down retires every endpoint and refuses new ones. An endpoint without polls retires once every stream has stopped. A panicking unit retires its endpoint, and what was in flight is lost. With the consumer gone a task ends without a retirement. | `orchestration/src/collector.rs` |
| M5 | **Failure policy.** Each failure class has a default retryability, which the source may override. Only `Connectivity`, `Authentication`, and `Timeout` count as failures in the breaker; every other outcome is a success sample. Only polls are samples: a connect, opened or failed, is none, and neither is a failure on an open stream. | `orchestration/src/status.rs`, `report.rs` |
| M6 | **Log walk and cursor.** The cursor is the newest member shipped, its stamp, the order the collection was read in, whether a probe established that order, and the collection's count. A poll opens the `$filter` answer from the cursor's second when the device filters and its clock does not read before that stamp, else the collection. It visits members newest first, stops at the cursor's member under its own stamp, drops the cursor on meeting that id under another stamp, ships the rest, and moves the cursor to the newest it shipped, once the batch is certain to ship. The order is the ids' when they number the entries, else the one the cursor remembers, probed or not, else oldest first. Where the ids say nothing, a walk doubts its order when the end it took for newest held only the cursor's member though the collection grew or the filtered answer was wider, or when the cursor's member is gone from a collection of the same size; it then reads the other end and, if that is stamped later, walks again the other way round. A collection in pages is read from its newest end: an oldest-first one is entered with `$skip` a budget's worth before its end, or where the cursor's member last sat if that is later, and a jump that does not meet that member is read again from the head. Pages are followed until a budget's worth of members is read, and the budget ends the visit too. A walk that left members unread without meeting the cursor's member says so at `@truncated`: members a jump skipped, members the budget cut, pages it did not read. Pages left unread in a collection listed oldest first hold its newest members, and are said whatever the walk met. | `sources/redfish/src/provider.rs` |

## Properties

| Id | Property | Rests on | Status |
|---|---|---|---|
| P1 | **Polls are not starved.** On a polled endpoint with the breaker closed, a due poll is dispatched within the time the bucket takes to refill one round of connects, whatever the device does to the streams. | D2, D3, D4, D6, D7, D9, E3, M1, M2 | replayed; checked (`spec/endpoint.qnt`, default rate policy): no wait with one poll and one stream, 4 ticks with two of each, where one round of connects refills in 6 |
| P2 | **Debt is bounded.** The bucket's balance never falls below one minus the largest cost it charges. | D2, D3 | replayed; checked (`spec/endpoint.qnt`, every instance) |
| P3 | **A stream comes back.** A stream stops only when a failure said retrying is pointless or its endpoint retires; a position the device does not honour never ends it. The next connect is dispatched the larger of the capped, staggered delay and the floor after the scheduler next scans the stream, whatever the breaker does, and a connect the device does not answer is given up at its deadline. | D2, E2, E3, E5, E7, M1, M2, M3 | replayed and checked (`spec/stream.qnt`) for when it stops, as far as the provider goes: a refused position opens live, and every stream the device closes ends retryably; replayed and checked (`spec/endpoint.qnt`) for the delay and the deadline, without stagger: the scan, the dispatch, and the giving up are each at most 3 ticks late. |
| P4 | **A lost position is reported.** Each position the device does not honour yields exactly one issue at `@gap`, at the latest as the first item of the instance that opens next. | E2, M3 | replayed; checked (`spec/stream.qnt`) |
| P5 | **A key names one event.** Endpoint, scope, and entry id never name two different events, across any number of device restarts. The stronger claim, that a run never spans a restart, does not hold on bmcweb (E8) and is not made. | E1, E2, E8, C3, M3 | replayed; checked (`spec/stream.qnt`) for bmc-mock, and for bmcweb while the position is the last one read; **fails on bmcweb for an older handed-back position** (`bmcweb_stale_position`) |
| P6 | **Retirement ends.** After removal, `Retired` follows the endpoint's last report and nothing follows it. It arrives once the polls in flight end: a poll is left to finish, and a connect in flight is aborted. While it waits, the endpoint cannot be added again, and a collector that is shutting down does not end its reports. | D9, E5, C2, M2, M4 | checked (`spec/lifecycle.qnt`) where every poll is bounded, and (`spec/collector.qnt`) across endpoints added again; fails where a poll is not bounded (`without_timeout`). Accepted: E5 bounds every poll the shipped sources make. |
| P7 | **A hung request is bounded.** A poll in flight holds its in-flight slot until the transport gives up on it, and the polls behind it wait that long. A connect holds no slot: one the device never answers holds no poll back. | D6, D9, E5, M1, M2 | replayed; checked (`spec/endpoint.qnt`, `hostile`). |
| P8 | **One status per acquisition.** Every acquisition that ends yields exactly one report; each endpoint's reports reach the consumer once and in order, its retirement closes exactly what it reported, and backpressure delays reports but never drops one. | C2, M4 | checked (`spec/collector.qnt`), for what the collector does with a report once it has one; that an acquisition yields one is by the report type and not modelled |
| P9 | **The breaker judges reachability.** Only endpoint-scoped failures of polls move the breaker toward open. | D5, M5 | assumed; `spec/endpoint.qnt` takes it as given |
| P10 | **Polls are not silenced.** Nothing a stream does holds the polls back beyond P1, and the breaker holds them for no longer than its cool-down and the waits of P1 and P7 around it. | D3, D4, D5, M1, M2, M5 | replayed; checked (`spec/endpoint.qnt`), and tested at the default policies (`orchestration/tests/defaults.rs`): 5 ticks at a cool-down of 8 with one poll and one stream (`hostile`), 3 beside two failing streams (`two_failing_streams`). |
| P11 | **No log entry is skipped unsaid.** After a poll, the consumer holds every entry the log does, but for those the log held when a poll said it was cut short. | E9, E10, E11, E12, E13, M6 | checked (`spec/logwalk.qnt`) for numbered ids in either order, whichever way the clock moves, in one document or in pages, under the budget, and for a refilled log; with `$filter` while the clock only moves on. Limits by design: ids that say nothing leave stamps to tell the order, which takes a later stamp (`unnumbered_newest_first_bursts`) and a clock that does not step back (`unnumbered_stepping_back`); a refill that lands the cursor's id on the cursor's second hides what lies behind it (`refilled_same_second`); `$filter` hides an entry stamped behind the cursor (`filtering_stepping_back`). |
| P12 | **No log entry ships twice.** A poll ships only what the consumer does not hold. Where the ids say nothing and the first guess of the order was wrong, the one or two polls that correct it ship again what the cursor was anchored behind. | E9, E10, E11, E12, M6 | checked (`spec/logwalk.qnt`) for numbered ids, whichever way the clock moves, with `$filter` or without. By design it does not hold for ids that say nothing under a clock that steps back (`unnumbered_stepping_back`). |
| P13 | **A cut keeps the newest.** What a poll leaves behind is older than what it ships. | E9, E13, M6 | checked (`spec/logwalk.qnt`) in one document and in pages, with `$filter` or without. By design it does not hold on a device that ignores `$skip`: the walk reads the pages from the start, the budget ends them before the newest, and every poll says so (`budgeted_ignoring_skip`). Nor where `$filter` hides an entry stamped behind the cursor (`filtering_stepping_back`). |
| P14 | **A cut is said only when there was one.** A poll that met the cursor's member says it was cut short only while the log holds something the consumer does not. A poll the budget stopped first cannot tell, and says so. | E13, M6 | checked (`spec/logwalk.qnt`) for a log listed either way round, in one document or in pages. |

## Defects the model must see

| Defect | Property | Found by | In the model |
|---|---|---|---|
| Streams that kept closing fell due at every refill and took each token before a poll could. It takes two streams, or a refill no faster than the first retry. One Redfish stream at the default rate does not starve polls, floor or not: its connect leaves a debt, and a bucket in debt keeps the stream from being scheduled (D2). | P1 | Review of #55 | `cheap_streams_before_floor` in `spec/endpoint.qnt` fails P1 and P10 |
| A refused resume position stopped the stream for good. | P3 | bmc-mock fleet run | `mock_before_reopen` in `spec/stream.qnt` fails P3 |
| After a bmcweb restart the stale position was answered with `EventBufferExceeded`, the run continued, and the restarted entry ids repeated under it. | P5 | The model | `bmcweb_before_new_run` in `spec/stream.qnt` fails P5 |
| An embedder hands back a position older than the last one read, bmcweb has restarted and issued that id again, and the stream resumes inside the range the run already delivered. Open. | P5 | Audit of the model | `bmcweb_stale_position` in `spec/stream.qnt` fails P5 |
| A poll the device never answers holds the retirement where nothing bounds the poll. Accepted: the shipped sources bound every poll (E5); a transport without a timeout is the embedder's to fix. | P6 | Design review | `without_timeout` in `spec/lifecycle.qnt` fails P6 |
| Opening an event stream had no bound, and a connect held the endpoint's one in-flight slot. A device that accepted the connection and never answered held it, and no poll of that endpoint was dispatched again. Fixed: a connect holds no slot, and is given up at a deadline. | P7, P3 | Triage of P6 | `hung_connect_before_apart` in `spec/endpoint.qnt` fails P7, P10 and P3; `hung_connect_before_deadline` fails P3 |
| A failed connect counted against the breaker, and the open breaker's probe was whatever strict priority handed it. With two or more streams whose connects failed to reach the endpoint, every probe was a connect, and polls the device would answer were not dispatched. At the defaults the polls stopped for 11 minutes with two streams, and for the rest of an hour's run with three, or with two whose connects took 20 seconds to fail. Fixed: the breaker is over the polls alone. | P10 | The model; then the real collector | `two_failing_streams_before_apart` in `spec/endpoint.qnt` fails P10 |
| After the device's clock stepped back, a log walk that doubted its order found the older end stamped later, took the collection for newest first, shipped the window again, and anchored the cursor on the oldest entry. A probed order outranked the ids, so every later poll stopped at that entry, and what the device wrote was not shipped, with no issue raised, until its clock passed the old stamps; the window then shipped again. On a device that honours `$filter` one idle poll after the step started it. Fixed: ids that number the entries are not doubted and outrank a probed order. | P11, P12 | The model; then the real walk | `stepping_back_before_ids_outrank` and `filtering_stepping_back_before_ids_outrank` in `spec/logwalk.qnt` fail P11 and P12 |
| A paged log listed newest first and longer than the budget said `@truncated` on every poll. Its pages are read up to the budget before any member is visited, and pages left unread counted as a cut even when the walk met the cursor's member in the first of them and the rest were older. Fixed: pages left unread in a newest-first collection are no cut once the cursor's member was met. | P14 | The model; then the real walk | `budgeted_newest_first_before_pages_behind` in `spec/logwalk.qnt` fails P14 |

## Specs

| Spec | Covers | Rust | Replayed |
|---|---|---|---|
| `spec/endpoint.qnt` | The endpoint subtree whole: bucket, priority and turns, the polls' in-flight bound and breaker, the reconnect ladder, floor, and connect deadline, against requests that fail, hang, or time out | `orchestration/src/recipe.rs`, the leaf in `orchestration/src/stream.rs` | yes |
| `spec/stream.qnt` | SSE cursor and runs across device restarts, for a refusing device and for bmcweb; when the stream stops | `sources/redfish/src/stream.rs`, the stop in `orchestration/src/stream.rs` | yes |
| `spec/lifecycle.qnt` | Removal and retirement, with a poll the device never answers | `orchestration/src/collector.rs`, the abort in `orchestration/src/stream.rs` | no |
| `spec/logwalk.qnt` | The Redfish log walk and its cursor against a log that rotates, is cleared and refilled, lists either way round, in one document or in pages, filters or not, and whose clock moves either way; the member budget and what a walk says of a cut | `sources/redfish/src/provider.rs` | no |
| `spec/collector.qnt` | The collector across endpoints: the shared report stream, removal, shutdown, a panic, an endpoint added again, a consumer that stalls or goes away | `orchestration/src/collector.rs` | no |

A spec names the assumption or property each action and invariant stands
for, by id. The instances of each spec, and which invariants each must
hold or fail, are the `checks` table in `tools/spec.mjs`.

## Running

`make spec` runs every instance, then draws the traces. It needs Node and
a Java newer than 11, and takes about nine minutes. `QUINT` overrides how
it starts Quint, otherwise the pinned version through `npx`. CI does not
run it; CI runs the replays and the fingerprint check in `cargo test`.

One check by hand:

```
npx --yes @informalsystems/quint@0.33.1 verify spec/endpoint.qnt \
    --main=steady --backend=tlc \
    --invariants pollServed debtBounded streamReturns pollsNotSilenced pollsDoNotOverlap
```

Run one `verify` at a time. TLC keeps its states under the temporary
directory and leaves them when killed.

## Working rule

1. A change to scheduling, stream, or lifecycle behaviour updates the spec
   and this register in the same change, spec first, and `make spec` draws
   the traces again.
2. A defect in that behaviour becomes a property, and the parameters that
   showed it stay as an instance that must fail.
3. A new assumption about the dispatcher, a device, or the embedder gets an
   id and a source before code relies on it.
4. A dependency bump of `nv-redfish` re-reads the D entries.
5. An entry's status only moves when the check exists in the repository.

## Known gaps

What the specs leave out:

- **Scale.** `endpoint.qnt` runs scaled down: a breaker window of 4 and
  not 32, a cool-down and retry cap of 8 to 16 ticks and not 60, a connect
  deadline of 2 and not 30, one or two polls and streams, one in-flight
  slot. Two polls and two streams against a failing device are simulated,
  not checked. Only P10 is tested at the defaults.
- **Time.** A tick hides sub-tick ordering and continuous accrual, so a
  bound may be a tick loose. Stagger is not modelled. A poll slower than
  its cadence under an in-flight bound above one is not modelled.
- **Breaker input.** Which failures count against the breaker (P9, M5) is
  given to `endpoint.qnt`, not checked by it.
- **Stream.** Three ids, two restarts, three runs, a history of two, no
  time, and two devices: bmc-mock and bmcweb. E8 is not a property: a
  silent resume after a restart loses events with nothing to show it. P4
  holds almost by construction.
- **Lifecycle.** One poll, one stream, no bucket, no breaker. No consumer
  that stops listening, no panicking unit.
- **Log walk.** No time budget, no wrong count or next link, no entry that
  projects no record, no unreadable member, no device that refuses or
  ignores `$filter`, no log that changes mid-walk. With `$filter`, an
  entry stamped behind the cursor stays out of view.
- **Collector.** No time and no dispatcher. Reports a panic loses inside
  the dispatcher's runtime, and a tokio runtime shut down under the tasks,
  are left out.
- **gNMI.** No spec. For timing a subscription is one stream of
  `endpoint.qnt`. The target's synchronisation message reaches only a
  `subscribe_events` caller, who owns the reconciliation: deleting what a
  new subscription did not send again by the time it synchronised. Through
  the collector nothing follows: every gNMI batch is `PARTIAL`.
- **The planner** is not catalogued.

What holds the code to a spec, and what does not:

- Only `endpoint.qnt` and `stream.qnt` are replayed. The other three are a
  reading of the code, and nothing fails when they drift from it.
- The endpoint replay compares which unit is dispatched at which tick and
  which connects are given up. It does not compare statuses, the breaker's
  state, or the bucket's balance.
- The stream replay's device is the spec's own. It holds the provider to
  the spec, not the spec's two devices to bmc-mock and bmcweb (E1, E2, E8).
- The replayed traces are a sample of 40 a spec, and their instances are
  not the checked ones (see *How specs are written*).
