// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Replays traces of `spec/stream.qnt` against the event stream provider,
//! through [`EventStream`] and a transport that plays the spec's device. A
//! trace is what one run of the spec did: the device issuing events,
//! restarting, and closing the stream, the provider's connect attempts, and
//! the embedder handing a position back. The replay performs each, and
//! holds the provider to the spec in what it can see: the id each open
//! asks the device to resume after, the run each event ships under, the
//! gap each lost position is reported with, a retryable end to every
//! stream the device closes, and the resume position after every step.
//!
//! `make spec` draws the traces, after checking the specs. The trace file
//! records the spec it was drawn from, and a spec changed since fails here
//! until the traces are drawn again.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::task::Poll;

use futures_util::stream::poll_fn;
use futures_util::stream::BoxStream;
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Payload;
use nv_telemetry_redfish::EventFrames;
use nv_telemetry_redfish::EventStream;
use nv_telemetry_redfish::RedfishService;
use nv_telemetry_redfish::RedfishTransport;
use nv_telemetry_redfish::ResumePosition;
use nv_telemetry_redfish::SseEvent;
use nv_telemetry_redfish::StreamRun;
use nv_telemetry_redfish::TransportFault;
use nv_telemetry_redfish::GAP_LOCATOR;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::SubscriptionItem;
use serde_json::json;
use serde_json::Value;

const ROOT: &str = "/redfish/v1";
const EVENT_SERVICE: &str = "/redfish/v1/EventService";
const SSE: &str = "/redfish/v1/EventService/SSE";

fn workspace_file(path: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// FNV-1a, as `tools/spec.mjs` computes it.
fn fingerprint(bytes: &[u8]) -> String {
    let hash = bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    format!("{hash:016x}")
}

fn numbers<const N: usize>(text: &str) -> [u64; N] {
    let mut numbers = text
        .split('.')
        .map(|number| number.parse().expect("a number"));
    std::array::from_fn(|_| numbers.next().expect("enough numbers"))
}

/// The spec's cursor after a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cursor {
    Live,
    Refused,
    Resume { boot: u64, id: u64, run: u64 },
}

impl Cursor {
    fn parse(word: &str) -> Self {
        match word.split_at(1) {
            ("L", _) => Self::Live,
            ("F", _) => Self::Refused,
            ("R", position) => {
                let [boot, id, run] = numbers(position);
                Self::Resume { boot, id, run }
            }
            _ => panic!("`{word}` is no cursor"),
        }
    }
}

/// What a step does.
#[derive(Clone, Copy, Debug)]
enum Act {
    Emits,
    Restarts,
    Closes,
    /// The stream opens without an id, in this run.
    OpensLive,
    /// The stream resumes after this boot's id.
    Resumes {
        boot: u64,
        id: u64,
    },
    /// The device refuses to resume after this boot's id.
    Refuses {
        boot: u64,
        id: u64,
    },
    /// The device answers this boot's id with a gap and goes on.
    Exceeds {
        boot: u64,
        id: u64,
    },
    /// The device sends this id, which ships under this run.
    Delivers {
        run: u64,
        id: u64,
    },
    /// The embedder hands back the position the step leaves.
    HandsBack,
}

impl Act {
    fn parse(word: &str) -> Self {
        let (act, rest) = word.split_at(1);
        match act {
            "e" => Self::Emits,
            "b" => Self::Restarts,
            "c" => Self::Closes,
            "l" => Self::OpensLive,
            "h" => Self::HandsBack,
            "r" => {
                let [boot, id] = numbers(rest);
                Self::Resumes { boot, id }
            }
            "f" => {
                let [boot, id] = numbers(rest);
                Self::Refuses { boot, id }
            }
            "x" => {
                let [boot, id] = numbers(rest);
                Self::Exceeds { boot, id }
            }
            "d" => {
                let [run, id] = numbers(rest);
                Self::Delivers { run, id }
            }
            _ => panic!("`{word}` is no step"),
        }
    }
}

/// One step of a trace: what happens, and the cursor it leaves.
#[derive(Clone, Copy, Debug)]
struct Step {
    act: Act,
    leaves: Cursor,
}

/// One instance of the spec: whether its device tags ids with its boot,
/// and the traces drawn from it.
struct Instance {
    name: String,
    tagged: bool,
    traces: Vec<Vec<Step>>,
}

/// The spec the traces were drawn from, and the instances.
fn traces() -> (String, Vec<Instance>) {
    let file = String::from_utf8(workspace_file("spec/traces/stream.txt")).expect("text");
    let spec = file.lines().find_map(|line| line.strip_prefix("spec "));
    let spec = spec.expect("the spec's fingerprint").to_owned();
    let mut instances: Vec<Instance> = Vec::new();
    for line in file.lines() {
        let mut words = line.split_whitespace();
        match words.next() {
            Some("instance") => instances.push(Instance {
                name: words.next().expect("a name").to_owned(),
                tagged: words.any(|constant| constant == "TAGGED=true"),
                traces: Vec::new(),
            }),
            Some("trace") => {
                let steps = words.map(|word| {
                    let (act, leaves) = word.split_once('=').expect("a step and its cursor");
                    Step {
                        act: Act::parse(act),
                        leaves: Cursor::parse(leaves),
                    }
                });
                let instance = instances.last_mut().expect("a trace follows its instance");
                instance.traces.push(steps.collect());
            }
            _ => {}
        }
    }
    (spec, instances)
}

/// One open stream's frames, as the device has queued them.
#[derive(Debug, Default)]
struct Line {
    frames: VecDeque<Result<SseEvent, TransportFault>>,
    over: bool,
}

/// The spec's device: its boot, the last id it issued, and its open stream.
#[derive(Debug, Default)]
struct Wire {
    boot: u64,
    issued: u64,
    line: Option<Arc<Mutex<Line>>>,
    /// The ids the provider asked to resume after, one per open.
    asked: Vec<Option<String>>,
    /// How the device answers the next open that names an id.
    refuses: bool,
    exceeds: bool,
}

#[derive(Debug)]
struct Device {
    tagged: bool,
    wire: Mutex<Wire>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Device {
    /// The SSE id of `boot`'s event `id`: marked with the boot on a device
    /// that tags its ids, the bare counter on one that does not.
    fn sse_id(&self, boot: u64, id: u64) -> String {
        if self.tagged {
            format!("{boot}-{id}")
        } else {
            id.to_string()
        }
    }

    /// Queues one frame on the open stream.
    fn sends(&self, frame: Result<SseEvent, TransportFault>) {
        if let Some(line) = &locked(&self.wire).line {
            locked(line).frames.push_back(frame);
        }
    }

    /// Ends the open stream once its frames are read.
    fn hangs_up(&self) {
        if let Some(line) = locked(&self.wire).line.take() {
            locked(&line).over = true;
        }
    }
}

/// An `Event` payload with one record.
fn event(id: u64, message_id: &str) -> Value {
    json!({
        "@odata.id": format!("{SSE}#/Event{id}"),
        "@odata.type": "#Event.v1_9_2.Event",
        "Id": id.to_string(),
        "Name": "Event Array",
        "Events": [{
            "@odata.id": format!("{SSE}#/Events/0"),
            "MemberId": "0",
            "EventId": id.to_string(),
            "EventType": "Alert",
            "EventTimestamp": "2026-03-01T10:05:00Z",
            "MessageId": message_id,
            "Message": "replayed",
            "MessageSeverity": "OK"
        }]
    })
}

/// The device as the provider reaches it.
#[derive(Debug)]
struct Link(Arc<Device>);

impl RedfishTransport for Link {
    async fn get(&self, uri: &str) -> Result<Value, TransportFault> {
        let document = match uri {
            ROOT => include_str!("fixtures/events/service-root.json"),
            EVENT_SERVICE => include_str!("fixtures/events/event-service.json"),
            _ => return Err(TransportFault::Status(404)),
        };
        Ok(serde_json::from_str(document).expect("a fixture is JSON"))
    }

    async fn events(&self, _: &str, after: Option<&str>) -> Result<EventFrames, TransportFault> {
        let mut wire = locked(&self.0.wire);
        wire.asked.push(after.map(str::to_owned));
        if after.is_some() && std::mem::take(&mut wire.refuses) {
            return Err(TransportFault::Status(400));
        }
        let line = Arc::new(Mutex::new(Line::default()));
        if std::mem::take(&mut wire.exceeds) {
            wire.issued += 1;
            let gap = SseEvent::new(
                Some(self.0.sse_id(wire.boot, wire.issued)),
                event(wire.issued, "Base.1.19.EventBufferExceeded"),
            );
            locked(&line).frames.push_back(Ok(gap));
        }
        wire.line = Some(Arc::clone(&line));
        Ok(Box::pin(poll_fn(move |_| {
            let mut line = locked(&line);
            match line.frames.pop_front() {
                Some(frame) => Poll::Ready(Some(frame)),
                None if line.over => Poll::Ready(None),
                // The replay queues a frame before it reads one, so nothing
                // waits here to be woken.
                None => Poll::Pending,
            }
        })))
    }
}

/// One trace's provider, with the device and the spec's runs beside it.
struct Replay {
    device: Arc<Device>,
    service: Arc<RedfishService<Link>>,
    provider: EventStream<Link>,
    /// The open stream's items.
    items: Option<BoxStream<'static, SubscriptionItem>>,
    /// The id of a refusal the spec has taken and the provider not yet met:
    /// the provider meets it and opens live in one attempt, the spec in two
    /// steps.
    refusal: Option<String>,
    /// The spec's runs by number, as the provider named them.
    runs: HashMap<u64, String>,
}

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("replayed")
        .build()
        .expect("a valid endpoint")
}

impl Replay {
    fn new(instance: &Instance) -> Self {
        let device = Arc::new(Device {
            tagged: instance.tagged,
            wire: Mutex::default(),
        });
        let service = Arc::new(RedfishService::new(Link(Arc::clone(&device))));
        Self {
            provider: EventStream::new(endpoint(), Arc::clone(&service)),
            device,
            service,
            items: None,
            refusal: None,
            runs: HashMap::new(),
        }
    }

    /// The spec's run `spec` is the provider's `named`, and no other is.
    fn same_run(&mut self, spec: u64, named: &str) -> Result<(), String> {
        if let Some(known) = self.runs.get(&spec) {
            return if known == named {
                Ok(())
            } else {
                Err(format!("run {spec} was `{known}` and is now `{named}`"))
            };
        }
        if self.runs.values().any(|known| known == named) {
            return Err(format!("run {spec} is named as an earlier run was"));
        }
        self.runs.insert(spec, named.to_owned());
        Ok(())
    }

    /// One connect attempt, which must ask the device for exactly `asks`.
    async fn connects(&mut self, asks: &[Option<String>]) -> Result<(), String> {
        let items = self.provider.perform().await;
        let items = items.map_err(|failure| format!("the connect failed: {failure:?}"))?;
        let asked = std::mem::take(&mut locked(&self.device.wire).asked);
        if asked != asks {
            return Err(format!(
                "the spec resumes after {asks:?}, the provider {asked:?}"
            ));
        }
        self.items = Some(items);
        Ok(())
    }

    /// The open stream's next item, which the device has sent already.
    fn pulls(&mut self) -> Result<SubscriptionItem, String> {
        let items = self.items.as_mut().ok_or("no stream is open")?;
        items
            .next()
            .now_or_never()
            .flatten()
            .ok_or_else(|| "the stream has no item".to_owned())
    }

    /// The next item is a gap and nothing else.
    fn reports_a_gap(&mut self) -> Result<(), String> {
        let item = self.pulls()?;
        let parts = item.map_err(|failure| format!("a gap is due, not {failure:?}"))?;
        let gaps = parts
            .issues()
            .iter()
            .filter(|issue| issue.path() == GAP_LOCATOR);
        if gaps.count() == 1 && parts.payloads().is_empty() {
            Ok(())
        } else {
            Err(format!("a gap alone is due, not {parts:?}"))
        }
    }

    /// The stream ends with a failure that says to try again.
    fn ends_retryably(&mut self) -> Result<(), String> {
        if self.items.is_none() {
            return Ok(());
        }
        let end = self.pulls()?;
        self.items = None;
        match end {
            Err(failure) if failure.retryable() == Some(true) => Ok(()),
            other => Err(format!("a retryable end is due, not {other:?}")),
        }
    }

    fn does(&mut self, act: Act, leaves: Cursor) -> Result<Option<Vec<Option<String>>>, String> {
        let boot = locked(&self.device.wire).boot;
        match act {
            Act::Emits => locked(&self.device.wire).issued += 1,
            Act::Restarts => {
                self.device.sends(Err(TransportFault::Transfer));
                self.device.hangs_up();
                self.ends_retryably()?;
                let mut wire = locked(&self.device.wire);
                wire.boot += 1;
                wire.issued = 0;
            }
            Act::Closes => {
                self.device.hangs_up();
                self.ends_retryably()?;
            }
            Act::Refuses { boot, id } => self.refusal = Some(self.device.sse_id(boot, id)),
            Act::OpensLive => {
                let mut asks = Vec::new();
                if let Some(refused) = &self.refusal {
                    locked(&self.device.wire).refuses = true;
                    asks.push(Some(refused.clone()));
                }
                asks.push(None);
                return Ok(Some(asks));
            }
            Act::Resumes { boot, id } => return Ok(Some(vec![Some(self.device.sse_id(boot, id))])),
            Act::Exceeds { boot, id } => {
                locked(&self.device.wire).exceeds = true;
                return Ok(Some(vec![Some(self.device.sse_id(boot, id))]));
            }
            Act::Delivers { run, id } => {
                let sse_id = self.device.sse_id(boot, id);
                self.device.sends(Ok(SseEvent::new(
                    Some(sse_id),
                    event(id, "ResourceEvent.1.3.ResourcePoweredOn"),
                )));
                let item = self.pulls()?;
                let parts = item.map_err(|failure| format!("an event is due, not {failure:?}"))?;
                let [(coverage, Payload::Logs(logs))] = parts.payloads() else {
                    return Err(format!("one logs batch is due, not {parts:?}"));
                };
                let named = coverage.scope().and_then(|scope| scope.scope().get(1));
                let named = named.ok_or("the batch's scope names no run")?.clone();
                self.same_run(run, &named)?;
                let shipped: Vec<_> = logs
                    .records()
                    .iter()
                    .map(|record| record.entry_id())
                    .collect();
                if shipped != [Some(id.to_string().as_str())] {
                    return Err(format!("event {id} is due, not {shipped:?}"));
                }
            }
            Act::HandsBack => {
                let Cursor::Resume { boot, id, run } = leaves else {
                    return Err("a position handed back is one to resume at".to_owned());
                };
                let named = self.runs.get(&run).ok_or("the run was never named")?;
                let run = StreamRun::new(named.clone()).map_err(|error| error.to_string())?;
                let position = ResumePosition::new(self.device.sse_id(boot, id), run)
                    .map_err(|error| error.to_string())?;
                self.provider = EventStream::new(endpoint(), Arc::clone(&self.service))
                    .with_resume_position(position);
            }
        }
        Ok(None)
    }

    async fn step(&mut self, step: Step) -> Result<(), String> {
        if let Some(asks) = self.does(step.act, step.leaves)? {
            self.connects(&asks).await?;
            // P4: a position the device did not honour is reported as the
            // instance's first item.
            if self.refusal.take().is_some() || matches!(step.act, Act::Exceeds { .. }) {
                self.reports_a_gap()?;
            }
        }
        if self.refusal.is_some() {
            return Ok(());
        }
        let position = self.provider.resume_position();
        match (step.leaves, position) {
            (Cursor::Live | Cursor::Refused, None) => Ok(()),
            (Cursor::Resume { boot, id, run }, Some(position))
                if position.last_event_id() == self.device.sse_id(boot, id) =>
            {
                self.same_run(run, position.run().as_str())
            }
            (leaves, position) => Err(format!(
                "the spec leaves {leaves:?}, the provider {position:?}"
            )),
        }
    }

    async fn follow(instance: &Instance, trace: &[Step]) -> Result<(), String> {
        let mut replay = Self::new(instance);
        for (at, step) in trace.iter().enumerate() {
            replay
                .step(*step)
                .await
                .map_err(|error| format!("step {at}, {step:?}: {error}"))?;
        }
        Ok(())
    }
}

#[test]
fn the_traces_are_of_the_spec_as_it_stands() {
    let (spec, _) = traces();
    assert_eq!(
        spec,
        fingerprint(&workspace_file("spec/stream.qnt")),
        "spec/stream.qnt changed since its traces were drawn: run `make spec`"
    );
}

#[tokio::test]
async fn the_provider_follows_every_trace() {
    let (_, instances) = traces();
    assert!(
        instances.iter().any(|instance| !instance.traces.is_empty()),
        "there are traces to follow"
    );
    for instance in &instances {
        for (number, trace) in instance.traces.iter().enumerate() {
            if let Err(error) = Replay::follow(instance, trace).await {
                panic!("{} trace {number}, {error}", instance.name);
            }
        }
    }
}
