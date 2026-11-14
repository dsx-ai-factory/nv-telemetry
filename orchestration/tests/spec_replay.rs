// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Replays traces of `spec/endpoint.qnt` against the collector, through its
//! public API and under a paused clock. A trace is what one run of the spec
//! did, step by step: the device's acts and the clock's ticks, which the
//! replay performs, and the collector's own dispatches, which the replay
//! holds the real collector to. Before every act of the device and every
//! tick, each unit must have been dispatched exactly as often as the spec
//! dispatched it.
//!
//! `make spec` draws the traces, after checking the specs. The trace file
//! records the spec it was drawn from, and a spec changed since fails here
//! until the traces are drawn again.

use std::collections::HashMap;
use std::future::pending;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use futures_util::stream::unfold;
use futures_util::stream::BoxStream;
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Origin;
use nv_telemetry_orchestration::plan;
use nv_telemetry_orchestration::BreakerPolicy;
use nv_telemetry_orchestration::Collector;
use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::EndpointUnits;
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::RatePolicy;
use nv_telemetry_orchestration::ReconnectPolicy;
use nv_telemetry_orchestration::Reports;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamUnit;
use nv_telemetry_orchestration::SystemClock;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProviderDeclaration;
use nv_telemetry_source::SubscriptionItem;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// One tick of the spec.
const TICK: Duration = Duration::from_secs(1);

/// Turns of the scheduler the collector gets to finish reacting to one
/// event; a reaction is a handful.
const TURNS: usize = 64;

type Performed<T> = Result<T, AcquisitionFailure>;
type Items = mpsc::UnboundedReceiver<SubscriptionItem>;

fn workspace_file(path: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path);
    std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// FNV-1a, as `tools/spec.mjs` computes it.
fn fingerprint(bytes: &[u8]) -> String {
    let hash = bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    format!("{hash:016x}")
}

/// One step of a trace.
#[derive(Clone, Copy, Debug)]
enum Step {
    Tick,
    /// The collector's own: a scan scheduled the ended streams.
    Settle,
    /// The collector's own: it dispatched this stream's connect.
    Connect(usize),
    /// The collector's own: it dispatched this poll.
    Poll(usize),
    /// The collector's own: it gave this stream's connect up at its deadline.
    GivesUp(usize),
    PollEnds {
        poll: usize,
        failed: bool,
    },
    ConnectOpens(usize),
    ConnectFails {
        stream: usize,
        scoped: bool,
        retryable: bool,
    },
    Delivers(usize),
    StreamEnds {
        stream: usize,
        retryable: bool,
    },
}

impl Step {
    fn parse(token: &str) -> Self {
        let flag = |at: usize, yes: char| token.chars().nth(at) == Some(yes);
        let unit = || {
            let digit = token.chars().nth(1).and_then(|digit| digit.to_digit(10));
            digit.unwrap_or_else(|| panic!("`{token}` names no unit")) as usize
        };
        match token.chars().next() {
            Some('t') => Self::Tick,
            Some('s') => Self::Settle,
            Some('c') => Self::Connect(unit()),
            Some('p') => Self::Poll(unit()),
            Some('g') => Self::GivesUp(unit()),
            Some('e') => Self::PollEnds {
                poll: unit(),
                failed: flag(2, '-'),
            },
            Some('o') => Self::ConnectOpens(unit()),
            Some('f') => Self::ConnectFails {
                stream: unit(),
                scoped: flag(2, 's'),
                retryable: flag(3, '+'),
            },
            Some('d') => Self::Delivers(unit()),
            Some('x') => Self::StreamEnds {
                stream: unit(),
                retryable: flag(2, '+'),
            },
            _ => panic!("`{token}` is no step"),
        }
    }

    /// Whether the collector takes the step by itself.
    const fn is_the_collectors(self) -> bool {
        matches!(
            self,
            Self::Settle | Self::Connect(_) | Self::Poll(_) | Self::GivesUp(_)
        )
    }
}

/// One instance of the spec: its constants and the traces drawn from it.
struct Instance {
    name: String,
    constants: HashMap<String, String>,
    traces: Vec<Vec<Step>>,
}

impl Instance {
    fn number(&self, constant: &str) -> u64 {
        let value = self.constants.get(constant);
        value
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("{}: {constant} is no number", self.name))
    }

    fn count(&self, constant: &str) -> u32 {
        u32::try_from(self.number(constant)).expect("a small constant")
    }

    fn ticks(&self, constant: &str) -> Duration {
        TICK * self.count(constant)
    }

    fn holds(&self, constant: &str) -> bool {
        self.constants.get(constant).map(String::as_str) == Some("true")
    }

    /// The spec's endpoint policy, a tick a second.
    fn endpoint_policy(&self) -> EndpointPolicy {
        assert!(
            self.holds("FLOORED") && self.holds("CONNECTS_APART"),
            "{}: the code has the floor, and its connects stand apart",
            self.name
        );
        let percent = u8::try_from(self.number("TRIP_PERCENT")).expect("a percentage");
        EndpointPolicy::default()
            .with_max_in_flight(NonZeroU32::new(self.count("MAX_IN_FLIGHT")).expect("a bound"))
            .with_rate(
                RatePolicy::default()
                    .with_burst(self.number("BURST"))
                    .with_refill(self.number("REFILL"), TICK),
            )
            .with_breaker(
                BreakerPolicy::default()
                    .with_failure_threshold(f32::from(percent) / 100.0)
                    .with_sample_window(self.count("SAMPLE_WINDOW"))
                    .with_min_samples(self.count("MIN_SAMPLES"))
                    .with_cool_down(self.ticks("COOL_DOWN")),
            )
    }

    /// The spec's reconnect policy; the spec has no stagger.
    fn reconnect_policy(&self) -> ReconnectPolicy {
        ReconnectPolicy::default()
            .with_first_retry(self.ticks("FIRST_RETRY"))
            .with_max_retry(self.ticks("MAX_RETRY"))
            .with_stagger_percent(0)
            .with_connect_deadline(self.ticks("CONNECT_DEADLINE"))
    }
}

/// The spec the traces were drawn from, and the instances.
fn traces() -> (String, Vec<Instance>) {
    let file = String::from_utf8(workspace_file("spec/traces/endpoint.txt")).expect("text");
    let spec = file.lines().find_map(|line| line.strip_prefix("spec "));
    let spec = spec.expect("the spec's fingerprint").to_owned();
    let mut instances: Vec<Instance> = Vec::new();
    for line in file.lines() {
        let mut words = line.split_whitespace();
        match words.next() {
            Some("instance") => instances.push(Instance {
                name: words.next().expect("a name").to_owned(),
                constants: words
                    .map(|pair| pair.split_once('=').expect("a constant and its value"))
                    .map(|(constant, value)| (constant.to_owned(), value.to_owned()))
                    .collect(),
                traces: Vec::new(),
            }),
            Some("trace") => instances
                .last_mut()
                .expect("a trace follows its instance")
                .traces
                .push(words.map(Step::parse).collect()),
            _ => {}
        }
    }
    (spec, instances)
}

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("replayed")
        .build()
        .expect("a valid endpoint")
}

fn parts() -> AcquisitionParts {
    AcquisitionParts::new(Vec::new(), Vec::new())
}

fn failure(class: AcquisitionFailureClass, retryable: bool) -> AcquisitionFailure {
    AcquisitionFailure::new(class).with_retryable(retryable)
}

/// What the replay shares with one unit: how often the collector asked for
/// it, and the request it has not answered yet.
struct Seat<T> {
    dispatched: AtomicUsize,
    unanswered: Mutex<Option<oneshot::Sender<Performed<T>>>>,
}

impl<T> Seat<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            dispatched: AtomicUsize::new(0),
            unanswered: Mutex::new(None),
        })
    }

    fn dispatched(&self) -> usize {
        self.dispatched.load(Ordering::SeqCst)
    }

    fn take(&self) -> Option<oneshot::Sender<Performed<T>>> {
        self.unanswered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The collector asks; the replay answers when the trace's device does.
    async fn asked(&self) -> Performed<T> {
        let (answer, answered) = oneshot::channel();
        self.dispatched.fetch_add(1, Ordering::SeqCst);
        *self
            .unanswered
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(answer);
        match answered.await {
            Ok(answer) => answer,
            // The trace is over: this request is never answered.
            Err(_) => pending().await,
        }
    }

    fn answer(&self, answer: Performed<T>) -> Result<(), String> {
        let unanswered = self
            .take()
            .ok_or("the collector has no such request open")?;
        unanswered
            .send(answer)
            .map_err(|_| "the collector dropped the request".to_owned())
    }
}

struct Unit<T> {
    endpoint: EndpointContext,
    origin: Origin,
    seat: Arc<Seat<T>>,
}

impl Acquire for Unit<AcquisitionParts> {
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Performed<AcquisitionParts> {
        self.seat.asked().await
    }
}

impl Acquire for Unit<Items> {
    type Output = BoxStream<'static, SubscriptionItem>;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Performed<Self::Output> {
        let items = self.seat.asked().await?;
        Ok(unfold(items, |mut items| async move {
            items.recv().await.map(|item| (item, items))
        })
        .boxed())
    }
}

/// One trace's collector, with the device and the spec's tally beside it.
struct Replay {
    _collector: Collector,
    reports: Reports,
    polls: Vec<Arc<Seat<AcquisitionParts>>>,
    streams: Vec<Arc<Seat<Items>>>,
    /// The open streams' sending ends.
    open: Vec<Option<mpsc::UnboundedSender<SubscriptionItem>>>,
    /// How often the spec dispatched each poll and each connect.
    polled: Vec<usize>,
    connected: Vec<usize>,
}

impl Replay {
    /// Seats the instance's endpoint: its polls and streams in the spec's
    /// order, under the spec's policy.
    fn seat(instance: &Instance) -> Self {
        let policy = instance.endpoint_policy();
        let reconnect = instance.reconnect_policy();

        let polls: Vec<_> = (0..instance.number("POLLS")).map(|_| Seat::new()).collect();
        let streams: Vec<_> = (0..instance.number("STREAMS"))
            .map(|_| Seat::new())
            .collect();
        let poll_class = |poll: usize| format!("replayed-poll-{poll}");
        let stream_class = |stream: usize| format!("replayed-stream-{stream}");
        let mut declarations = Vec::new();
        let mut needs = Needs::default();
        for poll in 0..polls.len() {
            let class = poll_class(poll);
            declarations.push(ProviderDeclaration::polled(
                class.clone(),
                class.clone(),
                instance.number("POLL_COST"),
            ));
            needs = needs.with_polls([PollNeed::new(
                endpoint(),
                class,
                "target",
                instance.ticks("CADENCE"),
            )]);
        }
        for stream in 0..streams.len() {
            let class = stream_class(stream);
            declarations.push(ProviderDeclaration::streamed(
                class.clone(),
                class.clone(),
                instance.number("CONNECT_COST"),
            ));
            needs = needs.with_streams([StreamNeed::new(endpoint(), class)]);
        }
        let plan = plan(needs, &declarations).expect("every need has its provider");

        let poll_units = polls.iter().enumerate().map(|(poll, seat)| {
            let class = poll_class(poll);
            let planned = plan
                .polls()
                .iter()
                .find(|planned| planned.origin().request_class() == class);
            let planned = planned.expect("the poll was planned").clone();
            let unit = Unit {
                endpoint: endpoint(),
                origin: planned.origin().clone(),
                seat: Arc::clone(seat),
            };
            PollUnit::new(planned, Arc::new(unit))
        });
        let stream_units = streams.iter().enumerate().map(|(stream, seat)| {
            let class = stream_class(stream);
            let planned = plan
                .streams()
                .iter()
                .find(|planned| planned.origin().request_class() == class);
            let planned = planned.expect("the stream was planned").clone();
            let unit = Unit {
                endpoint: endpoint(),
                origin: planned.origin().clone(),
                seat: Arc::clone(seat),
            };
            StreamUnit::new(planned, Arc::new(unit), reconnect.clone())
        });
        let (collector, reports) = Collector::new(SystemClock::default());
        collector
            .add(
                EndpointUnits::new(policy)
                    .with_polls(poll_units)
                    .with_streams(stream_units),
            )
            .expect("the spec's policy is one the collector takes");
        Self {
            _collector: collector,
            reports,
            open: streams.iter().map(|_| None).collect(),
            polled: vec![0; polls.len()],
            connected: vec![0; streams.len()],
            polls,
            streams,
        }
    }

    /// Lets the collector do all it can at this instant, the consumer
    /// taking every report.
    async fn settle(&mut self) {
        for _ in 0..TURNS {
            tokio::task::yield_now().await;
            while let Some(Some(_)) = self.reports.next().now_or_never() {}
        }
    }

    /// Whether the collector has dispatched each unit as often as the spec.
    fn agrees(&self) -> Result<(), String> {
        let polled: Vec<_> = self.polls.iter().map(|seat| seat.dispatched()).collect();
        let connected: Vec<_> = self.streams.iter().map(|seat| seat.dispatched()).collect();
        if polled == self.polled && connected == self.connected {
            return Ok(());
        }
        Err(format!(
            "the spec had dispatched polls {:?} and connects {:?}, the collector {polled:?} and {connected:?}",
            self.polled, self.connected
        ))
    }

    async fn step(&mut self, step: Step) -> Result<(), String> {
        if !step.is_the_collectors() {
            self.agrees()?;
        }
        match step {
            Step::Settle => {}
            Step::Connect(stream) => self.connected[stream] += 1,
            Step::Poll(poll) => self.polled[poll] += 1,
            Step::GivesUp(stream) => {
                let unanswered = self.streams[stream].take();
                if !unanswered.is_some_and(|unanswered| unanswered.is_closed()) {
                    return Err("the collector has not given the connect up".to_owned());
                }
            }
            Step::Tick => tokio::time::advance(TICK).await,
            Step::PollEnds { poll, failed } => self.polls[poll].answer(if failed {
                Err(failure(AcquisitionFailureClass::Connectivity, true))
            } else {
                Ok(parts())
            })?,
            Step::ConnectOpens(stream) => {
                let (sender, items) = mpsc::unbounded_channel();
                self.streams[stream].answer(Ok(items))?;
                self.open[stream] = Some(sender);
            }
            Step::ConnectFails {
                stream,
                scoped,
                retryable,
            } => {
                let class = if scoped {
                    AcquisitionFailureClass::Connectivity
                } else {
                    AcquisitionFailureClass::Device
                };
                self.streams[stream].answer(Err(failure(class, retryable)))?;
            }
            Step::Delivers(stream) => {
                let open = self.open[stream].as_ref().ok_or("the stream is not open")?;
                open.send(Ok(parts()))
                    .map_err(|_| "the collector dropped the stream")?;
            }
            Step::StreamEnds { stream, retryable } => {
                let open = self.open[stream].take().ok_or("the stream is not open")?;
                if !retryable {
                    // A clean close is retried; only a failure can say not to.
                    open.send(Err(failure(AcquisitionFailureClass::Protocol, false)))
                        .map_err(|_| "the collector dropped the stream")?;
                }
            }
        }
        if !step.is_the_collectors() {
            self.settle().await;
        }
        Ok(())
    }

    async fn follow(instance: &Instance, trace: &[Step]) -> Result<(), String> {
        let mut replay = Self::seat(instance);
        replay.settle().await;
        for (at, step) in trace.iter().enumerate() {
            replay
                .step(*step)
                .await
                .map_err(|error| format!("step {at}, {step:?}: {error}"))?;
        }
        // A trace stops where it was cut, perhaps short of the collector's
        // reaction to its last event: nothing more to hold it to.
        Ok(())
    }
}

#[test]
fn the_traces_are_of_the_spec_as_it_stands() {
    let (spec, _) = traces();
    assert_eq!(
        spec,
        fingerprint(&workspace_file("spec/endpoint.qnt")),
        "spec/endpoint.qnt changed since its traces were drawn: run `make spec`"
    );
}

#[tokio::test(start_paused = true)]
async fn the_collector_follows_every_trace() {
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
