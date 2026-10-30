// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The collector: where dispatcher runtimes run.
//!
//! Each added endpoint gets its own runtime over its admission subtree and
//! its own tokio task driving it. A scheduling turn walks one endpoint's
//! units, never the fleet's, and acquisitions spread across the
//! embedder's workers; a runtime shared by many endpoints would walk all
//! of their units on every dispatch. The fleet is the set of endpoints
//! added and not yet retired.
//!
//! Every report, polled or streamed, reaches one bounded [`Reports`]
//! stream, shared by every endpoint. While it is full, each endpoint's
//! runtime clock stands still: acquisitions in flight finish with their
//! true durations and wait, and nothing new falls due, so a slow consumer
//! delays dispatch instead of growing a queue or failing acquisitions. A
//! consumer should still hand reports on to its own bounded queues rather
//! than block on a sink.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use futures_util::stream::select_all;
use futures_util::Stream;
use futures_util::StreamExt as _;
use nv_redfish_dispatcher::ClockConfig;
use nv_redfish_dispatcher::ManualClock;
use nv_redfish_dispatcher::Runtime;
use nv_redfish_dispatcher::RuntimeConfig;
use nv_redfish_dispatcher::RuntimeOutput;
use nv_telemetry_model::EndpointContext;
use tokio::sync::mpsc;
use tokio::sync::Notify;

use crate::clock::Clock;
use crate::clock::SharedClock;
use crate::recipe::endpoint_subtree;
use crate::recipe::EndpointPolicy;
use crate::recipe::PollMeta;
use crate::recipe::PollUnit;
use crate::recipe::RecipeError;
use crate::recipe::Seated;
use crate::report::AcquisitionReport;
use crate::report::EndpointFault;
use crate::stream::StreamUnit;

/// How many outputs wait for the consumer before endpoints hold.
pub(crate) const REPORTS_CAPACITY: usize = 1024;

/// One endpoint's units under one admission policy.
#[derive(Debug)]
pub struct EndpointUnits {
    policy: EndpointPolicy,
    polls: Vec<PollUnit>,
    streams: Vec<StreamUnit>,
}

impl EndpointUnits {
    /// No units yet, admitted under `policy`.
    #[must_use]
    pub fn new(policy: EndpointPolicy) -> Self {
        Self {
            policy,
            polls: Vec::new(),
            streams: Vec::new(),
        }
    }

    /// Adds polled units.
    #[must_use]
    pub fn with_polls(mut self, polls: impl IntoIterator<Item = PollUnit>) -> Self {
        self.polls.extend(polls);
        self
    }

    /// Adds streamed units.
    #[must_use]
    pub fn with_streams(mut self, streams: impl IntoIterator<Item = StreamUnit>) -> Self {
        self.streams.extend(streams);
        self
    }
}

/// What the collector delivers.
// Reports are the payload and retirements rare: boxing the report would
// allocate per report to shrink only the rare variant.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Collected {
    /// One acquisition's report, or one stream item's.
    Report(AcquisitionReport),
    /// The endpoint's last report is delivered; nothing more arrives for it
    /// until it is added again. An endpoint retires when it is removed, on
    /// its own when it has no polls and its reconnect policy has stopped
    /// every stream, and when one of its units panics, though that
    /// retirement arrives only if the stream has room for it.
    Retired(EndpointContext),
}

/// The collector's outputs, in the order each endpoint produced them. Ends
/// once the collector is shut down and every endpoint has retired.
/// Dropping it shuts the collector down.
pub struct Reports {
    receiver: mpsc::Receiver<Collected>,
}

impl Stream for Reports {
    type Item = Collected;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Collected>> {
        self.receiver.poll_recv(cx)
    }
}

impl fmt::Debug for Reports {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reports")
            .field("queued", &self.receiver.len())
            .finish_non_exhaustive()
    }
}

/// Runs every added endpoint on its own dispatcher runtime and tokio task.
///
/// Must be used from within a tokio runtime with timers enabled:
/// [`Collector::add`] spawns the endpoint's task there. The clock's instants must be on tokio's
/// timeline, as [`SystemClock`](crate::SystemClock)'s are, because the
/// endpoint's task sleeps on tokio timers until the next unit is due.
/// Clones are handles to the same collector; dropping them stops nothing,
/// while [`Collector::shutdown`] or dropping the [`Reports`] does.
#[derive(Clone)]
pub struct Collector {
    registry: Arc<Registry>,
}

impl Collector {
    /// A collector stamping on `clock`, and the stream of its outputs.
    pub fn new(clock: impl Clock + 'static) -> (Self, Reports) {
        let (sender, receiver) = mpsc::channel(REPORTS_CAPACITY);
        let registry = Arc::new(Registry {
            clock: Arc::new(clock),
            state: Mutex::new(State {
                output: Some(sender),
                endpoints: HashMap::new(),
            }),
        });
        (Self { registry }, Reports { receiver })
    }

    /// Starts collecting from the endpoint the units share. Its first
    /// acquisitions are due at once.
    ///
    /// # Errors
    ///
    /// The [`RecipeError`] that keeps the units from forming an endpoint
    /// subtree; [`RecipeError::AlreadyCollected`] while the endpoint is
    /// collected or still retiring; [`RecipeError::ShutDown`] once the
    /// collector is shut down or its [`Reports`] are gone.
    ///
    /// # Panics
    ///
    /// When called outside a tokio runtime.
    pub fn add(&self, units: EndpointUnits) -> Result<(), RecipeError> {
        let EndpointUnits {
            policy,
            polls,
            streams,
        } = units;
        let timeline = Timeline::new(&self.registry.clock);
        let seated = endpoint_subtree(
            &policy,
            &self.registry.clock,
            timeline.start,
            polls,
            streams,
        )?;
        let mut state = self.registry.lock();
        let output = state
            .output
            .clone()
            .filter(|output| !output.is_closed())
            .ok_or(RecipeError::ShutDown)?;
        let stop = match state
            .endpoints
            .entry(seated.endpoint.endpoint_id().to_owned())
        {
            Entry::Occupied(entry) => {
                return Err(RecipeError::AlreadyCollected {
                    endpoint: entry.key().clone(),
                });
            }
            Entry::Vacant(entry) => Arc::clone(entry.insert(Arc::new(Notify::new()))),
        };
        drop(state);
        let seat = Seat {
            registry: Arc::clone(&self.registry),
            endpoint: Some(seated.endpoint.clone()),
            output,
            stop,
        };
        tokio::spawn(drive(seat, timeline, seated));
        Ok(())
    }

    /// Retires the endpoint: nothing new is dispatched, acquisitions in
    /// flight finish and are reported, open streams are cancelled, and then
    /// [`Collected::Retired`] arrives. `false` when the endpoint is not
    /// collected.
    pub fn remove(&self, endpoint_id: &str) -> bool {
        let state = self.registry.lock();
        let Some(stop) = state.endpoints.get(endpoint_id) else {
            return false;
        };
        stop.notify_one();
        true
    }

    /// Retires every endpoint and refuses new ones; [`Reports`] ends after
    /// the last [`Collected::Retired`].
    pub fn shutdown(&self) {
        let mut state = self.registry.lock();
        state.output = None;
        for stop in state.endpoints.values() {
            stop.notify_one();
        }
    }
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.registry.lock();
        f.debug_struct("Collector")
            .field("endpoints", &state.endpoints.len())
            .field("shut_down", &state.output.is_none())
            .finish_non_exhaustive()
    }
}

struct Registry {
    clock: SharedClock,
    state: Mutex<State>,
}

struct State {
    /// Cloned into each endpoint's task; gone once shut down.
    output: Option<mpsc::Sender<Collected>>,
    /// Endpoints collected or retiring, each with the signal that retires it.
    endpoints: HashMap<String, Arc<Notify>>,
}

impl Registry {
    /// The critical sections never panic, so a poisoned lock still holds a
    /// consistent state.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Forgets the endpoint and, with a slot to say so, delivers its
    /// retirement under the same lock: an `add` of the same id can only
    /// follow, so its reports cannot overtake the retirement.
    fn retire(&self, endpoint: EndpointContext, slot: Option<mpsc::Permit<'_, Collected>>) {
        let mut state = self.lock();
        state.endpoints.remove(endpoint.endpoint_id());
        if let Some(slot) = slot {
            slot.send(Collected::Retired(endpoint));
        }
    }
}

/// An endpoint's place in the registry, given up when its task ends.
/// Retiring gives it up after the endpoint's last report; dropped
/// unretired, as when a unit panics, it frees the id all the same.
struct Seat {
    registry: Arc<Registry>,
    endpoint: Option<EndpointContext>,
    output: mpsc::Sender<Collected>,
    stop: Arc<Notify>,
}

impl Seat {
    async fn retire(mut self, listened: bool) {
        let slot = if listened {
            self.output.reserve().await.ok()
        } else {
            None
        };
        if let Some(endpoint) = self.endpoint.take() {
            self.registry.retire(endpoint, slot);
        }
    }
}

impl Drop for Seat {
    fn drop(&mut self) {
        if let Some(endpoint) = self.endpoint.take() {
            self.registry
                .retire(endpoint, self.output.try_reserve().ok());
        }
    }
}

/// An endpoint runtime's clock, carried along the collector's. A manual
/// clock starts at the host's instant, which a paused tokio clock trails,
/// so the runtime's time is its own start plus how far the collector's
/// clock has moved since.
struct Timeline {
    clock: SharedClock,
    /// The runtime's instant at the start, which the subtree is built at.
    start: Instant,
    /// The collector's instant at the same start.
    origin: Instant,
    manual: ManualClock,
}

impl Timeline {
    fn new(clock: &SharedClock) -> Self {
        let manual = ManualClock::new();
        Self {
            clock: Arc::clone(clock),
            start: manual.now(),
            origin: clock.instant(),
            manual,
        }
    }

    /// Brings the runtime to the collector's present.
    fn advance(&self) {
        let elapsed = self.clock.instant().saturating_duration_since(self.origin);
        self.manual.advance_to(self.start + elapsed);
    }

    /// The tokio instant of a runtime deadline.
    fn deadline(&self, at: Instant) -> tokio::time::Instant {
        tokio::time::Instant::from_std(self.origin + at.saturating_duration_since(self.start))
    }
}

type EndpointRuntime = Runtime<AcquisitionReport, EndpointFault, PollMeta>;

/// One endpoint's task: drives its runtime, drains its streams, and sleeps
/// to the next due unit, until the endpoint is retired or nobody listens.
async fn drive(seat: Seat, timeline: Timeline, seated: Seated) {
    let Seated {
        subtree,
        streams,
        polled,
        ..
    } = seated;
    let mut runtime: EndpointRuntime = Runtime::new(
        RuntimeConfig {
            global_max_in_flight: NonZeroUsize::MAX,
            clock: ClockConfig::Manual(timeline.manual.clone()),
        },
        subtree,
    );
    let handle = runtime.handle();
    let mut streams = select_all(streams);
    let mut streaming = !streams.is_empty();
    let mut retiring = false;
    // Whether the sleep is set to the runtime's last deadline hint.
    let mut timed = false;
    // Reports the consumer has no room for yet. While any wait, the
    // runtime's clock stands still and the streams are not drained.
    let mut held = VecDeque::new();
    let output = &seat.output;

    let listened = {
        let mut stopped = pin!(seat.stop.notified());
        let mut closed = pin!(output.closed());
        let mut sleep = pin!(tokio::time::sleep(Duration::ZERO));
        'drive: loop {
            let holding = !held.is_empty();
            // Fixed order: retirement and a gone consumer before more output.
            tokio::select! {
                biased;
                () = &mut stopped, if !retiring => {
                    retiring = true;
                    // Dropping the reports cancels the open streams.
                    streams = select_all(Vec::new());
                    streaming = false;
                    handle.graceful_shutdown();
                },
                () = &mut closed => break 'drive false,
                slot = output.reserve(), if holding => match slot {
                    Ok(slot) => {
                        if let Some(report) = held.pop_front() {
                            slot.send(Collected::Report(report));
                        }
                    }
                    Err(_) => break 'drive false,
                },
                next = next_output(&mut runtime, &timeline, !holding) => match next {
                    RuntimeOutput::SleepUntil(at) => {
                        sleep.as_mut().reset(timeline.deadline(at));
                        timed = true;
                    }
                    RuntimeOutput::Work { result, .. } => {
                        held.extend(result.unwrap_or_else(|fault| vec![fault.into_report()]));
                    }
                    RuntimeOutput::Shutdown => break 'drive true,
                    RuntimeOutput::Runtime(_) => {}
                },
                report = streams.next(), if streaming && !holding => if let Some(report) = report {
                    held.push_back(report.unwrap_or_else(EndpointFault::into_report));
                } else {
                    // Every stream stopped for good.
                    streaming = false;
                    if !polled {
                        handle.graceful_shutdown();
                    }
                },
                () = &mut sleep, if timed && !holding => timed = false,
            }
        }
    };

    if listened {
        for report in held {
            if output.send(Collected::Report(report)).await.is_err() {
                break;
            }
        }
    }
    seat.retire(listened).await;
}

/// The runtime's next output; unless `advance` is off, its clock is
/// brought to the collector's present on every poll.
fn next_output<'a>(
    runtime: &'a mut EndpointRuntime,
    timeline: &'a Timeline,
    advance: bool,
) -> impl Future<Output = RuntimeOutput<AcquisitionReport, EndpointFault>> + 'a {
    std::future::poll_fn(move |cx| {
        if advance {
            timeline.advance();
        }
        Pin::new(&mut runtime.next()).poll(cx)
    })
}
