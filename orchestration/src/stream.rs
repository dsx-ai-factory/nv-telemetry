// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Streamed acquisition under an endpoint's admission.
//!
//! A stream is not a poll. Its connect attempt is one admission through the
//! endpoint's breaker and bucket, like a poll; the stream it opens is then
//! drained outside the dispatcher, because a work item completes once while
//! a stream yields for its lifetime. [`StreamUnit`] is the connect leaf the
//! recipe seats in the endpoint's subtree: due at construction, due again
//! at the instant its [`ReconnectPolicy`] chooses once an instance ends,
//! and never once that policy says stop. The runtime schedules every
//! reconnect as it schedules a poll's next tick, sleeping to the due
//! instant, so no loop and no timer exist beside the dispatcher.
//! [`StreamReports`] is the other half: the reports the embedder pulls, one
//! per item, at its own pace, so backpressure reaches the socket.
//!
//! A failed connect is reported through the dispatcher, where the breaker
//! samples it; a successful one earns no status, as the contract has it. A
//! failure mid-stream reaches the reports, not the dispatcher, so the
//! breaker learns of a dead stream only from the next connect attempt.
//! Dropping the reports cancels the stream: an attempt already taken by
//! the runtime still runs, but opens nothing.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use futures_util::stream::BoxStream;
use futures_util::Stream;
use nv_redfish_dispatcher::Completion;
use nv_redfish_dispatcher::QueueEvent;
use nv_redfish_dispatcher::QueueEventSink;
use nv_redfish_dispatcher::Readiness;
use nv_redfish_dispatcher::ScheduledWork;
use nv_redfish_dispatcher::Scheduler;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Origin;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::SubscriptionItem;

use crate::clock::Clock;
use crate::plan::PlannedStream;
use crate::report::assemble;
use crate::report::assemble_stream_item;
use crate::report::AcquisitionReport;
use crate::report::EndpointFault;
use crate::report::TelemetryWork;
use crate::status::resolved_retryable;

/// One report as the embedder pulls it: a poll's shape, one at a time.
pub type StreamReport = Result<AcquisitionReport, EndpointFault>;

/// The opened stream, erased so every connect attempt hands over one shape.
type Items = BoxStream<'static, SubscriptionItem>;

/// When a stream that ended is opened again. The delay after an instance
/// that delivered nothing doubles with each such instance in a row, up to
/// `max_retry`; an instance that delivered an item starts over at
/// `first_retry`. On top of that ladder rides the endpoint's own stagger:
/// a share of the delay fixed by a hash of the endpoint id, so a fleet
/// that lost a link does not reconnect in lockstep, while a test gets the
/// same instant every run. An end whose failure said retrying is pointless
/// stops the stream for good.
#[derive(Clone, Debug)]
pub struct ReconnectPolicy {
    first_retry: Duration,
    max_retry: Duration,
    stagger_percent: u8,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            first_retry: Duration::from_secs(2),
            max_retry: Duration::from_mins(1),
            stagger_percent: 25,
        }
    }
}

impl ReconnectPolicy {
    /// The delay after a productive instance, and after the first
    /// unproductive one.
    #[must_use]
    pub const fn with_first_retry(mut self, first_retry: Duration) -> Self {
        self.first_retry = first_retry;
        self
    }

    /// The longest delay the doubling reaches, before the stagger.
    #[must_use]
    pub const fn with_max_retry(mut self, max_retry: Duration) -> Self {
        self.max_retry = max_retry;
        self
    }

    /// The most an endpoint's stagger adds, as a percentage of the delay;
    /// each endpoint's own share is fixed in `0..=percent` by its id. Zero
    /// spreads nothing.
    #[must_use]
    pub const fn with_stagger_percent(mut self, percent: u8) -> Self {
        self.stagger_percent = percent;
        self
    }

    pub(crate) const fn first_retry(&self) -> Duration {
        self.first_retry
    }

    pub(crate) const fn max_retry(&self) -> Duration {
        self.max_retry
    }

    pub(crate) const fn stagger_percent(&self) -> u8 {
        self.stagger_percent
    }

    /// The delay before `endpoint_id`'s next instance. `unproductive` is how
    /// many instances in a row delivered nothing, the last one included,
    /// and is zero when the last one delivered; zero and one alike yield
    /// `first_retry`, and each further one doubles it up to the cap. `None`
    /// when the failure said retrying is pointless. The stagger rides above
    /// the cap: capped after it, a fleet at the cap would fall back into
    /// lockstep.
    #[must_use]
    pub fn delay(&self, end: StreamEnd, unproductive: u32, endpoint_id: &str) -> Option<Duration> {
        if !end.retryable() {
            return None;
        }
        let doublings = unproductive.saturating_sub(1).min(31);
        let base = self
            .first_retry
            .saturating_mul(1 << doublings)
            .min(self.max_retry);
        let share = place(endpoint_id) % (u32::from(self.stagger_percent) + 1);
        Some(base.saturating_add(base.saturating_mul(share) / 100))
    }
}

/// An endpoint's fixed place in a fleet: FNV-1a over its id, spelled out so
/// the spread survives a toolchain upgrade.
fn place(endpoint_id: &str) -> u32 {
    endpoint_id.bytes().fold(0x811c_9dc5, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    })
}

/// How one stream instance ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEnd {
    /// The connect attempt failed; its status went through the dispatcher.
    NotConnected {
        /// The failure's answer as its status carries it.
        retryable: bool,
    },
    /// The stream ended without a failure.
    Closed,
    /// The stream ended with a terminal failure, reported as its last item.
    Failed {
        /// The failure's answer as its status carries it.
        retryable: bool,
    },
}

impl StreamEnd {
    /// Whether opening another instance could plausibly succeed: `false`
    /// only when the failure that ended this one said so.
    #[must_use]
    pub const fn retryable(self) -> bool {
        match self {
            Self::NotConnected { retryable } | Self::Failed { retryable } => retryable,
            Self::Closed => true,
        }
    }
}

/// Where one stream stands, shared by the leaf inside the runtime, the
/// connect attempt it dispatches, and the reports outside.
struct Shared {
    phase: Phase,
    /// A stream a connect attempt opened, until the reports take it.
    opened: Option<Items>,
    /// The reports' waker while they wait for a stream or for the end.
    reports: Option<Waker>,
    /// The runtime's wake-up, once the leaf is seated.
    runtime: Option<QueueEventSink>,
}

impl Shared {
    /// Wakes the reports if they are parked.
    fn wake_reports(&mut self) {
        if let Some(waker) = self.reports.take() {
            waker.wake();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// A connect attempt is due: now, or at the instant.
    Due(Option<Instant>),
    /// The runtime took the connect attempt; it has not resolved.
    Connecting,
    /// The stream is open; the reports drain it.
    Streaming,
    /// The instance ended; the leaf decides the next attempt, or the stop.
    Ended { end: StreamEnd, productive: bool },
    /// Nothing more: the policy said stop, the reports were dropped, or
    /// the subtree is gone.
    Stopped,
}

type SharedState = Arc<Mutex<Shared>>;

/// The critical sections never panic, so a poisoned lock still holds a
/// consistent state.
fn lock(shared: &SharedState) -> MutexGuard<'_, Shared> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One planned stream bound to its acquisition unit: the connect leaf the
/// recipe seats in the endpoint's subtree.
pub struct StreamUnit {
    planned: PlannedStream,
    unit_endpoint: EndpointContext,
    unit_origin: Origin,
    policy: ReconnectPolicy,
    shared: SharedState,
    connect: Box<dyn FnMut() -> TelemetryWork + Send>,
    last_now: Option<Instant>,
    /// Instances in a row that delivered nothing, the last one included.
    unproductive: u32,
}

impl StreamUnit {
    /// Pairs a planned stream with its unit: the leaf for the recipe and
    /// the reports for the embedder. `clock` stamps the items and must be
    /// the clock the subtree is built with, as for a poll; the runtime's
    /// own clock times the reconnects. No checking happens here; the
    /// recipe compares the plan against the unit's identity when the
    /// subtree is built.
    pub fn new<A, C>(
        planned: PlannedStream,
        unit: Arc<A>,
        policy: ReconnectPolicy,
        clock: C,
    ) -> (Self, StreamReports)
    where
        A: Acquire + Send + Sync + 'static,
        A::Output: Stream<Item = SubscriptionItem> + Send + 'static,
        C: Clock + Clone + 'static,
    {
        let unit_endpoint = unit.endpoint().clone();
        let unit_origin = unit.origin().clone();
        let shared = Arc::new(Mutex::new(Shared {
            phase: Phase::Due(None),
            opened: None,
            reports: None,
            runtime: None,
        }));
        let connect = {
            let unit = Arc::clone(&unit);
            let clock = clock.clone();
            let shared = Arc::clone(&shared);
            Box::new(move || connect_future(Arc::clone(&unit), clock.clone(), Arc::clone(&shared)))
        };
        let reports = StreamReports {
            shared: Arc::clone(&shared),
            current: None,
            productive: false,
            assemble: Box::new(move |item| {
                assemble_stream_item(unit.as_ref(), clock.timestamp(), item)
            }),
        };
        let leaf = Self {
            planned,
            unit_endpoint,
            unit_origin,
            policy,
            shared,
            connect,
            last_now: None,
            unproductive: 0,
        };
        (leaf, reports)
    }

    pub(crate) fn planned(&self) -> &PlannedStream {
        &self.planned
    }

    pub(crate) fn unit_endpoint(&self) -> &EndpointContext {
        &self.unit_endpoint
    }

    pub(crate) fn unit_origin(&self) -> &Origin {
        &self.unit_origin
    }

    pub(crate) fn policy(&self) -> &ReconnectPolicy {
        &self.policy
    }

    fn due(phase: Phase, now: Instant) -> bool {
        match phase {
            Phase::Due(None) => true,
            Phase::Due(Some(at)) => now >= at,
            Phase::Connecting | Phase::Streaming | Phase::Ended { .. } | Phase::Stopped => false,
        }
    }

    /// Turns an ended instance into the next due instant, or the stop.
    fn schedule(
        &mut self,
        state: &mut Shared,
        end: StreamEnd,
        productive: bool,
        now: Instant,
    ) -> Readiness {
        self.unproductive = if productive {
            0
        } else {
            self.unproductive.saturating_add(1)
        };
        let next = self
            .policy
            .delay(
                end,
                self.unproductive,
                self.planned.endpoint().endpoint_id(),
            )
            .and_then(|delay| now.checked_add(delay));
        if let Some(at) = next {
            state.phase = Phase::Due(Some(at));
            Readiness::not_ready(Some(at))
        } else {
            state.phase = Phase::Stopped;
            state.wake_reports();
            Readiness::not_ready(None)
        }
    }
}

impl Scheduler<TelemetryWork> for StreamUnit {
    type Meta = ();

    fn update_ready(&mut self, now: Instant) -> Readiness {
        self.last_now = Some(now);
        let shared = Arc::clone(&self.shared);
        let mut state = lock(&shared);
        match state.phase {
            Phase::Due(None) => Readiness::ready(None),
            Phase::Due(Some(at)) => {
                if now >= at {
                    Readiness::ready(None)
                } else {
                    Readiness::not_ready(Some(at))
                }
            }
            Phase::Connecting | Phase::Streaming | Phase::Stopped => Readiness::not_ready(None),
            Phase::Ended { end, productive } => self.schedule(&mut state, end, productive, now),
        }
    }

    fn take_next(&mut self) -> Option<ScheduledWork<TelemetryWork, ()>> {
        let now = self.last_now?;
        let mut state = lock(&self.shared);
        if !Self::due(state.phase, now) {
            return None;
        }
        state.phase = Phase::Connecting;
        drop(state);
        Some(ScheduledWork::new((), (self.connect)()))
    }

    fn on_complete(&mut self, _completion: Completion<()>) {
        // The attempt recorded how it ended, with the failure's class in
        // hand; the next readiness scan reads it.
    }

    fn register_queue_event_sink(&mut self, sink: QueueEventSink) {
        lock(&self.shared).runtime = Some(sink);
    }
}

impl Drop for StreamUnit {
    fn drop(&mut self) {
        // The subtree is gone: the reports end once their stream does.
        let mut state = lock(&self.shared);
        state.phase = Phase::Stopped;
        state.wake_reports();
    }
}

impl fmt::Debug for StreamUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamUnit")
            .field("planned", &self.planned)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

/// The connect attempt as dispatcher work: `perform` under the endpoint's
/// admission. An opened stream goes to the reports and earns no status; a
/// failed attempt is assembled like a failed poll, so an endpoint-scoped
/// failure reaches the breaker.
fn connect_future<A, C>(unit: Arc<A>, clock: C, shared: SharedState) -> TelemetryWork
where
    A: Acquire + Send + Sync + 'static,
    A::Output: Stream<Item = SubscriptionItem> + Send + 'static,
    C: Clock + 'static,
{
    Box::pin(async move {
        let stopped = lock(&shared).phase == Phase::Stopped;
        if stopped {
            // The reports were dropped while this attempt waited: nothing
            // would read the stream, so no request is made.
            return Ok(Vec::new());
        }
        let begun = clock.instant();
        let at = clock.timestamp();
        match unit.perform().await {
            Ok(items) => {
                let mut state = lock(&shared);
                if state.phase != Phase::Stopped {
                    state.phase = Phase::Streaming;
                    state.opened = Some(Box::pin(items));
                    state.wake_reports();
                }
                Ok(Vec::new())
            }
            Err(failure) => {
                let duration = clock.instant().saturating_duration_since(begun);
                let mut state = lock(&shared);
                if state.phase != Phase::Stopped {
                    state.phase = Phase::Ended {
                        end: StreamEnd::NotConnected {
                            retryable: resolved_retryable(&failure),
                        },
                        productive: false,
                    };
                }
                drop(state);
                assemble(
                    unit.endpoint(),
                    unit.origin(),
                    at,
                    Some(duration),
                    Err(failure),
                )
                .map(|report| vec![report])
            }
        }
    })
}

/// The reports of one stream, pulled by the embedder: one per item, at the
/// consumer's pace. Ends when the stream's policy says stop, or when the
/// subtree seating its leaf is gone.
pub struct StreamReports {
    shared: SharedState,
    current: Option<Items>,
    /// Whether the current instance delivered an item.
    productive: bool,
    assemble: Box<dyn FnMut(SubscriptionItem) -> StreamReport + Send>,
}

impl StreamReports {
    /// Records the current instance's end and wakes the runtime, whose
    /// next readiness scan schedules what follows.
    fn ended(&mut self, end: StreamEnd) {
        self.current = None;
        let mut state = lock(&self.shared);
        if state.phase != Phase::Stopped {
            state.phase = Phase::Ended {
                end,
                productive: self.productive,
            };
        }
        let runtime = state.runtime.clone();
        drop(state);
        if let Some(runtime) = runtime {
            runtime.push(QueueEvent::WakeUp);
        }
    }
}

impl Stream for StreamReports {
    type Item = StreamReport;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(items) = this.current.as_mut() {
                match items.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        let terminal = item.as_ref().err().map(resolved_retryable);
                        let report = (this.assemble)(item);
                        match terminal {
                            Some(retryable) => this.ended(StreamEnd::Failed { retryable }),
                            None => this.productive = true,
                        }
                        return Poll::Ready(Some(report));
                    }
                    Poll::Ready(None) => this.ended(StreamEnd::Closed),
                    Poll::Pending => return Poll::Pending,
                }
            }
            let mut state = lock(&this.shared);
            if let Some(items) = state.opened.take() {
                drop(state);
                this.current = Some(items);
                this.productive = false;
                continue;
            }
            if state.phase == Phase::Stopped {
                return Poll::Ready(None);
            }
            state.reports = Some(cx.waker().clone());
            return Poll::Pending;
        }
    }
}

impl Drop for StreamReports {
    fn drop(&mut self) {
        let mut state = lock(&self.shared);
        state.phase = Phase::Stopped;
        state.opened = None;
    }
}

impl fmt::Debug for StreamReports {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamReports")
            .field("phase", &lock(&self.shared).phase)
            .field("draining", &self.current.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const ENDPOINT: &str = "bmc-lab-07";

    fn ladder() -> ReconnectPolicy {
        ReconnectPolicy::default()
            .with_first_retry(Duration::from_secs(2))
            .with_max_retry(Duration::from_secs(7))
            .with_stagger_percent(0)
    }

    #[test]
    fn unproductive_instances_double_the_delay_up_to_the_cap() {
        let policy = ladder();
        assert_eq!(
            policy.delay(StreamEnd::Closed, 0, ENDPOINT),
            Some(Duration::from_secs(2)),
            "a productive instance starts over"
        );
        assert_eq!(
            policy.delay(StreamEnd::Closed, 1, ENDPOINT),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            policy.delay(StreamEnd::NotConnected { retryable: true }, 2, ENDPOINT),
            Some(Duration::from_secs(4))
        );
        assert_eq!(
            policy.delay(StreamEnd::Failed { retryable: true }, 3, ENDPOINT),
            Some(Duration::from_secs(7)),
            "the cap holds"
        );
        assert_eq!(
            policy.delay(StreamEnd::Closed, u32::MAX, ENDPOINT),
            Some(Duration::from_secs(7)),
            "the doubling saturates rather than overflows"
        );
    }

    #[test]
    fn a_non_retryable_end_has_no_delay() {
        let policy = ladder();
        assert_eq!(
            policy.delay(StreamEnd::NotConnected { retryable: false }, 1, ENDPOINT),
            None
        );
        assert_eq!(
            policy.delay(StreamEnd::Failed { retryable: false }, 1, ENDPOINT),
            None
        );
        assert!(StreamEnd::Closed.retryable());
        assert!(!StreamEnd::Failed { retryable: false }.retryable());
    }

    #[test]
    fn the_stagger_is_the_endpoints_own_share_of_the_delay() {
        let base = Duration::from_secs(100);
        let policy = ReconnectPolicy::default()
            .with_first_retry(base)
            .with_max_retry(base)
            .with_stagger_percent(25);
        let delay = |id: &str| policy.delay(StreamEnd::Closed, 1, id).expect("retried");

        for id in [ENDPOINT, "bmc-lab-08", "rack-3/slot-12"] {
            let staggered = delay(id);
            assert!(
                (base..=Duration::from_secs(125)).contains(&staggered),
                "{id}: {staggered:?}"
            );
            assert_eq!(
                staggered,
                delay(id),
                "the share is the endpoint's, every time"
            );
        }
        let spread: BTreeSet<Duration> = (0..16)
            .map(|rack| delay(&format!("bmc-lab-{rack:02}")))
            .collect();
        assert!(spread.len() > 1, "a fleet spreads out");
        // Above the cap, so a fleet at the cap stays spread.
        assert!(delay("rack-3/slot-12") >= base);
    }
}
