// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A stream's connect attempt is dispatcher work under the endpoint's
//! admission, due at construction and again at the instant its reconnect
//! policy chooses; the stream it opens is drained through the reports the
//! embedder pulls, and the runtime never sees the items.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use futures_util::stream::Iter;
use futures_util::Stream;
use nv_redfish_dispatcher::ClockConfig;
use nv_redfish_dispatcher::ManualClock;
use nv_redfish_dispatcher::Runtime;
use nv_redfish_dispatcher::RuntimeConfig;
use nv_redfish_dispatcher::RuntimeOutput;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::Coverage;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::FailureClass;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Outcome;
use nv_telemetry_model::Payload;
use nv_telemetry_model::States;
use nv_telemetry_model::Timestamp;
use nv_telemetry_orchestration::endpoint_subtree;
use nv_telemetry_orchestration::plan;
use nv_telemetry_orchestration::AcquisitionReport;
use nv_telemetry_orchestration::Clock;
use nv_telemetry_orchestration::EndpointFault;
use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::EndpointSubtree;
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::PlannedStream;
use nv_telemetry_orchestration::PollMeta;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::RecipeError;
use nv_telemetry_orchestration::ReconnectPolicy;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamReport;
use nv_telemetry_orchestration::StreamReports;
use nv_telemetry_orchestration::StreamUnit;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProviderDeclaration;
use nv_telemetry_source::SubscriptionItem;

const BASE_SECONDS: i64 = 1_785_621_243;
const POLL_PROVIDER: &str = "fixture.sensor";
const POLL_CLASS: &str = "fixture-read";
const STREAM_PROVIDER: &str = "fixture.events";
const STREAM_CLASS: &str = "fixture-subscribe";
const FIRST_RETRY: Duration = Duration::from_secs(2);
const MAX_RETRY: Duration = Duration::from_secs(8);

type StreamRuntime = Runtime<AcquisitionReport, EndpointFault, PollMeta>;
type Connect = Result<Vec<SubscriptionItem>, AcquisitionFailure>;

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint")
}

fn other_endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("bmc-lab-08")
        .build()
        .expect("a valid endpoint")
}

fn origin(provider: &str, request_class: &str) -> Origin {
    Origin::builder()
        .provider(provider)
        .request_class(request_class)
        .build()
        .expect("a valid origin")
}

/// A hold on connect attempts: each pends here until the test opens it.
#[derive(Default)]
struct Gate {
    open: AtomicBool,
    held: Mutex<Option<Waker>>,
}

impl Gate {
    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        if let Some(waker) = self
            .held
            .lock()
            .expect("the gate mutex is never poisoned")
            .take()
        {
            waker.wake();
        }
    }
}

/// One attempt waiting at the gate.
struct Held<'a>(&'a Gate);

impl Future for Held<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.open.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        *self
            .0
            .held
            .lock()
            .expect("the gate mutex is never poisoned") = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// A subscription whose connect attempts are scripted: each opens a stream
/// of the given items, or fails; with a gate, each waits to be let through.
struct FixtureSubscription {
    endpoint: EndpointContext,
    origin: Origin,
    gate: Option<Arc<Gate>>,
    connects: Mutex<VecDeque<Connect>>,
}

impl FixtureSubscription {
    fn scripted(endpoint: EndpointContext, connects: Vec<Connect>) -> Arc<Self> {
        Arc::new(Self {
            endpoint,
            origin: origin(STREAM_PROVIDER, STREAM_CLASS),
            gate: None,
            connects: Mutex::new(connects.into()),
        })
    }

    fn held(endpoint: EndpointContext, gate: Arc<Gate>, connects: Vec<Connect>) -> Arc<Self> {
        Arc::new(Self {
            endpoint,
            origin: origin(STREAM_PROVIDER, STREAM_CLASS),
            gate: Some(gate),
            connects: Mutex::new(connects.into()),
        })
    }
}

impl Acquire for FixtureSubscription {
    type Output = Iter<std::vec::IntoIter<SubscriptionItem>>;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<Self::Output, AcquisitionFailure> {
        if let Some(gate) = &self.gate {
            Held(gate).await;
        }
        let items = self
            .connects
            .lock()
            .expect("the script mutex is never poisoned")
            .pop_front()
            .expect("the script covers every connect attempt")?;
        Ok(futures_util::stream::iter(items))
    }
}

/// A polled acquisition beside the stream: one states batch a tick.
struct FixturePoll {
    endpoint: EndpointContext,
    origin: Origin,
}

impl Acquire for FixturePoll {
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<AcquisitionParts, AcquisitionFailure> {
        Ok(states_parts())
    }
}

/// One planned poll of `endpoint` every 30 seconds, with its unit.
fn planned_poll(endpoint: EndpointContext, clock: &TestClock) -> PollUnit {
    let need = PollNeed::new(
        endpoint.clone(),
        POLL_CLASS,
        "fixture-target",
        Duration::from_secs(30),
    );
    let plan = plan(
        Needs::default().with_polls([need]),
        &[ProviderDeclaration::polled(POLL_PROVIDER, POLL_CLASS, 1)],
    )
    .expect("the fixture declaration polls");
    let unit = Arc::new(FixturePoll {
        endpoint,
        origin: origin(POLL_PROVIDER, POLL_CLASS),
    });
    PollUnit::new(plan.polls()[0].clone(), unit, clock)
}

/// One planned stream of `endpoint`, served by `provider`.
fn planned_stream(endpoint: EndpointContext, provider: &str) -> PlannedStream {
    plan(
        Needs::default().with_streams([StreamNeed::new(endpoint, STREAM_CLASS)]),
        &[ProviderDeclaration::streamed(provider, STREAM_CLASS, 1)],
    )
    .expect("the fixture declaration streams")
    .streams()[0]
        .clone()
}

/// The ladder alone: no stagger, so every deadline is exact arithmetic.
fn policy() -> ReconnectPolicy {
    ReconnectPolicy::default()
        .with_first_retry(FIRST_RETRY)
        .with_max_retry(MAX_RETRY)
        .with_stagger_percent(0)
}

#[derive(Clone)]
struct TestClock {
    manual: ManualClock,
    epoch: Instant,
}

impl TestClock {
    fn new(manual: &ManualClock) -> Self {
        Self {
            manual: manual.clone(),
            epoch: manual.now(),
        }
    }
}

impl Clock for TestClock {
    fn timestamp(&self) -> Timestamp {
        let elapsed = self.manual.now().saturating_duration_since(self.epoch);
        let seconds = BASE_SECONDS + i64::try_from(elapsed.as_secs()).expect("a short test");
        Timestamp::new(seconds, elapsed.subsec_nanos()).expect("subsecond nanos are in bound")
    }

    fn instant(&self) -> Instant {
        self.manual.now()
    }
}

fn at_offset(seconds: i64) -> Timestamp {
    Timestamp::new(BASE_SECONDS + seconds, 0).expect("a valid instant")
}

fn states_parts() -> AcquisitionParts {
    let coverage = Coverage::builder()
        .completeness(Completeness::Partial)
        .build()
        .expect("valid coverage");
    let states = States::builder()
        .build()
        .expect("an empty states payload is valid");
    AcquisitionParts::new(vec![(coverage, Payload::States(states))], Vec::new())
}

fn runtime(subtree: EndpointSubtree, manual: &ManualClock) -> StreamRuntime {
    Runtime::new(
        RuntimeConfig {
            global_max_in_flight: std::num::NonZeroUsize::MIN,
            clock: ClockConfig::Manual(manual.clone()),
        },
        subtree,
    )
}

/// One stream on the endpoint, alone in its subtree.
struct Rig {
    runtime: StreamRuntime,
    reports: StreamReports,
    manual: ManualClock,
}

fn rig_with(unit: Arc<FixtureSubscription>) -> Rig {
    let manual = ManualClock::new();
    let clock = TestClock::new(&manual);
    let (stream, reports) = StreamUnit::new(
        planned_stream(endpoint(), STREAM_PROVIDER),
        unit,
        policy(),
        clock.clone(),
    );
    let subtree = endpoint_subtree(&EndpointPolicy::default(), &clock, Vec::new(), vec![stream])
        .expect("a stream alone forms a subtree");
    Rig {
        runtime: runtime(subtree, &manual),
        reports,
        manual,
    }
}

/// A stream whose connect attempts are scripted.
fn rig(connects: Vec<Connect>) -> Rig {
    rig_with(FixtureSubscription::scripted(endpoint(), connects))
}

/// One driver turn: `Ready(output)` or `None` when the runtime is parked.
fn drive(runtime: &mut StreamRuntime) -> Option<RuntimeOutput<AcquisitionReport, EndpointFault>> {
    let mut next = pin!(runtime.next());
    match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

/// The runtime ran one work item: its reports, or its endpoint fault.
fn work_outcome(runtime: &mut StreamRuntime) -> Result<Vec<AcquisitionReport>, EndpointFault> {
    match drive(runtime) {
        Some(RuntimeOutput::Work { result, .. }) => result,
        Some(RuntimeOutput::SleepUntil(_)) => panic!("work is due, not a sleep hint"),
        Some(_) => panic!("work is due, not a runtime event"),
        None => panic!("work is due, but the runtime is parked"),
    }
}

/// The runtime hinted when to come back.
fn sleep_deadline(runtime: &mut StreamRuntime) -> Instant {
    match drive(runtime) {
        Some(RuntimeOutput::SleepUntil(at)) => at,
        Some(RuntimeOutput::Work { .. }) => panic!("a hint is due, not work"),
        Some(_) => panic!("a hint is due, not a runtime event"),
        None => panic!("a hint is due, but the runtime is parked"),
    }
}

/// One pull on the reports.
fn next(reports: &mut StreamReports) -> Poll<Option<StreamReport>> {
    Pin::new(reports).poll_next(&mut Context::from_waker(Waker::noop()))
}

/// A report the pull must yield.
fn pulled(reports: &mut StreamReports) -> StreamReport {
    match next(reports) {
        Poll::Ready(Some(report)) => report,
        Poll::Ready(None) => panic!("a report is due, but the reports are over"),
        Poll::Pending => panic!("a report is due, but none was pulled"),
    }
}

/// A waker that only remembers it was woken.
#[derive(Default)]
struct Flag(AtomicBool);

impl Flag {
    fn woken(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn a_stream_connects_at_construction_and_reports_each_item_as_pulled() {
    let Rig {
        mut runtime,
        mut reports,
        manual,
    } = rig(vec![
        Ok(vec![Ok(states_parts()), Ok(states_parts())]),
        Ok(Vec::new()),
    ]);

    // Nothing arrives before the runtime connects.
    assert!(next(&mut reports).is_pending());
    // Due at construction, the connect attempt runs under admission; a
    // connection earns no status.
    let none = work_outcome(&mut runtime).expect("the connect attempt succeeds");
    assert!(none.is_empty());
    // An open stream leaves the leaf nothing due and no hint to give.
    assert!(drive(&mut runtime).is_none());

    // Items come out as pulled, each stamped with the clock as it is read,
    // and none with a duration.
    manual.advance(Duration::from_secs(5));
    let first = pulled(&mut reports).expect("a clean item is a report");
    assert_eq!(first.status().outcome(), Outcome::Succeeded);
    assert_eq!(first.status().started_at(), &at_offset(5));
    assert_eq!(
        first.status().duration_nanos(),
        None,
        "an arrival is not an attempt"
    );
    assert_eq!(first.batches().len(), 1);
    manual.advance(Duration::from_secs(1));
    let second = pulled(&mut reports).expect("a clean item is a report");
    assert_eq!(second.status().started_at(), &at_offset(6));

    // The fixture closes after its items: the reports wait for the next
    // instance, and the runtime is asked to reconnect after the first
    // retry, once.
    assert!(next(&mut reports).is_pending());
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + FIRST_RETRY);
    assert!(drive(&mut runtime).is_none(), "the hint is given once");
    // When it comes due, the runtime connects again with nobody asking.
    manual.advance(FIRST_RETRY);
    let none = work_outcome(&mut runtime).expect("the second attempt succeeds");
    assert!(none.is_empty());
}

#[test]
fn an_ended_instance_wakes_a_parked_runtime() {
    let Rig {
        mut runtime,
        mut reports,
        manual,
    } = rig(vec![Ok(vec![Ok(states_parts())])]);
    work_outcome(&mut runtime).expect("the connect attempt succeeds");

    // With the stream open, the runtime parks on a waker that remembers.
    let flag = Arc::new(Flag::default());
    let waker = Waker::from(Arc::clone(&flag));
    let mut parked = pin!(runtime.next());
    assert!(parked
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());

    // An item is the reports' business; the runtime sleeps on.
    pulled(&mut reports).expect("a clean item is a report");
    assert!(!flag.woken());

    // The instance ends: the reports tell the runtime, which wakes and,
    // polled again, hints the reconnect.
    assert!(next(&mut reports).is_pending());
    assert!(flag.woken(), "the end of an instance wakes the runtime");
    match parked.as_mut().poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(RuntimeOutput::SleepUntil(at)) => {
            assert_eq!(at, manual.now() + FIRST_RETRY);
        }
        Poll::Ready(_) => panic!("a hint is due, not work"),
        Poll::Pending => panic!("the woken runtime has a hint to give"),
    }
}

#[test]
fn failed_connects_are_reported_through_the_dispatcher_and_back_off() {
    // A device answer is request-scoped and retried by default.
    let device = || Err(AcquisitionFailure::new(AcquisitionFailureClass::Device));
    let Rig {
        mut runtime,
        mut reports,
        manual,
    } = rig(vec![
        device(),
        device(),
        device(),
        Ok(vec![Ok(states_parts())]),
        device(),
    ]);

    // One failed report, no fault, and the next attempt after the first
    // retry.
    let failed = work_outcome(&mut runtime).expect("a device answer is the device's");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].status().outcome(), Outcome::Failed);
    assert_eq!(
        failed[0].status().failure_class(),
        Some(FailureClass::Device)
    );
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + FIRST_RETRY);
    manual.advance(FIRST_RETRY);

    // Each unproductive instance in a row doubles the delay, up to the cap.
    work_outcome(&mut runtime).expect("the second attempt is answered");
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + 2 * FIRST_RETRY);
    manual.advance(2 * FIRST_RETRY);
    work_outcome(&mut runtime).expect("the third attempt is answered");
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + MAX_RETRY);
    manual.advance(MAX_RETRY);

    // An instance that delivers starts the ladder over.
    let none = work_outcome(&mut runtime).expect("the fourth attempt succeeds");
    assert!(none.is_empty());
    pulled(&mut reports).expect("a clean item is a report");
    assert!(
        next(&mut reports).is_pending(),
        "the fixture closed after its item"
    );
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + FIRST_RETRY);
    manual.advance(FIRST_RETRY);
    work_outcome(&mut runtime).expect("the fifth attempt is answered");
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + FIRST_RETRY);
}

#[test]
fn a_connectivity_failure_on_connect_indicts_the_endpoint() {
    let Rig {
        mut runtime,
        reports: _reports,
        manual,
    } = rig(vec![Err(AcquisitionFailure::new(
        AcquisitionFailureClass::Connectivity,
    ))]);

    // Endpoint-scoped: the breaker samples it as a fault, and the stream is
    // still asked again, connectivity being retried by default.
    let fault = work_outcome(&mut runtime).expect_err("connectivity indicts the endpoint");
    assert_eq!(fault.status().outcome(), Outcome::Failed);
    assert_eq!(
        fault.status().failure_class(),
        Some(FailureClass::Connectivity)
    );
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + FIRST_RETRY);
}

#[test]
fn a_non_retryable_end_stops_the_stream() {
    // On connect: a protocol failure is not retried by default. Nothing is
    // due, no hint is given, and the reports are over.
    let Rig {
        mut runtime,
        mut reports,
        ..
    } = rig(vec![Err(AcquisitionFailure::new(
        AcquisitionFailureClass::Protocol,
    ))]);
    let failed = work_outcome(&mut runtime).expect("a protocol answer is the device's");
    assert_eq!(failed[0].status().retryable(), Some(false));
    assert!(drive(&mut runtime).is_none());
    assert!(matches!(next(&mut reports), Poll::Ready(None)));

    // Mid-stream: the terminal item is reported, then nothing.
    let Rig {
        mut runtime,
        mut reports,
        ..
    } = rig(vec![Ok(vec![
        Ok(states_parts()),
        Err(AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
            .with_detail("stream closed by the device")),
    ])]);
    work_outcome(&mut runtime).expect("the connect attempt succeeds");
    pulled(&mut reports).expect("the item before the failure");
    let terminal = pulled(&mut reports).expect("a protocol failure is request-scoped");
    assert_eq!(terminal.status().outcome(), Outcome::Failed);
    assert_eq!(terminal.status().retryable(), Some(false));
    assert_eq!(
        terminal.status().detail(),
        Some("stream closed by the device")
    );
    assert!(drive(&mut runtime).is_none());
    assert!(matches!(next(&mut reports), Poll::Ready(None)));
}

#[test]
fn a_retryable_terminal_failure_reconnects() {
    // The source's own answer overrides the protocol default.
    let Rig {
        mut runtime,
        mut reports,
        manual,
    } = rig(vec![Ok(vec![Err(AcquisitionFailure::new(
        AcquisitionFailureClass::Protocol,
    )
    .with_retryable(true))])]);
    work_outcome(&mut runtime).expect("the connect attempt succeeds");
    let terminal = pulled(&mut reports).expect("a protocol failure is request-scoped");
    assert_eq!(terminal.status().retryable(), Some(true));
    assert!(next(&mut reports).is_pending());
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + FIRST_RETRY);
}

#[test]
fn dropping_the_reports_stops_the_stream_before_it_connects() {
    // An empty script: performing would panic.
    let Rig {
        mut runtime,
        reports,
        ..
    } = rig(Vec::new());
    drop(reports);
    assert!(
        drive(&mut runtime).is_none(),
        "nothing is due, and nothing ever will be"
    );
}

#[test]
fn dropping_the_reports_while_an_attempt_runs_opens_nothing() {
    let gate = Arc::new(Gate::default());
    let Rig {
        mut runtime,
        reports,
        ..
    } = rig_with(FixtureSubscription::held(
        endpoint(),
        Arc::clone(&gate),
        vec![Ok(vec![Ok(states_parts())])],
    ));

    // The attempt is taken and waits at the gate; the runtime parks on it.
    assert!(drive(&mut runtime).is_none(), "the attempt is in flight");
    drop(reports);
    gate.open();
    // The attempt completes: the stream it opened is dropped unread, no
    // report comes of it, and nothing is due again.
    let none = work_outcome(&mut runtime).expect("the attempt completes");
    assert!(none.is_empty());
    assert!(drive(&mut runtime).is_none(), "nothing more is ever due");
}

#[test]
fn dropping_the_subtree_ends_the_reports_after_the_current_instance() {
    let Rig {
        mut runtime,
        mut reports,
        ..
    } = rig(vec![Ok(vec![Ok(states_parts()), Ok(states_parts())])]);
    work_outcome(&mut runtime).expect("the connect attempt succeeds");
    pulled(&mut reports).expect("the first item");

    // The subtree goes away with its runtime; the instance in hand still
    // delivers, and nothing follows it.
    drop(runtime);
    pulled(&mut reports).expect("the second item");
    assert!(matches!(next(&mut reports), Poll::Ready(None)));
}

#[test]
fn a_poll_and_a_stream_share_one_endpoints_subtree() {
    let manual = ManualClock::new();
    let clock = TestClock::new(&manual);
    let unit = FixtureSubscription::scripted(endpoint(), vec![Ok(vec![Ok(states_parts())])]);
    let (stream, mut reports) = StreamUnit::new(
        planned_stream(endpoint(), STREAM_PROVIDER),
        unit,
        policy(),
        clock.clone(),
    );
    let subtree = endpoint_subtree(
        &EndpointPolicy::default(),
        &clock,
        vec![planned_poll(endpoint(), &clock)],
        vec![stream],
    )
    .expect("a poll and a stream on one endpoint form a subtree");
    let mut runtime = runtime(subtree, &manual);

    // Both leaves are due at construction; the ring runs each as its own
    // work item, then hints once for the poll's next tick.
    let mut polled = 0;
    let mut connected = 0;
    let mut hints = 0;
    while let Some(output) = drive(&mut runtime) {
        match output {
            RuntimeOutput::Work { result, .. } => {
                let work = result.expect("neither leaf fails");
                match work.as_slice() {
                    [] => connected += 1,
                    [report] => {
                        assert_eq!(report.status().request_class(), POLL_CLASS);
                        assert_eq!(report.status().outcome(), Outcome::Succeeded);
                        polled += 1;
                    }
                    more => panic!("one report per poll, got {}", more.len()),
                }
            }
            RuntimeOutput::SleepUntil(at) => {
                assert_eq!(at, manual.now() + Duration::from_secs(30));
                hints += 1;
            }
            RuntimeOutput::Shutdown | RuntimeOutput::Runtime(_) => {
                panic!("neither work nor a hint")
            }
        }
    }
    assert_eq!((polled, connected, hints), (1, 1, 1));

    // The stream's item is pulled beside the polls; once the stream ends,
    // its reconnect is scheduled in the same subtree and is the earlier
    // deadline.
    let report = pulled(&mut reports).expect("a clean item is a report");
    assert_eq!(report.status().request_class(), STREAM_CLASS);
    assert!(next(&mut reports).is_pending());
    assert_eq!(sleep_deadline(&mut runtime), manual.now() + FIRST_RETRY);
}

#[test]
fn a_stream_unit_is_checked_against_its_plan() {
    let manual = ManualClock::new();
    let clock = TestClock::new(&manual);
    let default = EndpointPolicy::default();

    // The unit's endpoint disagrees with the plan's.
    let (stream, _reports) = StreamUnit::new(
        planned_stream(endpoint(), STREAM_PROVIDER),
        FixtureSubscription::scripted(other_endpoint(), Vec::new()),
        policy(),
        clock.clone(),
    );
    let Err(error) = endpoint_subtree(&default, &clock, Vec::new(), vec![stream]) else {
        panic!("the endpoints disagree");
    };
    assert!(matches!(error, RecipeError::EndpointMismatch { .. }));

    // The unit's origin disagrees with the plan's.
    let (stream, _reports) = StreamUnit::new(
        planned_stream(endpoint(), "fixture.other"),
        FixtureSubscription::scripted(endpoint(), Vec::new()),
        policy(),
        clock.clone(),
    );
    let Err(error) = endpoint_subtree(&default, &clock, Vec::new(), vec![stream]) else {
        panic!("the origins disagree");
    };
    assert!(matches!(error, RecipeError::OriginMismatch { .. }));

    // A poll on one endpoint and a stream on another: the polls name the
    // subtree's endpoint, and the stream is the one that does not belong.
    let (stream, _reports) = StreamUnit::new(
        planned_stream(other_endpoint(), STREAM_PROVIDER),
        FixtureSubscription::scripted(other_endpoint(), Vec::new()),
        policy(),
        clock.clone(),
    );
    let Err(error) = endpoint_subtree(
        &default,
        &clock,
        vec![planned_poll(endpoint(), &clock)],
        vec![stream],
    ) else {
        panic!("a subtree is one endpoint's admission scope");
    };
    assert!(matches!(
        error,
        RecipeError::MixedEndpoints { first, other }
            if first == "bmc-lab-07" && other == "bmc-lab-08"
    ));

    // Nothing to admit is not a subtree.
    let Err(error) = endpoint_subtree(&default, &clock, Vec::new(), Vec::new()) else {
        panic!("nothing to admit is not a subtree");
    };
    assert!(matches!(error, RecipeError::NoUnits));

    // A reconnect policy that would spin, never fire, or spread beyond the
    // delay is refused.
    for unusable in [
        ReconnectPolicy::default().with_first_retry(Duration::ZERO),
        ReconnectPolicy::default()
            .with_first_retry(Duration::from_secs(10))
            .with_max_retry(Duration::from_secs(1)),
        ReconnectPolicy::default().with_stagger_percent(101),
    ] {
        let (stream, _reports) = StreamUnit::new(
            planned_stream(endpoint(), STREAM_PROVIDER),
            FixtureSubscription::scripted(endpoint(), Vec::new()),
            unusable,
            clock.clone(),
        );
        let Err(error) = endpoint_subtree(&default, &clock, Vec::new(), vec![stream]) else {
            panic!("the policy is unusable");
        };
        assert!(matches!(error, RecipeError::InvalidPolicy(_)));
    }
}
