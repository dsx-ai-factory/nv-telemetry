// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The collector on tokio's paused clock: each endpoint's task sleeps to
//! its next due unit, and time jumps whenever every task waits.

use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use futures_util::stream::BoxStream;
use futures_util::StreamExt as _;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::Coverage;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::FailureClass;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Outcome;
use nv_telemetry_model::Payload;
use nv_telemetry_model::States;
use nv_telemetry_model::Timestamp;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProviderDeclaration;
use nv_telemetry_source::SubscriptionItem;
use tokio::sync::Notify;

use crate::collector::REPORTS_CAPACITY;
use crate::plan;
use crate::AcquisitionReport;
use crate::Clock;
use crate::Collected;
use crate::Collector;
use crate::EndpointPolicy;
use crate::EndpointUnits;
use crate::Needs;
use crate::PollNeed;
use crate::PollUnit;
use crate::RecipeError;
use crate::ReconnectPolicy;
use crate::Reports;
use crate::StreamNeed;
use crate::StreamUnit;

const BASE_SECONDS: i64 = 1_785_621_243;
const POLL_PROVIDER: &str = "fixture.sensor";
const POLL_CLASS: &str = "fixture-read";
const STREAM_PROVIDER: &str = "fixture.events";
const STREAM_CLASS: &str = "fixture-subscribe";
const CADENCE: Duration = Duration::from_secs(30);

/// Tokio's clock as orchestration's: wall time is the base plus however
/// far the paused clock has moved.
struct TokioClock {
    epoch: tokio::time::Instant,
}

impl TokioClock {
    fn new() -> Self {
        Self {
            epoch: tokio::time::Instant::now(),
        }
    }
}

impl Clock for TokioClock {
    fn timestamp(&self) -> Timestamp {
        let elapsed = self.epoch.elapsed();
        let seconds = BASE_SECONDS + i64::try_from(elapsed.as_secs()).expect("a short test");
        Timestamp::new(seconds, elapsed.subsec_nanos()).expect("subsecond nanos are in bound")
    }

    fn instant(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}

fn collector() -> (Collector, Reports) {
    Collector::new(TokioClock::new())
}

fn endpoint(id: &str) -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id(id)
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

/// A polled unit replaying a script, then succeeding; with a gate, each
/// acquisition says it started and waits to be let through.
struct FixturePoll {
    endpoint: EndpointContext,
    origin: Origin,
    script: Mutex<VecDeque<Result<AcquisitionParts, AcquisitionFailure>>>,
    gate: Option<Gate>,
}

struct Gate {
    started: Notify,
    open: Notify,
}

impl FixturePoll {
    fn new(endpoint: EndpointContext) -> Self {
        Self {
            endpoint,
            origin: origin(POLL_PROVIDER, POLL_CLASS),
            script: Mutex::new(VecDeque::new()),
            gate: None,
        }
    }

    fn scripted(mut self, script: Vec<Result<AcquisitionParts, AcquisitionFailure>>) -> Self {
        self.script = Mutex::new(script.into());
        self
    }

    fn gated(mut self) -> Self {
        self.gate = Some(Gate {
            started: Notify::new(),
            open: Notify::new(),
        });
        self
    }
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
        if let Some(gate) = &self.gate {
            gate.started.notify_one();
            gate.open.notified().await;
        }
        let scripted = self
            .script
            .lock()
            .expect("the script mutex is never poisoned")
            .pop_front();
        scripted.unwrap_or_else(|| Ok(states_parts()))
    }
}

/// A polled unit that breaks its contract.
struct PanickingPoll {
    endpoint: EndpointContext,
    origin: Origin,
}

impl Acquire for PanickingPoll {
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<AcquisitionParts, AcquisitionFailure> {
        panic!("the fixture panics mid-acquisition")
    }
}

/// A subscription with one connect attempt: a stream of the given items,
/// then held open, or a failure.
struct FixtureSubscription {
    endpoint: EndpointContext,
    origin: Origin,
    connect: Mutex<Option<Result<Vec<SubscriptionItem>, AcquisitionFailure>>>,
}

impl Acquire for FixtureSubscription {
    type Output = BoxStream<'static, SubscriptionItem>;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<Self::Output, AcquisitionFailure> {
        let items = self
            .connect
            .lock()
            .expect("the connect mutex is never poisoned")
            .take()
            .expect("one connect attempt")?;
        Ok(futures_util::stream::iter(items)
            .chain(futures_util::stream::pending())
            .boxed())
    }
}

fn poll_unit<A>(unit: Arc<A>) -> PollUnit
where
    A: Acquire<Output = AcquisitionParts> + Send + Sync + 'static,
{
    let need = PollNeed::new(
        unit.endpoint().clone(),
        POLL_CLASS,
        "fixture-target",
        CADENCE,
    );
    let plan = plan(
        Needs::default().with_polls([need]),
        &[ProviderDeclaration::polled(POLL_PROVIDER, POLL_CLASS, 1)],
    )
    .expect("the fixture declaration polls");
    PollUnit::new(plan.polls()[0].clone(), unit)
}

fn stream_unit(
    endpoint: EndpointContext,
    connect: Result<Vec<SubscriptionItem>, AcquisitionFailure>,
) -> StreamUnit {
    let plan = plan(
        Needs::default().with_streams([StreamNeed::new(endpoint.clone(), STREAM_CLASS)]),
        &[ProviderDeclaration::streamed(
            STREAM_PROVIDER,
            STREAM_CLASS,
            1,
        )],
    )
    .expect("the fixture declaration streams");
    let unit = Arc::new(FixtureSubscription {
        endpoint,
        origin: origin(STREAM_PROVIDER, STREAM_CLASS),
        connect: Mutex::new(Some(connect)),
    });
    StreamUnit::new(plan.streams()[0].clone(), unit, ReconnectPolicy::default())
}

fn polled(unit: FixturePoll) -> EndpointUnits {
    EndpointUnits::new(EndpointPolicy::default()).with_polls([poll_unit(Arc::new(unit))])
}

async fn next_report(reports: &mut Reports) -> AcquisitionReport {
    match reports.next().await {
        Some(Collected::Report(report)) => report,
        Some(Collected::Retired(endpoint)) => {
            panic!("a report is due, not `{}` retiring", endpoint.endpoint_id())
        }
        None => panic!("a report is due, but the reports ended"),
    }
}

async fn next_retired(reports: &mut Reports) -> EndpointContext {
    match reports.next().await {
        Some(Collected::Retired(endpoint)) => endpoint,
        Some(Collected::Report(report)) => panic!(
            "a retirement is due, not a report from `{}`",
            report.status().endpoint_id()
        ),
        None => panic!("a retirement is due, but the reports ended"),
    }
}

#[tokio::test(start_paused = true)]
async fn polls_and_streams_of_one_endpoint_reach_one_stream() {
    let (collector, mut reports) = collector();
    let lab = endpoint("bmc-lab-07");
    let units = polled(FixturePoll::new(lab.clone()))
        .with_streams([stream_unit(lab, Ok(vec![Ok(states_parts())]))]);
    collector
        .add(units)
        .expect("a poll and a stream form an endpoint");

    // Both are due at once: the poll's report and the stream's item.
    let mut classes = Vec::new();
    for _ in 0..2 {
        let report = next_report(&mut reports).await;
        assert_eq!(report.status().started_at(), &at_offset(0));
        classes.push(report.status().request_class().to_owned());
    }
    classes.sort();
    assert_eq!(classes, [POLL_CLASS, STREAM_CLASS]);

    // The poll comes round again on its cadence, give or take the
    // millisecond tokio's timers round up to.
    let next = next_report(&mut reports).await;
    assert_eq!(next.status().request_class(), POLL_CLASS);
    assert_eq!(next.status().started_at().seconds(), BASE_SECONDS + 30);
}

#[tokio::test(start_paused = true)]
async fn a_paused_clock_trailing_the_host_keeps_the_cadence() {
    // Real time passes while tokio's paused clock stands still.
    std::thread::sleep(Duration::from_millis(100));
    let (collector, mut reports) = collector();
    collector
        .add(polled(FixturePoll::new(endpoint("bmc-lab-07"))))
        .expect("one poll forms an endpoint");

    next_report(&mut reports).await;
    let next = next_report(&mut reports).await;
    let started = next.status().started_at();
    assert_eq!(started.seconds(), BASE_SECONDS + 30);
    assert!(started.nanos() < 10_000_000, "{started:?} is late");
}

#[tokio::test(start_paused = true)]
async fn a_stalled_consumer_holds_dispatch_without_failing_work_in_flight() {
    let (collector, mut reports) = collector();
    let lab = endpoint("bmc-lab-07");
    let unit = Arc::new(FixturePoll::new(lab.clone()).gated());
    let flood = (0..=REPORTS_CAPACITY).map(|_| Ok(states_parts())).collect();
    let units = EndpointUnits::new(
        EndpointPolicy::default().with_max_in_flight(NonZeroU32::new(2).expect("non-zero")),
    )
    .with_polls([poll_unit(Arc::clone(&unit))])
    .with_streams([stream_unit(lab, Ok(flood))]);
    collector
        .add(units)
        .expect("a poll and a stream form an endpoint");

    // The stream fills the reports; the device answers the poll two
    // seconds in, and nobody reads for a minute.
    let gate = unit.gate.as_ref().expect("a gated unit");
    gate.started.notified().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    gate.open.notify_one();
    tokio::time::sleep(Duration::from_mins(1)).await;

    let polled = loop {
        let report = next_report(&mut reports).await;
        if report.status().request_class() == POLL_CLASS {
            break report;
        }
    };
    assert_eq!(polled.status().outcome(), Outcome::Succeeded);
    let took = Duration::from_nanos(polled.status().duration_nanos().expect("a timed poll"));
    assert_eq!(took.as_secs(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_panicking_unit_retires_its_endpoint() {
    let (collector, mut reports) = collector();
    let lab = endpoint("bmc-lab-07");
    let unit = PanickingPoll {
        endpoint: lab.clone(),
        origin: origin(POLL_PROVIDER, POLL_CLASS),
    };
    let units =
        EndpointUnits::new(EndpointPolicy::default()).with_polls([poll_unit(Arc::new(unit))]);
    collector.add(units).expect("one poll forms an endpoint");

    assert_eq!(next_retired(&mut reports).await.endpoint_id(), "bmc-lab-07");
    collector
        .add(polled(FixturePoll::new(lab)))
        .expect("a panicked endpoint can be added again");
    assert_eq!(
        next_report(&mut reports).await.status().outcome(),
        Outcome::Succeeded
    );
}

#[tokio::test(start_paused = true)]
async fn an_endpoint_fault_arrives_as_a_report_without_batches() {
    let (collector, mut reports) = collector();
    let unit = FixturePoll::new(endpoint("bmc-lab-07")).scripted(vec![Err(
        AcquisitionFailure::new(AcquisitionFailureClass::Connectivity),
    )]);
    collector
        .add(polled(unit))
        .expect("one poll forms an endpoint");

    let report = next_report(&mut reports).await;
    assert!(report.batches().is_empty());
    assert_eq!(report.status().outcome(), Outcome::Failed);
    assert_eq!(
        report.status().failure_class(),
        Some(FailureClass::Connectivity)
    );
}

#[tokio::test(start_paused = true)]
async fn removing_finishes_the_acquisition_in_flight_and_then_retires() {
    let (collector, mut reports) = collector();
    let unit = Arc::new(FixturePoll::new(endpoint("bmc-lab-07")).gated());
    let units =
        EndpointUnits::new(EndpointPolicy::default()).with_polls([poll_unit(Arc::clone(&unit))]);
    collector.add(units).expect("one poll forms an endpoint");

    let gate = unit.gate.as_ref().expect("a gated unit");
    gate.started.notified().await;
    assert!(collector.remove("bmc-lab-07"));
    gate.open.notify_one();

    let report = next_report(&mut reports).await;
    assert_eq!(report.status().outcome(), Outcome::Succeeded);
    assert_eq!(next_retired(&mut reports).await.endpoint_id(), "bmc-lab-07");
    assert!(
        !collector.remove("bmc-lab-07"),
        "a retired endpoint is gone"
    );

    // Nothing more arrives for it, however long the collector runs.
    collector.shutdown();
    assert!(reports.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn removing_cancels_an_open_stream() {
    let (collector, mut reports) = collector();
    let lab = endpoint("bmc-lab-07");
    let units = EndpointUnits::new(EndpointPolicy::default())
        .with_streams([stream_unit(lab, Ok(vec![Ok(states_parts())]))]);
    collector.add(units).expect("a stream forms an endpoint");

    next_report(&mut reports).await;
    // The fixture holds its stream open; retiring does not wait for it.
    assert!(collector.remove("bmc-lab-07"));
    assert_eq!(next_retired(&mut reports).await.endpoint_id(), "bmc-lab-07");
}

#[tokio::test(start_paused = true)]
async fn an_endpoint_whose_streams_stopped_and_that_polls_nothing_retires() {
    let (collector, mut reports) = collector();
    let lab = endpoint("bmc-lab-07");
    let refused = AcquisitionFailure::new(AcquisitionFailureClass::Unsupported);
    let units = EndpointUnits::new(EndpointPolicy::default())
        .with_streams([stream_unit(lab, Err(refused))]);
    collector.add(units).expect("a stream forms an endpoint");

    let failed = next_report(&mut reports).await;
    assert_eq!(failed.status().retryable(), Some(false));
    assert_eq!(next_retired(&mut reports).await.endpoint_id(), "bmc-lab-07");
}

#[tokio::test(start_paused = true)]
async fn an_endpoint_is_added_once_until_it_has_retired() {
    let (collector, mut reports) = collector();
    let add = || collector.add(polled(FixturePoll::new(endpoint("bmc-lab-07"))));
    add().expect("the first add");
    assert!(matches!(
        add(),
        Err(RecipeError::AlreadyCollected { endpoint }) if endpoint == "bmc-lab-07"
    ));

    assert!(collector.remove("bmc-lab-07"));
    assert!(
        matches!(add(), Err(RecipeError::AlreadyCollected { .. })),
        "a retiring endpoint is still collected"
    );
    loop {
        if let Some(Collected::Retired(_)) = reports.next().await {
            break;
        }
    }
    add().expect("a retired endpoint can be added again");
    assert_eq!(
        next_report(&mut reports).await.status().started_at(),
        &at_offset(0)
    );

    let empty = EndpointUnits::new(EndpointPolicy::default());
    assert!(matches!(collector.add(empty), Err(RecipeError::NoUnits)));
}

#[tokio::test(start_paused = true)]
async fn shutdown_retires_every_endpoint_and_ends_the_reports() {
    let (collector, mut reports) = collector();
    for id in ["bmc-lab-07", "bmc-lab-08"] {
        collector
            .add(polled(FixturePoll::new(endpoint(id))))
            .expect("one poll forms an endpoint");
    }
    collector.shutdown();

    let mut retired = Vec::new();
    while let Some(collected) = reports.next().await {
        if let Collected::Retired(endpoint) = collected {
            retired.push(endpoint.endpoint_id().to_owned());
        }
    }
    retired.sort();
    assert_eq!(retired, ["bmc-lab-07", "bmc-lab-08"]);
    assert!(matches!(
        collector.add(polled(FixturePoll::new(endpoint("bmc-lab-09")))),
        Err(RecipeError::ShutDown)
    ));
}

#[tokio::test(start_paused = true)]
async fn dropping_the_reports_shuts_the_collector_down() {
    let (collector, reports) = collector();
    collector
        .add(polled(FixturePoll::new(endpoint("bmc-lab-07"))))
        .expect("one poll forms an endpoint");
    drop(reports);

    // The endpoint's task notices nobody listens and goes away unretired.
    tokio::time::sleep(CADENCE).await;
    assert!(!collector.remove("bmc-lab-07"));
    assert!(matches!(
        collector.add(polled(FixturePoll::new(endpoint("bmc-lab-08")))),
        Err(RecipeError::ShutDown)
    ));
}
