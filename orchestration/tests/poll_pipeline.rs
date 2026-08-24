// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The polling pipeline on a virtual timeline: scripted acquisitions
//! through the real recipe and the real dispatcher runtime, no protocol
//! crate, no async runtime — the driver is a bare poll with a no-op waker,
//! and time moves only when a test says so.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

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
use nv_telemetry_orchestration::BreakerPolicy;
use nv_telemetry_orchestration::Clock;
use nv_telemetry_orchestration::EndpointFault;
use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::PollMeta;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::RatePolicy;
use nv_telemetry_orchestration::RecipeError;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use nv_telemetry_source::ProviderDeclaration;

const PROVIDER: &str = "fixture.sensor";
const REQUEST_CLASS: &str = "fixture-read";
const BASE_SECONDS: i64 = 1_785_621_243;

type PollRuntime = Runtime<AcquisitionReport, EndpointFault, PollMeta>;

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint")
}

fn origin() -> Origin {
    Origin::builder()
        .provider(PROVIDER)
        .request_class(REQUEST_CLASS)
        .build()
        .expect("a valid origin")
}

/// A unit whose acquisitions replay a script. Running dry is a test bug.
struct FixtureUnit {
    endpoint: EndpointContext,
    origin: Origin,
    script: Mutex<VecDeque<Result<AcquisitionParts, AcquisitionFailure>>>,
}

impl FixtureUnit {
    fn scripted(
        origin: Origin,
        script: Vec<Result<AcquisitionParts, AcquisitionFailure>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            endpoint: endpoint(),
            origin,
            script: Mutex::new(script.into()),
        })
    }
}

impl Acquire for FixtureUnit {
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<AcquisitionParts, AcquisitionFailure> {
        self.script
            .lock()
            .expect("the script mutex is never poisoned")
            .pop_front()
            .expect("the script covers every dispatched tick")
    }
}

/// The virtual timeline as orchestration's clock: wall time is the base
/// plus whatever the manual clock has advanced.
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

/// One driver turn: `Ready(output)` or `None` when the runtime is parked.
fn drive(runtime: &mut PollRuntime) -> Option<RuntimeOutput<AcquisitionReport, EndpointFault>> {
    let mut next = pin!(runtime.next());
    match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

fn report(
    output: Option<RuntimeOutput<AcquisitionReport, EndpointFault>>,
) -> Result<AcquisitionReport, EndpointFault> {
    match output {
        Some(RuntimeOutput::Work { mut result, .. }) => match &mut result {
            Ok(reports) => {
                assert_eq!(reports.len(), 1, "one acquisition, one report");
                Ok(reports.pop().expect("just asserted"))
            }
            Err(_) => Err(result.expect_err("just matched")),
        },
        other => panic!("expected completed work, got {}", describe(other.as_ref())),
    }
}

fn sleep_deadline(output: Option<RuntimeOutput<AcquisitionReport, EndpointFault>>) -> Instant {
    match output {
        Some(RuntimeOutput::SleepUntil(deadline)) => deadline,
        other => panic!("expected a sleep hint, got {}", describe(other.as_ref())),
    }
}

fn describe(output: Option<&RuntimeOutput<AcquisitionReport, EndpointFault>>) -> String {
    match output {
        None => "a parked runtime".to_owned(),
        Some(RuntimeOutput::Work { result, .. }) => match result {
            Ok(_) => "completed work".to_owned(),
            Err(_) => "an endpoint fault".to_owned(),
        },
        Some(RuntimeOutput::SleepUntil(_)) => "a sleep hint".to_owned(),
        Some(RuntimeOutput::Shutdown) => "shutdown".to_owned(),
        Some(RuntimeOutput::Runtime(_)) => "a runtime event".to_owned(),
    }
}

fn states_parts(issues: Vec<ProjectionIssue>) -> AcquisitionParts {
    let coverage = Coverage::builder()
        .completeness(Completeness::Partial)
        .build()
        .expect("valid coverage");
    let states = States::builder()
        .build()
        .expect("an empty states payload is valid");
    AcquisitionParts::new(vec![(coverage, Payload::States(states))], issues)
}

fn pipeline(
    script: Vec<Result<AcquisitionParts, AcquisitionFailure>>,
    cadence: Duration,
    policy: &EndpointPolicy,
) -> (PollRuntime, ManualClock) {
    let manual = ManualClock::new();
    let clock = TestClock::new(&manual);
    let unit = FixtureUnit::scripted(origin(), script);

    let declarations = [ProviderDeclaration::polled(PROVIDER, REQUEST_CLASS, 1)];
    let needs = vec![PollNeed::new(
        endpoint(),
        REQUEST_CLASS,
        "fixture-target",
        cadence,
    )];
    let plan = plan(needs, &declarations).expect("the fixture declaration polls");
    let planned = plan.polls()[0].clone();

    let subtree = endpoint_subtree(policy, &clock, vec![PollUnit::new(planned, unit, &clock)])
        .expect("one unit forms a subtree");
    let runtime = Runtime::new(
        RuntimeConfig {
            global_max_in_flight: std::num::NonZeroUsize::MIN,
            clock: ClockConfig::Manual(manual.clone()),
        },
        subtree,
    );
    (runtime, manual)
}

#[test]
fn polls_run_on_the_cadence_with_one_hint_per_deadline() {
    let cadence = Duration::from_secs(30);
    let script = vec![
        Ok(states_parts(Vec::new())),
        Ok(states_parts(Vec::new())),
        Ok(states_parts(Vec::new())),
    ];
    let (mut runtime, manual) = pipeline(script, cadence, &EndpointPolicy::default());

    // The first tick is due at construction.
    let first = report(drive(&mut runtime)).expect("a clean success");
    assert_eq!(first.status().started_at(), &at_offset(0));
    assert_eq!(first.status().outcome(), Outcome::Succeeded);

    // The next deadline is hinted exactly once; then the runtime parks.
    let deadline = sleep_deadline(drive(&mut runtime));
    assert!(drive(&mut runtime).is_none(), "the hint is emitted once");

    manual.advance_to(deadline);
    let second = report(drive(&mut runtime)).expect("a clean success");
    assert_eq!(second.status().started_at(), &at_offset(30));

    // A long stall produces one catch-up tick, not a burst.
    let _hint = sleep_deadline(drive(&mut runtime));
    manual.advance(cadence * 10);
    let third = report(drive(&mut runtime)).expect("a clean success");
    assert_eq!(third.status().started_at(), &at_offset(330));
    assert!(matches!(
        drive(&mut runtime),
        Some(RuntimeOutput::SleepUntil(_))
    ));
    assert!(drive(&mut runtime).is_none(), "no queued burst follows");
}

#[test]
fn connectivity_failures_trip_the_breaker_and_still_report() {
    let cadence = Duration::from_secs(10);
    let cool_down = Duration::from_mins(1);
    let policy = EndpointPolicy::default().with_breaker(
        BreakerPolicy::default()
            .with_min_samples(4)
            .with_sample_window(8)
            .with_cool_down(cool_down),
    );
    let mut script: Vec<Result<AcquisitionParts, AcquisitionFailure>> = (0..4)
        .map(|_| {
            Err(AcquisitionFailure::new(
                AcquisitionFailureClass::Connectivity,
            ))
        })
        .collect();
    script.push(Ok(states_parts(Vec::new())));
    script.push(Ok(states_parts(Vec::new())));
    let (mut runtime, manual) = pipeline(script, cadence, &policy);

    // Four failures, each still delivering its status via the fault
    // channel while the breaker counts them.
    for tick in 0..4 {
        let fault = report(drive(&mut runtime)).expect_err("an endpoint-scoped failure");
        assert_eq!(
            fault.status().failure_class(),
            Some(FailureClass::Connectivity)
        );
        assert_eq!(fault.status().retryable(), Some(true));
        if tick < 3 {
            manual.advance_to(sleep_deadline(drive(&mut runtime)));
        }
    }

    // The fourth completion trips the breaker: the next hint is the
    // cool-down, far past the cadence, and nothing dispatches meanwhile.
    let reopen = sleep_deadline(drive(&mut runtime));
    let tripped_at = manual.now();
    assert_eq!(reopen, tripped_at + cool_down);
    manual.advance(cadence);
    assert!(
        drive(&mut runtime).is_none(),
        "an open breaker admits nothing"
    );

    // At the cool-down the half-open probe runs, succeeds, and closes the
    // breaker; polling resumes on the cadence.
    manual.advance_to(reopen);
    let probe = report(drive(&mut runtime)).expect("the probe succeeds");
    assert_eq!(probe.status().outcome(), Outcome::Succeeded);
    manual.advance_to(sleep_deadline(drive(&mut runtime)));
    let resumed = report(drive(&mut runtime)).expect("polling resumed");
    assert_eq!(resumed.status().outcome(), Outcome::Succeeded);
}

#[test]
fn request_scoped_failures_never_trip_the_breaker() {
    let cadence = Duration::from_secs(10);
    let policy = EndpointPolicy::default().with_breaker(
        BreakerPolicy::default()
            .with_min_samples(2)
            .with_sample_window(4),
    );
    let script = (0..6)
        .map(|_| Err(AcquisitionFailure::new(AcquisitionFailureClass::Protocol)))
        .collect();
    let (mut runtime, manual) = pipeline(script, cadence, &policy);

    // Six straight protocol failures: every one arrives as a completed
    // report on the success channel, and the next hint is always the
    // plain cadence — the breaker never opens.
    for tick in 0..6 {
        let failed = report(drive(&mut runtime)).expect("request-scoped failures are reports");
        assert_eq!(
            failed.status().failure_class(),
            Some(FailureClass::Protocol)
        );
        assert_eq!(failed.status().retryable(), Some(false));
        let deadline = sleep_deadline(drive(&mut runtime));
        assert_eq!(
            deadline,
            manual.now() + cadence,
            "tick {tick}: the hint is the cadence, not a cool-down"
        );
        manual.advance_to(deadline);
    }
}

#[test]
fn issues_ride_beside_batches_under_one_identity_and_instant() {
    let cadence = Duration::from_secs(30);
    let script = vec![
        Ok(states_parts(vec![ProjectionIssue::invalid(
            "Reading",
            "not a finite number",
        )])),
        Ok(states_parts(Vec::new())),
    ];
    let (mut runtime, manual) = pipeline(script, cadence, &EndpointPolicy::default());

    let with_issue = report(drive(&mut runtime)).expect("a success carrying one issue");
    assert_eq!(with_issue.batches().len(), 1);
    let issues = with_issue.issues().expect("one issue yields an envelope");
    assert_eq!(issues.endpoint(), &endpoint());
    assert_eq!(issues.origin(), &origin());
    assert_eq!(issues.at(), with_issue.batches()[0].window().start());
    assert_eq!(issues.at(), with_issue.status().started_at());
    assert_eq!(issues.issues().len(), 1);

    manual.advance_to(sleep_deadline(drive(&mut runtime)));
    let clean = report(drive(&mut runtime)).expect("a clean success");
    assert!(clean.issues().is_none(), "no issues, no envelope");
}

#[test]
fn the_rate_bucket_defers_a_tick_to_the_refill_instant() {
    let cadence = Duration::from_secs(10);
    let refill = Duration::from_secs(30);
    let policy = EndpointPolicy::default()
        .with_rate(RatePolicy::default().with_burst(1).with_refill(1, refill));
    let script = vec![Ok(states_parts(Vec::new())), Ok(states_parts(Vec::new()))];
    let (mut runtime, manual) = pipeline(script, cadence, &policy);

    // The first tick spends the whole burst.
    let start = manual.now();
    let first = report(drive(&mut runtime)).expect("the burst admits the first tick");
    assert_eq!(first.status().started_at(), &at_offset(0));

    // The next hint is the leaf's cadence; only once the tick is actually
    // due does the empty bucket gate it and re-hint the refill instant.
    let due = sleep_deadline(drive(&mut runtime));
    assert_eq!(due, start + cadence);
    manual.advance_to(due);
    let refill_at = sleep_deadline(drive(&mut runtime));
    assert_eq!(refill_at, start + refill);
    assert!(
        drive(&mut runtime).is_none(),
        "an empty bucket admits nothing at the cadence"
    );

    // The deferred tick runs at the refill, not at its cadence.
    manual.advance_to(refill_at);
    let second = report(drive(&mut runtime)).expect("the refill admits the second tick");
    assert_eq!(second.status().started_at(), &at_offset(30));
}

#[test]
fn two_request_classes_share_one_subtree() {
    let manual = ManualClock::new();
    let clock = TestClock::new(&manual);
    let chassis_origin = Origin::builder()
        .provider("fixture.chassis")
        .request_class("chassis-read")
        .build()
        .expect("a valid origin");

    let declarations = [
        ProviderDeclaration::polled(PROVIDER, REQUEST_CLASS, 1),
        ProviderDeclaration::polled("fixture.chassis", "chassis-read", 2),
    ];
    let needs = vec![
        PollNeed::new(
            endpoint(),
            REQUEST_CLASS,
            "fixture-target",
            Duration::from_secs(30),
        ),
        PollNeed::new(
            endpoint(),
            "chassis-read",
            "chassis-target",
            Duration::from_secs(30),
        ),
    ];
    let plan = plan(needs, &declarations).expect("both classes are served");
    assert_eq!(plan.polls()[0].cost(), 1);
    assert_eq!(plan.polls()[1].cost(), 2);

    let sensor_unit = FixtureUnit::scripted(origin(), vec![Ok(states_parts(Vec::new()))]);
    let chassis_unit = FixtureUnit::scripted(chassis_origin, vec![Ok(states_parts(Vec::new()))]);
    let subtree = endpoint_subtree(
        &EndpointPolicy::default(),
        &clock,
        vec![
            PollUnit::new(plan.polls()[0].clone(), sensor_unit, &clock),
            PollUnit::new(plan.polls()[1].clone(), chassis_unit, &clock),
        ],
    )
    .expect("two classes form one subtree");
    let mut runtime: PollRuntime = Runtime::new(
        RuntimeConfig {
            global_max_in_flight: std::num::NonZeroUsize::MIN,
            clock: ClockConfig::Manual(manual),
        },
        subtree,
    );

    // Both leaves are due at construction and dispatch in ring order, each
    // report carrying its own provider's identity; then exactly one hint.
    let first = report(drive(&mut runtime)).expect("a clean success");
    assert_eq!(first.status().provider(), PROVIDER);
    assert_eq!(first.status().request_class(), REQUEST_CLASS);
    let second = report(drive(&mut runtime)).expect("a clean success");
    assert_eq!(second.status().provider(), "fixture.chassis");
    assert_eq!(second.status().request_class(), "chassis-read");
    let _hint = sleep_deadline(drive(&mut runtime));
    assert!(drive(&mut runtime).is_none(), "the hint is emitted once");
}

#[test]
fn recipe_checks_survive_erasure() {
    let manual = ManualClock::new();
    let clock = TestClock::new(&manual);
    let other_endpoint = || {
        EndpointContext::builder()
            .endpoint_id("bmc-lab-08")
            .build()
            .expect("a valid endpoint")
    };
    let declarations = [ProviderDeclaration::polled(PROVIDER, REQUEST_CLASS, 1)];
    let planned = |endpoint: EndpointContext, target: &str| {
        let needs = vec![PollNeed::new(
            endpoint,
            REQUEST_CLASS,
            target,
            Duration::from_secs(30),
        )];
        plan(needs, &declarations)
            .expect("the fixture declaration polls")
            .polls()[0]
            .clone()
    };

    // A unit whose origin disagrees with its plan.
    let foreign = Origin::builder()
        .provider("fixture.other")
        .request_class(REQUEST_CLASS)
        .build()
        .expect("a valid origin");
    let unit = FixtureUnit::scripted(foreign, Vec::new());
    let Err(error) = endpoint_subtree(
        &EndpointPolicy::default(),
        &clock,
        vec![PollUnit::new(planned(endpoint(), "t"), unit, &clock)],
    ) else {
        panic!("the origins disagree");
    };
    assert!(matches!(error, RecipeError::OriginMismatch { .. }));

    // A unit whose endpoint disagrees with its plan.
    let unit = Arc::new(FixtureUnit {
        endpoint: other_endpoint(),
        origin: origin(),
        script: Mutex::new(VecDeque::new()),
    });
    let Err(error) = endpoint_subtree(
        &EndpointPolicy::default(),
        &clock,
        vec![PollUnit::new(planned(endpoint(), "t"), unit, &clock)],
    ) else {
        panic!("the endpoints disagree");
    };
    assert!(matches!(error, RecipeError::EndpointMismatch { .. }));

    // Two planned polls spanning two endpoints in one subtree.
    let unit_a = FixtureUnit::scripted(origin(), Vec::new());
    let unit_b = Arc::new(FixtureUnit {
        endpoint: other_endpoint(),
        origin: origin(),
        script: Mutex::new(VecDeque::new()),
    });
    let Err(error) = endpoint_subtree(
        &EndpointPolicy::default(),
        &clock,
        vec![
            PollUnit::new(planned(endpoint(), "a"), unit_a, &clock),
            PollUnit::new(planned(other_endpoint(), "b"), unit_b, &clock),
        ],
    ) else {
        panic!("a subtree is one endpoint's admission scope");
    };
    assert!(matches!(error, RecipeError::MixedEndpoints { .. }));
}
