// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end milestone: the real providers, the real recipe, the real
//! dispatcher runtime, a mocked device, and a virtual timeline. An
//! embedder-shaped test — protocol crates and orchestration meet here, not
//! in the orchestration crate's own tests. The run is mixed: a sensor, a
//! chassis, and a log service interleave under one endpoint's admission
//! stack, so one round carries all four payload kinds from three providers.

use std::future::Future;
use std::pin::pin;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use futures_util::Stream;
use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Expect;
use nv_redfish_dispatcher::ClockConfig;
use nv_redfish_dispatcher::ManualClock;
use nv_redfish_dispatcher::Runtime;
use nv_redfish_dispatcher::RuntimeConfig;
use nv_redfish_dispatcher::RuntimeOutput;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::FailureClass;
use nv_telemetry_model::Outcome;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Timestamp;
use nv_telemetry_orchestration::endpoint_subtree;
use nv_telemetry_orchestration::plan;
use nv_telemetry_orchestration::AcquisitionReport;
use nv_telemetry_orchestration::Clock;
use nv_telemetry_orchestration::EndpointFault;
use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::PollMeta;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::ReconnectPolicy;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamReport;
use nv_telemetry_orchestration::StreamReports;
use nv_telemetry_orchestration::StreamUnit;
use nv_telemetry_redfish::ChassisRead;
use nv_telemetry_redfish::EventStream;
use nv_telemetry_redfish::LogRead;
use nv_telemetry_redfish::SensorRead;

const SENSOR: &str = "/redfish/v1/Chassis/1U/Sensors/CPU1Temp";
const CHASSIS: &str = "/redfish/v1/Chassis/1U";
const LOG_SERVICE: &str = "/redfish/v1/Systems/1/LogServices/SEL";
const LOG_ENTRIES: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries";
const LOG_ENTRY: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries/1";
const SENSOR_FIXTURE: &str = include_str!("../fixtures/sensor.json");
const CHASSIS_FIXTURE: &str = include_str!("../fixtures/chassis.json");
const SERVICE_ROOT: &str = "/redfish/v1";
const SERVICE_ROOT_FIXTURE: &str = include_str!("../fixtures/service-root.json");
const LOG_SERVICE_FIXTURE: &str = include_str!("../fixtures/log-service.json");
const LOG_ENTRIES_FIXTURE: &str = include_str!("../fixtures/log-entries.json");
const LOG_ENTRY_FIXTURE: &str = include_str!("../fixtures/log-entry.json");
const BASE_SECONDS: i64 = 1_785_621_243;

#[derive(Clone)]
struct TestClock {
    manual: ManualClock,
    epoch: Instant,
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

type PollRuntime = Runtime<AcquisitionReport, EndpointFault, PollMeta>;

fn describe(output: Option<&RuntimeOutput<AcquisitionReport, EndpointFault>>) -> &'static str {
    match output {
        None => "a parked runtime",
        Some(RuntimeOutput::Work { result: Ok(_), .. }) => "completed work",
        Some(RuntimeOutput::Work { result: Err(_), .. }) => "an endpoint fault",
        Some(RuntimeOutput::SleepUntil(_)) => "a sleep hint",
        Some(RuntimeOutput::Shutdown) => "shutdown",
        Some(RuntimeOutput::Runtime(_)) => "a runtime event",
    }
}

fn drive(runtime: &mut PollRuntime) -> Option<RuntimeOutput<AcquisitionReport, EndpointFault>> {
    let mut next = pin!(runtime.next());
    match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

fn work(runtime: &mut PollRuntime, context: &str) -> AcquisitionReport {
    match drive(runtime) {
        Some(RuntimeOutput::Work {
            result: Ok(mut reports),
            ..
        }) => reports.pop().expect("one acquisition, one report"),
        other => panic!("{context}: expected work, got {}", describe(other.as_ref())),
    }
}

/// One primed round: a sensor report, a chassis report, then a log report —
/// all four payload kinds under the three providers' identities. The log
/// read yields its records once: `first` is the round that ships them, and
/// every later round finds the same entry already shipped.
fn assert_mixed_round(runtime: &mut PollRuntime, endpoint: &EndpointContext, first: bool) {
    let sensor_report = work(runtime, "sensor turn");
    assert_eq!(sensor_report.status().outcome(), Outcome::Succeeded);
    assert!(
        sensor_report
            .batches()
            .iter()
            .any(|batch| matches!(batch.payload(), Payload::Readings(_))),
        "the sensor fixture yields readings"
    );
    for batch in sensor_report.batches() {
        assert_eq!(batch.endpoint(), endpoint);
        assert_eq!(batch.origin().provider(), SensorRead::<()>::PROVIDER);
        assert_eq!(batch.window().start(), sensor_report.status().started_at());
    }

    let chassis_report = work(runtime, "chassis turn");
    assert_eq!(chassis_report.status().outcome(), Outcome::Succeeded);
    assert!(
        chassis_report
            .batches()
            .iter()
            .any(|batch| matches!(batch.payload(), Payload::Inventory(_))),
        "the chassis fixture yields inventory"
    );
    assert!(
        chassis_report
            .batches()
            .iter()
            .any(|batch| matches!(batch.payload(), Payload::States(_))),
        "the chassis fixture yields states"
    );
    for batch in chassis_report.batches() {
        assert_eq!(batch.origin().provider(), ChassisRead::<()>::PROVIDER);
    }

    let log_report = work(runtime, "log turn");
    assert_eq!(log_report.status().outcome(), Outcome::Succeeded);
    if first {
        assert!(
            log_report
                .batches()
                .iter()
                .any(|batch| matches!(batch.payload(), Payload::Logs(_))),
            "the log fixture yields records"
        );
    } else {
        assert!(
            log_report.batches().is_empty(),
            "the log read remembers where its last walk ended: the same entry is not a record twice"
        );
    }
    for batch in log_report.batches() {
        assert_eq!(batch.origin().provider(), LogRead::<()>::PROVIDER);
    }
    assert!(
        sensor_report.issues().is_none()
            && chassis_report.issues().is_none()
            && log_report.issues().is_none(),
        "the nominal fixtures are clean"
    );
}

/// Primes `rounds` of the mixed poll. The mock is strict-FIFO, so priming
/// follows dispatch order: the ring visits targets in needs order each
/// round, and the log read asks three times — service, entries collection,
/// member — plus, on its second round only, the service root, to learn
/// whether the device filters; this one does not.
fn prime_rounds(bmc: &Bmc<nv_redfish_bmc_mock::Error>, rounds: usize) {
    for round in 0..rounds {
        bmc.expect(Expect::get(SENSOR, SENSOR_FIXTURE));
        bmc.expect(Expect::get(CHASSIS, CHASSIS_FIXTURE));
        if round == 1 {
            bmc.expect(Expect::get(SERVICE_ROOT, SERVICE_ROOT_FIXTURE));
        }
        bmc.expect(Expect::get(LOG_SERVICE, LOG_SERVICE_FIXTURE));
        bmc.expect(Expect::get(LOG_ENTRIES, LOG_ENTRIES_FIXTURE));
        bmc.expect(Expect::get(LOG_ENTRY, LOG_ENTRY_FIXTURE));
    }
}

#[test]
fn a_mocked_endpoint_polls_all_providers_end_to_end() {
    let manual = ManualClock::new();
    let clock = TestClock {
        manual: manual.clone(),
        epoch: manual.now(),
    };

    let endpoint = EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint");
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    prime_rounds(&bmc, 3);

    let cadence = Duration::from_secs(30);
    let plan = plan(
        Needs::default().with_polls([
            PollNeed::new(
                endpoint.clone(),
                SensorRead::<()>::REQUEST_CLASS,
                SENSOR,
                cadence,
            ),
            PollNeed::new(
                endpoint.clone(),
                ChassisRead::<()>::REQUEST_CLASS,
                CHASSIS,
                cadence,
            ),
            PollNeed::new(
                endpoint.clone(),
                LogRead::<()>::REQUEST_CLASS,
                LOG_SERVICE,
                cadence,
            ),
        ]),
        &[
            SensorRead::<()>::declaration(),
            ChassisRead::<()>::declaration(),
            LogRead::<()>::declaration(),
        ],
    )
    .expect("all three declarations poll");
    let sensor_unit = Arc::new(SensorRead::new(
        endpoint.clone(),
        plan.polls()[0].target().to_owned().into(),
        Arc::clone(&bmc),
    ));
    let chassis_unit = Arc::new(ChassisRead::new(
        endpoint.clone(),
        plan.polls()[1].target().to_owned().into(),
        Arc::clone(&bmc),
    ));
    let log_unit = Arc::new(LogRead::new(
        endpoint.clone(),
        plan.polls()[2].target().to_owned().into(),
        Arc::clone(&bmc),
    ));

    let subtree = endpoint_subtree(
        &EndpointPolicy::default(),
        &clock,
        vec![
            PollUnit::new(plan.polls()[0].clone(), sensor_unit, &clock),
            PollUnit::new(plan.polls()[1].clone(), chassis_unit, &clock),
            PollUnit::new(plan.polls()[2].clone(), log_unit, &clock),
        ],
        Vec::new(),
    )
    .expect("three providers form one subtree");
    let mut runtime: PollRuntime = Runtime::new(
        RuntimeConfig {
            global_max_in_flight: std::num::NonZeroUsize::MIN,
            clock: ClockConfig::Manual(manual.clone()),
        },
        subtree,
    );

    // Three primed rounds; after each, one cadence hint moves the clock.
    for round in 0..3 {
        assert_mixed_round(&mut runtime, &endpoint, round == 0);
        match drive(&mut runtime) {
            Some(RuntimeOutput::SleepUntil(deadline)) => manual.advance_to(deadline),
            other => panic!(
                "round {round}: expected the cadence hint, got {}",
                describe(other.as_ref())
            ),
        }
    }

    // The fourth round finds the mock unprimed: a harness failure the
    // provider classifies as Internal — reported, request-scoped, and the
    // breaker untouched.
    let report = work(&mut runtime, "unprimed tick");
    assert_eq!(report.status().outcome(), Outcome::Failed);
    assert_eq!(
        report.status().failure_class(),
        Some(FailureClass::Internal)
    );
    assert!(
        report.batches().is_empty(),
        "a failed request emits no batch"
    );
}

const EVENT_SERVICE: &str = "/redfish/v1/EventService";
const SSE: &str = "/redfish/v1/EventService/SSE";
const EVENT_SERVICE_FIXTURE: &str = include_str!("../fixtures/event-service.json");
const EVENTS_FIXTURE: &str = include_str!("../fixtures/events.json");

/// A report the stream's reports must yield on this pull.
fn pulled(reports: &mut StreamReports) -> StreamReport {
    match Pin::new(&mut *reports).poll_next(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Some(report)) => report,
        Poll::Ready(None) => panic!("a report is due, but the reports are over"),
        Poll::Pending => panic!("a report is due, but none was pulled"),
    }
}

#[test]
fn a_mocked_endpoint_streams_events_end_to_end() {
    let manual = ManualClock::new();
    let clock = TestClock {
        manual: manual.clone(),
        epoch: manual.now(),
    };
    let endpoint = EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint");
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    // One connect attempt asks for the root, then the event service, then
    // the stream, which the mock ends after its scripted payloads.
    bmc.expect(Expect::get(SERVICE_ROOT, SERVICE_ROOT_FIXTURE));
    bmc.expect(Expect::get(EVENT_SERVICE, EVENT_SERVICE_FIXTURE));
    bmc.expect(Expect::stream(SSE, EVENTS_FIXTURE));

    let plan = plan(
        Needs::default().with_streams([StreamNeed::new(
            endpoint.clone(),
            EventStream::<()>::REQUEST_CLASS,
        )]),
        &[EventStream::<()>::declaration()],
    )
    .expect("the stream is planned");
    let events = Arc::new(EventStream::new(endpoint, Arc::clone(&bmc)));
    let (stream, mut pulls) = StreamUnit::new(
        plan.streams()[0].clone(),
        events,
        ReconnectPolicy::default(),
        clock.clone(),
    );
    let subtree = endpoint_subtree(&EndpointPolicy::default(), &clock, Vec::new(), vec![stream])
        .expect("a stream alone forms a subtree");
    let mut runtime: PollRuntime = Runtime::new(
        RuntimeConfig {
            global_max_in_flight: std::num::NonZeroUsize::MIN,
            clock: ClockConfig::Manual(manual.clone()),
        },
        subtree,
    );

    // Due at construction, the connect attempt runs as work that earns no
    // status.
    match drive(&mut runtime) {
        Some(RuntimeOutput::Work {
            result: Ok(none), ..
        }) => assert!(none.is_empty(), "a connection is not a report"),
        other => panic!(
            "expected the connect attempt, got {}",
            describe(other.as_ref())
        ),
    }
    // The stream in hand, the reports are pulled: the payload, then the
    // device closing the stream.
    manual.advance(Duration::from_secs(5));
    let reports = [pulled(&mut pulls), pulled(&mut pulls)];
    // Nothing follows until the runtime reconnects, which it is asked to
    // do after the policy's first retry.
    assert!(Pin::new(&mut pulls)
        .poll_next(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    match drive(&mut runtime) {
        Some(RuntimeOutput::SleepUntil(at)) => {
            // The default policy's first retry, plus this endpoint's own
            // stagger of at most a quarter of it.
            let first_retry = manual.now() + Duration::from_secs(2);
            assert!(
                (first_retry..=first_retry + Duration::from_millis(500)).contains(&at),
                "the reconnect is due after the first retry"
            );
        }
        other => panic!(
            "expected the reconnect hint, got {}",
            describe(other.as_ref())
        ),
    }

    let event = reports[0].as_ref().expect("a payload is a report");
    assert_eq!(event.status().outcome(), Outcome::Succeeded);
    assert_eq!(
        event.status().started_at(),
        &Timestamp::new(BASE_SECONDS + 5, 0).expect("a valid instant")
    );
    assert_eq!(event.batches().len(), 1);
    let batch = &event.batches()[0];
    assert_eq!(batch.origin().provider(), EventStream::<()>::PROVIDER);
    assert_eq!(
        batch.origin().request_class(),
        EventStream::<()>::REQUEST_CLASS
    );
    let scope = batch.coverage().scope().expect("a scoped batch");
    assert_eq!(scope.kind(), "event-service");
    assert_eq!(scope.id(), "EventService");
    let Payload::Logs(logs) = batch.payload() else {
        panic!("events project into logs");
    };
    assert_eq!(logs.records().len(), 1);
    assert_eq!(logs.records()[0].entry_id(), Some("7"));

    let closed = reports[1]
        .as_ref()
        .expect("a protocol failure is request-scoped, not a fault");
    assert_eq!(closed.status().outcome(), Outcome::Failed);
    assert_eq!(
        closed.status().failure_class(),
        Some(FailureClass::Protocol)
    );
    assert_eq!(
        closed.status().detail(),
        Some("the device closed the event stream")
    );
}
