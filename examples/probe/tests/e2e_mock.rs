// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end milestone: the real providers, the real collector, a mocked
//! device, and tokio's paused clock as the timeline. An embedder-shaped
//! test — protocol crates and orchestration meet here, not in the
//! orchestration crate's own tests. The run is mixed: a sensor, a chassis,
//! a log service, and an update service interleave under one endpoint's
//! admission stack, so one round carries all four payload kinds from four
//! providers.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use futures_util::StreamExt as _;
use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Expect;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::FailureClass;
use nv_telemetry_model::Outcome;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Timestamp;
use nv_telemetry_orchestration::plan;
use nv_telemetry_orchestration::AcquisitionReport;
use nv_telemetry_orchestration::Clock;
use nv_telemetry_orchestration::Collected;
use nv_telemetry_orchestration::Collector;
use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::EndpointUnits;
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::Plan;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::ReconnectPolicy;
use nv_telemetry_orchestration::Reports;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamUnit;
use nv_telemetry_probe::BmcTransport;
use nv_telemetry_redfish::ChassisRead;
use nv_telemetry_redfish::EventStream;
use nv_telemetry_redfish::FirmwareRead;
use nv_telemetry_redfish::LogRead;
use nv_telemetry_redfish::RedfishService;
use nv_telemetry_redfish::SensorRead;
use serde_json::json;
use serde_json::Value as Json;

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
const UPDATE_SERVICE: &str = "/redfish/v1/UpdateService";
const FIRMWARE_INVENTORY: &str = "/redfish/v1/UpdateService/FirmwareInventory";
const FIRMWARE_ITEM: &str = "/redfish/v1/UpdateService/FirmwareInventory/HostBMC_0";
const UPDATE_SERVICE_FIXTURE: &str = include_str!("../fixtures/update-service.json");
const FIRMWARE_INVENTORY_FIXTURE: &str = include_str!("../fixtures/firmware-inventory.json");
const FIRMWARE_ITEM_FIXTURE: &str = include_str!("../fixtures/firmware-item.json");
const BASE_SECONDS: i64 = 1_785_621_243;

/// Tokio's paused clock as orchestration's: wall time is the base plus
/// however far the clock has moved.
struct TokioClock {
    epoch: tokio::time::Instant,
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
    Collector::new(TokioClock {
        epoch: tokio::time::Instant::now(),
    })
}

/// How far past the base a stamp lies.
fn offset(at: &Timestamp) -> Duration {
    let seconds = u64::try_from(at.seconds() - BASE_SECONDS).expect("stamped after the base");
    Duration::new(seconds, at.nanos())
}

/// The endpoint's service over the mock. Its first read classifies the
/// platform from the service root, so every script opens with one.
fn service(
    bmc: &Arc<Bmc<nv_redfish_bmc_mock::Error>>,
) -> Arc<RedfishService<BmcTransport<Bmc<nv_redfish_bmc_mock::Error>>>> {
    bmc.expect(Expect::get(SERVICE_ROOT, SERVICE_ROOT_FIXTURE));
    Arc::new(RedfishService::new(BmcTransport::new(Arc::clone(bmc))))
}

async fn report(reports: &mut Reports, context: &str) -> AcquisitionReport {
    match reports.next().await {
        Some(Collected::Report(report)) => report,
        Some(Collected::Retired(_)) => panic!("{context}: expected a report, got the retirement"),
        None => panic!("{context}: expected a report, got the end"),
    }
}

/// One primed round: a sensor report, a chassis report, a log report, then
/// a firmware report — all four payload kinds under the four providers'
/// identities. The log read yields its records once: `first` is the round
/// that ships them, and every later round finds the same entry already
/// shipped. Every report of round `round` starts at its cadence.
async fn assert_mixed_round(reports: &mut Reports, endpoint: &EndpointContext, round: i64) {
    let first = round == 0;
    let sensor_report = report(reports, "sensor turn").await;
    assert_eq!(
        sensor_report.status().started_at().seconds(),
        BASE_SECONDS + 30 * round,
        "the round runs on the cadence"
    );
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

    let chassis_report = report(reports, "chassis turn").await;
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

    let log_report = report(reports, "log turn").await;
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

    let firmware_report = report(reports, "firmware turn").await;
    assert_eq!(firmware_report.status().outcome(), Outcome::Succeeded);
    assert!(
        firmware_report
            .batches()
            .iter()
            .any(|batch| matches!(batch.payload(), Payload::Inventory(_))),
        "the firmware fixture yields inventory"
    );
    for batch in firmware_report.batches() {
        assert_eq!(batch.origin().provider(), FirmwareRead::<()>::PROVIDER);
    }
    assert!(
        sensor_report.issues().is_none()
            && chassis_report.issues().is_none()
            && log_report.issues().is_none()
            && firmware_report.issues().is_none(),
        "the nominal fixtures are clean"
    );
}

/// Primes `rounds` of the mixed poll. The mock is strict-FIFO, so priming
/// follows dispatch order: the ring visits targets in needs order each
/// round, and the log read asks three times — service, entries collection,
/// member — plus, on its second round only, the service root, to learn
/// whether the device filters; this one does not. The firmware read asks
/// three times too: update service, inventory collection, member.
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
        bmc.expect(Expect::get(UPDATE_SERVICE, UPDATE_SERVICE_FIXTURE));
        bmc.expect(Expect::get(FIRMWARE_INVENTORY, FIRMWARE_INVENTORY_FIXTURE));
        bmc.expect(Expect::get(FIRMWARE_ITEM, FIRMWARE_ITEM_FIXTURE));
    }
}

/// One need per polled provider, every thirty seconds, all four resolved.
fn mixed_plan(endpoint: &EndpointContext) -> Plan {
    let cadence = Duration::from_secs(30);
    plan(
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
            PollNeed::new(
                endpoint.clone(),
                FirmwareRead::<()>::REQUEST_CLASS,
                UPDATE_SERVICE,
                cadence,
            ),
        ]),
        &[
            SensorRead::<()>::declaration(),
            ChassisRead::<()>::declaration(),
            LogRead::<()>::declaration(),
            FirmwareRead::<()>::declaration(),
        ],
    )
    .expect("all four declarations poll")
}

#[tokio::test(start_paused = true)]
async fn a_mocked_endpoint_polls_all_providers_end_to_end() {
    let endpoint = EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint");
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    let service = service(&bmc);
    prime_rounds(&bmc, 3);

    let plan = mixed_plan(&endpoint);
    let sensor_unit = Arc::new(SensorRead::new(
        endpoint.clone(),
        plan.polls()[0].target(),
        Arc::clone(&service),
    ));
    let chassis_unit = Arc::new(ChassisRead::new(
        endpoint.clone(),
        plan.polls()[1].target(),
        Arc::clone(&service),
    ));
    let log_unit = Arc::new(LogRead::new(
        endpoint.clone(),
        plan.polls()[2].target(),
        Arc::clone(&service),
    ));
    let firmware_unit = Arc::new(FirmwareRead::new(
        endpoint.clone(),
        plan.polls()[3].target(),
        Arc::clone(&service),
    ));

    let (collector, mut reports) = collector();
    collector
        .add(EndpointUnits::new(EndpointPolicy::default()).with_polls([
            PollUnit::new(plan.polls()[0].clone(), sensor_unit),
            PollUnit::new(plan.polls()[1].clone(), chassis_unit),
            PollUnit::new(plan.polls()[2].clone(), log_unit),
            PollUnit::new(plan.polls()[3].clone(), firmware_unit),
        ]))
        .expect("four providers form one endpoint");

    // Three primed rounds, one cadence apart.
    for round in 0..3 {
        assert_mixed_round(&mut reports, &endpoint, round).await;
    }

    // The fourth round finds the mock unprimed: a harness failure the
    // provider classifies as Internal — reported, request-scoped, and the
    // breaker untouched.
    let report = report(&mut reports, "unprimed tick").await;
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

/// The fixture's one event payload, carrying `id` as the device's event id.
fn event(id: &str) -> Json {
    let mut payloads: Vec<Json> = serde_json::from_str(EVENTS_FIXTURE).expect("fixture JSON");
    let mut event = payloads.remove(0);
    event["Id"] = json!(id);
    event["Events"][0]["EventId"] = json!(id);
    event
}

#[tokio::test(start_paused = true)]
async fn a_mocked_endpoint_streams_events_end_to_end() {
    let endpoint = EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint");
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    // The root a connect reads classifies the platform too, so the script
    // holds no separate one.
    let service = Arc::new(RedfishService::new(BmcTransport::new(Arc::clone(&bmc))));
    // One connect attempt asks for the root, then the event service, then
    // the stream, which the mock ends after its scripted payloads. The
    // reconnect the runtime schedules asks the same three, the stream now
    // resumed after the id the first instance read: the expectation matches
    // nothing else.
    bmc.expect(Expect::get(SERVICE_ROOT, SERVICE_ROOT_FIXTURE));
    bmc.expect(Expect::get(EVENT_SERVICE, EVENT_SERVICE_FIXTURE));
    bmc.expect(Expect::stream_events(SSE, None, [(Some("7"), event("7"))]));
    bmc.expect(Expect::get(SERVICE_ROOT, SERVICE_ROOT_FIXTURE));
    bmc.expect(Expect::get(EVENT_SERVICE, EVENT_SERVICE_FIXTURE));
    bmc.expect(Expect::stream_events(
        SSE,
        Some("7"),
        [(Some("8"), event("8"))],
    ));

    let plan = plan(
        Needs::default().with_streams([StreamNeed::new(
            endpoint.clone(),
            EventStream::<()>::REQUEST_CLASS,
        )]),
        &[EventStream::<()>::declaration()],
    )
    .expect("the stream is planned");
    let events = Arc::new(EventStream::new(endpoint, service));
    let stream = StreamUnit::new(
        plan.streams()[0].clone(),
        events,
        ReconnectPolicy::default(),
    );
    let (collector, mut reports) = collector();
    collector
        .add(EndpointUnits::new(EndpointPolicy::default()).with_streams([stream]))
        .expect("a stream alone forms an endpoint");

    // Due at once, the connect attempt earns no status; the stream it opens
    // yields the payload, then the device closing the stream.
    let event = report(&mut reports, "the first event").await;
    assert_eq!(offset(event.status().started_at()), Duration::ZERO);
    let run = event_scope(&event, "7");

    let closed = report(&mut reports, "the device closing").await;
    assert_eq!(closed.status().outcome(), Outcome::Failed);
    assert_eq!(
        closed.status().failure_class(),
        Some(FailureClass::Protocol)
    );
    assert_eq!(
        closed.status().detail(),
        Some("the device closed the event stream")
    );

    // The collector reconnects on its own after the policy's first retry,
    // plus this endpoint's own stagger of at most a quarter of it (and the
    // millisecond tokio's timers round up to). The provider resumes after
    // the id it read: the mock's next expectation matches only a request
    // carrying that id. The instance it opens ships under the scope the
    // first one left.
    let resumed = report(&mut reports, "the resumed event").await;
    let first_retry = Duration::from_secs(2);
    assert!(
        (first_retry..=first_retry + Duration::from_millis(501))
            .contains(&offset(resumed.status().started_at())),
        "the reconnect is due after the first retry"
    );
    assert_eq!(
        event_scope(&resumed, "8"),
        run,
        "a resumed instance continues the run"
    );
}

/// One event payload's report: a succeeded status carrying one logs batch
/// of this provider's origin, scoped to the event service, with one record
/// whose `entry_id` is the device's. Returns the scope's segments, which
/// name the run.
fn event_scope(report: &AcquisitionReport, entry_id: &str) -> Vec<String> {
    assert_eq!(report.status().outcome(), Outcome::Succeeded);
    assert_eq!(report.batches().len(), 1);
    let batch = &report.batches()[0];
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
    assert_eq!(logs.records()[0].entry_id(), Some(entry_id));
    scope.scope().to_vec()
}
