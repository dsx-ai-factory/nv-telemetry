// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end milestone: the real providers, the real recipe, the real
//! dispatcher runtime, a mocked device, and a virtual timeline. An
//! embedder-shaped test — protocol crates and orchestration meet here, not
//! in the orchestration crate's own tests. The run is mixed: a sensor and
//! a chassis interleave under one endpoint's admission stack, so one round
//! carries all three payload kinds from two providers.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

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
use nv_telemetry_orchestration::PollMeta;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_redfish::ChassisRead;
use nv_telemetry_redfish::SensorRead;

const SENSOR: &str = "/redfish/v1/Chassis/1U/Sensors/CPU1Temp";
const CHASSIS: &str = "/redfish/v1/Chassis/1U";
const SENSOR_FIXTURE: &str = include_str!("../fixtures/sensor.json");
const CHASSIS_FIXTURE: &str = include_str!("../fixtures/chassis.json");
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

/// One primed round: a sensor report then a chassis report, all three
/// payload kinds under both providers' identities.
fn assert_mixed_round(runtime: &mut PollRuntime, endpoint: &EndpointContext) {
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
    assert!(
        sensor_report.issues().is_none() && chassis_report.issues().is_none(),
        "the nominal fixtures are clean"
    );
}

#[test]
fn a_mocked_endpoint_polls_both_providers_end_to_end() {
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
    // The mock is strict-FIFO, so priming follows dispatch order: the ring
    // visits targets in needs order each round.
    for _ in 0..3 {
        bmc.expect(Expect::get(SENSOR, SENSOR_FIXTURE));
        bmc.expect(Expect::get(CHASSIS, CHASSIS_FIXTURE));
    }

    let cadence = Duration::from_secs(30);
    let plan = plan(
        vec![
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
        ],
        &[
            SensorRead::<()>::declaration(),
            ChassisRead::<()>::declaration(),
        ],
    )
    .expect("both declarations poll");
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

    let subtree = endpoint_subtree(
        &EndpointPolicy::default(),
        &clock,
        vec![
            PollUnit::new(plan.polls()[0].clone(), sensor_unit, &clock),
            PollUnit::new(plan.polls()[1].clone(), chassis_unit, &clock),
        ],
    )
    .expect("two providers form one subtree");
    let mut runtime: PollRuntime = Runtime::new(
        RuntimeConfig {
            global_max_in_flight: std::num::NonZeroUsize::MIN,
            clock: ClockConfig::Manual(manual.clone()),
        },
        subtree,
    );

    // Three primed rounds; after each, one cadence hint moves the clock.
    for round in 0..3 {
        assert_mixed_round(&mut runtime, &endpoint);
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
