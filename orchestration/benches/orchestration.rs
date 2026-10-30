// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Instruction-count benchmarks (gungraun / Valgrind Callgrind) for
//! orchestration, through the public API only — the numbers are what an
//! embedder pays.
//!
//! Two questions, one group each:
//!
//! - `deliver`: everything one steady-state poll pays above the wire,
//!   from the endpoint's timer firing to the report in the consumer's hand
//!   — the turn through the four-layer admission stack, the leaf's
//!   `acquire`, the status-and-issues doctrine for each kind of outcome,
//!   the completion's bookkeeping and next hint, and the reports channel;
//!   alone, and contended by a dense endpoint. Batches pass through already
//!   validated, so payload size is priced in the model crate's benchmarks,
//!   not here. Tokio runs on one thread with its clock paused, so the
//!   timeline is virtual and the counts deterministic.
//! - `configure`: what a config change pays — planning a fleet-size need
//!   list and adding an endpoint from its planned polls.
//!
//! Sizes are deliberately modest: instruction counts are deterministic,
//! so a thousand-need plan answers the scaling question without a
//! million-need run, and Callgrind makes big inputs slow.

// The `library_benchmark` macro expands to code the workspace's
// `unused_qualifications` lint misreads as our own spans — and, when the
// attribute carries a `config`, it additionally wraps the function and emits
// an `Option`-returning `__get_config`, tripping `unreachable_pub` and
// `clippy::unnecessary_wraps` on spans this file cannot edit. A benchmark
// takes its setup output by value because that is how gungraun hands it
// over — borrowing instead would measure a call shape no consumer uses.
#![allow(
    unused_qualifications,
    unreachable_pub,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps
)]

#[cfg(unix)]
mod unix {
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use futures_util::StreamExt as _;
    use gungraun::library_benchmark;
    use gungraun::Callgrind;
    use gungraun::EventKind;
    use gungraun::LibraryBenchmarkConfig;
    use nv_telemetry_model::Completeness;
    use nv_telemetry_model::Coverage;
    use nv_telemetry_model::EndpointContext;
    use nv_telemetry_model::Origin;
    use nv_telemetry_model::Payload;
    use nv_telemetry_model::States;
    use nv_telemetry_model::Timestamp;
    use nv_telemetry_orchestration::plan;
    use nv_telemetry_orchestration::Clock;
    use nv_telemetry_orchestration::Collected;
    use nv_telemetry_orchestration::Collector;
    use nv_telemetry_orchestration::EndpointPolicy;
    use nv_telemetry_orchestration::EndpointUnits;
    use nv_telemetry_orchestration::Needs;
    use nv_telemetry_orchestration::Plan;
    use nv_telemetry_orchestration::PollNeed;
    use nv_telemetry_orchestration::PollUnit;
    use nv_telemetry_orchestration::Reports;
    use nv_telemetry_source::Acquire;
    use nv_telemetry_source::AcquisitionFailure;
    use nv_telemetry_source::AcquisitionFailureClass;
    use nv_telemetry_source::AcquisitionParts;
    use nv_telemetry_source::ProjectionIssue;
    use nv_telemetry_source::ProviderDeclaration;

    /// Needs in a fleet-size plan: endpoints × sensors an embedder might
    /// resolve in one configuration pass.
    const FLEET_NEEDS: usize = 1024;

    /// Units under one endpoint: a dense chassis.
    const ENDPOINT_UNITS: usize = 64;

    /// Issues in the envelope-bearing case.
    const ISSUES: usize = 16;

    /// The regression gate for benchmarks whose counts sit under a few
    /// tens of thousands of instructions, where a single inlining decision
    /// moved by unrelated code is several percent by itself. The bulk
    /// cases hold the strict default set at `main!`.
    const MICRO_IR_LIMIT: f64 = 15.0;

    const PROVIDER: &str = "redfish.sensor.odata";
    const REQUEST_CLASS: &str = "sensor-read";
    const BASE_SECONDS: i64 = 1_785_621_243;

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

    fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::polled(PROVIDER, REQUEST_CLASS, 1)
    }

    /// A unit whose every acquisition is a stored clone: the leaf costs
    /// nothing but the doctrine around it, which is what these benchmarks
    /// price.
    struct FixtureUnit {
        endpoint: EndpointContext,
        origin: Origin,
        outcome: Result<AcquisitionParts, AcquisitionFailure>,
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
            self.outcome.clone()
        }
    }

    /// Tokio's paused clock as orchestration's; wall time stays at the
    /// base, as no count depends on it.
    struct PausedClock;

    impl Clock for PausedClock {
        fn timestamp(&self) -> Timestamp {
            Timestamp::new(BASE_SECONDS, 0).expect("a valid instant")
        }

        fn instant(&self) -> Instant {
            tokio::time::Instant::now().into_std()
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

    fn issues(count: usize) -> Vec<ProjectionIssue> {
        (0..count)
            .map(|index| {
                ProjectionIssue::invalid(
                    format!("sensor.reading[{index}]"),
                    "a fixture projection defect",
                )
            })
            .collect()
    }

    fn fleet_needs(count: usize) -> Vec<PollNeed> {
        (0..count)
            .map(|index| {
                PollNeed::new(
                    endpoint(),
                    REQUEST_CLASS,
                    format!("/redfish/v1/Chassis/1U/Sensors/S{index}"),
                    Duration::from_secs(30),
                )
            })
            .collect()
    }

    fn units(
        count: usize,
        outcome: &Result<AcquisitionParts, AcquisitionFailure>,
    ) -> EndpointUnits {
        let plan = plan(
            Needs::default().with_polls(fleet_needs(count)),
            &[declaration()],
        )
        .expect("the declaration polls");
        let polls = plan.polls().iter().map(|planned| {
            let unit = Arc::new(FixtureUnit {
                endpoint: endpoint(),
                origin: origin(),
                outcome: outcome.clone(),
            });
            PollUnit::new(planned.clone(), unit)
        });
        EndpointUnits::new(EndpointPolicy::default()).with_polls(polls)
    }

    /// A collector on a one-thread tokio runtime with its clock paused.
    struct Rig {
        tokio: tokio::runtime::Runtime,
        collector: Collector,
        reports: Reports,
    }

    fn rig() -> Rig {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("a current-thread runtime");
        let (collector, reports) = {
            let _entered = tokio.enter();
            Collector::new(PausedClock)
        };
        Rig {
            tokio,
            collector,
            reports,
        }
    }

    fn next(rig: &mut Rig) -> Collected {
        rig.tokio
            .block_on(rig.reports.next())
            .expect("the collector runs")
    }

    /// An endpoint whose first round has been delivered: its next report
    /// is a steady-state poll, one cadence away.
    fn steady(count: usize, outcome: Result<AcquisitionParts, AcquisitionFailure>) -> Rig {
        let mut rig = rig();
        {
            let _entered = rig.tokio.enter();
            rig.collector
                .add(units(count, &outcome))
                .expect("the units form an endpoint");
        }
        for _ in 0..count {
            next(&mut rig);
        }
        rig
    }

    fn adding(count: usize) -> (Rig, EndpointUnits) {
        (rig(), units(count, &Ok(states_parts(Vec::new()))))
    }

    // --- deliver: one steady-state poll, timer to consumer ---

    #[library_benchmark(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::default().soft_limits([(EventKind::Ir, MICRO_IR_LIMIT)])))]
    #[bench::success(steady(1, Ok(states_parts(Vec::new()))))]
    #[bench::success_with_issues(steady(1, Ok(states_parts(issues(ISSUES)))))]
    #[bench::request_scoped_failure(steady(
        1,
        Err(AcquisitionFailure::new(AcquisitionFailureClass::Protocol))
    ))]
    #[bench::endpoint_scoped_failure(steady(
        1,
        Err(AcquisitionFailure::new(AcquisitionFailureClass::Connectivity))
    ))]
    pub fn deliver_report(mut rig: Rig) -> (Rig, Collected) {
        let collected = black_box(next(&mut rig));
        (rig, collected)
    }

    // Its own benchmark rather than a case of `deliver_report`, because the
    // gate is per benchmark and a dense endpoint is bulk-sized: it holds
    // the strict default while the one-unit poll needs the micro one.
    #[library_benchmark]
    #[bench::dense_erased(steady(ENDPOINT_UNITS, Ok(states_parts(Vec::new()))))]
    pub fn deliver_report_dense(mut rig: Rig) -> (Rig, Collected) {
        let collected = black_box(next(&mut rig));
        (rig, collected)
    }

    // --- configure: what a config change pays ---

    #[library_benchmark]
    #[bench::matched_fleet(fleet_needs(FLEET_NEEDS))]
    pub fn plan_fleet(needs: Vec<PollNeed>) -> Plan {
        black_box(
            plan(Needs::default().with_polls(needs), &[declaration()])
                .expect("the declaration polls"),
        )
    }

    #[library_benchmark]
    #[bench::erased_chassis(adding(ENDPOINT_UNITS))]
    pub fn add_endpoint((rig, units): (Rig, EndpointUnits)) -> Rig {
        {
            let _entered = rig.tokio.enter();
            black_box(rig.collector.add(units)).expect("the units form an endpoint");
        }
        rig
    }
}

#[cfg(unix)]
use unix::add_endpoint;
#[cfg(unix)]
use unix::deliver_report;
#[cfg(unix)]
use unix::deliver_report_dense;
#[cfg(unix)]
use unix::plan_fleet;

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = deliver;
    benchmarks = deliver_report, deliver_report_dense
);

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = configure;
    benchmarks = plan_fleet, add_endpoint
);

// The regression gate lives here rather than on the CI command line, so the
// thresholds sit next to the benchmarks they judge and a benchmark can carry
// its own — a command-line limit would override every one of them.
#[cfg(unix)]
gungraun::main!(
    config = gungraun::LibraryBenchmarkConfig::default()
        .tool(gungraun::Callgrind::default().soft_limits([(gungraun::EventKind::Ir, 2.0)]));
    library_benchmark_groups = deliver,
    configure
);

#[cfg(not(unix))]
fn main() {}
