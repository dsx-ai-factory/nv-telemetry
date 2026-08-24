// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Instruction-count benchmarks (gungraun / Valgrind Callgrind) for
//! orchestration, through the public API only — the numbers are what an
//! embedder pays.
//!
//! Three questions, one group each:
//!
//! - `assemble`: the per-acquisition doctrine price — one status built,
//!   the issues envelope when issues exist, the breaker-channel split.
//!   Batches pass through already validated, so payload size is priced in
//!   the model crate's benchmarks, not here.
//! - `tick`: everything one poll pays above the wire — a full runtime
//!   turn through the four-layer admission stack, the leaf's `acquire`,
//!   and assembly, on a virtual timeline with a trivial payload; alone,
//!   contended by a dense subtree, and paired with the follow-up turn
//!   that applies the completion and hints the next deadline.
//! - `configure`: what a config change pays — planning a fleet-size need
//!   list and building an endpoint subtree from its planned polls.
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
    use std::future::Future;
    use std::hint::black_box;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;
    use std::time::Instant;

    use gungraun::library_benchmark;
    use gungraun::Callgrind;
    use gungraun::EventKind;
    use gungraun::LibraryBenchmarkConfig;
    use nv_redfish_dispatcher::ClockConfig;
    use nv_redfish_dispatcher::ManualClock;
    use nv_redfish_dispatcher::Runtime;
    use nv_redfish_dispatcher::RuntimeConfig;
    use nv_redfish_dispatcher::RuntimeOutput;
    use nv_telemetry_model::Completeness;
    use nv_telemetry_model::Coverage;
    use nv_telemetry_model::EndpointContext;
    use nv_telemetry_model::Origin;
    use nv_telemetry_model::Payload;
    use nv_telemetry_model::States;
    use nv_telemetry_model::Timestamp;
    use nv_telemetry_orchestration::assemble;
    use nv_telemetry_orchestration::endpoint_subtree;
    use nv_telemetry_orchestration::plan;
    use nv_telemetry_orchestration::AcquisitionReport;
    use nv_telemetry_orchestration::Clock;
    use nv_telemetry_orchestration::EndpointFault;
    use nv_telemetry_orchestration::EndpointPolicy;
    use nv_telemetry_orchestration::Plan;
    use nv_telemetry_orchestration::PollMeta;
    use nv_telemetry_orchestration::PollNeed;
    use nv_telemetry_orchestration::PollUnit;
    use nv_telemetry_source::acquire;
    use nv_telemetry_source::Acquire;
    use nv_telemetry_source::Acquired;
    use nv_telemetry_source::AcquisitionFailure;
    use nv_telemetry_source::AcquisitionFailureClass;
    use nv_telemetry_source::AcquisitionParts;
    use nv_telemetry_source::ProjectionIssue;
    use nv_telemetry_source::ProviderDeclaration;

    /// Needs in a fleet-size plan: endpoints × sensors an embedder might
    /// resolve in one configuration pass.
    const FLEET_NEEDS: usize = 1024;

    /// Units under one endpoint subtree: a dense chassis.
    const SUBTREE_UNITS: usize = 64;

    /// Issues in the envelope-bearing assembly case.
    const ISSUES: usize = 16;

    /// The regression gate for benchmarks whose counts sit under a few
    /// tens of thousands of instructions, where a single inlining decision
    /// moved by unrelated code is several percent by itself. The bulk
    /// `configure` cases hold the strict default set at `main!`.
    const MICRO_IR_LIMIT: f64 = 15.0;

    const PROVIDER: &str = "redfish.sensor.odata";
    const REQUEST_CLASS: &str = "sensor-read";
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

    fn at() -> Timestamp {
        Timestamp::new(BASE_SECONDS, 0).expect("a valid instant")
    }

    fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::polled(PROVIDER, REQUEST_CLASS, 1)
    }

    /// A unit whose acquisition is a stored clone: the leaf costs nothing
    /// but the doctrine around it, which is what these benchmarks price.
    struct FixtureUnit {
        endpoint: EndpointContext,
        origin: Origin,
        parts: AcquisitionParts,
    }

    impl FixtureUnit {
        fn shared() -> Arc<Self> {
            Arc::new(Self {
                endpoint: endpoint(),
                origin: origin(),
                parts: states_parts(Vec::new()),
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
            Ok(self.parts.clone())
        }
    }

    /// A frozen timeline: instants come from the manual clock, wall time
    /// is the base — deterministic instruction counts need no real clock.
    #[derive(Clone)]
    struct FrozenClock {
        manual: ManualClock,
    }

    impl Clock for FrozenClock {
        fn timestamp(&self) -> Timestamp {
            at()
        }

        fn instant(&self) -> Instant {
            self.manual.now()
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

    /// The fixture never awaits anything, so one poll completes it.
    fn acquired(parts: AcquisitionParts) -> Acquired {
        let unit = FixtureUnit {
            endpoint: endpoint(),
            origin: origin(),
            parts,
        };
        let future = pin!(acquire(&unit, at()));
        match future.poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(outcome) => outcome.expect("the fixture acquisition succeeds"),
            Poll::Pending => unreachable!("the fixture future is ready on first poll"),
        }
    }

    /// Everything `assemble` takes, in hand as the driver has it.
    struct AssembleInput {
        endpoint: EndpointContext,
        origin: Origin,
        at: Timestamp,
        outcome: Result<Acquired, AcquisitionFailure>,
    }

    fn assemble_input(outcome: Result<Acquired, AcquisitionFailure>) -> AssembleInput {
        AssembleInput {
            endpoint: endpoint(),
            origin: origin(),
            at: at(),
            outcome,
        }
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

    /// Everything `endpoint_subtree` takes: planned polls paired with
    /// their units, plus the policy and clock.
    struct SubtreeInput {
        policy: EndpointPolicy,
        clock: FrozenClock,
        units: Vec<PollUnit>,
    }

    fn subtree_input(count: usize) -> SubtreeInput {
        let plan = plan(fleet_needs(count), &[declaration()]).expect("the declaration polls");
        let clock = FrozenClock {
            manual: ManualClock::new(),
        };
        let units = plan
            .polls()
            .iter()
            .map(|planned| PollUnit::new(planned.clone(), FixtureUnit::shared(), &clock))
            .collect();
        SubtreeInput {
            policy: EndpointPolicy::default(),
            clock,
            units,
        }
    }

    /// A runtime whose leaves are all due at construction: the next turn
    /// is a full admit-dispatch-acquire-assemble cycle, contended by
    /// however many peers the subtree holds.
    fn primed_runtime(units: usize) -> PollRuntime {
        let input = subtree_input(units);
        let manual = input.clock.manual.clone();
        let subtree = endpoint_subtree(&input.policy, &input.clock, input.units)
            .expect("the units form a subtree");
        Runtime::new(
            RuntimeConfig {
                global_max_in_flight: std::num::NonZeroUsize::MIN,
                clock: ClockConfig::Manual(manual),
            },
            subtree,
        )
    }

    /// One driver turn, panicking on a parked runtime — every benched
    /// timeline has a turn to give.
    fn turn(runtime: &mut PollRuntime) -> RuntimeOutput<AcquisitionReport, EndpointFault> {
        let mut next = pin!(runtime.next());
        match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("the runtime had a turn to give"),
        }
    }

    // --- assemble: the per-acquisition doctrine price ---

    #[library_benchmark(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::default().soft_limits([(EventKind::Ir, MICRO_IR_LIMIT)])))]
    #[bench::success(assemble_input(Ok(acquired(states_parts(Vec::new())))))]
    #[bench::success_with_issues(assemble_input(Ok(acquired(states_parts(issues(ISSUES))))))]
    #[bench::request_scoped_failure(assemble_input(Err(AcquisitionFailure::new(
        AcquisitionFailureClass::Protocol
    ))))]
    #[bench::endpoint_scoped_failure(assemble_input(Err(AcquisitionFailure::new(
        AcquisitionFailureClass::Connectivity
    ))))]
    pub fn assemble_outcome(input: AssembleInput) -> Result<AcquisitionReport, EndpointFault> {
        black_box(assemble(
            &input.endpoint,
            &input.origin,
            input.at,
            Duration::from_millis(12),
            input.outcome,
        ))
    }

    // --- tick: one runtime turn through the admission stack ---

    /// The first turn, which every benched timeline owes as work.
    fn work_turn(runtime: &mut PollRuntime) -> RuntimeOutput<AcquisitionReport, EndpointFault> {
        match turn(runtime) {
            output @ RuntimeOutput::Work { .. } => output,
            _ => panic!("the first tick is due at construction"),
        }
    }

    #[library_benchmark(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::default().soft_limits([(EventKind::Ir, MICRO_IR_LIMIT)])))]
    #[bench::one_unit(primed_runtime(1))]
    pub fn runtime_tick(
        mut runtime: PollRuntime,
    ) -> RuntimeOutput<AcquisitionReport, EndpointFault> {
        black_box(work_turn(&mut runtime))
    }

    // Its own benchmark rather than a case of `runtime_tick`, because the
    // gate is per benchmark and a dense turn is bulk-sized: it holds the
    // strict default while the one-unit turn needs the micro one.
    #[library_benchmark]
    #[bench::dense_erased(primed_runtime(SUBTREE_UNITS))]
    pub fn runtime_tick_dense(
        mut runtime: PollRuntime,
    ) -> RuntimeOutput<AcquisitionReport, EndpointFault> {
        black_box(work_turn(&mut runtime))
    }

    // The steady-state pair: the work turn, then the turn that applies the
    // completion — breaker sample, slot release — and hints the next
    // deadline. Together they are one poll's full price to the driver;
    // `runtime_tick` alone stops before the bookkeeping lands.
    #[library_benchmark(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::default().soft_limits([(EventKind::Ir, MICRO_IR_LIMIT)])))]
    #[bench::work_then_hint(primed_runtime(1))]
    pub fn poll_cycle(mut runtime: PollRuntime) -> RuntimeOutput<AcquisitionReport, EndpointFault> {
        black_box(work_turn(&mut runtime));
        match turn(&mut runtime) {
            output @ RuntimeOutput::SleepUntil(_) => black_box(output),
            _ => panic!("the completed turn hints the next deadline"),
        }
    }

    // --- configure: what a config change pays ---

    #[library_benchmark]
    #[bench::matched_fleet(fleet_needs(FLEET_NEEDS))]
    pub fn plan_fleet(needs: Vec<PollNeed>) -> Plan {
        black_box(plan(needs, &[declaration()]).expect("the declaration polls"))
    }

    #[library_benchmark]
    #[bench::erased_chassis(subtree_input(SUBTREE_UNITS))]
    pub fn build_subtree(input: SubtreeInput) -> nv_telemetry_orchestration::EndpointSubtree {
        black_box(
            endpoint_subtree(&input.policy, &input.clock, input.units)
                .expect("the units form a subtree"),
        )
    }
}

#[cfg(unix)]
use unix::assemble_outcome;
#[cfg(unix)]
use unix::build_subtree;
#[cfg(unix)]
use unix::plan_fleet;
#[cfg(unix)]
use unix::poll_cycle;
#[cfg(unix)]
use unix::runtime_tick;
#[cfg(unix)]
use unix::runtime_tick_dense;

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = assemble;
    benchmarks = assemble_outcome
);

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = tick;
    benchmarks = runtime_tick, runtime_tick_dense, poll_cycle
);

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = configure;
    benchmarks = plan_fleet, build_subtree
);

// The regression gate lives here rather than on the CI command line, so the
// thresholds sit next to the benchmarks they judge and a benchmark can carry
// its own — a command-line limit would override every one of them.
#[cfg(unix)]
gungraun::main!(
    config = gungraun::LibraryBenchmarkConfig::default()
        .tool(gungraun::Callgrind::default().soft_limits([(gungraun::EventKind::Ir, 2.0)]));
    library_benchmark_groups = assemble,
    tick,
    configure
);

#[cfg(not(unix))]
fn main() {}
