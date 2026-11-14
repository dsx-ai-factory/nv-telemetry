// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The collector at the library's default policies, under a paused clock.
//! `spec/endpoint.qnt` checks P10 of `docs/FORMAL.md` at scaled-down
//! numbers; this holds the real collector to it at the numbers it ships
//! with: streams whose connects fail do not silence a poll the device
//! answers.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::BoxStream;
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Origin;
use nv_telemetry_orchestration::plan;
use nv_telemetry_orchestration::Collector;
use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::EndpointUnits;
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::ReconnectPolicy;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamUnit;
use nv_telemetry_orchestration::SystemClock;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProviderDeclaration;
use nv_telemetry_source::SubscriptionItem;

const SECOND: Duration = Duration::from_secs(1);
const CADENCE: u64 = 5;
const RUN_SECONDS: u64 = 3600;
/// A connect's cost in tokens, as the Redfish event stream declares it.
const CONNECT_COST: u64 = 3;

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint")
}

struct HealthyPoll {
    endpoint: EndpointContext,
    origin: Origin,
    polls: Arc<AtomicUsize>,
}

impl Acquire for HealthyPoll {
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<AcquisitionParts, AcquisitionFailure> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Ok(AcquisitionParts::new(Vec::new(), Vec::new()))
    }
}

struct UnreachableStream {
    endpoint: EndpointContext,
    origin: Origin,
    /// How long the device takes to fail the connect.
    takes: Duration,
}

impl Acquire for UnreachableStream {
    type Output = BoxStream<'static, SubscriptionItem>;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<Self::Output, AcquisitionFailure> {
        tokio::time::sleep(self.takes).await;
        Err(AcquisitionFailure::new(AcquisitionFailureClass::Connectivity).with_retryable(true))
    }
}

/// The seconds of an hour at which the poll was dispatched, beside
/// `streams` streams whose connects fail after `takes`.
fn poll_seconds(streams: usize, stagger_percent: u8, takes: Duration) -> Vec<u64> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a runtime");
    let classes: Vec<String> = (0..streams)
        .map(|n| format!("fixture-subscribe-{n}"))
        .collect();
    let needs = Needs::default()
        .with_polls([PollNeed::new(
            endpoint(),
            "fixture-read",
            "sensor",
            Duration::from_secs(CADENCE),
        )])
        .with_streams(
            classes
                .iter()
                .map(|class| StreamNeed::new(endpoint(), class.as_str())),
        );
    let mut declarations = vec![ProviderDeclaration::polled(
        "fixture.sensor",
        "fixture-read",
        1,
    )];
    for (n, class) in classes.iter().enumerate() {
        declarations.push(ProviderDeclaration::streamed(
            format!("fixture.events.{n}").as_str(),
            class.as_str(),
            CONNECT_COST,
        ));
    }
    let planned = plan(needs, &declarations).expect("a plan");
    let polls = Arc::new(AtomicUsize::new(0));
    let planned_poll = planned.polls().first().expect("one poll").clone();
    let poll = PollUnit::new(
        planned_poll.clone(),
        Arc::new(HealthyPoll {
            endpoint: endpoint(),
            origin: planned_poll.origin().clone(),
            polls: Arc::clone(&polls),
        }),
    );
    let units = planned.streams().iter().map(|stream| {
        StreamUnit::new(
            stream.clone(),
            Arc::new(UnreachableStream {
                endpoint: endpoint(),
                origin: stream.origin().clone(),
                takes,
            }),
            ReconnectPolicy::default().with_stagger_percent(stagger_percent),
        )
    });

    let _context = runtime.enter();
    let (collector, mut reports) = Collector::new(SystemClock::default());
    collector
        .add(
            EndpointUnits::new(EndpointPolicy::default())
                .with_polls([poll])
                .with_streams(units),
        )
        .expect("seated");
    let mut at = Vec::new();
    for second in 0..RUN_SECONDS {
        runtime.block_on(async {
            for _ in 0..64 {
                tokio::task::yield_now().await;
                while let Some(Some(_)) = reports.next().now_or_never() {}
            }
        });
        at.resize(polls.load(Ordering::SeqCst), second);
        runtime.block_on(tokio::time::advance(SECOND));
    }
    at
}

/// The longest stretch of the hour without a poll, its start and end
/// included.
fn longest_silence(at: &[u64]) -> u64 {
    let ends = [0]
        .into_iter()
        .chain(at.iter().copied())
        .chain([RUN_SECONDS]);
    let starts = [0].into_iter().chain(at.iter().copied());
    starts
        .zip(ends.skip(1))
        .map(|(from, to)| to - from)
        .max()
        .unwrap_or(RUN_SECONDS)
}

#[test]
fn failing_streams_do_not_silence_a_healthy_poll() {
    let cases = [
        ("two streams, no stagger", 2, 0, Duration::ZERO),
        ("three streams", 3, 25, Duration::ZERO),
        ("two streams, connects 20 s to fail", 2, 25, 20 * SECOND),
    ];
    for (case, streams, stagger_percent, takes) in cases {
        let silence = longest_silence(&poll_seconds(streams, stagger_percent, takes));
        // A poll waits for its cadence, and behind each stream's connect
        // for the bucket to earn that connect's cost at a token a second.
        let bound = CADENCE + streams as u64 * CONNECT_COST;
        assert!(silence <= bound, "{case}: no poll for {silence} s");
    }
}
