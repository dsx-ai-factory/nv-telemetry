// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The standard endpoint subtree: planned polls and streams in, one
//! dispatcher-ready scheduler out.
//!
//! The milestone-1 slice of the architecture's admission stack — endpoint
//! admission over the endpoint breaker over the rate bucket over the unit
//! leaves. Units of different request classes share one round-robin
//! ring; per-class lanes and the class breaker are deliberately absent
//! until class-level policy exists. The collector seats each subtree on
//! its own runtime; policy types here convert privately into dispatcher
//! configuration, so the dispatcher's own vocabulary never leaks into
//! embedder policy.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use nv_redfish_dispatcher::schedulers::BoundedConcurrency;
use nv_redfish_dispatcher::schedulers::CircuitBreaker;
use nv_redfish_dispatcher::schedulers::CircuitBreakerConfig;
use nv_redfish_dispatcher::schedulers::FixedCost;
use nv_redfish_dispatcher::schedulers::PeriodicLeaf;
use nv_redfish_dispatcher::schedulers::RoundRobin;
use nv_redfish_dispatcher::schedulers::TokenBucket;
use nv_redfish_dispatcher::schedulers::TokenBucketConfig;
use nv_redfish_dispatcher::CostUnits;
use nv_redfish_dispatcher::WithCost;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Origin;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionParts;

use crate::clock::SharedClock;
use crate::plan::PlannedPoll;
use crate::report::poll_future;
use crate::report::TelemetryWork;
use crate::stream::ReconnectPolicy;
use crate::stream::StreamReports;
use crate::stream::StreamUnit;

/// When the endpoint breaker opens and how it recovers.
#[derive(Clone, Debug)]
pub struct BreakerPolicy {
    failure_threshold: f32,
    sample_window: u32,
    min_samples: u32,
    cool_down: Duration,
}

impl Default for BreakerPolicy {
    fn default() -> Self {
        Self {
            failure_threshold: 0.5,
            sample_window: 32,
            min_samples: 4,
            cool_down: Duration::from_mins(1),
        }
    }
}

impl BreakerPolicy {
    /// Failure fraction in `0.0..=1.0` that opens the breaker.
    #[must_use]
    pub fn with_failure_threshold(mut self, threshold: f32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// How many recent outcomes the trip decision looks at.
    #[must_use]
    pub fn with_sample_window(mut self, window: u32) -> Self {
        self.sample_window = window;
        self
    }

    /// Outcomes required before the threshold is evaluated at all.
    #[must_use]
    pub fn with_min_samples(mut self, samples: u32) -> Self {
        self.min_samples = samples;
        self
    }

    /// How long an open breaker rests before probing the endpoint again.
    #[must_use]
    pub fn with_cool_down(mut self, cool_down: Duration) -> Self {
        self.cool_down = cool_down;
        self
    }
}

fn breaker_config(policy: &BreakerPolicy) -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        failure_threshold: policy.failure_threshold,
        sample_window: policy.sample_window,
        min_samples: policy.min_samples,
        cool_down: policy.cool_down,
    }
}

/// How fast requests may leave for the endpoint.
#[derive(Clone, Debug)]
pub struct RatePolicy {
    burst: u64,
    refill: u64,
    per: Duration,
}

impl Default for RatePolicy {
    fn default() -> Self {
        Self {
            burst: 4,
            refill: 1,
            per: Duration::from_secs(1),
        }
    }
}

impl RatePolicy {
    /// Maximum stored tokens: how many requests may leave back to back.
    #[must_use]
    pub fn with_burst(mut self, burst: u64) -> Self {
        self.burst = burst;
        self
    }

    /// Tokens accrued per `per` — the sustained rate. A zero `per` is the
    /// dispatcher's unlimited-rate mode: the bucket is always full and
    /// `burst`/`refill` are inert.
    #[must_use]
    pub fn with_refill(mut self, refill: u64, per: Duration) -> Self {
        self.refill = refill;
        self.per = per;
        self
    }
}

fn bucket_config(policy: &RatePolicy) -> TokenBucketConfig {
    TokenBucketConfig {
        capacity: CostUnits::new(policy.burst),
        refill_amount: CostUnits::new(policy.refill),
        refill_interval: policy.per,
    }
}

/// One endpoint's admission policy.
#[derive(Clone, Debug)]
pub struct EndpointPolicy {
    max_in_flight: NonZeroU32,
    breaker: BreakerPolicy,
    rate: RatePolicy,
}

impl Default for EndpointPolicy {
    fn default() -> Self {
        Self {
            max_in_flight: NonZeroU32::MIN,
            breaker: BreakerPolicy::default(),
            rate: RatePolicy::default(),
        }
    }
}

impl EndpointPolicy {
    /// How many requests may be in flight against the endpoint at once.
    /// One is the default: a stalled device queues behind itself instead
    /// of eating fleet-wide capacity.
    #[must_use]
    pub fn with_max_in_flight(mut self, max_in_flight: NonZeroU32) -> Self {
        self.max_in_flight = max_in_flight;
        self
    }

    /// The endpoint breaker's policy.
    #[must_use]
    pub fn with_breaker(mut self, breaker: BreakerPolicy) -> Self {
        self.breaker = breaker;
        self
    }

    /// The endpoint's rate policy.
    #[must_use]
    pub fn with_rate(mut self, rate: RatePolicy) -> Self {
        self.rate = rate;
        self
    }
}

/// The meta every unit under an endpoint subtree carries.
pub(crate) type PollMeta = WithCost<()>;

/// The standard endpoint subtree: endpoint admission over the endpoint
/// breaker over the rate bucket over the unit leaves.
pub(crate) type EndpointSubtree = BoundedConcurrency<
    TelemetryWork,
    CircuitBreaker<TelemetryWork, TokenBucket<TelemetryWork, RoundRobin<TelemetryWork, PollMeta>>>,
>;

/// One endpoint's subtree, ready for its runtime, and the reports of the
/// streams seated in it.
pub(crate) struct Seated {
    pub(crate) endpoint: EndpointContext,
    pub(crate) subtree: EndpointSubtree,
    pub(crate) streams: Vec<StreamReports>,
    /// Whether any poll is seated: without one, the endpoint has nothing
    /// left to do once its streams stop.
    pub(crate) polled: bool,
}

/// Why an endpoint could not be added.
#[derive(Debug)]
pub enum RecipeError {
    /// A subtree with no units schedules nothing and hides it.
    NoUnits,
    /// A policy value would silently disable, permanently jam, or crash
    /// the admission stack; each is a configuration error, refused loudly.
    InvalidPolicy(&'static str),
    /// A planned acquisition and the unit built for it disagree about the
    /// endpoint — a wiring bug that would stamp every product with the
    /// wrong device.
    EndpointMismatch {
        /// The endpoint id the plan resolved.
        planned: String,
        /// The endpoint id the unit carries.
        unit: String,
    },
    /// A planned acquisition and the unit built for it disagree about the
    /// origin — a wiring bug that would stamp every product with the wrong
    /// provider.
    OriginMismatch {
        /// What the plan resolved.
        planned: Box<Origin>,
        /// What the unit carries.
        unit: Box<Origin>,
    },
    /// The unit list spans more than one endpoint: a subtree is one
    /// endpoint's admission scope, and sharing its breaker across devices
    /// would quarantine healthy ones.
    MixedEndpoints {
        /// The first endpoint id seen.
        first: String,
        /// The differing endpoint id.
        other: String,
    },
    /// The endpoint is collected already, or still retiring.
    AlreadyCollected {
        /// The endpoint id.
        endpoint: String,
    },
    /// The collector was shut down, or its reports were dropped.
    ShutDown,
}

impl std::fmt::Display for RecipeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoUnits => f.write_str("an endpoint subtree needs at least one unit"),
            Self::InvalidPolicy(what) => write!(f, "endpoint policy: {what}"),
            Self::EndpointMismatch { planned, unit } => write!(
                f,
                "planned endpoint `{planned}` but the unit carries `{unit}`"
            ),
            Self::OriginMismatch { planned, unit } => write!(
                f,
                "planned origin `{}/{}` but the unit carries `{}/{}`",
                planned.provider(),
                planned.request_class(),
                unit.provider(),
                unit.request_class()
            ),
            Self::MixedEndpoints { first, other } => write!(
                f,
                "one subtree, one endpoint: got both `{first}` and `{other}`"
            ),
            Self::AlreadyCollected { endpoint } => {
                write!(f, "endpoint `{endpoint}` is collected already")
            }
            Self::ShutDown => f.write_str("the collector is shut down"),
        }
    }
}

impl std::error::Error for RecipeError {}

/// The longest breaker cool-down and rate interval a policy accepts;
/// beyond it, instant arithmetic stops being trustworthy and a "policy"
/// is really a disablement.
const MAX_POLICY_INTERVAL: Duration = Duration::from_hours(366 * 24);

/// Refuses policy values that would silently disable, permanently jam, or
/// crash the admission stack.
fn validate(policy: &EndpointPolicy) -> Result<(), RecipeError> {
    let breaker = &policy.breaker;
    if !(breaker.failure_threshold > 0.0 && breaker.failure_threshold <= 1.0) {
        // NaN fails this comparison too: a breaker that can never trip.
        return Err(RecipeError::InvalidPolicy(
            "failure threshold must be within (0.0, 1.0]",
        ));
    }
    if breaker.sample_window == 0 || breaker.min_samples == 0 {
        return Err(RecipeError::InvalidPolicy(
            "breaker windows need at least one sample",
        ));
    }
    if breaker.min_samples > breaker.sample_window {
        return Err(RecipeError::InvalidPolicy(
            "min samples beyond the sample window can never be reached",
        ));
    }
    if breaker.cool_down.is_zero() || breaker.cool_down > MAX_POLICY_INTERVAL {
        return Err(RecipeError::InvalidPolicy(
            "cool-down must be positive and at most a year",
        ));
    }
    let rate = &policy.rate;
    // A zero interval is the dispatcher's documented unlimited-rate mode:
    // the bucket is always full and every other rate field is inert.
    if !rate.per.is_zero() {
        if rate.burst == 0 || rate.refill == 0 {
            // Zero refill would spend the burst and then park polling
            // forever with no error and no wake-up.
            return Err(RecipeError::InvalidPolicy(
                "rate burst and refill must be positive",
            ));
        }
        if rate.per > MAX_POLICY_INTERVAL {
            return Err(RecipeError::InvalidPolicy(
                "refill interval must be at most a year",
            ));
        }
    }
    Ok(())
}

/// Refuses reconnect delays that would spin, never fire, or overflow.
fn validate_reconnect(policy: &ReconnectPolicy) -> Result<(), RecipeError> {
    if policy.first_retry().is_zero() {
        return Err(RecipeError::InvalidPolicy(
            "reconnect first retry must be positive",
        ));
    }
    if policy.max_retry() < policy.first_retry() {
        return Err(RecipeError::InvalidPolicy(
            "reconnect max retry must not be below the first retry",
        ));
    }
    if policy.max_retry() > MAX_POLICY_INTERVAL {
        return Err(RecipeError::InvalidPolicy(
            "reconnect delays must be at most a year",
        ));
    }
    if policy.stagger_percent() > 100 {
        return Err(RecipeError::InvalidPolicy(
            "reconnect stagger must be at most 100 percent",
        ));
    }
    Ok(())
}

/// One planned poll bound to its acquisition unit, erased to the future
/// level so units of different types share one subtree.
pub struct PollUnit {
    planned: PlannedPoll,
    unit_endpoint: EndpointContext,
    unit_origin: Origin,
    work: Box<dyn Fn(&SharedClock) -> TelemetryWork + Send>,
}

impl PollUnit {
    /// Pairs a planned poll with its unit. The collector's clock stamps
    /// every acquisition. No checking happens here; the collector compares
    /// the plan against the unit's identity when the endpoint is added.
    pub fn new<A>(planned: PlannedPoll, unit: Arc<A>) -> Self
    where
        A: Acquire<Output = AcquisitionParts> + Send + Sync + 'static,
    {
        let unit_endpoint = unit.endpoint().clone();
        let unit_origin = unit.origin().clone();
        Self {
            planned,
            unit_endpoint,
            unit_origin,
            work: Box::new(move |clock| poll_future(Arc::clone(&unit), Arc::clone(clock))),
        }
    }
}

impl std::fmt::Debug for PollUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollUnit")
            .field("planned", &self.planned)
            .finish_non_exhaustive()
    }
}

/// Builds one endpoint's subtree from its planned polls and streams, each
/// bound to its unit and stamping on `clock`. Leaf epochs and the bucket
/// epoch are `now`, on the timeline of the runtime that will drive the
/// subtree. Units of different request classes, polled or
/// streamed, share the round-robin ring; per-class lanes and the class
/// breaker remain deliberately absent.
///
/// # Errors
///
/// [`RecipeError::NoUnits`] when there is neither a poll nor a stream;
/// [`RecipeError::InvalidPolicy`] for an endpoint or reconnect policy that
/// would disable, jam, or crash the stack; [`RecipeError::EndpointMismatch`]
/// / [`RecipeError::OriginMismatch`] when a planned acquisition and its
/// unit disagree; and [`RecipeError::MixedEndpoints`] when the units span
/// more than one endpoint.
pub(crate) fn endpoint_subtree(
    policy: &EndpointPolicy,
    clock: &SharedClock,
    now: Instant,
    units: Vec<PollUnit>,
    streams: Vec<StreamUnit>,
) -> Result<Seated, RecipeError> {
    let endpoint = units
        .first()
        .map(|unit| unit.planned.endpoint())
        .or_else(|| streams.first().map(|stream| stream.planned().endpoint()))
        .ok_or(RecipeError::NoUnits)?
        .clone();
    let subtree_endpoint = endpoint.endpoint_id().to_owned();
    let polled = !units.is_empty();
    validate(policy)?;

    let mut lanes = RoundRobin::new();
    for unit in units {
        let planned = &unit.planned;
        if planned.endpoint().endpoint_id() != subtree_endpoint {
            return Err(RecipeError::MixedEndpoints {
                first: subtree_endpoint,
                other: planned.endpoint().endpoint_id().to_owned(),
            });
        }
        if planned.endpoint() != &unit.unit_endpoint {
            return Err(RecipeError::EndpointMismatch {
                planned: planned.endpoint().endpoint_id().to_owned(),
                unit: unit.unit_endpoint.endpoint_id().to_owned(),
            });
        }
        if planned.origin() != &unit.unit_origin {
            return Err(RecipeError::OriginMismatch {
                planned: Box::new(planned.origin().clone()),
                unit: Box::new(unit.unit_origin.clone()),
            });
        }
        let cost = CostUnits::new(planned.cost());
        let work = unit.work;
        let clock = Arc::clone(clock);
        let leaf = PeriodicLeaf::new(now, planned.cadence(), move || work(&clock));
        lanes.add_child(FixedCost::new(cost, leaf));
    }
    let mut reports = Vec::with_capacity(streams.len());
    for stream in streams {
        let planned = stream.planned();
        if planned.endpoint().endpoint_id() != subtree_endpoint {
            return Err(RecipeError::MixedEndpoints {
                first: subtree_endpoint,
                other: planned.endpoint().endpoint_id().to_owned(),
            });
        }
        if planned.endpoint() != stream.unit_endpoint() {
            return Err(RecipeError::EndpointMismatch {
                planned: planned.endpoint().endpoint_id().to_owned(),
                unit: stream.unit_endpoint().endpoint_id().to_owned(),
            });
        }
        if planned.origin() != stream.unit_origin() {
            return Err(RecipeError::OriginMismatch {
                planned: Box::new(planned.origin().clone()),
                unit: Box::new(stream.unit_origin().clone()),
            });
        }
        validate_reconnect(stream.policy())?;
        let cost = CostUnits::new(planned.cost());
        let (leaf, stream_reports) = stream.bind(clock);
        lanes.add_child(FixedCost::new(cost, leaf));
        reports.push(stream_reports);
    }

    let subtree = BoundedConcurrency::new(
        policy.max_in_flight,
        CircuitBreaker::new(
            breaker_config(&policy.breaker),
            TokenBucket::new(now, bucket_config(&policy.rate), lanes),
        ),
    );
    Ok(Seated {
        endpoint,
        subtree,
        streams: reports,
        polled,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_policy_validates() {
        validate(&EndpointPolicy::default()).expect("the shipped defaults are usable");
    }

    #[test]
    fn a_disabling_or_jamming_policy_is_refused() {
        let bad = [
            EndpointPolicy::default()
                .with_breaker(BreakerPolicy::default().with_failure_threshold(0.0)),
            EndpointPolicy::default()
                .with_breaker(BreakerPolicy::default().with_failure_threshold(1.5)),
            EndpointPolicy::default()
                .with_breaker(BreakerPolicy::default().with_failure_threshold(f32::NAN)),
            EndpointPolicy::default().with_breaker(BreakerPolicy::default().with_sample_window(0)),
            EndpointPolicy::default().with_breaker(BreakerPolicy::default().with_min_samples(0)),
            EndpointPolicy::default().with_breaker(
                BreakerPolicy::default()
                    .with_sample_window(4)
                    .with_min_samples(5),
            ),
            EndpointPolicy::default()
                .with_breaker(BreakerPolicy::default().with_cool_down(Duration::ZERO)),
            EndpointPolicy::default()
                .with_breaker(BreakerPolicy::default().with_cool_down(Duration::MAX)),
            EndpointPolicy::default().with_rate(RatePolicy::default().with_burst(0)),
            EndpointPolicy::default()
                .with_rate(RatePolicy::default().with_refill(0, Duration::from_secs(1))),
            EndpointPolicy::default()
                .with_rate(RatePolicy::default().with_refill(1, Duration::MAX)),
        ];
        for policy in bad {
            let error = validate(&policy).expect_err("an unusable policy is refused");
            assert!(matches!(error, RecipeError::InvalidPolicy(_)));
        }
    }

    #[test]
    fn a_zero_interval_is_the_unlimited_rate_not_an_error() {
        // The dispatcher ignores every other rate field in this mode, so
        // even the otherwise-jamming zeros validate.
        let unlimited = EndpointPolicy::default().with_rate(
            RatePolicy::default()
                .with_refill(0, Duration::ZERO)
                .with_burst(0),
        );
        validate(&unlimited).expect("a zero interval means unlimited, not misconfigured");
    }
}
