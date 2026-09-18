// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The static planner: needs in, planned polls and streams out.
//!
//! Selection is deterministic and explainable — exactly one declaration of
//! the need's mode serves each need's request class — and validation is
//! loud at plan time: a declaration whose identity cannot form a wire
//! `Origin` fails here, never at status-build time. Capability probing,
//! provider preference, and demotion arrive with later milestones; a plan
//! produced here is complete because nothing in it can be unresolved yet.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::Origin;
use nv_telemetry_source::AcquisitionMode;
use nv_telemetry_source::ProviderDeclaration;

/// One thing the embedder wants polled: a protocol-scoped target on one
/// endpoint, at a cadence, served by the provider declaring its request
/// class. The target is opaque to orchestration — for Redfish it is the
/// resource's `OData` id.
#[derive(Clone, Debug)]
pub struct PollNeed {
    endpoint: EndpointContext,
    request_class: String,
    target: String,
    cadence: Duration,
}

impl PollNeed {
    /// A need for `target` on `endpoint`, polled every `cadence` by the
    /// provider declaring `request_class`.
    #[must_use]
    pub fn new(
        endpoint: EndpointContext,
        request_class: impl Into<String>,
        target: impl Into<String>,
        cadence: Duration,
    ) -> Self {
        Self {
            endpoint,
            request_class: request_class.into(),
            target: target.into(),
            cadence,
        }
    }

    /// The endpoint the target lives on.
    #[must_use]
    pub fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    /// The request class of the provider that serves this need.
    #[must_use]
    pub fn request_class(&self) -> &str {
        &self.request_class
    }

    /// The protocol-scoped locator of what to poll.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// How often to poll.
    #[must_use]
    pub fn cadence(&self) -> Duration {
        self.cadence
    }
}

/// One resolved poll: the need plus the provider the plan selected for it,
/// already spelled as the wire `Origin` every batch and status will carry.
#[derive(Clone, Debug)]
pub struct PlannedPoll {
    endpoint: EndpointContext,
    target: String,
    origin: Origin,
    cadence: Duration,
    cost: u64,
}

impl PlannedPoll {
    /// The endpoint the poll runs against.
    #[must_use]
    pub fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    /// The protocol-scoped locator to poll.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// The selected provider's identity.
    #[must_use]
    pub fn origin(&self) -> &Origin {
        &self.origin
    }

    /// How often the poll runs.
    #[must_use]
    pub fn cadence(&self) -> Duration {
        self.cadence
    }

    /// The declared request weight, in dispatcher token units.
    #[must_use]
    pub fn cost(&self) -> u64 {
        self.cost
    }
}

/// One thing the embedder wants streamed: one endpoint's subscription,
/// served by the provider declaring its request class. What is subscribed
/// to is the provider's business — a Redfish event stream is named by the
/// service root, a gNMI subscription by its paths — so a stream need
/// carries no target.
#[derive(Clone, Debug)]
pub struct StreamNeed {
    endpoint: EndpointContext,
    request_class: String,
}

impl StreamNeed {
    /// A need for `endpoint`'s stream from the provider declaring
    /// `request_class`.
    #[must_use]
    pub fn new(endpoint: EndpointContext, request_class: impl Into<String>) -> Self {
        Self {
            endpoint,
            request_class: request_class.into(),
        }
    }

    /// The endpoint to subscribe to.
    #[must_use]
    pub fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    /// The request class of the provider that serves this need.
    #[must_use]
    pub fn request_class(&self) -> &str {
        &self.request_class
    }
}

/// One resolved stream: the need plus the provider the plan selected for
/// it, already spelled as the wire `Origin` every batch and status will
/// carry.
#[derive(Clone, Debug)]
pub struct PlannedStream {
    endpoint: EndpointContext,
    origin: Origin,
    cost: u64,
}

impl PlannedStream {
    /// The endpoint the stream is opened against.
    #[must_use]
    pub fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    /// The selected provider's identity.
    #[must_use]
    pub fn origin(&self) -> &Origin {
        &self.origin
    }

    /// The declared weight of one connect attempt, in dispatcher token
    /// units.
    #[must_use]
    pub fn cost(&self) -> u64 {
        self.cost
    }
}

/// What the embedder wants: its polls and its streams, each resolved
/// against the declarations of its mode.
#[derive(Clone, Debug, Default)]
pub struct Needs {
    polls: Vec<PollNeed>,
    streams: Vec<StreamNeed>,
}

impl Needs {
    /// Polls to plan, in the order they should appear in the plan.
    #[must_use]
    pub fn with_polls(mut self, polls: impl IntoIterator<Item = PollNeed>) -> Self {
        self.polls.extend(polls);
        self
    }

    /// Streams to plan, in the order they should appear in the plan.
    #[must_use]
    pub fn with_streams(mut self, streams: impl IntoIterator<Item = StreamNeed>) -> Self {
        self.streams.extend(streams);
        self
    }

    /// The polls wanted.
    #[must_use]
    pub fn polls(&self) -> &[PollNeed] {
        &self.polls
    }

    /// The streams wanted.
    #[must_use]
    pub fn streams(&self) -> &[StreamNeed] {
        &self.streams
    }
}

/// The resolved plan: every need, served.
#[derive(Clone, Debug)]
pub struct Plan {
    polls: Vec<PlannedPoll>,
    streams: Vec<PlannedStream>,
}

impl Plan {
    /// The planned polls, in need order.
    #[must_use]
    pub fn polls(&self) -> &[PlannedPoll] {
        &self.polls
    }

    /// The planned streams, in need order.
    #[must_use]
    pub fn streams(&self) -> &[PlannedStream] {
        &self.streams
    }
}

/// What one declaration offers a need of its mode and request class.
struct Offer {
    origin: Origin,
    cost: u64,
}

fn mode_word(mode: AcquisitionMode) -> &'static str {
    match mode {
        AcquisitionMode::Polled => "polled",
        AcquisitionMode::Streamed => "streamed",
        _ => "declared",
    }
}

/// The longest cadence a plan accepts. Anything slower is a configuration
/// error: instant arithmetic near `Duration::MAX` would silently turn
/// "poll every N" into "poll once, then never again".
const MAX_CADENCE: Duration = Duration::from_hours(366 * 24);

/// Why a plan could not be produced.
#[derive(Debug)]
pub enum PlanError {
    /// No declaration of the need's mode serves its request class.
    NoProviderFor {
        /// The class the need asked for.
        request_class: String,
        /// The mode the need asked for.
        mode: AcquisitionMode,
    },
    /// Two declarations of one mode claim one request class; provider
    /// preference does not exist yet, so this is a loud configuration
    /// error rather than a silent list-order coin toss.
    AmbiguousProviders {
        /// The doubly-claimed class.
        request_class: String,
        /// The mode both declare.
        mode: AcquisitionMode,
    },
    /// A declaration's identity cannot form a wire `Origin`.
    InvalidDeclaration {
        /// The declared provider name, as far as it could be read.
        provider: String,
        /// What the origin rejected.
        error: Invalid,
    },
    /// A declaration's request cost is zero, which would disable rate
    /// limiting entirely.
    ZeroCost {
        /// The declared provider name.
        provider: String,
    },
    /// A need's cadence is zero or beyond the year-long maximum.
    InvalidCadence {
        /// The need's target.
        target: String,
    },
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoProviderFor {
                request_class,
                mode,
            } => write!(
                f,
                "no {} declaration serves request class `{request_class}`",
                mode_word(*mode)
            ),
            Self::AmbiguousProviders {
                request_class,
                mode,
            } => write!(
                f,
                "more than one {} declaration serves request class \
                 `{request_class}`; provider preference does not exist yet",
                mode_word(*mode)
            ),
            Self::InvalidDeclaration { provider, error } => write!(
                f,
                "declaration for `{provider}` cannot form a wire origin: {error}"
            ),
            Self::ZeroCost { provider } => write!(
                f,
                "declaration for `{provider}` costs zero, which would disable \
                 rate limiting"
            ),
            Self::InvalidCadence { target } => write!(
                f,
                "cadence for `{target}` must be positive and at most a year"
            ),
        }
    }
}

impl std::error::Error for PlanError {}

/// Resolves needs against declarations: exactly one declaration of the
/// need's mode serves each need's request class, so one class may be
/// offered polled by one provider and streamed by another.
///
/// Every declaration is validated whether or not a need references it. No
/// needs is an empty plan, not an error.
///
/// # Errors
///
/// [`PlanError::NoProviderFor`] when nothing of the need's mode serves its
/// class; [`PlanError::AmbiguousProviders`] when two declarations of one
/// mode claim one class — loud until provider preference exists;
/// [`PlanError::InvalidDeclaration`] and [`PlanError::ZeroCost`] when a
/// declaration cannot be honored; and [`PlanError::InvalidCadence`] for a
/// zero or beyond-a-year cadence.
pub fn plan(needs: Needs, declarations: &[ProviderDeclaration]) -> Result<Plan, PlanError> {
    let Needs { polls, streams } = needs;
    let mut polled: HashMap<&str, Offer> = HashMap::new();
    let mut streamed: HashMap<&str, Offer> = HashMap::new();
    for declaration in declarations {
        let offers = match declaration.mode() {
            AcquisitionMode::Polled => &mut polled,
            AcquisitionMode::Streamed => &mut streamed,
            // A mode this build does not know serves no need it can plan.
            _ => continue,
        };
        if offers
            .insert(declaration.request_class(), offer(declaration)?)
            .is_some()
        {
            return Err(PlanError::AmbiguousProviders {
                request_class: declaration.request_class().to_owned(),
                mode: declaration.mode(),
            });
        }
    }

    let polls = polls
        .into_iter()
        .map(|need| {
            let offer = polled.get(need.request_class.as_str()).ok_or_else(|| {
                PlanError::NoProviderFor {
                    request_class: need.request_class.clone(),
                    mode: AcquisitionMode::Polled,
                }
            })?;
            if need.cadence.is_zero() || need.cadence > MAX_CADENCE {
                return Err(PlanError::InvalidCadence {
                    target: need.target,
                });
            }
            Ok(PlannedPoll {
                endpoint: need.endpoint,
                target: need.target,
                origin: offer.origin.clone(),
                cadence: need.cadence,
                cost: offer.cost,
            })
        })
        .collect::<Result<_, _>>()?;
    let streams = streams
        .into_iter()
        .map(|need| {
            let offer = streamed.get(need.request_class.as_str()).ok_or_else(|| {
                PlanError::NoProviderFor {
                    request_class: need.request_class.clone(),
                    mode: AcquisitionMode::Streamed,
                }
            })?;
            Ok(PlannedStream {
                endpoint: need.endpoint,
                origin: offer.origin.clone(),
                cost: offer.cost,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(Plan { polls, streams })
}

/// Validates one declaration into what it offers.
fn offer(declaration: &ProviderDeclaration) -> Result<Offer, PlanError> {
    if declaration.cost() == 0 {
        return Err(PlanError::ZeroCost {
            provider: declaration.provider().to_owned(),
        });
    }
    let origin = Origin::builder()
        .provider(declaration.provider())
        .request_class(declaration.request_class())
        .build()
        .map_err(|error| PlanError::InvalidDeclaration {
            provider: declaration.provider().to_owned(),
            error,
        })?;
    Ok(Offer {
        origin,
        cost: declaration.cost(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint() -> EndpointContext {
        EndpointContext::builder()
            .endpoint_id("bmc-lab-07")
            .build()
            .expect("a valid endpoint")
    }

    #[test]
    fn the_matching_declaration_serves_each_need() {
        let declarations = [
            ProviderDeclaration::polled("redfish.sensor.odata", "sensor-read", 1),
            ProviderDeclaration::polled("redfish.chassis.odata", "chassis-read", 2),
        ];
        let needs = vec![
            PollNeed::new(
                endpoint(),
                "sensor-read",
                "/redfish/v1/Chassis/1U/Sensors/S1",
                Duration::from_secs(30),
            ),
            PollNeed::new(
                endpoint(),
                "chassis-read",
                "/redfish/v1/Chassis/1U",
                Duration::from_mins(1),
            ),
        ];

        let plan = plan(Needs::default().with_polls(needs), &declarations)
            .expect("both classes are served");
        assert_eq!(plan.polls().len(), 2);
        assert_eq!(plan.polls()[0].origin().provider(), "redfish.sensor.odata");
        assert_eq!(plan.polls()[0].cost(), 1);
        assert_eq!(plan.polls()[1].origin().provider(), "redfish.chassis.odata");
        assert_eq!(plan.polls()[1].origin().request_class(), "chassis-read");
        assert_eq!(plan.polls()[1].cost(), 2);
        assert_eq!(plan.polls()[1].cadence(), Duration::from_mins(1));
    }

    #[test]
    fn a_need_without_a_provider_is_loud() {
        let declarations = [ProviderDeclaration::polled("p", "sensor-read", 1)];
        let needs = vec![PollNeed::new(
            endpoint(),
            "chassis-read",
            "/redfish/v1/Chassis/1U",
            Duration::from_secs(30),
        )];
        let error = plan(Needs::default().with_polls(needs), &declarations)
            .expect_err("nothing serves the class");
        assert!(
            matches!(&error, PlanError::NoProviderFor { request_class, .. } if request_class == "chassis-read")
        );
    }

    #[test]
    fn two_declarations_for_one_class_are_ambiguous_even_unreferenced() {
        let declarations = [
            ProviderDeclaration::polled("redfish.sensor.odata", "sensor-read", 1),
            ProviderDeclaration::polled("redfish.sensor.telemetry", "sensor-read", 2),
        ];
        let error =
            plan(Needs::default(), &declarations).expect_err("preference does not exist yet");
        assert!(
            matches!(&error, PlanError::AmbiguousProviders { request_class, .. } if request_class == "sensor-read")
        );
    }

    #[test]
    fn streamed_declarations_do_not_change_polled_resolution() {
        let declarations = [
            ProviderDeclaration::polled("redfish.sensor.odata", "sensor-read", 1),
            ProviderDeclaration::streamed("gnmi.dynamic", "sensor-read", 9),
        ];

        let needs = vec![PollNeed::new(
            endpoint(),
            "sensor-read",
            "/redfish/v1/Chassis/1U/Sensors/S1",
            Duration::from_secs(30),
        )];

        let plan = plan(Needs::default().with_polls(needs), &declarations)
            .expect("the polled provider serves the need");

        assert_eq!(plan.polls().len(), 1);
        assert_eq!(plan.polls()[0].origin().provider(), "redfish.sensor.odata");
        assert_eq!(plan.polls()[0].cost(), 1);
    }

    #[test]
    fn a_streamed_declaration_cannot_satisfy_a_poll_need() {
        let declarations = [ProviderDeclaration::streamed(
            "gnmi.dynamic",
            "sensor-read",
            1,
        )];

        let needs = vec![PollNeed::new(
            endpoint(),
            "sensor-read",
            "/interfaces/interface/state/counters",
            Duration::from_secs(30),
        )];

        let error = plan(Needs::default().with_polls(needs), &declarations)
            .expect_err("poll planning ignores streams");

        assert!(
            matches!(&error, PlanError::NoProviderFor { request_class, .. } if request_class == "sensor-read")
        );
    }

    #[test]
    fn a_stream_need_is_served_by_the_streamed_declaration_of_its_class() {
        let declarations = [
            ProviderDeclaration::polled("redfish.sensor.odata", "sensor-read", 1),
            ProviderDeclaration::streamed("redfish.event-service.sse", "event-stream", 3),
        ];
        let plan = plan(
            Needs::default().with_streams([StreamNeed::new(endpoint(), "event-stream")]),
            &declarations,
        )
        .expect("the streamed declaration serves the need");
        assert!(plan.polls().is_empty());
        assert_eq!(plan.streams().len(), 1);
        assert_eq!(plan.streams()[0].endpoint(), &endpoint());
        assert_eq!(
            plan.streams()[0].origin().provider(),
            "redfish.event-service.sse"
        );
        assert_eq!(plan.streams()[0].origin().request_class(), "event-stream");
        assert_eq!(plan.streams()[0].cost(), 3);
    }

    #[test]
    fn one_class_may_be_offered_polled_and_streamed_by_different_providers() {
        let declarations = [
            ProviderDeclaration::polled("redfish.sensor.odata", "sensor-read", 1),
            ProviderDeclaration::streamed("gnmi.dynamic", "sensor-read", 9),
        ];
        let plan = plan(
            Needs::default()
                .with_polls([PollNeed::new(
                    endpoint(),
                    "sensor-read",
                    "/redfish/v1/Chassis/1U/Sensors/S1",
                    Duration::from_secs(30),
                )])
                .with_streams([StreamNeed::new(endpoint(), "sensor-read")]),
            &declarations,
        )
        .expect("each mode has one provider");
        assert_eq!(plan.polls()[0].origin().provider(), "redfish.sensor.odata");
        assert_eq!(plan.streams()[0].origin().provider(), "gnmi.dynamic");
        assert_eq!(plan.streams()[0].cost(), 9);
    }

    #[test]
    fn a_stream_need_without_a_streamed_declaration_is_loud() {
        let declarations = [ProviderDeclaration::polled("p", "sensor-read", 1)];
        let error = plan(
            Needs::default().with_streams([StreamNeed::new(endpoint(), "sensor-read")]),
            &declarations,
        )
        .expect_err("a polled declaration cannot stream");
        assert!(matches!(
            &error,
            PlanError::NoProviderFor { request_class, mode: AcquisitionMode::Streamed }
                if request_class == "sensor-read"
        ));
        assert!(error.to_string().contains("no streamed declaration"));
    }

    #[test]
    fn two_streamed_declarations_for_one_class_are_ambiguous() {
        let declarations = [
            ProviderDeclaration::streamed("a", "event-stream", 1),
            ProviderDeclaration::streamed("b", "event-stream", 1),
        ];
        let error =
            plan(Needs::default(), &declarations).expect_err("preference does not exist yet");
        assert!(matches!(
            &error,
            PlanError::AmbiguousProviders {
                mode: AcquisitionMode::Streamed,
                ..
            }
        ));
    }

    #[test]
    fn no_needs_is_an_empty_plan() {
        let plan = plan(Needs::default(), &[]).expect("nothing to serve is not an error");
        assert!(plan.polls().is_empty());
        assert!(plan.streams().is_empty());
    }

    #[test]
    fn an_invalid_declaration_fails_at_plan_time() {
        let declarations = [ProviderDeclaration::polled("", "sensor-read", 1)];
        let error =
            plan(Needs::default(), &declarations).expect_err("an empty provider is invalid");
        assert!(
            matches!(&error, PlanError::InvalidDeclaration { provider, .. } if provider.is_empty())
        );

        let declarations = [ProviderDeclaration::polled("p", "c", 0)];
        let error = plan(Needs::default(), &declarations).expect_err("zero cost disables rating");
        assert!(matches!(error, PlanError::ZeroCost { .. }));
    }

    #[test]
    fn a_zero_or_unbounded_cadence_fails_at_plan_time() {
        let declarations = [ProviderDeclaration::polled("p", "c", 1)];
        for cadence in [Duration::ZERO, Duration::MAX] {
            let needs = vec![PollNeed::new(endpoint(), "c", "/redfish/v1/S/1", cadence)];
            let error = plan(Needs::default().with_polls(needs), &declarations)
                .expect_err("an unusable cadence is refused");
            assert!(matches!(error, PlanError::InvalidCadence { .. }));
        }
    }
}
