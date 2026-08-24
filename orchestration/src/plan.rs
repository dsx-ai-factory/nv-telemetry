// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The static planner: needs in, planned polls out.
//!
//! Selection is deterministic and explainable — exactly one polled
//! declaration serves each need's request class — and validation is loud
//! at plan time: a declaration whose identity cannot form a wire `Origin`
//! fails here, never at status-build time. Capability probing, provider
//! preference, and demotion arrive with later milestones; a plan produced
//! here is complete because nothing in it can be unresolved yet.

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

/// The resolved plan: every need, served.
#[derive(Clone, Debug)]
pub struct Plan {
    polls: Vec<PlannedPoll>,
}

impl Plan {
    /// The planned polls, in need order.
    #[must_use]
    pub fn polls(&self) -> &[PlannedPoll] {
        &self.polls
    }
}

/// The longest cadence a plan accepts. Anything slower is a configuration
/// error: instant arithmetic near `Duration::MAX` would silently turn
/// "poll every N" into "poll once, then never again".
const MAX_CADENCE: Duration = Duration::from_hours(366 * 24);

/// Why a plan could not be produced.
#[derive(Debug)]
pub enum PlanError {
    /// No polled declaration serves a need's request class.
    NoProviderFor {
        /// The class the need asked for.
        request_class: String,
    },
    /// Two polled declarations claim one request class; provider
    /// preference does not exist yet, so this is a loud configuration
    /// error rather than a silent list-order coin toss.
    AmbiguousProviders {
        /// The doubly-claimed class.
        request_class: String,
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
            Self::NoProviderFor { request_class } => write!(
                f,
                "no polled declaration serves request class `{request_class}`"
            ),
            Self::AmbiguousProviders { request_class } => write!(
                f,
                "more than one polled declaration serves request class \
                 `{request_class}`; provider preference does not exist yet"
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

/// Resolves needs against declarations: exactly one polled declaration
/// serves each need's request class.
///
/// Every declaration is validated whether or not a need references it —
/// a broken declaration is a configuration error worth hearing about at
/// plan time, not when a need eventually arrives. No needs is an empty
/// plan, not an error.
///
/// # Errors
///
/// [`PlanError::NoProviderFor`] when nothing serves a need's class;
/// [`PlanError::AmbiguousProviders`] when two declarations claim one class
/// — loud until provider preference exists;
/// [`PlanError::InvalidDeclaration`] and [`PlanError::ZeroCost`] when a
/// declaration cannot be honored; and
/// [`PlanError::InvalidCadence`] for a zero or beyond-a-year cadence.
pub fn plan(needs: Vec<PollNeed>, declarations: &[ProviderDeclaration]) -> Result<Plan, PlanError> {
    let mut offers: HashMap<&str, (Origin, u64)> = HashMap::new();
    for declaration in declarations {
        if declaration.mode() != AcquisitionMode::Polled {
            continue;
        }
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
        if offers
            .insert(declaration.request_class(), (origin, declaration.cost()))
            .is_some()
        {
            return Err(PlanError::AmbiguousProviders {
                request_class: declaration.request_class().to_owned(),
            });
        }
    }

    let polls = needs
        .into_iter()
        .map(|need| {
            let (origin, cost) = offers.get(need.request_class.as_str()).ok_or_else(|| {
                PlanError::NoProviderFor {
                    request_class: need.request_class.clone(),
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
                origin: origin.clone(),
                cadence: need.cadence,
                cost: *cost,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(Plan { polls })
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

        let plan = plan(needs, &declarations).expect("both classes are served");
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
        let error = plan(needs, &declarations).expect_err("nothing serves the class");
        assert!(
            matches!(&error, PlanError::NoProviderFor { request_class } if request_class == "chassis-read")
        );
    }

    #[test]
    fn two_declarations_for_one_class_are_ambiguous_even_unreferenced() {
        let declarations = [
            ProviderDeclaration::polled("redfish.sensor.odata", "sensor-read", 1),
            ProviderDeclaration::polled("redfish.sensor.telemetry", "sensor-read", 2),
        ];
        let error = plan(Vec::new(), &declarations).expect_err("preference does not exist yet");
        assert!(
            matches!(&error, PlanError::AmbiguousProviders { request_class } if request_class == "sensor-read")
        );
    }

    #[test]
    fn no_needs_is_an_empty_plan() {
        let plan = plan(Vec::new(), &[]).expect("nothing to serve is not an error");
        assert!(plan.polls().is_empty());
    }

    #[test]
    fn an_invalid_declaration_fails_at_plan_time() {
        let declarations = [ProviderDeclaration::polled("", "sensor-read", 1)];
        let error = plan(Vec::new(), &declarations).expect_err("an empty provider is invalid");
        assert!(
            matches!(&error, PlanError::InvalidDeclaration { provider, .. } if provider.is_empty())
        );

        let declarations = [ProviderDeclaration::polled("p", "c", 0)];
        let error = plan(Vec::new(), &declarations).expect_err("zero cost disables rating");
        assert!(matches!(error, PlanError::ZeroCost { .. }));
    }

    #[test]
    fn a_zero_or_unbounded_cadence_fails_at_plan_time() {
        let declarations = [ProviderDeclaration::polled("p", "c", 1)];
        for cadence in [Duration::ZERO, Duration::MAX] {
            let needs = vec![PollNeed::new(endpoint(), "c", "/redfish/v1/S/1", cadence)];
            let error = plan(needs, &declarations).expect_err("an unusable cadence is refused");
            assert!(matches!(error, PlanError::InvalidCadence { .. }));
        }
    }
}
