// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Status assembly and the failure-class policy tables.
//!
//! `AcquisitionStatus` is built here, never by a source: a refused
//! admission must produce a status for a unit that never ran, timing is
//! only observable where the future is polled, and a source that cannot
//! author its own status cannot misreport one.
//!
//! The assembly functions are infallible because every input is
//! pre-validated: identity comes from types the model already accepted,
//! and failure detail is bounded by the source crate to the status
//! schema's byte limit. A pin test holds the limits equalities those
//! `expect`s rest on.

use std::time::Duration;

use nv_telemetry_model::AcquisitionStatus;
use nv_telemetry_model::AcquisitionStatusBuilder;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Outcome;
use nv_telemetry_model::Timestamp;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;

/// The class's policy-default retryability, applied when the source left
/// [`AcquisitionFailure::retryable`] unrefined. A source's explicit value
/// always wins — a 401 and a 403 classify alike but retry differently.
#[must_use]
pub fn default_retryable(class: AcquisitionFailureClass) -> bool {
    match class {
        // The device may come back; the request was never judged.
        AcquisitionFailureClass::Connectivity
        | AcquisitionFailureClass::Timeout
        | AcquisitionFailureClass::Device => true,
        // Authentication, Protocol, Unsupported, Internal: repeating the
        // same request changes nothing — the credentials, the request
        // shape, the capability, or our own code is wrong. A class this
        // non-exhaustive enum grows later lands here too: not retried
        // until someone judges it.
        _ => false,
    }
}

/// Whether the class is an endpoint-scoped fact the endpoint breaker
/// samples: the source crate's own line, so a provider walking a collection
/// and the breaker scope agree on which failures indict the endpoint.
#[must_use]
pub const fn trips_endpoint_breaker(class: AcquisitionFailureClass) -> bool {
    class.is_endpoint_scoped()
}

/// The status a completed, successful acquisition earns. `retryable` and
/// `failure_class` stay absent: success carries no retry question, and the
/// wrapper rule forbids a class without a failure.
///
/// # Panics
///
/// Never in practice: the identity fields come from already-validated
/// types whose bounds equal the status's — the limits pin test below holds
/// that equality.
#[must_use]
pub fn success_status(
    endpoint: &EndpointContext,
    origin: &Origin,
    at: Timestamp,
    duration: Duration,
) -> AcquisitionStatus {
    base(endpoint, origin, at)
        .outcome(Outcome::Succeeded)
        .duration_nanos(nanos(duration))
        .build()
        .expect("pre-validated identity forms a valid status")
}

/// The status a completed, failed acquisition earns. The stamped
/// `retryable` is the resolved value: the source's refinement when it made
/// one, the class's policy default otherwise — so consumers always see the
/// policy applied, never the open question.
///
/// # Panics
///
/// Never in practice: identity bounds are held by the limits pin test
/// below, and the failure's detail is bounded by the source crate to the
/// status schema's byte limit.
#[must_use]
pub fn failed_status(
    endpoint: &EndpointContext,
    origin: &Origin,
    at: Timestamp,
    duration: Option<Duration>,
    failure: &AcquisitionFailure,
) -> AcquisitionStatus {
    let retryable = failure
        .retryable()
        .unwrap_or_else(|| default_retryable(failure.class()));
    let mut builder = base(endpoint, origin, at)
        .outcome(Outcome::Failed)
        .failure_class(failure.class().into())
        .retryable(retryable);
    if let Some(duration) = duration {
        builder = builder.duration_nanos(nanos(duration));
    }
    if let Some(detail) = failure.detail() {
        builder = builder.detail(detail);
    }
    builder
        .build()
        .expect("pre-validated identity and bounded detail form a valid status")
}

/// The status for a unit refused admission: it never ran, so there is no
/// duration — the absence is the wire's spelling of "never ran".
#[must_use]
pub fn refused_status(
    endpoint: &EndpointContext,
    origin: &Origin,
    at: Timestamp,
    reason: &str,
) -> AcquisitionStatus {
    // Routed through AcquisitionFailure so the reason shares the one
    // detail-bounding implementation. The schema has no refusal class;
    // Internal is the honest nearest fact — the collector, not the device,
    // declined the work.
    let failure = AcquisitionFailure::new(AcquisitionFailureClass::Internal)
        .with_retryable(true)
        .with_detail(format!("admission refused: {reason}"));
    failed_status(endpoint, origin, at, None, &failure)
}

fn base(endpoint: &EndpointContext, origin: &Origin, at: Timestamp) -> AcquisitionStatusBuilder {
    AcquisitionStatus::builder()
        .endpoint_id(endpoint.endpoint_id())
        .provider(origin.provider())
        .request_class(origin.request_class())
        .started_at(at)
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use nv_telemetry_model::limits;
    use nv_telemetry_model::FailureClass;

    use super::*;

    fn endpoint() -> EndpointContext {
        EndpointContext::builder()
            .endpoint_id("bmc-lab-07")
            .build()
            .expect("a valid endpoint")
    }

    fn origin() -> Origin {
        Origin::builder()
            .provider("redfish.sensor.odata")
            .request_class("sensor-read")
            .build()
            .expect("a valid origin")
    }

    fn at() -> Timestamp {
        Timestamp::new(1_785_621_243, 0).expect("a valid instant")
    }

    #[test]
    fn the_status_limits_admit_every_validated_identity() {
        // The assembly `expect`s rest on these equalities: any identity the
        // context and origin accepted also satisfies the status's bounds.
        assert_eq!(
            limits::ACQUISITIONSTATUS_ENDPOINT_ID_MAX_LEN,
            limits::ENDPOINTCONTEXT_ENDPOINT_ID_MAX_LEN
        );
        assert_eq!(
            limits::ACQUISITIONSTATUS_PROVIDER_MAX_LEN,
            limits::ORIGIN_PROVIDER_MAX_LEN
        );
        assert_eq!(
            limits::ACQUISITIONSTATUS_REQUEST_CLASS_MAX_LEN,
            limits::ORIGIN_REQUEST_CLASS_MAX_LEN
        );
    }

    #[test]
    fn a_success_carries_duration_and_no_retry_question() {
        let status = success_status(&endpoint(), &origin(), at(), Duration::from_secs(2));
        assert_eq!(status.outcome(), Outcome::Succeeded);
        assert_eq!(status.duration_nanos(), Some(2_000_000_000));
        assert_eq!(status.failure_class(), None);
        assert_eq!(status.retryable(), None);
        assert_eq!(status.endpoint_id(), "bmc-lab-07");
        assert_eq!(status.provider(), "redfish.sensor.odata");
        assert_eq!(status.request_class(), "sensor-read");
        assert_eq!(status.started_at(), &at());
    }

    #[test]
    fn a_failure_resolves_retryable_and_copies_the_classified_facts() {
        // Unrefined: the class's policy default is stamped.
        let failure = AcquisitionFailure::new(AcquisitionFailureClass::Connectivity)
            .with_detail("connection refused");
        let status = failed_status(
            &endpoint(),
            &origin(),
            at(),
            Some(Duration::from_millis(90)),
            &failure,
        );
        assert_eq!(status.outcome(), Outcome::Failed);
        assert_eq!(status.failure_class(), Some(FailureClass::Connectivity));
        assert_eq!(status.retryable(), Some(true));
        assert_eq!(status.detail(), Some("connection refused"));
        assert_eq!(status.duration_nanos(), Some(90_000_000));

        // Refined: the source's judgment wins over the default.
        let refined =
            AcquisitionFailure::new(AcquisitionFailureClass::Protocol).with_retryable(true);
        let status = failed_status(&endpoint(), &origin(), at(), None, &refined);
        assert_eq!(status.retryable(), Some(true));
        assert_eq!(status.detail(), None);
        assert_eq!(status.duration_nanos(), None);
    }

    #[test]
    fn a_refusal_never_ran_so_it_has_no_duration() {
        let status = refused_status(&endpoint(), &origin(), at(), "endpoint queue full");
        assert_eq!(status.outcome(), Outcome::Failed);
        assert_eq!(status.failure_class(), Some(FailureClass::Internal));
        assert_eq!(status.retryable(), Some(true));
        assert_eq!(status.duration_nanos(), None);
        assert_eq!(
            status.detail(),
            Some("admission refused: endpoint queue full")
        );
    }

    #[test]
    fn the_policy_tables_cover_every_class() {
        let rows = [
            (AcquisitionFailureClass::Connectivity, true, true),
            (AcquisitionFailureClass::Authentication, false, true),
            (AcquisitionFailureClass::Timeout, true, true),
            (AcquisitionFailureClass::Protocol, false, false),
            (AcquisitionFailureClass::Unsupported, false, false),
            (AcquisitionFailureClass::Device, true, false),
            (AcquisitionFailureClass::Internal, false, false),
        ];
        for (class, retryable, trips) in rows {
            assert_eq!(default_retryable(class), retryable, "{class:?}");
            assert_eq!(trips_endpoint_breaker(class), trips, "{class:?}");
        }
    }
}
