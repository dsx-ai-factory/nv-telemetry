// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What one acquisition produced, paired and stamped: the report the
//! driver fans out, and the leaf future that yields it.
//!
//! Every acquisition yields exactly one [`AcquisitionStatus`], and the
//! report type makes anything else unrepresentable. Which channel carries
//! it encodes breaker scope: an endpoint-scoped failure travels as
//! [`EndpointFault`] on the error side, so the dispatcher's endpoint
//! breaker — which sees only success or failure — counts exactly the
//! classes that indict the endpoint as failures. Every other outcome,
//! including request-scoped failures, travels as a successful completion
//! carrying its own status — and registers as a *success* sample in the
//! breaker's window deliberately: a `Protocol`, `Unsupported`, or `Device`
//! failure is an answer from the endpoint, so it is evidence of the
//! reachability the breaker samples, and an endpoint mixing connectivity
//! faults with such answers rightly trips later than one that never
//! answers. `Internal` — our own bug — says nothing about the endpoint and
//! rides the same channel; that imprecision is accepted. Both channels
//! reach the driver; no status is ever lost to scheduling policy.

use std::sync::Arc;
use std::time::Duration;

use nv_telemetry_model::AcquisitionStatus;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::ObservationBatch;
use nv_telemetry_model::Origin;
use nv_telemetry_model::ProjectionIssues;
use nv_telemetry_model::Timestamp;
use nv_telemetry_source::acquire;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::Acquired;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;

use crate::clock::Clock;
use crate::status::failed_status;
use crate::status::success_status;
use crate::status::trips_endpoint_breaker;

/// Everything one completed acquisition produced: its batches, its issues
/// envelope when any source field failed to project, and exactly one
/// status — all stamped with the same admitted identity and instant.
#[derive(Clone, Debug)]
pub struct AcquisitionReport {
    batches: Vec<ObservationBatch>,
    issues: Option<ProjectionIssues>,
    status: AcquisitionStatus,
}

impl AcquisitionReport {
    /// The validated batches, empty when the acquisition failed.
    #[must_use]
    pub fn batches(&self) -> &[ObservationBatch] {
        &self.batches
    }

    /// The issues envelope, present only when the acquisition produced
    /// issues — an acquisition without issues emits no envelope at all.
    #[must_use]
    pub fn issues(&self) -> Option<&ProjectionIssues> {
        self.issues.as_ref()
    }

    /// The one status this acquisition earned.
    #[must_use]
    pub fn status(&self) -> &AcquisitionStatus {
        &self.status
    }

    /// Consumes into the three streams' items.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<ObservationBatch>,
        Option<ProjectionIssues>,
        AcquisitionStatus,
    ) {
        (self.batches, self.issues, self.status)
    }
}

/// An endpoint-scoped failure, carrying the status it earned. Rides the
/// error channel so the endpoint breaker samples it; the driver still
/// receives it as a completed work output. Boxed because the error arm
/// travels through every completion the dispatcher hands back.
#[derive(Clone, Debug)]
pub struct EndpointFault {
    status: Box<AcquisitionStatus>,
}

impl EndpointFault {
    /// The status the failed acquisition earned.
    #[must_use]
    pub fn status(&self) -> &AcquisitionStatus {
        &self.status
    }

    /// Consumes into the status stream's item.
    #[must_use]
    pub fn into_status(self) -> AcquisitionStatus {
        *self.status
    }
}

/// The dispatcher payload shape: one boxed acquisition per tick.
pub type TelemetryWork = nv_redfish_dispatcher::FutureWork<AcquisitionReport, EndpointFault>;

/// Assembles one acquisition's outcome into its report. Pure: the entire
/// status-and-issues doctrine, with no clock and no runtime, so every
/// branch is directly testable.
///
/// # Errors
///
/// An [`EndpointFault`] when the failure's class is endpoint-scoped — not
/// an error so much as the channel that lets the endpoint breaker sample
/// exactly those classes. The fault still carries the status the driver
/// reports.
pub fn assemble(
    endpoint: &EndpointContext,
    origin: &Origin,
    at: Timestamp,
    duration: Duration,
    outcome: Result<Acquired, AcquisitionFailure>,
) -> Result<AcquisitionReport, EndpointFault> {
    match outcome {
        Ok(acquired) => Ok(assemble_success(endpoint, origin, at, duration, acquired)),
        Err(failure) => {
            let status = failed_status(endpoint, origin, at, Some(duration), &failure);
            if trips_endpoint_breaker(failure.class()) {
                Err(EndpointFault {
                    status: Box::new(status),
                })
            } else {
                Ok(AcquisitionReport {
                    batches: Vec::new(),
                    issues: None,
                    status,
                })
            }
        }
    }
}

fn assemble_success(
    endpoint: &EndpointContext,
    origin: &Origin,
    at: Timestamp,
    duration: Duration,
    acquired: Acquired,
) -> AcquisitionReport {
    let (batches, issues) = acquired.into_parts();
    let issues = match issues_envelope(endpoint, origin, at, issues) {
        Ok(issues) => issues,
        // A failed envelope is a producer bug, and a failed request emits
        // no batch: everything the acquisition yielded is discarded for
        // one loud Internal status, mirroring `acquire`'s own envelope-bug
        // rule one layer down.
        Err(error) => {
            let failure = AcquisitionFailure::new(AcquisitionFailureClass::Internal)
                .with_detail(format!("issues envelope bug: {error}"));
            return AcquisitionReport {
                batches: Vec::new(),
                issues: None,
                status: failed_status(endpoint, origin, at, Some(duration), &failure),
            };
        }
    };
    AcquisitionReport {
        batches: batches.into_vec(),
        issues,
        status: success_status(endpoint, origin, at, duration),
    }
}

fn issues_envelope(
    endpoint: &EndpointContext,
    origin: &Origin,
    at: Timestamp,
    issues: Box<[nv_telemetry_source::ProjectionIssue]>,
) -> Result<Option<ProjectionIssues>, nv_telemetry_model::Invalid> {
    if issues.is_empty() {
        return Ok(None);
    }
    let issues = issues
        .into_vec()
        .into_iter()
        .map(nv_telemetry_source::ProjectionIssue::into_model)
        .collect::<Result<Vec<_>, _>>()?;
    ProjectionIssues::builder()
        .endpoint(endpoint.clone())
        .origin(origin.clone())
        .at(at)
        .issues(issues)
        .build()
        .map(Some)
}

/// The erasure adapter: one boxed acquisition future per tick, closing
/// over the shared unit and the clock. The admitted instant is sampled at
/// first poll — where the dispatcher dispatches — and duration is measured
/// monotonically around the acquisition alone.
pub fn poll_future<A, C>(unit: Arc<A>, clock: C) -> TelemetryWork
where
    A: Acquire<Output = AcquisitionParts> + Send + Sync + 'static,
    C: Clock + 'static,
{
    Box::pin(async move {
        let begun = clock.instant();
        let at = clock.timestamp();
        let outcome = acquire(unit.as_ref(), at).await;
        let duration = clock.instant().saturating_duration_since(begun);
        assemble(unit.endpoint(), unit.origin(), at, duration, outcome).map(|report| vec![report])
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use nv_telemetry_model::Completeness;
    use nv_telemetry_model::Coverage;
    use nv_telemetry_model::FailureClass;
    use nv_telemetry_model::Outcome;
    use nv_telemetry_model::Payload;
    use nv_telemetry_model::States;
    use nv_telemetry_source::ProjectionIssue;

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

    /// A unit whose acquisition yields the given parts. `Acquired` can only
    /// be produced through `acquire`, so tests route through the real
    /// stamping boundary.
    struct FixtureUnit {
        endpoint: EndpointContext,
        origin: Origin,
        parts: AcquisitionParts,
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

    fn states_payload() -> (Coverage, Payload) {
        let coverage = Coverage::builder()
            .completeness(Completeness::Partial)
            .build()
            .expect("valid coverage");
        let states = States::builder()
            .build()
            .expect("an empty states payload is valid");
        (coverage, Payload::States(states))
    }

    #[test]
    fn a_success_pairs_batches_issues_and_status_under_one_identity() {
        let parts = AcquisitionParts::new(
            vec![states_payload()],
            vec![ProjectionIssue::invalid("Reading", "not finite")],
        );
        let report = assemble(
            &endpoint(),
            &origin(),
            at(),
            Duration::from_millis(5),
            Ok(acquired(parts)),
        )
        .expect("a request-scoped outcome is a report");

        assert_eq!(report.batches().len(), 1);
        let issues = report.issues().expect("one issue yields an envelope");
        assert_eq!(issues.endpoint(), &endpoint());
        assert_eq!(issues.origin(), &origin());
        assert_eq!(issues.at(), &at());
        assert_eq!(issues.issues().len(), 1);
        assert_eq!(report.status().outcome(), Outcome::Succeeded);
        assert_eq!(report.status().started_at(), &at());
        assert_eq!(report.batches()[0].window().start(), &at());
    }

    #[test]
    fn no_issues_means_no_envelope() {
        let parts = AcquisitionParts::new(vec![states_payload()], Vec::new());
        let report = assemble(
            &endpoint(),
            &origin(),
            at(),
            Duration::ZERO,
            Ok(acquired(parts)),
        )
        .expect("a clean success is a report");
        assert!(report.issues().is_none());
    }

    #[test]
    fn an_envelope_bug_discards_everything_for_one_loud_status() {
        // Two issues sharing a path violate the envelope's identity rule:
        // the batches are dropped with them, because a failed request emits
        // no batch.
        let parts = AcquisitionParts::new(
            vec![states_payload()],
            vec![
                ProjectionIssue::missing("Id"),
                ProjectionIssue::invalid("Id", "also unusable"),
            ],
        );
        let report = assemble(
            &endpoint(),
            &origin(),
            at(),
            Duration::ZERO,
            Ok(acquired(parts)),
        )
        .expect("an internal fault is request-scoped");

        assert!(report.batches().is_empty());
        assert!(report.issues().is_none());
        assert_eq!(report.status().outcome(), Outcome::Failed);
        assert_eq!(
            report.status().failure_class(),
            Some(FailureClass::Internal)
        );
        let detail = report.status().detail().expect("the bug is named");
        assert!(detail.starts_with("issues envelope bug: "));
    }

    #[test]
    fn breaker_scope_decides_the_channel_and_no_status_is_lost() {
        let connectivity = AcquisitionFailure::new(AcquisitionFailureClass::Connectivity);
        let fault = assemble(
            &endpoint(),
            &origin(),
            at(),
            Duration::from_secs(1),
            Err(connectivity),
        )
        .expect_err("an endpoint-scoped failure rides the error channel");
        assert_eq!(
            fault.status().failure_class(),
            Some(FailureClass::Connectivity)
        );
        assert_eq!(fault.status().retryable(), Some(true));

        let unsupported = AcquisitionFailure::new(AcquisitionFailureClass::Unsupported);
        let report = assemble(
            &endpoint(),
            &origin(),
            at(),
            Duration::from_secs(1),
            Err(unsupported),
        )
        .expect("a request-scoped failure rides the success channel");
        assert!(report.batches().is_empty());
        assert_eq!(
            report.status().failure_class(),
            Some(FailureClass::Unsupported)
        );
        assert_eq!(report.status().retryable(), Some(false));
    }
}
