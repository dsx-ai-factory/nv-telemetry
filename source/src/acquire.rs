// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The acquisition unit: one admitted piece of protocol work.

use std::future::Future;

use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Timestamp;

use crate::Acquired;
use crate::AcquisitionFailure;
use crate::AcquisitionFailureClass;
use crate::AcquisitionParts;

/// One admitted unit of protocol work against one endpoint.
///
/// The planner decides what runs and the dispatcher decides when; a unit
/// only knows *how*: build the request, parse the response into the shared
/// observation model, classify the failure. It chooses no cadence, performs
/// no retries or backoff, publishes to no sink, derives no health, and
/// updates no external state.
///
/// Identity is readable without running the unit, so an admission the
/// dispatcher refuses can still be reported against the right endpoint and
/// origin — which is also why sources never build
/// [`AcquisitionStatus`](nv_telemetry_model::AcquisitionStatus) themselves:
/// the refused unit never ran, and timing is only observable where the
/// future is polled. Sources contribute identity and the classified outcome;
/// orchestration stamps the status.
///
/// The endpoint and origin must remain stable for the admitted unit's entire
/// lifetime. Streamed callers read the same identity for every notification
/// and terminal failure in one stream instance.
///
/// Deliberately not object-safe: the planner plans against declarations,
/// which are plain data, and a caller needing uniform task leaves writes its
/// own erasure adapter — trivial to add outside the contract, impossible to
/// remove from it.
pub trait Acquire {
    /// What the provider hook yields: [`AcquisitionParts`] for collection
    /// units, [`Capability`] for probes. Later acquisition stages seat their
    /// private artifacts here without those artifacts entering the data
    /// plane.
    type Output;

    /// The endpoint this unit is bound to, exactly as its batches carry it.
    fn endpoint(&self) -> &EndpointContext;

    /// Provider identity and request class, exactly as batches and status
    /// carry them — the strings dispatcher lanes and breakers are keyed by.
    fn origin(&self) -> &Origin;

    /// Provider hook for the single acquisition this unit exists for.
    ///
    /// Collection implementations return envelope-free payloads and issues;
    /// callers use [`acquire`] to stamp them with the admitted unit's identity
    /// and collection instant. The hook receives no timestamp and therefore
    /// cannot substitute one. Cancellation is dropping the future.
    fn perform(&self) -> impl Future<Output = Result<Self::Output, AcquisitionFailure>> + Send;
}

/// Executes a collection unit and stamps its successful output.
///
/// This is the non-overridable collection boundary: endpoint and origin are
/// captured from the admitted unit, and `at` comes directly from its caller.
/// A provider can produce only envelope-free [`AcquisitionParts`], so it
/// cannot attribute batches independently or replace the collection instant.
///
/// # Errors
///
/// Returns the provider's classified failure, or `Internal` if already
/// validated acquisition parts cannot form their model envelopes.
pub async fn acquire<A>(acquisition: &A, at: Timestamp) -> Result<Acquired, AcquisitionFailure>
where
    A: Acquire<Output = AcquisitionParts> + ?Sized,
{
    // Capture scheduling identity before polling. The same values are what a
    // dispatcher can read for admission/status, and the provider hook never
    // receives an alternate envelope to mutate.
    let endpoint = acquisition.endpoint().clone();
    let origin = acquisition.origin().clone();
    let parts = acquisition.perform().await?;
    stamp(&endpoint, &origin, at, parts)
}

/// Stamps envelope-free parts with the identity and instant that own them.
/// The non-overridable boundary shared by [`acquire`] (once per polled
/// request) and [`crate::stamp_item`] (once per streamed item).
///
/// # Errors
///
/// Returns `Internal` if already validated acquisition parts cannot form
/// their model envelopes.
pub(crate) fn stamp(
    endpoint: &EndpointContext,
    origin: &Origin,
    at: Timestamp,
    parts: AcquisitionParts,
) -> Result<Acquired, AcquisitionFailure> {
    Acquired::from_parts(endpoint, origin, at, parts).map_err(|error| envelope_bug(&error))
}

fn envelope_bug(error: &Invalid) -> AcquisitionFailure {
    AcquisitionFailure::new(AcquisitionFailureClass::Internal)
        .with_retryable(false)
        .with_detail(format!("acquisition envelope bug: {error}"))
}

/// What a capability probe learns about one provider on one endpoint.
///
/// A probe that could not reach the device learned nothing: that is an
/// [`AcquisitionFailure`], never `Unsupported`, so capability stays unknown
/// and the need stays unresolved rather than wrongly ruled out.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Capability {
    /// Requests of this provider's classes are expected to succeed here.
    Supported,
    /// The endpoint answered and the answer rules the provider out.
    Unsupported {
        /// Why, kept so a resolved plan can explain the rejection.
        reason: String,
    },
}
