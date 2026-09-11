// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Failure classification: transport errors become the facts policy runs on.
//!
//! The mapping lives here, next to the transport that produces the errors,
//! because classification is each protocol crate's obligation — the contract
//! owns the class *semantics* (connectivity and authentication may trip the
//! endpoint breaker; unsupported and protocol failures scope to their
//! request class) and this module owns which Redfish failure is which.

use nv_telemetry_source::AcquisitionFailure;
#[cfg(any(feature = "bmc-http", test))]
use nv_telemetry_source::AcquisitionFailureClass;

/// Classifies a transport error into an [`AcquisitionFailure`].
///
/// Implemented for each `Bmc` error type this crate can run over, so the
/// provider stays generic over transports while classification stays
/// concrete and testable.
pub trait ClassifyError {
    /// The classified facts: class, retryability, operator-facing detail.
    fn classify(&self) -> AcquisitionFailure;
}

/// Classifies an HTTP status the endpoint answered with.
///
/// Kept pure so the table is testable without constructing transport
/// errors. The one policy-laden row: **404 is `Unsupported`, not `Device`**.
/// The right reaction to a planned URI vanishing is request-class demotion
/// and a catalog refresh — Unsupported's lane — while Device would invite
/// retries of a URI that will 404 forever. Detail deliberately carries only
/// classified facts: response bodies and transport displays are untrusted
/// and may contain credentials, query secrets, or device-sensitive data.
// Rows that share an outcome stay separate rows: each carries its own
// rationale, and merging them would hide which statuses were considered.
#[allow(clippy::match_same_arms)]
#[cfg(any(feature = "bmc-http", test))]
fn classify_status(status: u16) -> AcquisitionFailure {
    let (class, retryable) = match status {
        401 | 403 => (AcquisitionFailureClass::Authentication, false),
        404 | 405 | 406 | 410 | 501 => (AcquisitionFailureClass::Unsupported, false),
        // The device could not parse a plain GET: a device-quirk
        // conversation, scoped to the request class.
        400 => (AcquisitionFailureClass::Protocol, false),
        // The endpoint stated its own timeout.
        408 => (AcquisitionFailureClass::Timeout, true),
        // The endpoint self-reports overload; the dispatcher's rate policy
        // absorbs it, and a retry plausibly succeeds.
        429 => (AcquisitionFailureClass::Device, true),
        // A 304 surfacing here means the transport's cache should have
        // absorbed it and did not: our misuse, not the device's fault.
        304 => (AcquisitionFailureClass::Internal, false),
        500..=599 => (AcquisitionFailureClass::Device, true),
        _ => (AcquisitionFailureClass::Protocol, false),
    };
    AcquisitionFailure::new(class)
        .with_retryable(retryable)
        .with_detail(format!("HTTP {status}"))
}

#[cfg(feature = "bmc-http")]
mod http {
    use nv_redfish::bmc_http::reqwest::BmcError;
    use nv_telemetry_source::AcquisitionFailure;
    use nv_telemetry_source::AcquisitionFailureClass;

    use super::classify_status;
    use super::ClassifyError;

    impl ClassifyError for BmcError {
        // The match is exhaustive on purpose: a transport error the table
        // has not classified must fail this crate's build, not reach the
        // dispatcher unclassified. Arms that share an outcome stay separate
        // for the same reason the status rows do — each carries its own
        // rationale.
        #[allow(clippy::match_same_arms)]
        fn classify(&self) -> AcquisitionFailure {
            let with = |class, retryable: bool, detail: &'static str| {
                AcquisitionFailure::new(class)
                    .with_retryable(retryable)
                    .with_detail(detail)
            };
            match self {
                // A connect that timed out is still unreachability: the
                // endpoint-breaker case, so the connect check runs first.
                BmcError::ReqwestError(error) if error.is_connect() => with(
                    AcquisitionFailureClass::Connectivity,
                    true,
                    "Redfish endpoint connection failed",
                ),
                BmcError::ReqwestError(error) if error.is_timeout() => with(
                    AcquisitionFailureClass::Timeout,
                    true,
                    "Redfish request timed out",
                ),
                // The request never left this process.
                BmcError::ReqwestError(error) if error.is_builder() || error.is_request() => with(
                    AcquisitionFailureClass::Internal,
                    false,
                    "Redfish request construction failed",
                ),
                // A redirect loop is device misbehavior a retry repeats.
                BmcError::ReqwestError(error) if error.is_redirect() => with(
                    AcquisitionFailureClass::Protocol,
                    false,
                    "Redfish redirect handling failed",
                ),
                // Mid-body failures are plausibly transient.
                BmcError::ReqwestError(_) => with(
                    AcquisitionFailureClass::Protocol,
                    true,
                    "Redfish HTTP transfer failed",
                ),
                BmcError::InvalidResponse { status, .. } => classify_status(status.as_u16()),
                // The device answered; the answer did not parse. The class
                // remains the corpus-entry trigger without copying device
                // data into operator-facing detail.
                BmcError::JsonError(_) | BmcError::DecodeError(_) => with(
                    AcquisitionFailureClass::Protocol,
                    false,
                    "Redfish response did not match its schema",
                ),
                // Our serialization, construction, or cache bug.
                BmcError::EncodeError(_)
                | BmcError::InvalidRequest(_)
                | BmcError::CacheError(_) => with(
                    AcquisitionFailureClass::Internal,
                    false,
                    "Redfish client state is invalid",
                ),
                // Eviction after a 304: a retry re-fetches and succeeds, but
                // the cause is our cache sizing.
                BmcError::CacheMiss => with(
                    AcquisitionFailureClass::Internal,
                    true,
                    "Redfish cache entry was unavailable",
                ),
                // Event-stream rows, reachable once a stream provider exists
                // and pinned now against the mock's SSE service. None is
                // endpoint-scoped: a stream that misbehaves says nothing
                // about the endpoint's other request classes, so none may
                // trip the endpoint breaker. A frame or transfer the decoder
                // could not finish is retried by reopening; a frame above
                // the client's own bound comes back identical on resume, so
                // a retry cannot succeed until the bound changes.
                BmcError::SseStreamError(_) => with(
                    AcquisitionFailureClass::Protocol,
                    true,
                    "Redfish event stream could not be decoded",
                ),
                BmcError::SseEventTooLarge { .. } => with(
                    AcquisitionFailureClass::Protocol,
                    false,
                    "Redfish event exceeded the client's frame bound",
                ),
                // The device went quiet for longer than the client allows.
                // Heartbeat comments do not reset that clock, so an alive but
                // idle stream ends here too; the window is the client's
                // policy, not the endpoint's health.
                BmcError::SseIdleTimeout { .. } => with(
                    AcquisitionFailureClass::Protocol,
                    true,
                    "Redfish event stream idle beyond the client's window",
                ),
            }
        }
    }
}

#[cfg(any(feature = "bmc-mock", test))]
mod mock {
    use nv_telemetry_source::AcquisitionFailure;
    use nv_telemetry_source::AcquisitionFailureClass;

    use super::ClassifyError;

    impl ClassifyError for nv_redfish_bmc_mock::Error {
        /// A mock error is a test-harness failure, honestly `Internal`.
        fn classify(&self) -> AcquisitionFailure {
            AcquisitionFailure::new(AcquisitionFailureClass::Internal)
                .with_retryable(false)
                .with_detail("Redfish mock transport failed")
        }
    }
}

#[cfg(test)]
mod tests {
    use nv_telemetry_source::AcquisitionFailureClass;

    use super::classify_status;

    #[test]
    fn the_status_table_routes_policy() {
        let class = |status: u16| classify_status(status).class();
        let retry = |status: u16| classify_status(status).retryable();

        assert_eq!(class(401), AcquisitionFailureClass::Authentication);
        assert_eq!(class(403), AcquisitionFailureClass::Authentication);
        assert_eq!(
            retry(401),
            Some(false),
            "identical credentials cannot succeed"
        );

        // A vanished URI wants replan and catalog refresh, not retries.
        assert_eq!(class(404), AcquisitionFailureClass::Unsupported);
        assert_eq!(class(501), AcquisitionFailureClass::Unsupported);
        assert_eq!(retry(404), Some(false));

        assert_eq!(class(400), AcquisitionFailureClass::Protocol);
        assert_eq!(class(408), AcquisitionFailureClass::Timeout);
        assert_eq!(retry(408), Some(true));
        assert_eq!(class(429), AcquisitionFailureClass::Device);
        assert_eq!(retry(429), Some(true));
        assert_eq!(class(500), AcquisitionFailureClass::Device);
        assert_eq!(class(503), AcquisitionFailureClass::Device);
        assert_eq!(retry(503), Some(true));
        assert_eq!(class(304), AcquisitionFailureClass::Internal);
    }

    #[test]
    fn detail_never_copies_an_untrusted_response_body() {
        let failure = classify_status(503);
        assert_eq!(failure.detail(), Some("HTTP 503"));
    }

    #[cfg(feature = "bmc-http")]
    #[test]
    fn event_stream_failures_scope_to_their_request_class() {
        use std::time::Duration;

        use nv_redfish::bmc_http::reqwest::BmcError;

        use super::ClassifyError;

        // A stream that ends, overflows, or idles out is not an unreachable
        // endpoint: none of these may sample the endpoint breaker.
        let idle = BmcError::SseIdleTimeout {
            idle: Duration::from_secs(20),
        }
        .classify();
        assert_eq!(idle.class(), AcquisitionFailureClass::Protocol);
        assert_eq!(idle.retryable(), Some(true));
        assert!(!idle.class().is_endpoint_scoped());

        let oversized = BmcError::SseEventTooLarge { limit: 4096 }.classify();
        assert_eq!(oversized.class(), AcquisitionFailureClass::Protocol);
        assert_eq!(
            oversized.retryable(),
            Some(false),
            "the same frame comes back on resume"
        );
    }
}
