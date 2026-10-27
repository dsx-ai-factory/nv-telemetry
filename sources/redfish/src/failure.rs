// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Failure classification: transport errors become the facts policy runs on.
//!
//! The contract owns the class *semantics* (connectivity and authentication
//! may trip the endpoint breaker; unsupported and protocol failures scope
//! to their request class). The embedder's transport classifies its own
//! client's failures, with the HTTP status table here; this module
//! classifies what happens past the transport: a document that does not
//! match its type, and nv-redfish's own refusals.

use nv_redfish::core::RepairError;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;

use crate::transport::LinkError;

/// Classifies an error of the transport the providers read through into an
/// [`AcquisitionFailure`].
pub(crate) trait ClassifyError {
    /// The classified facts: class, retryability, operator-facing detail.
    fn classify(&self) -> AcquisitionFailure;
}

/// Classifies an HTTP status the endpoint answered with: the table a
/// [`crate::RedfishTransport`] uses for its client's status errors.
///
/// The one policy-laden row: **404 is `Unsupported`, not `Device`**.
/// The right reaction to a planned URI vanishing is request-class demotion
/// and a catalog refresh — Unsupported's lane — while Device would invite
/// retries of a URI that will 404 forever. Detail deliberately carries only
/// classified facts: response bodies and transport displays are untrusted
/// and may contain credentials, query secrets, or device-sensitive data.
// Rows that share an outcome stay separate rows: each carries its own
// rationale, and merging them would hide which statuses were considered.
#[allow(clippy::match_same_arms)]
#[must_use]
pub fn classify_status(status: u16) -> AcquisitionFailure {
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

/// The link's failures: the embedder's classification stands, a document
/// that did not match its type is a protocol answer, and a refused
/// operation is this crate's bug.
impl ClassifyError for LinkError {
    fn classify(&self) -> AcquisitionFailure {
        match self {
            Self::Transport(failure) => failure.clone(),
            Self::Decode(_) => schema_mismatch(),
            Self::Refused(_) => AcquisitionFailure::new(AcquisitionFailureClass::Internal)
                .with_retryable(false)
                .with_detail("a read asked the transport for an operation it never performs"),
        }
    }
}

fn schema_mismatch() -> AcquisitionFailure {
    AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
        .with_retryable(false)
        .with_detail("Redfish response did not match its schema")
}

/// A transport wrapped in nv-redfish's compatibility layer: its own failures
/// classify as the transport does, and a document that did not decode after
/// the platform's repairs is a protocol answer.
impl<E: ClassifyError> ClassifyError for RepairError<E> {
    fn classify(&self) -> AcquisitionFailure {
        match self {
            Self::Transport(error) => error.classify(),
            Self::Decode(_) => schema_mismatch(),
        }
    }
}

#[cfg(test)]
mod mock {
    use nv_telemetry_source::AcquisitionFailure;
    use nv_telemetry_source::AcquisitionFailureClass;

    use super::ClassifyError;

    /// The unit tests' mock: a harness failure is `Internal`, except a
    /// response scripted as `NotSupported`, which stands for a 501.
    impl ClassifyError for nv_redfish_bmc_mock::Error {
        fn classify(&self) -> AcquisitionFailure {
            if let nv_redfish_bmc_mock::Error::ErrorResponse(inner) = self {
                let refused = inner
                    .downcast_ref::<nv_redfish_bmc_mock::Error>()
                    .is_some_and(|error| matches!(error, nv_redfish_bmc_mock::Error::NotSupported));
                if refused {
                    return AcquisitionFailure::new(AcquisitionFailureClass::Unsupported)
                        .with_retryable(false)
                        .with_detail("Redfish mock answered not supported");
                }
            }
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

    #[test]
    fn the_compat_layer_keeps_the_transport_class_and_scopes_decode_failures() {
        use nv_redfish::core::repair::decode;
        use nv_redfish::core::RepairError;

        use super::ClassifyError;
        use crate::transport::LinkError;

        // The embedder's classification stands.
        let refused = classify_status(501);
        let transport = RepairError::Transport(LinkError::Transport(refused.clone())).classify();
        assert_eq!(transport, refused);

        let undecodable = decode::<u32>(serde_json::json!("not a number")).unwrap_err();
        let decode = RepairError::<LinkError>::Decode(undecodable).classify();
        assert_eq!(decode.class(), AcquisitionFailureClass::Protocol);
        assert_eq!(decode.retryable(), Some(false));
        assert!(!decode.class().is_endpoint_scoped());
    }
}
