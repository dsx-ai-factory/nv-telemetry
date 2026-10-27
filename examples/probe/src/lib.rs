// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The probe's transport: the part of the embedder role that reaches the
//! device. nv-redfish's HTTP client, or its in-process mock, as
//! nv-telemetry's [`RedfishTransport`], with each client's failures
//! classified here, by the embedder that knows them.

use std::sync::Arc;

use futures_util::StreamExt as _;
use nv_redfish::bmc_http::reqwest::BmcError;
use nv_redfish::core::ODataId;
use nv_redfish::core::Raw;
use nv_redfish::Bmc;
use nv_telemetry_redfish::classify_status;
use nv_telemetry_redfish::EventFrames;
use nv_telemetry_redfish::RedfishTransport;
use nv_telemetry_redfish::SseEvent;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use serde_json::Value;

/// Classifies a client's error into an [`AcquisitionFailure`].
pub trait Classify {
    /// The classified facts: class, retryability, operator-facing detail.
    fn classify(&self) -> AcquisitionFailure;
}

/// An nv-redfish `Bmc` as nv-telemetry's transport.
#[derive(Debug)]
pub struct BmcTransport<B>(Arc<B>);

impl<B> BmcTransport<B> {
    /// The transport `bmc` is.
    #[must_use]
    pub fn new(bmc: Arc<B>) -> Self {
        Self(bmc)
    }
}

impl<B> RedfishTransport for BmcTransport<B>
where
    B: Bmc + 'static,
    B::Error: Classify,
{
    async fn get(&self, uri: &str) -> Result<Value, AcquisitionFailure> {
        let raw = self
            .0
            .get::<Raw>(&ODataId::from(uri.to_owned()))
            .await
            .map_err(|error| error.classify())?;
        // A transport that caches keeps its own reference.
        Ok(Arc::try_unwrap(raw).map_or_else(|raw| raw.value().clone(), Raw::into_value))
    }

    async fn events(
        &self,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> Result<EventFrames, AcquisitionFailure> {
        let events = self
            .0
            .stream_events::<Value>(uri, last_event_id)
            .await
            .map_err(|error| error.classify())?;
        Ok(Box::pin(events.map(|event| {
            event
                .map(|event| SseEvent::new(event.last_event_id, event.data))
                .map_err(|error| error.classify())
        })))
    }
}

impl Classify for BmcError {
    // The match is exhaustive on purpose: a transport error the table
    // has not classified must fail this build, not reach the
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
            BmcError::EncodeError(_) | BmcError::InvalidRequest(_) | BmcError::CacheError(_) => {
                with(
                    AcquisitionFailureClass::Internal,
                    false,
                    "Redfish client state is invalid",
                )
            }
            // Reads invoke no actions; reaching this is our bug.
            BmcError::ActionNotSupported => with(
                AcquisitionFailureClass::Internal,
                false,
                "Redfish action is not advertised",
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

/// A mock failure is a harness failure, `Internal`, except a response
/// scripted as `NotSupported`, which stands for the device answering 501.
impl Classify for nv_redfish_bmc_mock::Error {
    fn classify(&self) -> AcquisitionFailure {
        if let Self::ErrorResponse(inner) = self {
            let refused = inner
                .downcast_ref::<Self>()
                .is_some_and(|error| matches!(error, Self::NotSupported));
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nv_redfish::bmc_http::reqwest::BmcError;
    use nv_telemetry_source::AcquisitionFailureClass;

    use super::Classify;

    #[test]
    fn event_stream_failures_scope_to_their_request_class() {
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
