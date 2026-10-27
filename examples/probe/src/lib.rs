// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The probe's transport: the part of the embedder role that reaches the
//! device. nv-redfish's HTTP client, or its in-process mock, as
//! nv-telemetry's [`RedfishTransport`], each client's errors told as the
//! [`TransportFault`] they are.

use std::sync::Arc;

use futures_util::StreamExt as _;
use nv_redfish::bmc_http::reqwest::BmcError;
use nv_redfish::core::ODataId;
use nv_redfish::core::Raw;
use nv_redfish::Bmc;
use nv_telemetry_redfish::EventFrames;
use nv_telemetry_redfish::RedfishTransport;
use nv_telemetry_redfish::SseEvent;
use nv_telemetry_redfish::TransportFault;
use serde_json::Value;

/// A client's error as the fault it is.
pub trait Fault {
    /// What went wrong between the client and the service.
    fn fault(&self) -> TransportFault;
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
    B::Error: Fault,
{
    async fn get(&self, uri: &str) -> Result<Value, TransportFault> {
        let raw = self
            .0
            .get::<Raw>(&ODataId::from(uri.to_owned()))
            .await
            .map_err(|error| error.fault())?;
        // A transport that caches keeps its own reference.
        Ok(Arc::try_unwrap(raw).map_or_else(|raw| raw.value().clone(), Raw::into_value))
    }

    async fn events(
        &self,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> Result<EventFrames, TransportFault> {
        let events = self
            .0
            .stream_events::<Value>(uri, last_event_id)
            .await
            .map_err(|error| error.fault())?;
        Ok(Box::pin(events.map(|event| {
            event
                .map(|event| SseEvent::new(event.last_event_id, event.data))
                .map_err(|error| error.fault())
        })))
    }
}

impl Fault for BmcError {
    // Exhaustive on purpose: an error this table has not placed must fail
    // the build, not reach the dispatcher as the wrong fault.
    fn fault(&self) -> TransportFault {
        match self {
            // A connect that timed out is still unreachability, so the
            // connect check runs first.
            Self::ReqwestError(error) if error.is_connect() => TransportFault::Connect,
            Self::ReqwestError(error) if error.is_timeout() => TransportFault::Timeout,
            Self::ReqwestError(error) if error.is_builder() || error.is_request() => {
                TransportFault::Client
            }
            Self::ReqwestError(error) if error.is_redirect() => TransportFault::Redirect,
            Self::ReqwestError(_) => TransportFault::Transfer,
            Self::InvalidResponse { status, .. } => TransportFault::Status(status.as_u16()),
            Self::JsonError(_) | Self::DecodeError(_) => TransportFault::Body,
            // Reads invoke no actions, so this too is the client's state.
            Self::EncodeError(_)
            | Self::InvalidRequest(_)
            | Self::CacheError(_)
            | Self::ActionNotSupported => TransportFault::Client,
            Self::CacheMiss => TransportFault::CacheMiss,
            Self::SseStreamError(_) => TransportFault::StreamDecode,
            Self::SseEventTooLarge { .. } => TransportFault::StreamFrameTooLarge,
            Self::SseIdleTimeout { .. } => TransportFault::StreamIdle,
        }
    }
}

/// A scripted status is the service answering it, `NotSupported` stands
/// for a 501, and any other mock failure is the harness's own.
impl Fault for nv_redfish_bmc_mock::Error {
    fn fault(&self) -> TransportFault {
        let scripted = match self {
            Self::ErrorResponse(inner) => inner.downcast_ref::<Self>(),
            other => Some(other),
        };
        match scripted {
            Some(Self::HttpStatus(status)) => TransportFault::Status(*status),
            Some(Self::NotSupported) => TransportFault::Status(501),
            _ => TransportFault::Client,
        }
    }
}
