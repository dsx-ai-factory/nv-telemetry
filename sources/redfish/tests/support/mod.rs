// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::Mutex;

use futures_util::StreamExt as _;
use nv_redfish::core::ODataId;
use nv_redfish::core::Raw;
use nv_redfish::Bmc as _;
use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Error;
use nv_telemetry_redfish::EventFrames;
use nv_telemetry_redfish::RedfishService;
use nv_telemetry_redfish::RedfishTransport;
use nv_telemetry_redfish::SseEvent;
use nv_telemetry_redfish::TransportFault;
use serde_json::Value;

/// A service root naming no vendor: a platform without repairs.
const SPEC_ROOT: &str = include_str!("../fixtures/logs/service-root.json");

/// The scripted mock as an embedder's transport. Over a [`platform`], the
/// first service root read, the one that classifies the platform, is
/// answered out of the script, so each test scripts only what its provider
/// reads.
pub(crate) struct MockTransport {
    bmc: Arc<Bmc<Error>>,
    root: Mutex<Option<Value>>,
}

impl MockTransport {
    /// Every read scripted, the service root included: an event stream
    /// classifies from the root each open reads.
    pub(crate) fn scripted(bmc: &Arc<Bmc<Error>>) -> Self {
        Self {
            bmc: Arc::clone(bmc),
            root: Mutex::new(None),
        }
    }
}

/// The service over `bmc`, on a platform without repairs.
pub(crate) fn service(bmc: &Arc<Bmc<Error>>) -> Arc<RedfishService<MockTransport>> {
    platform(bmc, SPEC_ROOT)
}

/// The service over `bmc`, on the platform `root` names.
pub(crate) fn platform(bmc: &Arc<Bmc<Error>>, root: &str) -> Arc<RedfishService<MockTransport>> {
    let transport = MockTransport::scripted(bmc);
    *transport.root.lock().expect("not poisoned") =
        Some(serde_json::from_str(root).expect("a service root fixture is JSON"));
    Arc::new(RedfishService::new(transport))
}

/// A scripted status is the service answering it, `NotSupported` stands
/// for a 501, and any other mock failure is the harness's own.
fn fault(error: &Error) -> TransportFault {
    let scripted = match error {
        Error::ErrorResponse(inner) => inner.downcast_ref::<Error>(),
        other => Some(other),
    };
    match scripted {
        Some(Error::HttpStatus(status)) => TransportFault::Status(*status),
        Some(Error::NotSupported) => TransportFault::Status(501),
        _ => TransportFault::Client,
    }
}

impl RedfishTransport for MockTransport {
    async fn get(&self, uri: &str) -> Result<Value, TransportFault> {
        if uri == "/redfish/v1" {
            let root = self.root.lock().expect("not poisoned").take();
            if let Some(root) = root {
                return Ok(root);
            }
        }
        let raw = self
            .bmc
            .get::<Raw>(&ODataId::from(uri.to_owned()))
            .await
            .map_err(|error| fault(&error))?;
        Ok(raw.value().clone())
    }

    async fn events(
        &self,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> Result<EventFrames, TransportFault> {
        let events = self
            .bmc
            .stream_events::<Value>(uri, last_event_id)
            .await
            .map_err(|error| fault(&error))?;
        Ok(Box::pin(events.map(|event| {
            event
                .map(|event| SseEvent::new(event.last_event_id, event.data))
                .map_err(|error| fault(&error))
        })))
    }
}
