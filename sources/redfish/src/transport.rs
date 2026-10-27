// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The transport seam: what an embedder supplies, and the endpoint handle
//! the providers read through.
//!
//! An embedder implements [`RedfishTransport`] over its own HTTP client and
//! classifies its own failures, so no nv-redfish type crosses this crate's
//! public API and an nv-redfish release never forces one of this crate's.
//! Inside, [`Link`] implements nv-redfish's `Bmc` over the transport, for
//! reads only, so typed decoding, the platform's repairs, and the service
//! root's navigation run as nv-redfish defines them.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

use futures_util::stream::BoxStream;
use futures_util::StreamExt as _;
use nv_redfish::core::query::ExpandQuery;
use nv_redfish::core::repair::decode;
use nv_redfish::core::repair::decode_raw;
use nv_redfish::core::Action;
use nv_redfish::core::BmcError;
use nv_redfish::core::BoxTryStream;
use nv_redfish::core::DecodeError;
use nv_redfish::core::EntityTypeRef;
use nv_redfish::core::Expandable;
use nv_redfish::core::FilterQuery;
use nv_redfish::core::ModificationResponse;
use nv_redfish::core::MultipartUpdateRequest;
use nv_redfish::core::ODataETag;
use nv_redfish::core::ODataId;
use nv_redfish::core::Raw;
use nv_redfish::core::Repair;
use nv_redfish::core::RepairError;
use nv_redfish::core::SessionCreateResponse;
use nv_redfish::core::StreamEvent;
use nv_redfish::core::UploadReader;
use nv_redfish::quirks::BmcQuirks;
use nv_redfish::quirks::CompatBmc;
use nv_redfish::Bmc;
use nv_telemetry_source::AcquisitionFailure;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::failure::ClassifyError;

/// One Redfish service's documents and events, as an embedder's HTTP client
/// reaches them.
///
/// URIs are the service's own `@odata.id` references, relative to the
/// service, query options included. Every failure arrives classified: the
/// embedder knows its client's errors, and [`crate::classify_status`] is the
/// table for an HTTP status the service answered with.
pub trait RedfishTransport: Send + Sync + 'static {
    /// The document at `uri`, as the service sent it.
    fn get(&self, uri: &str) -> impl Future<Output = Result<Value, AcquisitionFailure>> + Send;

    /// The server-sent event stream at `uri`, resumed after
    /// `last_event_id` (sent as `Last-Event-ID`) when one is given.
    ///
    /// Each item is one event's `data` with the SSE id in effect after it.
    /// A failed item ends the stream for the provider reading it.
    fn events(
        &self,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> impl Future<Output = Result<EventFrames, AcquisitionFailure>> + Send;
}

/// An open event stream, as [`RedfishTransport::events`] yields it.
pub type EventFrames = BoxStream<'static, Result<SseEvent, AcquisitionFailure>>;

/// One server-sent event.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SseEvent {
    /// The SSE id in effect after this event: set by the event, inherited
    /// from an earlier one, or `None` when the service sends none.
    pub last_event_id: Option<String>,
    /// The event's `data`, parsed as JSON.
    pub data: Value,
}

impl SseEvent {
    /// An event carrying `data`, with `last_event_id` in effect after it.
    #[must_use]
    pub fn new(last_event_id: Option<String>, data: Value) -> Self {
        Self {
            last_event_id,
            data,
        }
    }
}

/// One endpoint's Redfish service, shared by every provider that reads it.
///
/// The platform is classified from the service root by the first read, or
/// by each of the first reads that start together: every document after it
/// carries that platform's repairs. A
/// classification that fails is that read's failure, and the next read
/// tries again.
pub struct RedfishService<T> {
    link: Arc<Link<T>>,
    // The platform rather than the layer over it: naming the layer would
    // bound every type holding a service, down to `SensorRead<()>`, on the
    // transport trait.
    quirks: Mutex<Option<Arc<BmcQuirks>>>,
}

impl<T> RedfishService<T> {
    /// The service `transport` reaches.
    #[must_use]
    pub fn new(transport: T) -> Self {
        Self {
            link: Arc::new(Link(transport)),
            quirks: Mutex::new(None),
        }
    }
}

impl<T: RedfishTransport> RedfishService<T> {
    /// The transport behind the platform's repairs, classified on first use.
    pub(crate) async fn compat(&self) -> Result<CompatBmc<Link<T>>, AcquisitionFailure> {
        let classified = lock(&self.quirks).clone();
        if let Some(quirks) = classified {
            return Ok(CompatBmc::new(Arc::clone(&self.link), quirks));
        }
        // The lock is not held across the read, so reads that start together
        // on an unreachable endpoint fail together rather than each waiting
        // out the one before it; two that both classify agree. The root is
        // read raw: a root that does not fully decode still names its vendor.
        let compat = CompatBmc::classify(Arc::clone(&self.link))
            .await
            .map_err(|error| error.classify())?;
        *lock(&self.quirks) = Some(Arc::clone(compat.quirks()));
        Ok(compat)
    }
}

/// The slot is only ever replaced whole, so a poisoned lock still holds a
/// consistent value.
fn lock<V>(slot: &Mutex<V>) -> MutexGuard<'_, V> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<T> fmt::Debug for RedfishService<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedfishService").finish_non_exhaustive()
    }
}

/// nv-redfish's `Bmc` over an embedder's transport: reads and event
/// streams; every other operation is refused before anything is sent.
pub(crate) struct Link<T>(T);

/// What the link fails with.
#[derive(Debug)]
pub(crate) enum LinkError {
    /// The transport's own failure, classified by the embedder.
    Transport(AcquisitionFailure),
    /// The document arrived and did not match its type.
    Decode(DecodeError),
    /// An operation this crate never performs.
    Refused(&'static str),
}

impl fmt::Display for LinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(failure) => failure.fmt(f),
            Self::Decode(error) => write!(f, "document did not decode: {error}"),
            Self::Refused(operation) => write!(f, "{operation} is not a read"),
        }
    }
}

impl std::error::Error for LinkError {}

impl BmcError for LinkError {
    fn into_decode_error(self) -> Result<DecodeError, Self> {
        match self {
            Self::Decode(error) => Ok(error),
            other => Err(other),
        }
    }
}

impl<T: RedfishTransport> Link<T> {
    async fn document<D: for<'de> Deserialize<'de>>(&self, id: &ODataId) -> Result<D, LinkError> {
        let document = self
            .0
            .get(&id.to_string())
            .await
            .map_err(LinkError::Transport)?;
        decode(document).map_err(LinkError::Decode)
    }

    async fn frames(
        &self,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> Result<EventFrames, LinkError> {
        self.0
            .events(uri, last_event_id)
            .await
            .map_err(LinkError::Transport)
    }
}

// Expand and filter are refused like writes: the providers build their own
// query URIs, and nv-redfish's wrappers expand only where a service
// advertises it and a caller asks.
impl<T: RedfishTransport> Bmc for Link<T> {
    type Error = LinkError;

    async fn expand<E: Expandable>(
        &self,
        _: &ODataId,
        _: ExpandQuery,
    ) -> Result<Arc<E>, LinkError> {
        Err(LinkError::Refused("$expand"))
    }

    async fn get<E: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        id: &ODataId,
    ) -> Result<Arc<E>, LinkError> {
        self.document(id).await.map(Arc::new)
    }

    async fn filter<E: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        _: &ODataId,
        _: FilterQuery,
    ) -> Result<Arc<E>, LinkError> {
        Err(LinkError::Refused("$filter"))
    }

    async fn get_repaired<E: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        id: &ODataId,
        repair: &impl Repair,
    ) -> Result<Arc<E>, RepairError<LinkError>> {
        decode_raw(self.get::<Raw>(id).await, repair)
    }

    async fn expand_repaired<E: Expandable>(
        &self,
        _: &ODataId,
        _: ExpandQuery,
        _: &impl Repair,
    ) -> Result<Arc<E>, RepairError<LinkError>> {
        Err(RepairError::Transport(LinkError::Refused("$expand")))
    }

    async fn filter_repaired<E: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        _: &ODataId,
        _: FilterQuery,
        _: &impl Repair,
    ) -> Result<Arc<E>, RepairError<LinkError>> {
        Err(RepairError::Transport(LinkError::Refused("$filter")))
    }

    async fn create<V: Send + Sync + Serialize, R: Send + Sync + for<'de> Deserialize<'de>>(
        &self,
        _: &ODataId,
        _: &V,
    ) -> Result<ModificationResponse<R>, LinkError> {
        Err(LinkError::Refused("create"))
    }

    async fn create_session<
        V: Send + Sync + Serialize,
        R: Send + Sync + for<'de> Deserialize<'de>,
    >(
        &self,
        _: &ODataId,
        _: &V,
    ) -> Result<SessionCreateResponse<R>, LinkError> {
        Err(LinkError::Refused("session creation"))
    }

    async fn update<
        V: Sync + Send + Serialize,
        R: Send + Sync + Sized + for<'de> Deserialize<'de>,
    >(
        &self,
        _: &ODataId,
        _: Option<&ODataETag>,
        _: &V,
    ) -> Result<ModificationResponse<R>, LinkError> {
        Err(LinkError::Refused("update"))
    }

    async fn delete<R: EntityTypeRef + for<'de> Deserialize<'de>>(
        &self,
        _: &ODataId,
    ) -> Result<ModificationResponse<R>, LinkError> {
        Err(LinkError::Refused("delete"))
    }

    async fn action<
        A: Send + Sync + Serialize,
        R: Send + Sync + Sized + for<'de> Deserialize<'de>,
    >(
        &self,
        _: &Action<A, R>,
        _: &A,
    ) -> Result<ModificationResponse<R>, LinkError> {
        Err(LinkError::Refused("action"))
    }

    async fn poll<R: Send + Sync + Sized + for<'de> Deserialize<'de>>(
        &self,
        _: &ODataId,
    ) -> Result<ModificationResponse<R>, LinkError> {
        Err(LinkError::Refused("task poll"))
    }

    async fn multipart_update<U, V, R>(
        &self,
        _: &str,
        _: MultipartUpdateRequest<'_, U, V>,
    ) -> Result<ModificationResponse<R>, LinkError>
    where
        U: UploadReader,
        R: Send + Sync + for<'de> Deserialize<'de>,
        V: Send + Sync + Serialize,
    {
        Err(LinkError::Refused("update upload"))
    }

    nv_redfish::core::if_update_service_deprecated! {
        async fn http_push_uri_update<U, R>(
            &self,
            _: &str,
            _: nv_redfish::core::HttpPushUriUpdateRequest<U>,
        ) -> Result<ModificationResponse<R>, LinkError>
        where
            U: UploadReader,
            R: Send + Sync + for<'de> Deserialize<'de>,
        {
            Err(LinkError::Refused("update upload"))
        }
    }

    async fn stream<E: Sized + for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        uri: &str,
    ) -> Result<BoxTryStream<E, LinkError>, LinkError> {
        let frames = self.frames(uri, None).await?;
        Ok(Box::pin(frames.map(|frame| {
            let frame = frame.map_err(LinkError::Transport)?;
            decode(frame.data).map_err(LinkError::Decode)
        })))
    }

    async fn stream_events<E: Sized + for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> Result<BoxTryStream<StreamEvent<E>, LinkError>, LinkError> {
        let frames = self.frames(uri, last_event_id).await?;
        Ok(Box::pin(frames.map(|frame| {
            let SseEvent {
                last_event_id,
                data,
            } = frame.map_err(LinkError::Transport)?;
            Ok(StreamEvent {
                last_event_id,
                data: decode(data).map_err(LinkError::Decode)?,
            })
        })))
    }
}
