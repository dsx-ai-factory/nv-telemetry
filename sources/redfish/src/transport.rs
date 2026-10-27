// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The transport seam: what an embedder supplies, and the endpoint handle
//! the providers read through.
//!
//! An embedder implements [`RedfishTransport`] over its own HTTP client and
//! reports what went wrong as a [`TransportFault`], so no nv-redfish type
//! crosses this crate's public API and an nv-redfish release never forces
//! one of this crate's. Inside, [`Link`] implements nv-redfish's `Bmc` over
//! the transport, for reads only, so typed decoding, the platform's
//! repairs, and the service root's navigation run as nv-redfish defines
//! them.

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
use nv_redfish::core::Repair;
use nv_redfish::core::RepairError;
use nv_redfish::core::SessionCreateResponse;
use nv_redfish::core::StreamEvent;
use nv_redfish::core::UploadReader;
use nv_redfish::quirks::BmcQuirks;
use nv_redfish::quirks::CompatBmc;
use nv_redfish::quirks::RootEvidence;
use nv_redfish::schema::service_root::ServiceRoot;
use nv_redfish::Bmc;
use nv_telemetry_source::AcquisitionFailure;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::failure::ClassifyError;
use crate::failure::TransportFault;

/// One Redfish service's documents and events, as an embedder's HTTP client
/// reaches them.
///
/// URIs are the service's own `@odata.id` references, relative to the
/// service, query options included as written (`$expand=*($levels=1)`,
/// `$filter=Created gt '…'`): the client encodes them for the wire. A
/// failure is reported as the fault the client saw; what it means for the
/// endpoint is this crate's to decide.
pub trait RedfishTransport: Send + Sync + 'static {
    /// The document at `uri`, as the service sent it.
    fn get(&self, uri: &str) -> impl Future<Output = Result<Value, TransportFault>> + Send;

    /// The server-sent event stream at `uri`, resumed after
    /// `last_event_id` (sent as `Last-Event-ID`) when one is given.
    ///
    /// Each item is one event's `data` with the SSE id in effect after it.
    /// A failed item ends the stream for the provider reading it. Once the
    /// stream is open its failures are its own: a body that breaks off is
    /// [`TransportFault::Transfer`], and silence past the client's window is
    /// [`TransportFault::StreamIdle`], never [`TransportFault::Timeout`],
    /// which would trip the endpoint's breaker for every request class.
    fn events(
        &self,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> impl Future<Output = Result<EventFrames, TransportFault>> + Send;
}

/// An open event stream, as [`RedfishTransport::events`] yields it.
pub type EventFrames = BoxStream<'static, Result<SseEvent, TransportFault>>;

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
/// carries that platform's repairs. A classification that fails is that
/// read's failure, and the next read tries again. A fault that indicts the
/// endpoint — unreachable, timed out, or refusing the credentials — drops
/// the classification, so the first read after a BMC comes back, perhaps on
/// new firmware, classifies it afresh; so does every event stream open, from
/// the root it reads anyway.
pub struct RedfishService<T> {
    link: Arc<Link<T>>,
}

impl<T> RedfishService<T> {
    /// The service `transport` reaches.
    #[must_use]
    pub fn new(transport: T) -> Self {
        Self {
            link: Arc::new(Link {
                transport,
                platform: Platform::default(),
            }),
        }
    }
}

impl<T: RedfishTransport> RedfishService<T> {
    /// The transport behind the platform's repairs, classified on first use.
    pub(crate) async fn compat(&self) -> Result<CompatBmc<Link<T>>, AcquisitionFailure> {
        if let Some(quirks) = self.link.platform.get() {
            return Ok(CompatBmc::new(Arc::clone(&self.link), quirks));
        }
        // No lock is held across the read, so reads that start together on
        // an unreachable endpoint fail together rather than each waiting out
        // the one before it; two that both classify agree. The root is read
        // raw: a root that does not fully decode still names its vendor.
        let root = self
            .link
            .document(&ODataId::service_root())
            .await
            .map_err(|error| error.classify())?;
        Ok(self.classified(&root))
    }

    /// The service root, read once: the platform is classified afresh from
    /// it, and it is then repaired for that platform like any other document.
    pub(crate) async fn root(&self) -> Result<Root<T>, AcquisitionFailure> {
        let root = self
            .link
            .document(&ODataId::service_root())
            .await
            .map_err(|error| error.classify())?;
        let bmc = self.classified(&root);
        let document = bmc
            .decode(root)
            .map_err(|error| LinkError::Decode(error).classify())?;
        Ok(Root { bmc, document })
    }

    /// The layer for the platform `root` names, kept for the reads after.
    fn classified(&self, root: &Value) -> CompatBmc<Link<T>> {
        let quirks = Arc::new(BmcQuirks::classify(&RootEvidence::from_root(root)));
        self.link.platform.set(Arc::clone(&quirks));
        CompatBmc::new(Arc::clone(&self.link), quirks)
    }
}

/// The service root, decoded for its platform, and the layer that platform
/// puts over the transport.
pub(crate) struct Root<T: RedfishTransport> {
    pub(crate) bmc: CompatBmc<Link<T>>,
    pub(crate) document: ServiceRoot,
}

impl<T> fmt::Debug for RedfishService<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedfishService").finish_non_exhaustive()
    }
}

/// The endpoint's classified platform, once known.
// The platform rather than the layer over it: naming the layer would bound
// every type holding a service, down to `SensorRead<()>`, on the transport
// trait.
#[derive(Clone, Default)]
struct Platform(Arc<Mutex<Option<Arc<BmcQuirks>>>>);

impl Platform {
    fn get(&self) -> Option<Arc<BmcQuirks>> {
        self.slot().clone()
    }

    fn set(&self, quirks: Arc<BmcQuirks>) {
        *self.slot() = Some(quirks);
    }

    /// The fault as a link failure, forgetting the platform when the fault
    /// indicts the endpoint.
    fn failed(&self, fault: TransportFault) -> LinkError {
        if fault.classify().class().is_endpoint_scoped() {
            *self.slot() = None;
        }
        LinkError::Transport(fault)
    }

    /// The slot is only ever replaced whole, so a poisoned lock still holds
    /// a consistent value.
    fn slot(&self) -> MutexGuard<'_, Option<Arc<BmcQuirks>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// nv-redfish's `Bmc` over an embedder's transport: every read, expanded
/// and filtered ones included, and event streams; writes are refused before
/// anything is sent.
pub(crate) struct Link<T> {
    transport: T,
    platform: Platform,
}

/// What the link fails with.
#[derive(Debug)]
pub(crate) enum LinkError {
    /// The transport's own fault.
    Transport(TransportFault),
    /// The document arrived and did not match its type.
    Decode(DecodeError),
    /// A write, which this crate never performs.
    Refused(&'static str),
}

impl fmt::Display for LinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(fault) => write!(f, "transport fault: {fault:?}"),
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
    async fn document(&self, id: &ODataId) -> Result<Value, LinkError> {
        self.transport
            .get(&id.to_string())
            .await
            .map_err(|fault| self.platform.failed(fault))
    }

    /// The stream's frames, each failed one told to the platform as it
    /// arrives.
    async fn frames(
        &self,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> Result<BoxStream<'static, Result<SseEvent, LinkError>>, LinkError> {
        let frames = self
            .transport
            .events(uri, last_event_id)
            .await
            .map_err(|fault| self.platform.failed(fault))?;
        let platform = self.platform.clone();
        Ok(Box::pin(frames.map(move |frame| {
            frame.map_err(|fault| platform.failed(fault))
        })))
    }
}

/// `id` with a query option appended, after any the id already carries:
/// a continuation or monitor token must survive an added `$expand`.
fn with_query(id: &ODataId, query: &str) -> ODataId {
    let id = id.to_string();
    if query.is_empty() {
        return id.into();
    }
    let separator = if id.contains('?') { '&' } else { '?' };
    format!("{id}{separator}{query}").into()
}

// Every read nv-redfish's wrappers may make is served, `$expand` and
// `$filter` included; writes are refused, since telemetry never changes a
// device.
impl<T: RedfishTransport> Bmc for Link<T> {
    type Error = LinkError;

    async fn expand<E: Expandable>(
        &self,
        id: &ODataId,
        query: ExpandQuery,
    ) -> Result<Arc<E>, LinkError> {
        self.get(&with_query(id, &query.to_query_string())).await
    }

    async fn get<E: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        id: &ODataId,
    ) -> Result<Arc<E>, LinkError> {
        let document = self.document(id).await?;
        decode(document).map(Arc::new).map_err(LinkError::Decode)
    }

    async fn filter<E: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        id: &ODataId,
        query: FilterQuery,
    ) -> Result<Arc<E>, LinkError> {
        self.get(&with_query(id, &query.to_query_string())).await
    }

    async fn get_repaired<E: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        id: &ODataId,
        repair: &impl Repair,
    ) -> Result<Arc<E>, RepairError<LinkError>> {
        // Repaired in place: the transport's document is already the owned
        // JSON a repair works on.
        let mut document = self.document(id).await.map_err(RepairError::Transport)?;
        repair.repair(&mut document);
        decode(document).map(Arc::new).map_err(RepairError::Decode)
    }

    async fn expand_repaired<E: Expandable>(
        &self,
        id: &ODataId,
        query: ExpandQuery,
        repair: &impl Repair,
    ) -> Result<Arc<E>, RepairError<LinkError>> {
        self.get_repaired(&with_query(id, &query.to_query_string()), repair)
            .await
    }

    async fn filter_repaired<E: EntityTypeRef + for<'de> Deserialize<'de> + 'static>(
        &self,
        id: &ODataId,
        query: FilterQuery,
        repair: &impl Repair,
    ) -> Result<Arc<E>, RepairError<LinkError>> {
        self.get_repaired(&with_query(id, &query.to_query_string()), repair)
            .await
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
            decode(frame?.data).map_err(LinkError::Decode)
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
            } = frame?;
            Ok(StreamEvent {
                last_event_id,
                data: decode(data).map_err(LinkError::Decode)?,
            })
        })))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use nv_redfish::core::query::ExpandQuery;
    use nv_redfish::core::FilterQuery;
    use nv_redfish::core::NoRepair;
    use nv_redfish::core::ODataId;
    use nv_redfish::core::Raw;
    use nv_redfish::Bmc as _;
    use serde_json::json;
    use serde_json::Value;

    use super::EventFrames;
    use super::Link;
    use super::Platform;
    use super::RedfishTransport;
    use crate::failure::TransportFault;

    /// Answers every GET with an empty document and keeps the URIs asked.
    #[derive(Default)]
    struct Recorder(Mutex<Vec<String>>);

    impl RedfishTransport for Recorder {
        async fn get(&self, uri: &str) -> Result<Value, TransportFault> {
            self.0.lock().expect("not poisoned").push(uri.to_owned());
            Ok(json!({}))
        }

        async fn events(&self, _: &str, _: Option<&str>) -> Result<EventFrames, TransportFault> {
            Err(TransportFault::Client)
        }
    }

    #[tokio::test]
    async fn expanded_and_filtered_reads_keep_the_query_the_id_carries() {
        let link = Link {
            transport: Recorder::default(),
            platform: Platform::default(),
        };
        let collection = ODataId::from("/redfish/v1/UpdateService/FirmwareInventory".to_owned());
        let paged =
            ODataId::from("/redfish/v1/Systems/1/LogServices/SEL/Entries?$skip=50".to_owned());
        link.expand::<Raw>(&collection, ExpandQuery::all())
            .await
            .expect("an expanded read is served");
        link.expand_repaired::<Raw>(&paged, ExpandQuery::all(), &NoRepair)
            .await
            .expect("an expanded page is served");
        link.filter::<Raw>(&paged, FilterQuery::eq(&"Severity", "Critical".to_owned()))
            .await
            .expect("a filtered read is served");
        assert_eq!(
            *link.transport.0.lock().expect("not poisoned"),
            [
                "/redfish/v1/UpdateService/FirmwareInventory?$expand=*($levels=1)",
                "/redfish/v1/Systems/1/LogServices/SEL/Entries?$skip=50&$expand=*($levels=1)",
                "/redfish/v1/Systems/1/LogServices/SEL/Entries?$skip=50&$filter=Severity eq 'Critical'",
            ]
        );
    }
}
