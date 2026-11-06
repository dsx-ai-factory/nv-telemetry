// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Readings and states from configured `gNMI` subscriptions and Get requests.
//!
//! The caller supplies transport, endpoint identity, and mappings for individual leaves.
//! [`ConfiguredSubscription`] opens one STREAM subscription per acquisition.
//! Each notification produces one acquisition item; synchronization messages
//! produce none. Samples carry the device timestamp in `observed_at`.
//! Orchestration supplies batch timestamps and acquisition status.
//!
//! Opening errors are returned by [`Acquire::perform`]. Later stream failures
//! produce one failed item and end the stream. Dropping the connect future or
//! stream cancels the subscription. Orchestration owns reconnects; the caller
//! configures transport security and credentials.
//!
//! Mappings accept numeric values, strings, or booleans. Strings keep the spelling
//! the target sends for the requested encoding, including identityref module names
//! in `JSON_IETF` (for example, `openconfig-platform-types:ACTIVE`); classifying them
//! is the consumer's policy. Boolean values are preserved, including `false`.
//! An invalid mapped value produces a projection
//! issue; valid updates in the same notification are still collected. Readings
//! and states from the same notification share the same acquisition item.
//! Deletes that affect a mapped leaf produce issues and cannot remove earlier
//! observations. The provider validates mappings and received values without a
//! YANG schema.
//! Unsupported request paths can cause the target to reject Subscribe.
//!
//! ```
//! use nv_telemetry_gnmi::{ConfiguredSubscription, Mapping, NumericType,
//!     PathElement, StateIdentity, SubscriptionPath, SubscriptionSpec};
//! use nv_telemetry_model::{EndpointContext, SignalDescriptor, SignalKey, Subject};
//!
//! fn subscription<T>(transport: T) -> Result<ConfiguredSubscription<T>, Box<dyn std::error::Error>> {
//!     let endpoint = EndpointContext::builder().endpoint_id("switch-a").build()?;
//!     let subject = Subject::builder().kind("interface").id("eth0").build()?;
//!     let key = SignalKey::builder().subject(subject.clone()).facet("in-octets").build()?;
//!     let descriptor = SignalDescriptor::builder().key(key).unit("By").build()?;
//!     let interface = vec![
//!         PathElement::new("interfaces"),
//!         PathElement::new("interface").with_key("name", "eth0"),
//!     ];
//!     let mut counter = interface.clone();
//!     counter.extend(["state", "counters", "in-octets"].map(PathElement::new));
//!     let mut status = interface.clone();
//!     status.extend(["state", "oper-status"].map(PathElement::new));
//!     let spec = SubscriptionSpec::new(vec![SubscriptionPath::new(interface)], vec![
//!         Mapping::new(SubscriptionPath::new(counter), descriptor, NumericType::Uint),
//!         Mapping::string_state(SubscriptionPath::new(status),
//!             StateIdentity::new(subject, "oper-status")),
//!     ])?;
//!     Ok(ConfiguredSubscription::new(endpoint, transport, spec)?)
//! }
//! ```
//!
//! A collection mapping projects the same leaf across list entries, using keys
//! from each received path to build the subject:
//!
//! ```
//! use nv_telemetry_gnmi::{IdentityValue, Mapping, NumericType, PathElement,
//!     ReadingTemplate, StateTemplate, SubjectTemplate, SubscriptionPath, SubscriptionSpec};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let path = |names: &[&str]| SubscriptionPath::new(
//!     names.iter().map(|name| PathElement::new(*name)).collect());
//!
//! // Element 1 is "interface" in the full path, including any notification prefix.
//! let subject = SubjectTemplate::new("interface", IdentityValue::path_key(1, "name"))
//!     .with_scope(vec![IdentityValue::literal("switch-a")]);
//!
//! let spec = SubscriptionSpec::new(vec![path(&["interfaces", "interface"])], vec![
//!     Mapping::collection_reading(path(&["interfaces", "interface", "state", "counters", "in-octets"]),
//!         subject.clone(), ReadingTemplate::new(NumericType::Uint)
//!             .with_facet("in-octets").with_unit("By")),
//!     Mapping::collection_boolean_state(path(&["interfaces", "interface", "state", "enabled"]),
//!         subject, StateTemplate::new("enabled")),
//! ])?;
//!
//! // Use spec with ConfiguredSubscription::new(endpoint, transport, spec).
//! # Ok(())
//! # }
//! ```
//!
//! Use [`SubscriptionSpec::from_requests`] to select a subscription mode for each path.
//! The example requests counter samples every 10 seconds and status updates when
//! values change. Suppression skips unchanged counter samples; the heartbeat asks
//! the target to resend values every 60 seconds even when they have not changed.
//!
//! ```
//! use std::time::Duration;
//! use nv_telemetry_gnmi::{IdentityValue, Mapping, NumericType, PathElement,
//!     ReadingTemplate, StateTemplate, SubjectTemplate, SubscriptionPath,
//!     SubscriptionRequest, SubscriptionSpec};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let path = |names: &[&str]| SubscriptionPath::new(
//!     names.iter().map(|name| PathElement::new(*name)).collect());
//!
//! let counter = path(&["telemetry", "entry", "count"]);
//! let status = path(&["telemetry", "entry", "status"]);
//! // Element 1 is "entry"; its "name" key supplies each sensor's ID.
//! let subject = SubjectTemplate::new("sensor", IdentityValue::path_key(1, "name"));
//!
//! let spec = SubscriptionSpec::from_requests(vec![
//!     SubscriptionRequest::sample(counter.clone(), Duration::from_secs(10))
//!         .with_suppress_redundant(true)
//!         .with_heartbeat_interval(Duration::from_secs(60)),
//!     SubscriptionRequest::on_change(status.clone())
//!         .with_heartbeat_interval(Duration::from_secs(60)),
//! ], vec![
//!     Mapping::collection_reading(counter, subject.clone(),
//!         ReadingTemplate::new(NumericType::Uint).with_facet("count").with_unit("1")),
//!     Mapping::collection_string_state(status, subject, StateTemplate::new("status")),
//! ])?;
//!
//! // Use spec with ConfiguredSubscription::new(endpoint, transport, spec).
//! # Ok(())
//! # }
//! ```
//!
//! Collection paths have fixed element names and optional literal key filters.
//! Extra response keys are allowed. Selected keys must be present, nonempty, and
//! within the model's subject limits; invalid keys produce issues for that update.
//! Within a notification, only the final update for each complete keyed path is
//! projected. Different paths collide when they produce the same reading key or
//! state `(subject, facet, name)`. All observations for a colliding identity are
//! omitted with projection issues. Invalid values do not participate in collisions.
//! Each notification is projected independently.
//!
//! Updates may address individual leaves or JSON objects containing mapped leaves.
//! Object member names extend the update path and match mapping elements exactly,
//! including module prefixes. Keys must come from the received path; JSON fields
//! do not supply list keys. Arrays on mapped branches produce issues.
//! A `JSON_IETF` member produces an issue when its branch can reach a mapping only
//! if a module qualifier is ignored. That branch is skipped.
//!
//! For example, an update at `/telemetry/entry[name=sensor-a]/state` with
//! `{"count":42,"enabled":false}` supplies the mapped `state/count` and
//! `state/enabled` leaves for `sensor-a`.
//!
//! Repeated update paths keep only the final update, including malformed objects.
//! Overlapping object and leaf updates keep the final contribution to each leaf.
//! An absent member does not imply deletion. Malformed JSON or duplicate member
//! names in a traversed object discard the update's contribution with an issue.
//! An invalid mapped scalar produces an issue while valid sibling values are kept.
//! Objects that cannot reach a mapping are skipped. Their syntax is still checked,
//! but their member names are not checked for duplicates.
//! Member issue locations use JSON Pointer escaping. Locations on mapped branches
//! and qualified-name mismatches must fit the model's path limit, including
//! acquisition indexes. An overlong location rejects that object.
//! Arrays are opaque; their contents are not checked for duplicate member names.
//!
//! Use [`Mapping::boolean_state`] to collect a boolean state. A subscription may
//! contain only state mappings.
//!
//! [`ConfiguredGet`] collects small snapshots through the existing polling APIs.
//! One Get call produces one acquisition report, even when the target returns
//! several notifications. Each notification retains its device timestamp and its
//! issues are indexed under `notification[index]`. An empty response is a protocol
//! failure. Get uses the same leaf and JSON object projection as subscriptions.
//!
//! The caller supplies the transport and adds the returned poll to the
//! collector with the endpoint's other units.
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use nv_telemetry_gnmi::{ConfiguredGet, GetDataType, GetSpec, Mapping,
//!     NumericType, PathElement, SubscriptionPath};
//! use nv_telemetry_model::{EndpointContext, SignalDescriptor, SignalKey, Subject};
//! use nv_telemetry_orchestration::{plan, Needs, PollNeed, PollUnit};
//! use nv_telemetry_source::{Acquire, AcquisitionParts};
//!
//! fn poll<T>(endpoint: EndpointContext, transport: T)
//!     -> Result<PollUnit, Box<dyn std::error::Error>>
//! where ConfiguredGet<T>: Acquire<Output = AcquisitionParts> + Send + Sync + 'static,
//! {
//!     let path = SubscriptionPath::new(vec![PathElement::new("count")]);
//!     let subject = Subject::builder().kind("sensor").id("sensor-a").build()?;
//!     let key = SignalKey::builder().subject(subject).facet("count").build()?;
//!     let descriptor = SignalDescriptor::builder().key(key).unit("1").build()?;
//!     let spec = GetSpec::new(vec![path.clone()],
//!         vec![Mapping::new(path, descriptor, NumericType::Uint)])?
//!         .with_data_type(GetDataType::State);
//!     let unit = Arc::new(ConfiguredGet::new(endpoint.clone(), transport, spec)?);
//!     let needs = Needs::default().with_polls([PollNeed::new(endpoint,
//!         ConfiguredGet::<T>::REQUEST_CLASS, "sensor-count", Duration::from_secs(10))]);
//!     let planned = plan(needs, &[ConfiguredGet::<T>::declaration()])?;
//!     let poll = planned.polls().first().ok_or("missing Get poll")?.clone();
//!
//!     Ok(PollUnit::new(poll, unit))
//! }
//! ```
//!
//! Run `make codegen` to regenerate the bindings and `make check-codegen`
//! to check them. Builds use the checked-in files and do not need `protoc`.

mod projection;
#[path = "generated/protocol/mod.rs"]
mod protocol;
mod spec;

use std::pin::Pin;
use std::sync::Arc;

use futures_util::stream;
use futures_util::Stream;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::Origin;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass as Class;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProviderDeclaration;
use nv_telemetry_source::SubscriptionItem;
pub use spec::Encoding;
pub use spec::GetDataType;
pub use spec::GetSpec;
pub use spec::IdentityValue;
pub use spec::InvalidGet;
pub use spec::InvalidSubscription;
pub use spec::Mapping;
pub use spec::NumericType;
pub use spec::PathElement;
pub use spec::ReadingTemplate;
pub use spec::StateIdentity;
pub use spec::StateTemplate;
pub use spec::SubjectTemplate;
pub use spec::SubscriptionPath;
pub use spec::SubscriptionRequest;
pub use spec::SubscriptionSpec;
use tonic::codegen::Body;
use tonic::codegen::Bytes;
use tonic::codegen::StdError;
use tonic::Code;

/// A gNMI subscription with configured reading and state mappings for one endpoint.
///
/// The supplied transport must implement tonic's `GrpcService` contract and be
/// cloneable, `Send`, and `Sync`. Each call to [`Acquire::perform`] clones the
/// transport and opens a separate gRPC stream. The returned stream is
/// `Send + 'static` and does not borrow the subscription.
#[derive(Debug)]
pub struct ConfiguredSubscription<T> {
    endpoint: EndpointContext,
    origin: Origin,
    transport: T,
    spec: Arc<SubscriptionSpec>,
}

impl<T> ConfiguredSubscription<T> {
    /// Provider identity used in declarations and observations.
    pub const PROVIDER: &str = "gnmi.configured";

    /// Request class used for subscription admission.
    pub const REQUEST_CLASS: &str = "configured-subscribe";

    /// Creates a subscription for the endpoint. [`Acquire::perform`] sends the request.
    ///
    /// # Errors
    ///
    /// Returns a model validation error if the provider origin cannot be built.
    pub fn new(
        endpoint: EndpointContext,
        transport: T,
        spec: SubscriptionSpec,
    ) -> Result<Self, Invalid> {
        let origin = Origin::builder()
            .provider(Self::PROVIDER)
            .request_class(Self::REQUEST_CLASS)
            .build()?;

        Ok(Self {
            endpoint,
            origin,
            transport,
            spec: Arc::new(spec),
        })
    }

    /// Declares a streamed provider with an admission cost of one.
    #[must_use]
    pub fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::streamed(Self::PROVIDER, Self::REQUEST_CLASS, 1)
    }
}

impl<T> Acquire for ConfiguredSubscription<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone + Send + Sync + 'static,
    T::Error: Into<StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    type Output = Pin<Box<dyn Stream<Item = SubscriptionItem> + Send + 'static>>;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<Self::Output, AcquisitionFailure> {
        use protocol::gnmi::subscribe_response::Response;

        let mut client = protocol::gnmi::g_nmi_client::GNmiClient::new(self.transport.clone());

        // Keep tonic's default decoding limit to bound each incoming response.
        let response = client
            .subscribe(stream::iter([self.spec.request()]))
            .await
            .map_err(|status| failure("Subscribe", status.code()))?
            .into_inner();

        let spec = Arc::clone(&self.spec);

        Ok(Box::pin(stream::unfold(
            Some((response, spec)),
            |state| async move {
                let (mut response, spec) = state?;

                loop {
                    let item = match response.message().await {
                        Ok(Some(message)) => match message.response {
                            Some(Response::SyncResponse(_)) => continue,
                            Some(Response::Update(notification)) => {
                                let item = projection::project(&spec.projection, notification).map(
                                    |(payloads, issues)| AcquisitionParts::new(payloads, issues),
                                );

                                let next = item.is_ok().then_some((response, spec));

                                return Some((item, next));
                            }
                            #[allow(deprecated)]
                            Some(Response::Error(error)) => {
                                Err(legacy_failure("Subscribe", error.code))
                            }
                            None => Err(failure("Subscribe", Code::DataLoss)),
                        },
                        Err(status) => Err(failure("Subscribe", status.code())),
                        Ok(None) => Err(AcquisitionFailure::new(Class::Protocol)
                            .with_retryable(true)
                            .with_detail("gNMI STREAM closed without a terminal error")),
                    };

                    // Report the failure once; orchestration decides whether to reconnect.
                    return Some((item, None));
                }
            },
        )))
    }
}

/// A configured unary Get acquisition for one endpoint.
///
/// Each [`Acquire::perform`] call sends one RPC and projects all returned
/// notifications into one acquisition. Device timestamps are retained per
/// notification. Coverage is partial; missing leaves do not imply deletion.
/// Transport security, credentials, and deadlines are supplied by the caller.
#[derive(Debug)]
pub struct ConfiguredGet<T> {
    endpoint: EndpointContext,
    origin: Origin,
    transport: T,
    spec: GetSpec,
}

impl<T> ConfiguredGet<T> {
    /// Provider identity used in declarations and observations.
    pub const PROVIDER: &str = ConfiguredSubscription::<T>::PROVIDER;

    /// Request class used for Get admission.
    pub const REQUEST_CLASS: &str = "configured-get";

    /// Creates an acquisition. The validated specification is sent by [`Acquire::perform`].
    ///
    /// # Errors
    ///
    /// Returns a model validation error if the provider origin cannot be built.
    pub fn new(endpoint: EndpointContext, transport: T, spec: GetSpec) -> Result<Self, Invalid> {
        let origin = Origin::builder()
            .provider(Self::PROVIDER)
            .request_class(Self::REQUEST_CLASS)
            .build()?;

        Ok(Self {
            endpoint,
            origin,
            transport,
            spec,
        })
    }

    /// Declares a polled provider with an admission cost of one.
    #[must_use]
    pub fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::polled(Self::PROVIDER, Self::REQUEST_CLASS, 1)
    }
}

impl<T> Acquire for ConfiguredGet<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone + Send + Sync + 'static,
    T::Error: Into<StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<Self::Output, AcquisitionFailure> {
        let mut client = protocol::gnmi::g_nmi_client::GNmiClient::new(self.transport.clone());

        // The unary client checks final gRPC status before releasing the response.
        // Keep its default decoding limit, as for Subscribe.
        let response = client
            .get(self.spec.request())
            .await
            .map_err(|status| failure("Get", status.code()))?
            .into_inner();

        #[allow(deprecated)]
        if let Some(error) = response.error {
            return Err(legacy_failure("Get", error.code));
        }

        if response.notification.is_empty() {
            return Err(AcquisitionFailure::new(Class::Protocol)
                .with_retryable(false)
                .with_detail("gNMI Get returned no notifications"));
        }

        let mut payloads = Vec::new();
        let mut issues = Vec::new();

        for (index, notification) in response.notification.into_iter().enumerate() {
            let (projected, notification_issues) =
                projection::project(&self.spec.projection, notification)?;

            // Preserve notification boundaries and timestamps; overlapping request
            // paths can return the same leaf at different snapshot times.
            payloads.extend(projected);

            issues.extend(
                notification_issues
                    .into_iter()
                    .map(|issue| issue.at_index("notification", index)),
            );
        }

        Ok(AcquisitionParts::new(payloads, issues))
    }
}

fn legacy_failure(operation: &str, code: u32) -> AcquisitionFailure {
    let decoded = i32::try_from(code)
        .ok()
        .map(Code::from_i32)
        .filter(|decoded| *decoded as u32 == code)
        .unwrap_or(Code::DataLoss);

    failure(operation, decoded)
}

// Report the gRPC code without copying error text from the target.
fn failure(operation: &str, code: Code) -> AcquisitionFailure {
    let (class, retryable) = match code {
        Code::Unavailable => (Class::Connectivity, true),
        Code::DeadlineExceeded => (Class::Timeout, true),
        Code::Unauthenticated | Code::PermissionDenied => (Class::Authentication, false),
        Code::InvalidArgument
        | Code::Unimplemented
        | Code::NotFound
        | Code::AlreadyExists
        | Code::FailedPrecondition => (Class::Unsupported, false),
        Code::Cancelled | Code::Unknown | Code::Aborted | Code::ResourceExhausted => {
            (Class::Device, true)
        }
        // Tonic uses OutOfRange for oversized frames and Internal for decode errors.
        Code::Ok | Code::DataLoss | Code::Internal | Code::OutOfRange => (Class::Protocol, false),
    };

    AcquisitionFailure::new(class)
        .with_retryable(retryable)
        .with_detail(format!("gNMI {operation} failed with gRPC code {code:?}"))
}

#[cfg(test)]
mod tests;
