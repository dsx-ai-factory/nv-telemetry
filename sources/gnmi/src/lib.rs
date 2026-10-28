// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Readings and states from configured `gNMI` subscriptions.
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
//! Collection paths have fixed element names and optional literal key filters.
//! Extra response keys are allowed. Selected keys must be present, nonempty, and
//! within the model's subject limits; invalid keys produce issues for that update.
//! Within a notification, only the final update for each complete keyed path is
//! projected. Different paths collide when they produce the same reading key or
//! state `(subject, facet, name)`. All observations for a colliding identity are
//! omitted with projection issues. Invalid values do not participate in collisions.
//! Each notification is projected independently.
//!
//! The target must send updates for individual leaves, even when the request
//! selects a parent path. Ancestor updates produce projection issues; the provider
//! does not traverse JSON subtrees. Use
//! [`Mapping::boolean_state`] to collect a boolean state. A subscription may
//! contain only state mappings.
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
use nv_telemetry_source::ProviderDeclaration;
use nv_telemetry_source::SubscriptionItem;
pub use spec::Encoding;
pub use spec::IdentityValue;
pub use spec::InvalidSubscription;
pub use spec::Mapping;
pub use spec::NumericType;
pub use spec::PathElement;
pub use spec::ReadingTemplate;
pub use spec::StateIdentity;
pub use spec::StateTemplate;
pub use spec::SubjectTemplate;
pub use spec::SubscriptionPath;
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
            .map_err(|status| failure(status.code()))?
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
                                let item = projection::project(&spec, notification);
                                let next = item.is_ok().then_some((response, spec));

                                return Some((item, next));
                            }
                            #[allow(deprecated)]
                            Some(Response::Error(error)) => {
                                let code = i32::try_from(error.code)
                                    .ok()
                                    .map(Code::from_i32)
                                    .filter(|code| *code as u32 == error.code)
                                    .unwrap_or(Code::DataLoss);

                                Err(failure(code))
                            }
                            None => Err(failure(Code::DataLoss)),
                        },
                        Err(status) => Err(failure(status.code())),
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

// Report the gRPC code without copying error text from the target.
fn failure(code: Code) -> AcquisitionFailure {
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
        .with_detail(format!("gNMI Subscribe failed with gRPC code {code:?}"))
}

#[cfg(test)]
mod tests;
