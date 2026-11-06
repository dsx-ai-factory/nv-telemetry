// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Source paths and deletion facts for caller-owned notification consumers.

use nv_telemetry_model::SignalKey;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_source::AcquisitionParts;

use crate::PathElement;
use crate::SubscriptionPath;

/// A resolved source path, including its gNMI origin and target.
///
/// An empty element sequence identifies the root. An unspecified origin resolves
/// to `openconfig`; an unspecified target stays empty. Target names are compared
/// literally, so callers should configure a target when the server names one.
/// These fields describe the gNMI data tree, not the batch's provider `Origin`.
///
/// ```
/// # use nv_telemetry_gnmi::SourcePath;
/// # fn inspect(path: &SourcePath) {
/// let origin = path.origin();
/// let elements = path.elements();
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourcePath {
    pub(super) path: SubscriptionPath,
    pub(super) origin: String,
    pub(super) target: String,
}

impl SourcePath {
    /// The full path after applying the notification prefix.
    #[must_use]
    pub fn elements(&self) -> &[PathElement] {
        &self.path.0
    }

    /// The effective gNMI data origin, including the protocol default.
    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// The effective gNMI target, or empty when unspecified.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Whether this path covers another source in the same origin and target.
    ///
    /// Element names must form a prefix and every key on this path must match.
    /// An unkeyed ancestor covers all entries beneath it. Endpoint and subscription
    /// identity belong to the caller and must be checked before using this method.
    #[must_use]
    pub fn covers(&self, other: &Self) -> bool {
        self.origin == other.origin
            && self.target == other.target
            && self.path.contains(&other.path)
    }
}

/// The identity of an emitted observation, independent of its value and source.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObservationIdentity {
    /// A numeric reading's signal key.
    Reading(SignalKey),

    /// A state observation's subject, optional facet, and name.
    State {
        /// The subject described by the state.
        subject: Subject,

        /// The signal facet; absence is distinct from every named facet.
        facet: Option<String>,

        /// The name of the observed state.
        name: String,
    },
}

/// An emitted observation and the concrete leaf path that supplied it.
///
/// Bindings are produced after value validation, duplicate handling, and collision
/// rejection. JSON object values bind to their expanded leaf paths.
///
/// ```
/// # use nv_telemetry_gnmi::SourceBinding;
/// # fn inspect(binding: &SourceBinding) {
/// let identity = binding.identity();
/// let source = binding.source();
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceBinding {
    pub(super) identity: ObservationIdentity,
    pub(super) source: SourcePath,
}

impl SourceBinding {
    /// The identity used in the notification's emitted readings or states.
    #[must_use]
    pub fn identity(&self) -> &ObservationIdentity {
        &self.identity
    }

    /// The resolved path that supplied the emitted value.
    #[must_use]
    pub fn source(&self) -> &SourcePath {
        &self.source
    }

    /// Consumes the binding into its observation identity and source path.
    #[must_use]
    pub fn into_parts(self) -> (ObservationIdentity, SourcePath) {
        (self.identity, self.source)
    }
}

/// A projected notification and the source facts needed for caller-owned cleanup.
///
/// Every emitted observation has 1 binding. Deletions identify paths that can affect
/// configured mappings; they do not assert which observations a consumer retains.
/// The caller scopes retained ownership by endpoint and subscription instance,
/// accepts the acquisition parts, then applies deletes before replacement updates.
///
/// ```
/// # use nv_telemetry_gnmi::ProjectedNotification;
/// # fn consume(notification: ProjectedNotification) {
/// let observed_at = notification.observed_at();
/// let (parts, bindings, deletions) = notification.into_parts();
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedNotification {
    pub(super) parts: AcquisitionParts,
    pub(super) bindings: Vec<SourceBinding>,
    pub(super) deletions: Vec<SourcePath>,
    pub(super) observed_at: Timestamp,
}

impl ProjectedNotification {
    /// The notification's observations and projection issues, before stamping.
    #[must_use]
    pub fn parts(&self) -> &AcquisitionParts {
        &self.parts
    }

    /// The source of each emitted observation; list order has no meaning.
    #[must_use]
    pub fn bindings(&self) -> &[SourceBinding] {
        &self.bindings
    }

    /// Relevant deleted paths, preserving their received order and repetitions.
    #[must_use]
    pub fn deletions(&self) -> &[SourcePath] {
        &self.deletions
    }

    /// The device timestamp shared by the notification's values and deletions.
    #[must_use]
    pub fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Separates parts for stamping from the caller's source ownership metadata.
    #[must_use]
    pub fn into_parts(self) -> (AcquisitionParts, Vec<SourceBinding>, Vec<SourcePath>) {
        (self.parts, self.bindings, self.deletions)
    }
}
