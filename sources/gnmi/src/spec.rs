// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use nv_telemetry_model::Invalid;
use nv_telemetry_model::SignalDescriptor;
use nv_telemetry_model::SignalKey;
use nv_telemetry_model::StateObservation;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value;
use nv_telemetry_model::ValueRange;

use crate::protocol::gnmi;

/// The numeric representation required by a mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumericType {
    /// Native signed integers, or JSON integer numbers and quoted integers,
    /// parsed directly as `i64`.
    Int,

    /// Native unsigned integers, or JSON integer numbers and quoted integers,
    /// parsed directly as `u64`.
    Uint,

    /// Native float or double values, or JSON numbers and quoted numbers,
    /// parsed as finite `f64`.
    Double,
}

/// The requested JSON encoding. The provider accepts either encoding in responses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Encoding {
    /// Standard JSON.
    #[default]
    Json,

    /// JSON with IETF YANG encoding rules.
    JsonIetf,
}

/// One concrete path element. Keys select entries in a list.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PathElement {
    pub(super) name: String,
    pub(super) keys: BTreeMap<String, String>,
}

impl PathElement {
    /// Creates an element without keys. [`SubscriptionSpec::new`] validates the name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            keys: BTreeMap::new(),
        }
    }

    /// Sets a key, replacing any previous value for that key.
    #[must_use]
    pub fn with_key(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.keys.insert(key.into(), value.into());
        self
    }

    // Elements are compatible when names match and shared keys have equal values.
    fn compatible(&self, other: &Self) -> bool {
        self.name == other.name
            && self
                .keys
                .iter()
                .all(|(key, value)| other.keys.get(key).is_none_or(|other| other == value))
    }
}

/// An absolute path using the subscription's origin and target.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubscriptionPath(pub(super) Vec<PathElement>);

impl SubscriptionPath {
    /// Creates a path. [`SubscriptionSpec::new`] rejects empty paths and wildcards.
    #[must_use]
    pub fn new(elements: Vec<PathElement>) -> Self {
        Self(elements)
    }

    pub(super) fn contains(&self, other: &Self) -> bool {
        self.0.len() <= other.0.len()
            && self.0.iter().zip(&other.0).all(|(a, b)| {
                a.name == b.name
                    && a.keys
                        .iter()
                        .all(|(key, value)| b.keys.get(key) == Some(value))
            })
    }

    fn compatible_prefix(&self, other: &Self) -> bool {
        self.0.iter().zip(&other.0).all(|(a, b)| a.compatible(b))
    }

    fn valid(&self) -> bool {
        !self.0.is_empty()
            && self.0.iter().all(|element| {
                concrete(&element.name)
                    && element
                        .keys
                        .iter()
                        .all(|(key, value)| concrete(key) && concrete(value))
            })
    }

    fn wire(&self) -> gnmi::Path {
        gnmi::Path {
            elem: self
                .0
                .iter()
                .map(|element| gnmi::PathElem {
                    name: element.name.clone(),
                    key: element
                        .keys
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                })
                .collect(),
            ..Default::default()
        }
    }
}

/// A request path, subscription mode, and options for that mode.
///
/// [`SubscriptionSpec::from_requests`] validates the mode and options before acquisition.
/// The target decides which paths, modes, and intervals it supports.
///
/// ```
/// use std::time::Duration;
/// use nv_telemetry_gnmi::{PathElement, SubscriptionPath, SubscriptionRequest};
///
/// let path = SubscriptionPath::new(vec![PathElement::new("counter")]);
/// let request = SubscriptionRequest::sample(path, Duration::from_secs(10))
///     .with_suppress_redundant(true)
///     .with_heartbeat_interval(Duration::from_secs(60));
/// ```
#[derive(Clone, Debug)]
pub struct SubscriptionRequest {
    path: SubscriptionPath,
    mode: gnmi::SubscriptionMode,
    sample_interval: Duration,
    heartbeat_interval: Duration,
    suppress_redundant: bool,
}

impl SubscriptionRequest {
    /// Uses `TARGET_DEFINED` to let the target choose a subscription mode for each leaf.
    #[must_use]
    pub fn target_defined(path: SubscriptionPath) -> Self {
        Self {
            path,
            mode: gnmi::SubscriptionMode::TargetDefined,
            sample_interval: Duration::ZERO,
            heartbeat_interval: Duration::ZERO,
            suppress_redundant: false,
        }
    }

    /// Uses `ON_CHANGE` to request initial values, then updates when values change.
    #[must_use]
    pub fn on_change(path: SubscriptionPath) -> Self {
        Self {
            mode: gnmi::SubscriptionMode::OnChange,
            ..Self::target_defined(path)
        }
    }

    /// Uses `SAMPLE` to request periodic samples.
    ///
    /// Zero requests the target's fastest supported rate. The interval must fit in
    /// an unsigned 64-bit count of nanoseconds.
    #[must_use]
    pub fn sample(path: SubscriptionPath, interval: Duration) -> Self {
        Self {
            mode: gnmi::SubscriptionMode::Sample,
            sample_interval: interval,
            ..Self::target_defined(path)
        }
    }

    /// Requests that unchanged values be resent at this interval.
    ///
    /// Nonzero intervals require `ON_CHANGE` or `SAMPLE` mode and must fit in
    /// an unsigned 64-bit count of nanoseconds. Zero leaves the heartbeat unset.
    #[must_use]
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Requests that the target skip unchanged samples. The default is `false`.
    ///
    /// Enabling suppression requires `SAMPLE` mode. A configured heartbeat
    /// still requests values to be resent even when they have not changed.
    #[must_use]
    pub fn with_suppress_redundant(mut self, suppress: bool) -> Self {
        self.suppress_redundant = suppress;
        self
    }

    fn wire(self) -> Result<gnmi::Subscription, InvalidSubscription> {
        if self.mode == gnmi::SubscriptionMode::TargetDefined && !self.heartbeat_interval.is_zero()
        {
            return Err(InvalidSubscription(
                "heartbeat_interval requires ON_CHANGE or SAMPLE mode",
            ));
        }

        if self.mode != gnmi::SubscriptionMode::Sample && self.suppress_redundant {
            return Err(InvalidSubscription(
                "suppress_redundant requires SAMPLE mode",
            ));
        }

        let sample_interval = u64::try_from(self.sample_interval.as_nanos())
            .map_err(|_| InvalidSubscription("sample_interval exceeds u64 nanoseconds"))?;

        let heartbeat_interval = u64::try_from(self.heartbeat_interval.as_nanos())
            .map_err(|_| InvalidSubscription("heartbeat_interval exceeds u64 nanoseconds"))?;

        Ok(gnmi::Subscription {
            path: Some(self.path.wire()),
            mode: self.mode.into(),
            sample_interval,
            heartbeat_interval,
            suppress_redundant: self.suppress_redundant,
        })
    }
}

/// One subject component, supplied literally or taken from a received path key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityValue(Component);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Component {
    Literal(String),
    PathKey(KeySelector),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct KeySelector {
    // Position in the full path, including the notification prefix.
    element: usize,
    key: String,
}

impl IdentityValue {
    /// Creates a fixed subject component.
    #[must_use]
    pub fn literal(value: impl Into<String>) -> Self {
        Self(Component::Literal(value.into()))
    }

    /// Uses a key's value from the path element at the given zero-based position.
    ///
    /// Positions include notification prefix elements. [`SubscriptionSpec::new`]
    /// validates the selector.
    #[must_use]
    pub fn path_key(element: usize, key: impl Into<String>) -> Self {
        Self(Component::PathKey(KeySelector {
            element,
            key: key.into(),
        }))
    }
}

/// Defines how a collection mapping builds subjects from literals and path keys.
///
/// Scope components keep their configured order. At least one scope component
/// or the ID must select a path key. Values are preserved without normalization.
/// [`SubscriptionSpec::new`] validates literals and selectors. Selected values are
/// checked against the model's subject limits when projecting each update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectTemplate {
    kind: String,
    scope: Vec<IdentityValue>,
    id: IdentityValue,
}

impl SubjectTemplate {
    /// Creates a subject template without scope components.
    #[must_use]
    pub fn new(kind: impl Into<String>, id: IdentityValue) -> Self {
        Self {
            kind: kind.into(),
            scope: Vec::new(),
            id,
        }
    }

    /// Sets ordered scope components, replacing any previous scope.
    #[must_use]
    pub fn with_scope(mut self, scope: Vec<IdentityValue>) -> Self {
        self.scope = scope;
        self
    }

    pub(super) fn subject(&self, path: &SubscriptionPath) -> Result<Subject, &'static str> {
        self.build(|selector| {
            path.0
                .get(selector.element)
                .and_then(|element| element.keys.get(&selector.key))
                .cloned()
                .ok_or("subject key is missing from the received path")
        })
    }

    fn validate(&self, path: &SubscriptionPath) -> Result<Subject, InvalidSubscription> {
        if self.selectors().next().is_none() {
            return Err(InvalidSubscription(
                "collection subjects must select at least one path key",
            ));
        }

        // Received values are checked per update. A literal key filter or a
        // placeholder stands in for them so the model checks the configured parts.
        self.build(|selector| {
            if !concrete(&selector.key) {
                return Err("subject key name must be concrete and nonempty");
            }

            let element = path
                .0
                .get(selector.element)
                .ok_or("subject key element is outside the path")?;

            Ok(element
                .keys
                .get(&selector.key)
                .cloned()
                .unwrap_or_else(|| "key".into()))
        })
        .map_err(InvalidSubscription)
    }

    fn build(
        &self,
        key: impl Fn(&KeySelector) -> Result<String, &'static str>,
    ) -> Result<Subject, &'static str> {
        let component = |value: &IdentityValue| match &value.0 {
            Component::Literal(value) => Ok(value.clone()),
            Component::PathKey(selector) => key(selector),
        };

        let scope = self.scope.iter().map(component).collect::<Result<_, _>>()?;

        Subject::builder()
            .kind(self.kind.clone())
            .scope(scope)
            .id(component(&self.id)?)
            .build()
            .map_err(|_| "subject components are empty or exceed model limits")
    }

    fn selectors(&self) -> impl Iterator<Item = &KeySelector> {
        self.scope
            .iter()
            .chain(std::iter::once(&self.id))
            .filter_map(|value| match &value.0 {
                Component::Literal(_) => None,
                Component::PathKey(selector) => Some(selector),
            })
    }
}

/// Numeric type and descriptor metadata shared by a collection's readings.
///
/// [`SubscriptionSpec::new`] validates the metadata using the model's rules.
#[derive(Clone, Debug)]
pub struct ReadingTemplate {
    pub(super) numeric_type: NumericType,
    facet: Option<String>,
    kind: Option<String>,
    unit: Option<String>,
    range: Option<ValueRange>,
}

impl ReadingTemplate {
    /// Creates a template with the required numeric representation.
    #[must_use]
    pub fn new(numeric_type: NumericType) -> Self {
        Self {
            numeric_type,
            facet: None,
            kind: None,
            unit: None,
            range: None,
        }
    }

    /// Sets the signal facet on each derived subject.
    #[must_use]
    pub fn with_facet(mut self, facet: impl Into<String>) -> Self {
        self.facet = Some(facet.into());
        self
    }

    /// Sets the descriptor kind.
    #[must_use]
    pub fn with_kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    /// Sets the descriptor unit.
    #[must_use]
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// Sets the descriptor range.
    #[must_use]
    pub fn with_range(mut self, range: ValueRange) -> Self {
        self.range = Some(range);
        self
    }

    pub(super) fn descriptor(&self, subject: Subject) -> Result<SignalDescriptor, Invalid> {
        let mut key = SignalKey::builder().subject(subject);

        if let Some(facet) = &self.facet {
            key = key.facet(facet.clone());
        }

        let mut descriptor = SignalDescriptor::builder().key(key.build()?);

        if let Some(kind) = &self.kind {
            descriptor = descriptor.kind(kind.clone());
        }

        if let Some(unit) = &self.unit {
            descriptor = descriptor.unit(unit.clone());
        }

        if let Some(range) = &self.range {
            descriptor = descriptor.range(range.clone());
        }

        descriptor.build()
    }
}

/// A state name and optional facet shared by a collection's subjects.
///
/// [`SubscriptionSpec::new`] validates the name and facet.
#[derive(Clone, Debug)]
pub struct StateTemplate {
    name: String,
    facet: Option<String>,
}

impl StateTemplate {
    /// Creates a template for a named state.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            facet: None,
        }
    }

    /// Associates each state with one signal facet of its subject.
    #[must_use]
    pub fn with_facet(mut self, facet: impl Into<String>) -> Self {
        self.facet = Some(facet.into());
        self
    }

    pub(super) fn observation(
        &self,
        subject: Subject,
        value: Value,
        observed_at: Option<Timestamp>,
    ) -> Result<StateObservation, Invalid> {
        let mut builder = StateObservation::builder()
            .subject(subject)
            .name(self.name.clone())
            .value(value);

        if let Some(facet) = &self.facet {
            builder = builder.facet(facet.clone());
        }

        if let Some(observed_at) = observed_at {
            builder = builder.observed_at(observed_at);
        }

        builder.build()
    }
}

#[derive(Clone, Debug)]
pub(super) enum CollectionTarget {
    Reading(ReadingTemplate),
    State {
        template: StateTemplate,
        value_type: StateType,
    },
}

impl CollectionTarget {
    // Readings are keyed by facet, states by facet and name; the two never collide.
    fn same_identity(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Reading(a), Self::Reading(b)) => a.facet == b.facet,
            (Self::State { template: a, .. }, Self::State { template: b, .. }) => {
                a.facet == b.facet && a.name == b.name
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum MappingKind {
    Collection {
        subject: SubjectTemplate,
        target: CollectionTarget,
    },
    Reading {
        descriptor: SignalDescriptor,
        numeric_type: NumericType,
    },
    State {
        target: StateIdentity,
        value_type: StateType,
    },
}

#[derive(Clone, Copy, Debug)]
pub(super) enum StateType {
    String,
    Boolean,
}

/// The subject, name, and optional signal facet of a state observation.
///
/// [`SubscriptionSpec::new`] validates the identity using the model's rules.
#[derive(Clone, Debug)]
pub struct StateIdentity {
    pub(super) subject: Subject,
    pub(super) template: StateTemplate,
}

impl StateIdentity {
    /// Creates an identity for a named state of the subject.
    #[must_use]
    pub fn new(subject: Subject, name: impl Into<String>) -> Self {
        Self {
            subject,
            template: StateTemplate::new(name),
        }
    }

    /// Associates the state with one signal facet of the subject.
    #[must_use]
    pub fn with_facet(mut self, facet: impl Into<String>) -> Self {
        self.template = self.template.with_facet(facet);
        self
    }

    pub(super) fn observation(
        &self,
        value: Value,
        observed_at: Option<Timestamp>,
    ) -> Result<StateObservation, Invalid> {
        self.template
            .observation(self.subject.clone(), value, observed_at)
    }
}

/// A mapping from a leaf path to a numeric reading or a named state.
///
/// Exact and collection mappings match the complete sequence of element names.
/// Exact mappings also require all keys to match. Collection mappings treat configured
/// keys as literal filters and allow extra received keys. A collection mapping uses a
/// [`SubjectTemplate`] to build subjects. [`SubscriptionSpec::new`] validates mappings
/// before acquisition.
///
/// State mappings need no numeric descriptor. Their identity is
/// `(subject, facet, name)`. An absent facet is not a wildcard over the subject's
/// signals.
#[derive(Clone, Debug)]
pub struct Mapping {
    pub(super) path: SubscriptionPath,
    pub(super) kind: MappingKind,
}

impl Mapping {
    /// Creates a mapping using an already validated descriptor.
    #[must_use]
    pub fn new(
        path: SubscriptionPath,
        descriptor: SignalDescriptor,
        numeric_type: NumericType,
    ) -> Self {
        Self {
            path,
            kind: MappingKind::Reading {
                descriptor,
                numeric_type,
            },
        }
    }

    /// Creates a state mapping that preserves string values as received.
    ///
    /// Accepts native strings and scalar JSON or `JSON_IETF` strings. Values are
    /// not normalized or classified: identityref values keep module names, such as
    /// `openconfig-platform-types:ACTIVE` in `JSON_IETF`. Strings that exceed the
    /// model's limit produce an issue without an observation.
    /// [`SubscriptionSpec::new`] validates the state identity.
    #[must_use]
    pub fn string_state(path: SubscriptionPath, identity: StateIdentity) -> Self {
        Self {
            path,
            kind: MappingKind::State {
                target: identity,
                value_type: StateType::String,
            },
        }
    }

    /// Creates a state mapping that preserves boolean values, including `false`.
    ///
    /// Accepts native booleans and scalar JSON or `JSON_IETF` booleans. Strings and
    /// numbers are not converted to booleans.
    /// [`SubscriptionSpec::new`] validates the state identity.
    #[must_use]
    pub fn boolean_state(path: SubscriptionPath, identity: StateIdentity) -> Self {
        Self {
            path,
            kind: MappingKind::State {
                target: identity,
                value_type: StateType::Boolean,
            },
        }
    }

    /// Maps numeric leaves across list entries using subjects derived from path keys.
    #[must_use]
    pub fn collection_reading(
        path: SubscriptionPath,
        subject: SubjectTemplate,
        reading: ReadingTemplate,
    ) -> Self {
        Self {
            path,
            kind: MappingKind::Collection {
                subject,
                target: CollectionTarget::Reading(reading),
            },
        }
    }

    /// Maps string states across list entries, using the same rules as
    /// [`Self::string_state`].
    #[must_use]
    pub fn collection_string_state(
        path: SubscriptionPath,
        subject: SubjectTemplate,
        state: StateTemplate,
    ) -> Self {
        Self {
            path,
            kind: MappingKind::Collection {
                subject,
                target: CollectionTarget::State {
                    template: state,
                    value_type: StateType::String,
                },
            },
        }
    }

    /// Maps boolean states across list entries, preserving `false`.
    #[must_use]
    pub fn collection_boolean_state(
        path: SubscriptionPath,
        subject: SubjectTemplate,
        state: StateTemplate,
    ) -> Self {
        Self {
            path,
            kind: MappingKind::Collection {
                subject,
                target: CollectionTarget::State {
                    template: state,
                    value_type: StateType::Boolean,
                },
            },
        }
    }

    pub(super) fn covers(&self, ancestor: &SubscriptionPath) -> bool {
        if !matches!(self.kind, MappingKind::Collection { .. }) {
            return ancestor.contains(&self.path);
        }

        ancestor.0.len() <= self.path.0.len() && ancestor.compatible_prefix(&self.path)
    }

    // Collections with the same output template over compatible keyed elements
    // produce the same identity for every entry both receive.
    fn same_collection_identity(&self, other: &Self) -> bool {
        let (
            MappingKind::Collection { subject, target },
            MappingKind::Collection {
                subject: other_subject,
                target: other_target,
            },
        ) = (&self.kind, &other.kind)
        else {
            return false;
        };

        let keyed_prefix = subject
            .selectors()
            .map(|selector| selector.element + 1)
            .max()
            .unwrap_or_default();

        subject == other_subject
            && target.same_identity(other_target)
            && self
                .path
                .0
                .iter()
                .zip(&other.path.0)
                .take(keyed_prefix)
                .all(|(a, b)| a.compatible(b))
    }
}

/// Validated request paths and signal mappings for one subscription.
///
/// Request paths select what the target sends. Mappings match leaf paths
/// after notification prefixes are applied. A parent request is supported when
/// the target sends updates for individual leaves. The provider does not traverse
/// JSON subtrees.
/// Validation checks the configuration without a YANG schema or target lookup.
#[derive(Clone, Debug)]
pub struct SubscriptionSpec {
    pub(super) mappings: Vec<Mapping>,
    collections: BTreeMap<Vec<String>, Vec<usize>>,
    requests: Vec<gnmi::Subscription>,
    pub(super) origin: String,
    pub(super) target: String,
    encoding: Encoding,
}

impl SubscriptionSpec {
    /// Validates paths and mappings with `TARGET_DEFINED` mode for every path.
    ///
    /// Use [`Self::from_requests`] to configure subscription modes and their options per path.
    ///
    /// # Errors
    ///
    /// Returns an error if paths or mappings fail the checks in [`Self::from_requests`].
    pub fn new(
        requests: Vec<SubscriptionPath>,
        mappings: Vec<Mapping>,
    ) -> Result<Self, InvalidSubscription> {
        Self::from_requests(
            requests
                .into_iter()
                .map(SubscriptionRequest::target_defined)
                .collect(),
            mappings,
        )
    }

    /// Validates request paths, subscription modes, options, and mappings before acquisition.
    ///
    /// Requests keep their order, including duplicate and overlapping paths.
    /// Intervals are converted to nanoseconds during validation. The target decides
    /// whether it supports the requested paths, modes, and intervals.
    ///
    /// Each mapping must be covered by a request path, and mapping paths must not
    /// overlap. Fixed reading keys and state identities `(subject, facet, name)`
    /// must be unique. Collection mappings with the same subject template, facet,
    /// and state name are rejected unless literal key filters keep their entries
    /// apart. A reading and a state may describe the same signal.
    /// Collection subjects are checked again against received keys. Output identity
    /// collisions between concrete paths are reported when projecting a notification.
    ///
    /// # Errors
    ///
    /// Returns an error for empty requests or mappings, invalid paths, mappings
    /// outside the requests, overlapping mapping paths, duplicate output identities,
    /// invalid templates or state identities, options invalid for the selected mode,
    /// or intervals exceeding an unsigned 64-bit count of nanoseconds.
    pub fn from_requests(
        requests: Vec<SubscriptionRequest>,
        mut mappings: Vec<Mapping>,
    ) -> Result<Self, InvalidSubscription> {
        if requests.is_empty() || mappings.is_empty() {
            return Err(InvalidSubscription(
                "requests and mappings must be nonempty",
            ));
        }

        if requests.iter().any(|request| !request.path.valid())
            || mappings.iter().any(|mapping| !mapping.path.valid())
        {
            return Err(InvalidSubscription(
                "paths must contain nonempty concrete elements and keys",
            ));
        }

        let mut paths = BTreeSet::new();
        let mut keys = BTreeSet::new();
        let mut state_keys = BTreeSet::new();
        let mut collection_mappings: Vec<&Mapping> = Vec::new();

        for mapping in &mappings {
            if !paths.insert(&mapping.path) {
                return Err(InvalidSubscription("mapping paths must be unique"));
            }

            if !requests
                .iter()
                .any(|request| request.path.contains(&mapping.path))
            {
                return Err(InvalidSubscription(
                    "every mapping must be below a request path",
                ));
            }

            match &mapping.kind {
                MappingKind::Collection {
                    subject: subject_template,
                    target,
                } => {
                    let subject = subject_template.validate(&mapping.path)?;

                    match target {
                        CollectionTarget::Reading(template) => {
                            template
                                .descriptor(subject)
                                .map_err(|_| InvalidSubscription("invalid reading template"))?;
                        }
                        CollectionTarget::State { template, .. } => {
                            template
                                .observation(subject, Value::bool(false), None)
                                .map_err(|_| InvalidSubscription("invalid state template"))?;
                        }
                    }

                    if collection_mappings
                        .iter()
                        .any(|other| mapping.same_collection_identity(other))
                    {
                        return Err(InvalidSubscription(
                            "collection output identities must be unique",
                        ));
                    }

                    collection_mappings.push(mapping);
                }
                MappingKind::Reading { descriptor, .. } => {
                    if !keys.insert(descriptor.key()) {
                        return Err(InvalidSubscription("reading signal keys must be unique"));
                    }
                }
                MappingKind::State { target, .. } => {
                    target
                        .observation(Value::bool(false), None)
                        .map_err(|_| InvalidSubscription("invalid state identity"))?;

                    // All updates in a notification share a timestamp. Different paths
                    // must not produce the same state identity at that timestamp.
                    if !state_keys.insert((
                        &target.subject,
                        &target.template.facet,
                        &target.template.name,
                    )) {
                        return Err(InvalidSubscription("state identities must be unique"));
                    }
                }
            }
        }

        // Projection uses binary search to find exact mapping paths.
        mappings.sort_by(|a, b| a.path.cmp(&b.path));

        let collections = collection_index(&mappings)?;

        // Validate and encode subscription modes and options once, before any acquisition.
        let requests = requests
            .into_iter()
            .map(SubscriptionRequest::wire)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            collections,
            mappings,
            requests,
            origin: String::new(),
            target: String::new(),
            encoding: Encoding::default(),
        })
    }

    pub(super) fn mapping(&self, path: &SubscriptionPath) -> Option<&Mapping> {
        if let Some(mapping) = self
            .mappings
            .binary_search_by(|mapping| mapping.path.cmp(path))
            .ok()
            .and_then(|index| self.mappings.get(index))
        {
            return Some(mapping);
        }

        let names: Vec<_> = path.0.iter().map(|element| element.name.clone()).collect();

        self.collections
            .get(&names)?
            .iter()
            .filter_map(|index| self.mappings.get(*index))
            .find(|mapping| mapping.path.contains(path))
    }

    /// Sets the gNMI origin for all request paths. Empty leaves the origin unspecified.
    #[must_use]
    pub fn with_origin(mut self, origin: impl Into<String>) -> Self {
        self.origin = origin.into();
        self
    }

    /// Sets the gNMI target for all request paths. Empty leaves the target unspecified.
    #[must_use]
    pub fn with_target(mut self, target: impl Into<String>) -> Self {
        self.target = target.into();
        self
    }

    /// Selects the requested encoding; the default is [`Encoding::Json`].
    #[must_use]
    pub fn with_encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    pub(super) fn request(&self) -> gnmi::SubscribeRequest {
        gnmi::SubscribeRequest {
            request: Some(gnmi::subscribe_request::Request::Subscribe(
                gnmi::SubscriptionList {
                    prefix: Some(gnmi::Path {
                        origin: self.origin.clone(),
                        target: self.target.clone(),
                        ..Default::default()
                    }),
                    subscription: self.requests.clone(),
                    mode: gnmi::subscription_list::Mode::Stream.into(),
                    encoding: match self.encoding {
                        Encoding::Json => gnmi::Encoding::Json,
                        Encoding::JsonIetf => gnmi::Encoding::JsonIetf,
                    }
                    .into(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }
}

/// An invalid set of request paths or signal mappings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidSubscription(&'static str);

impl fmt::Display for InvalidSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for InvalidSubscription {}

fn concrete(value: &str) -> bool {
    !value.is_empty() && !value.contains('*') && value != "..."
}

fn collection_index(
    mappings: &[Mapping],
) -> Result<BTreeMap<Vec<String>, Vec<usize>>, InvalidSubscription> {
    let mut collections: BTreeMap<Vec<String>, Vec<usize>> = BTreeMap::new();

    for (index, mapping) in mappings.iter().enumerate() {
        if matches!(mapping.kind, MappingKind::Collection { .. }) {
            collections
                .entry(
                    mapping
                        .path
                        .0
                        .iter()
                        .map(|element| element.name.clone())
                        .collect(),
                )
                .or_default()
                .push(index);
        }
    }

    // Only collection rules can intersect different configured paths. Grouping
    // by element names avoids comparing unrelated exact rules pairwise.
    for (index, mapping) in mappings.iter().enumerate() {
        let names: Vec<_> = mapping
            .path
            .0
            .iter()
            .map(|element| element.name.clone())
            .collect();

        for other in collections
            .get(&names)
            .into_iter()
            .flatten()
            .filter_map(|other| (*other != index).then(|| mappings.get(*other)).flatten())
        {
            let overlaps = if matches!(mapping.kind, MappingKind::Collection { .. }) {
                mapping.path.compatible_prefix(&other.path)
            } else {
                other.path.contains(&mapping.path)
            };

            if overlaps {
                return Err(InvalidSubscription("mapping paths must not overlap"));
            }
        }
    }

    Ok(collections)
}
