// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use nv_telemetry_model::limits;
use nv_telemetry_model::SignalDescriptor;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Value;

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

#[derive(Clone, Debug)]
pub(super) enum MappingKind {
    Reading {
        descriptor: SignalDescriptor,
        numeric_type: NumericType,
    },
    State {
        target: StateTarget,
        value_type: StateType,
    },
}

#[derive(Clone, Debug)]
pub(super) enum StateType {
    Enum(Vec<String>),
    Boolean,
}

impl StateType {
    fn validate(&mut self) -> Result<(), InvalidSubscription> {
        let Self::Enum(values) = self else {
            return Ok(());
        };

        if values.is_empty() {
            return Err(InvalidSubscription("enum values must be nonempty"));
        }

        let mut known = BTreeSet::new();

        for value in values.iter() {
            if !known.insert(value) || Value::string(value).is_err() {
                return Err(InvalidSubscription("invalid or duplicate enum value"));
            }
        }

        // Keep accepted strings sorted for projection's binary search.
        values.sort_unstable();
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(super) struct StateTarget {
    pub subject: Subject,
    pub name: String,
    pub facet: Option<String>,
}

/// A mapping from one exact leaf path to a numeric reading or a named state.
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

    /// Creates a state mapping that accepts only strings listed in `values`.
    ///
    /// Accepts native strings and scalar JSON or `JSON_IETF` strings. Matching
    /// is exact, and the provider preserves the received spelling. Unknown
    /// strings produce projection issues.
    /// [`SubscriptionSpec::new`] validates the state identity and rejects an
    /// empty list, duplicate entries, or strings exceeding the model's byte limit.
    #[must_use]
    pub fn enum_state(
        path: SubscriptionPath,
        subject: Subject,
        name: impl Into<String>,
        facet: Option<String>,
        values: Vec<String>,
    ) -> Self {
        Self {
            path,
            kind: MappingKind::State {
                target: StateTarget {
                    subject,
                    name: name.into(),
                    facet,
                },
                value_type: StateType::Enum(values),
            },
        }
    }

    /// Creates a state mapping that preserves boolean values, including `false`.
    ///
    /// Accepts native booleans and scalar JSON or `JSON_IETF` booleans. Strings and
    /// numbers are not converted to booleans.
    /// [`SubscriptionSpec::new`] validates the state identity.
    #[must_use]
    pub fn boolean_state(
        path: SubscriptionPath,
        subject: Subject,
        name: impl Into<String>,
        facet: Option<String>,
    ) -> Self {
        Self {
            path,
            kind: MappingKind::State {
                target: StateTarget {
                    subject,
                    name: name.into(),
                    facet,
                },
                value_type: StateType::Boolean,
            },
        }
    }
}

/// Validated request paths and signal mappings for one subscription.
///
/// Request paths select what the target sends. Mappings match exact leaf paths
/// after notification prefixes are applied. A parent request is supported when
/// the target sends updates for individual leaves. The provider does not traverse
/// JSON subtrees.
/// Validation checks the configuration without a YANG schema or target lookup.
#[derive(Clone, Debug)]
pub struct SubscriptionSpec {
    pub(super) mappings: Vec<Mapping>,
    requests: Vec<SubscriptionPath>,
    pub(super) origin: String,
    pub(super) target: String,
    encoding: Encoding,
}

impl SubscriptionSpec {
    /// Validates request paths and mappings before acquisition.
    ///
    /// Each mapping must be covered by a request path. Mapping paths, reading
    /// keys, and state identities `(subject, facet, name)` must each be unique.
    /// A reading and a state may describe the same signal.
    ///
    /// # Errors
    ///
    /// Returns an error for empty requests or mappings, invalid paths, mappings outside
    /// the requests, duplicate paths or output identities, or invalid state configuration.
    pub fn new(
        requests: Vec<SubscriptionPath>,
        mut mappings: Vec<Mapping>,
    ) -> Result<Self, InvalidSubscription> {
        if requests.is_empty() || mappings.is_empty() {
            return Err(InvalidSubscription(
                "requests and mappings must be nonempty",
            ));
        }

        if requests.iter().any(|path| !path.valid())
            || mappings.iter().any(|mapping| !mapping.path.valid())
        {
            return Err(InvalidSubscription(
                "paths must contain nonempty concrete elements and keys",
            ));
        }

        let mut paths = BTreeSet::new();
        let mut keys = BTreeSet::new();
        let mut state_keys = BTreeSet::new();

        for mapping in &mut mappings {
            if !paths.insert(&mapping.path) {
                return Err(InvalidSubscription("mapping paths must be unique"));
            }

            if !requests
                .iter()
                .any(|request| request.contains(&mapping.path))
            {
                return Err(InvalidSubscription(
                    "every mapping must be below a request path",
                ));
            }

            match &mut mapping.kind {
                MappingKind::Reading { descriptor, .. } => {
                    if !keys.insert(descriptor.key()) {
                        return Err(InvalidSubscription("reading signal keys must be unique"));
                    }
                }
                MappingKind::State { target, value_type } => {
                    if target.name.is_empty()
                        || target.name.len() > limits::STATEOBSERVATION_NAME_MAX_LEN as usize
                        || target.facet.as_ref().is_some_and(|facet| {
                            facet.is_empty()
                                || facet.len() > limits::STATEOBSERVATION_FACET_MAX_LEN as usize
                        })
                    {
                        return Err(InvalidSubscription("invalid state name or facet"));
                    }

                    // All updates in a notification share a timestamp. Different paths
                    // must not produce the same state identity at that timestamp.
                    if !state_keys.insert((&target.subject, &target.facet, &target.name)) {
                        return Err(InvalidSubscription("state identities must be unique"));
                    }

                    value_type.validate()?;
                }
            }
        }

        // Projection uses binary search to find exact mapping paths.
        mappings.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(Self {
            mappings,
            requests,
            origin: String::new(),
            target: String::new(),
            encoding: Encoding::default(),
        })
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
                    subscription: self
                        .requests
                        .iter()
                        .map(|path| gnmi::Subscription {
                            path: Some(path.wire()),
                            mode: gnmi::SubscriptionMode::TargetDefined.into(),
                            ..Default::default()
                        })
                        .collect(),
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
