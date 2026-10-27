// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use nv_telemetry_model::SignalDescriptor;

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

/// A mapping from one exact leaf path to a signal and numeric type.
#[derive(Clone, Debug)]
pub struct Mapping {
    pub(super) path: SubscriptionPath,
    pub(super) descriptor: SignalDescriptor,
    pub(super) numeric_type: NumericType,
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
            descriptor,
            numeric_type,
        }
    }
}

/// Validated request paths and signal mappings for one subscription.
///
/// Request paths select what the target sends. Mappings match exact leaf paths
/// after notification prefixes are applied. A parent request requires separate
/// updates for mapped leaves; JSON subtrees are not traversed.
/// Validation checks the configuration, without a YANG schema or target lookup.
#[derive(Clone, Debug)]
pub struct SubscriptionSpec {
    pub(super) mappings: Vec<Mapping>,
    requests: Vec<SubscriptionPath>,
    pub(super) origin: String,
    pub(super) target: String,
    encoding: Encoding,
}

impl SubscriptionSpec {
    /// Checks that every mapping is covered by a request path and has a unique
    /// path and signal key.
    ///
    /// # Errors
    ///
    /// Returns an error for empty requests or mappings, invalid paths, mappings outside
    /// the requests, or duplicate mapping paths or output signal identities.
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

        for mapping in &mappings {
            if !paths.insert(&mapping.path) || !keys.insert(mapping.descriptor.key()) {
                return Err(InvalidSubscription(
                    "mapping paths and output signal identities must be unique",
                ));
            }

            if !requests
                .iter()
                .any(|request| request.contains(&mapping.path))
            {
                return Err(InvalidSubscription(
                    "every mapping must be below a request path",
                ));
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
