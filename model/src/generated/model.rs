// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from `nv.telemetry.v1` by `make codegen`. Do not edit.
//!
//! The validated model. Every type here upholds the schema's invariants for
//! as long as it exists: construction is a builder whose `build` validates,
//! decoding is `TryFrom` over the wire type running the same `check`, and the
//! fields are private so no path around either exists. Encoding rebuilds the
//! wire form, which is also where canonicalization shows: what comes out is
//! the validated representation, not the bytes that arrived.
//!
//! Absence is reshaped away where the schema says so. A `required` field is a
//! plain value, a required oneof is an enum, and an enum field cannot carry
//! the unspecified value — a value this build does not recognize decodes as
//! `Unrecognized`, because a newer producer naming something real is not an
//! error.

// Generated code holds the line on correctness lints; the pedantic group is
// style advice for humans and is exactly where a clippy release breaks a
// checked-in file that no one edited.
#![allow(clippy::pedantic, dead_code)]

use std::collections::BTreeMap;

use crate::invalid;
use crate::rules;
use crate::value;
use crate::Invalid;
use crate::NumericValue;
use crate::Timestamp;
use crate::Value;
use crate::Violation;

use super::limits;
use super::wire;

/// Validated form of `nv.telemetry.v1.AcquisitionStatus.FailureClass`.
///
/// The unspecified value is unrepresentable: conversion rejects it, because
/// every `nv.telemetry.v1.AcquisitionStatus.FailureClass` field in the contract declares `reject_unspecified`. A value
/// newer than this build decodes as [`FailureClass::Unrecognized`] instead of
/// failing, so additive schema evolution does not break older consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum FailureClass {
    /// `FAILURE_CLASS_CONNECTIVITY`.
    Connectivity,
    /// `FAILURE_CLASS_AUTHENTICATION`.
    Authentication,
    /// `FAILURE_CLASS_TIMEOUT`.
    Timeout,
    /// `FAILURE_CLASS_PROTOCOL`.
    Protocol,
    /// `FAILURE_CLASS_UNSUPPORTED`.
    Unsupported,
    /// `FAILURE_CLASS_DEVICE`.
    Device,
    /// `FAILURE_CLASS_INTERNAL`.
    Internal,
    /// A value newer than this build. Interpreting it is the consumer's
    /// decision; re-encoding preserves it.
    Unrecognized(i32),
}
impl TryFrom<i32> for FailureClass {
    type Error = Violation;
    fn try_from(value: i32) -> Result<Self, Violation> {
        match value {
            0 => Err(Violation::Unspecified),
            1 => Ok(Self::Connectivity),
            2 => Ok(Self::Authentication),
            3 => Ok(Self::Timeout),
            4 => Ok(Self::Protocol),
            5 => Ok(Self::Unsupported),
            6 => Ok(Self::Device),
            7 => Ok(Self::Internal),
            other => Ok(Self::Unrecognized(other)),
        }
    }
}
impl From<FailureClass> for i32 {
    fn from(value: FailureClass) -> Self {
        match value {
            FailureClass::Connectivity => 1,
            FailureClass::Authentication => 2,
            FailureClass::Timeout => 3,
            FailureClass::Protocol => 4,
            FailureClass::Unsupported => 5,
            FailureClass::Device => 6,
            FailureClass::Internal => 7,
            FailureClass::Unrecognized(other) => other,
        }
    }
}
impl crate::canonical::Canonical for FailureClass {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        i32::from(*self).cmp(&i32::from(*other))
    }
}
impl FailureClass {
    /// Rejects an `Unrecognized` that aliases the unspecified value
    /// or a variant this build recognizes: its bytes would not
    /// round-trip — 0 is refused by decode outright, and an alias
    /// silently comes back as the recognized variant instead of
    /// itself.
    fn check(self) -> Result<(), Violation> {
        match self {
            Self::Unrecognized(value) => {
                match Self::try_from(value) {
                    Ok(Self::Unrecognized(_)) => Ok(()),
                    Ok(_) => {
                        Err(
                            Violation::Rule(
                                "an unrecognized value must not alias a recognized one",
                            ),
                        )
                    }
                    Err(violation) => Err(violation),
                }
            }
            _ => Ok(()),
        }
    }
}
/// Validated form of `nv.telemetry.v1.AcquisitionStatus.Outcome`.
///
/// The unspecified value is unrepresentable: conversion rejects it, because
/// every `nv.telemetry.v1.AcquisitionStatus.Outcome` field in the contract declares `reject_unspecified`. A value
/// newer than this build decodes as [`Outcome::Unrecognized`] instead of
/// failing, so additive schema evolution does not break older consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Outcome {
    /// `OUTCOME_SUCCEEDED`.
    Succeeded,
    /// `OUTCOME_FAILED`.
    Failed,
    /// A value newer than this build. Interpreting it is the consumer's
    /// decision; re-encoding preserves it.
    Unrecognized(i32),
}
impl TryFrom<i32> for Outcome {
    type Error = Violation;
    fn try_from(value: i32) -> Result<Self, Violation> {
        match value {
            0 => Err(Violation::Unspecified),
            1 => Ok(Self::Succeeded),
            2 => Ok(Self::Failed),
            other => Ok(Self::Unrecognized(other)),
        }
    }
}
impl From<Outcome> for i32 {
    fn from(value: Outcome) -> Self {
        match value {
            Outcome::Succeeded => 1,
            Outcome::Failed => 2,
            Outcome::Unrecognized(other) => other,
        }
    }
}
impl crate::canonical::Canonical for Outcome {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        i32::from(*self).cmp(&i32::from(*other))
    }
}
impl Outcome {
    /// Rejects an `Unrecognized` that aliases the unspecified value
    /// or a variant this build recognizes: its bytes would not
    /// round-trip — 0 is refused by decode outright, and an alias
    /// silently comes back as the recognized variant instead of
    /// itself.
    fn check(self) -> Result<(), Violation> {
        match self {
            Self::Unrecognized(value) => {
                match Self::try_from(value) {
                    Ok(Self::Unrecognized(_)) => Ok(()),
                    Ok(_) => {
                        Err(
                            Violation::Rule(
                                "an unrecognized value must not alias a recognized one",
                            ),
                        )
                    }
                    Err(violation) => Err(violation),
                }
            }
            _ => Ok(()),
        }
    }
}
/// Validated form of `nv.telemetry.v1.Completeness`.
///
/// The unspecified value is unrepresentable: conversion rejects it, because
/// every `nv.telemetry.v1.Completeness` field in the contract declares `reject_unspecified`. A value
/// newer than this build decodes as [`Completeness::Unrecognized`] instead of
/// failing, so additive schema evolution does not break older consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Completeness {
    /// `COMPLETENESS_COMPLETE`.
    Complete,
    /// `COMPLETENESS_PARTIAL`.
    Partial,
    /// A value newer than this build. Interpreting it is the consumer's
    /// decision; re-encoding preserves it.
    Unrecognized(i32),
}
impl TryFrom<i32> for Completeness {
    type Error = Violation;
    fn try_from(value: i32) -> Result<Self, Violation> {
        match value {
            0 => Err(Violation::Unspecified),
            1 => Ok(Self::Complete),
            2 => Ok(Self::Partial),
            other => Ok(Self::Unrecognized(other)),
        }
    }
}
impl From<Completeness> for i32 {
    fn from(value: Completeness) -> Self {
        match value {
            Completeness::Complete => 1,
            Completeness::Partial => 2,
            Completeness::Unrecognized(other) => other,
        }
    }
}
impl crate::canonical::Canonical for Completeness {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        i32::from(*self).cmp(&i32::from(*other))
    }
}
impl Completeness {
    /// Rejects an `Unrecognized` that aliases the unspecified value
    /// or a variant this build recognizes: its bytes would not
    /// round-trip — 0 is refused by decode outright, and an alias
    /// silently comes back as the recognized variant instead of
    /// itself.
    fn check(self) -> Result<(), Violation> {
        match self {
            Self::Unrecognized(value) => {
                match Self::try_from(value) {
                    Ok(Self::Unrecognized(_)) => Ok(()),
                    Ok(_) => {
                        Err(
                            Violation::Rule(
                                "an unrecognized value must not alias a recognized one",
                            ),
                        )
                    }
                    Err(violation) => Err(violation),
                }
            }
            _ => Ok(()),
        }
    }
}
/// Validated form of `nv.telemetry.v1.ProjectionIssue.IssueKind`.
///
/// The unspecified value is unrepresentable: conversion rejects it, because
/// every `nv.telemetry.v1.ProjectionIssue.IssueKind` field in the contract declares `reject_unspecified`. A value
/// newer than this build decodes as [`IssueKind::Unrecognized`] instead of
/// failing, so additive schema evolution does not break older consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum IssueKind {
    /// `ISSUE_KIND_MISSING_REQUIRED`.
    MissingRequired,
    /// `ISSUE_KIND_INVALID`.
    Invalid,
    /// A value newer than this build. Interpreting it is the consumer's
    /// decision; re-encoding preserves it.
    Unrecognized(i32),
}
impl TryFrom<i32> for IssueKind {
    type Error = Violation;
    fn try_from(value: i32) -> Result<Self, Violation> {
        match value {
            0 => Err(Violation::Unspecified),
            1 => Ok(Self::MissingRequired),
            2 => Ok(Self::Invalid),
            other => Ok(Self::Unrecognized(other)),
        }
    }
}
impl From<IssueKind> for i32 {
    fn from(value: IssueKind) -> Self {
        match value {
            IssueKind::MissingRequired => 1,
            IssueKind::Invalid => 2,
            IssueKind::Unrecognized(other) => other,
        }
    }
}
impl crate::canonical::Canonical for IssueKind {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        i32::from(*self).cmp(&i32::from(*other))
    }
}
impl IssueKind {
    /// Rejects an `Unrecognized` that aliases the unspecified value
    /// or a variant this build recognizes: its bytes would not
    /// round-trip — 0 is refused by decode outright, and an alias
    /// silently comes back as the recognized variant instead of
    /// itself.
    fn check(self) -> Result<(), Violation> {
        match self {
            Self::Unrecognized(value) => {
                match Self::try_from(value) {
                    Ok(Self::Unrecognized(_)) => Ok(()),
                    Ok(_) => {
                        Err(
                            Violation::Rule(
                                "an unrecognized value must not alias a recognized one",
                            ),
                        )
                    }
                    Err(violation) => Err(violation),
                }
            }
            _ => Ok(()),
        }
    }
}
/// Validated form of `nv.telemetry.v1.Severity`.
///
/// The unspecified value is unrepresentable: conversion rejects it, because
/// every `nv.telemetry.v1.Severity` field in the contract declares `reject_unspecified`. A value
/// newer than this build decodes as [`Severity::Unrecognized`] instead of
/// failing, so additive schema evolution does not break older consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Severity {
    /// `SEVERITY_DEBUG`.
    Debug,
    /// `SEVERITY_INFO`.
    Info,
    /// `SEVERITY_WARNING`.
    Warning,
    /// `SEVERITY_ERROR`.
    Error,
    /// `SEVERITY_CRITICAL`.
    Critical,
    /// A value newer than this build. Interpreting it is the consumer's
    /// decision; re-encoding preserves it.
    Unrecognized(i32),
}
impl TryFrom<i32> for Severity {
    type Error = Violation;
    fn try_from(value: i32) -> Result<Self, Violation> {
        match value {
            0 => Err(Violation::Unspecified),
            1 => Ok(Self::Debug),
            2 => Ok(Self::Info),
            3 => Ok(Self::Warning),
            4 => Ok(Self::Error),
            5 => Ok(Self::Critical),
            other => Ok(Self::Unrecognized(other)),
        }
    }
}
impl From<Severity> for i32 {
    fn from(value: Severity) -> Self {
        match value {
            Severity::Debug => 1,
            Severity::Info => 2,
            Severity::Warning => 3,
            Severity::Error => 4,
            Severity::Critical => 5,
            Severity::Unrecognized(other) => other,
        }
    }
}
impl crate::canonical::Canonical for Severity {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        i32::from(*self).cmp(&i32::from(*other))
    }
}
impl Severity {
    /// Rejects an `Unrecognized` that aliases the unspecified value
    /// or a variant this build recognizes: its bytes would not
    /// round-trip — 0 is refused by decode outright, and an alias
    /// silently comes back as the recognized variant instead of
    /// itself.
    fn check(self) -> Result<(), Violation> {
        match self {
            Self::Unrecognized(value) => {
                match Self::try_from(value) {
                    Ok(Self::Unrecognized(_)) => Ok(()),
                    Ok(_) => {
                        Err(
                            Violation::Rule(
                                "an unrecognized value must not alias a recognized one",
                            ),
                        )
                    }
                    Err(violation) => Err(violation),
                }
            }
            _ => Ok(()),
        }
    }
}
/// Validated form of `nv.telemetry.v1.AcquisitionStatus`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`AcquisitionStatusBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquisitionStatus {
    endpoint_id: String,
    provider: String,
    request_class: String,
    outcome: Outcome,
    failure_class: Option<FailureClass>,
    retryable: Option<bool>,
    started_at: Timestamp,
    duration_nanos: Option<u64>,
    detail: Option<String>,
}
impl crate::canonical::Canonical for AcquisitionStatus {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.endpoint_id
            .cmp(&other.endpoint_id)
            .then_with(|| self.provider.cmp(&other.provider))
            .then_with(|| self.request_class.cmp(&other.request_class))
            .then_with(|| i32::from(self.outcome).cmp(&i32::from(other.outcome)))
            .then_with(|| crate::canonical::cmp_option(
                self.failure_class.as_ref(),
                other.failure_class.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.retryable.as_ref(),
                other.retryable.as_ref(),
            ))
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.started_at,
                &other.started_at,
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.duration_nanos.as_ref(),
                other.duration_nanos.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.detail.as_ref(),
                other.detail.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for AcquisitionStatus {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::str_value(state, &self.endpoint_id);
        crate::canonical::tag(state, 2);
        crate::canonical::str_value(state, &self.provider);
        crate::canonical::tag(state, 3);
        crate::canonical::str_value(state, &self.request_class);
        crate::canonical::tag(state, 4);
        crate::canonical::i32_value(state, i32::from(self.outcome));
        if let Some(value) = self.failure_class {
            crate::canonical::tag(state, 5);
            crate::canonical::i32_value(state, i32::from(value));
        }
        if let Some(value) = self.retryable {
            crate::canonical::tag(state, 6);
            crate::canonical::bool_value(state, value);
        }
        crate::canonical::tag(state, 7);
        crate::canonical::Digest::digest(&self.started_at, state);
        if let Some(value) = self.duration_nanos {
            crate::canonical::tag(state, 8);
            crate::canonical::u64_value(state, value);
        }
        if let Some(element) = &self.detail {
            crate::canonical::tag(state, 9);
            crate::canonical::str_value(state, element);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for AcquisitionStatus {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        ::prost::encoding::string::encode(1, &self.endpoint_id, buf);
        ::prost::encoding::string::encode(2, &self.provider, buf);
        ::prost::encoding::string::encode(3, &self.request_class, buf);
        ::prost::encoding::int32::encode(4, &i32::from(self.outcome), buf);
        if let Some(value) = self.failure_class {
            ::prost::encoding::int32::encode(5, &i32::from(value), buf);
        }
        if let Some(value) = &self.retryable {
            ::prost::encoding::bool::encode(6, value, buf);
        }
        crate::encode::nested(7, &self.started_at, buf);
        if let Some(value) = &self.duration_nanos {
            ::prost::encoding::uint64::encode(8, value, buf);
        }
        if let Some(element) = &self.detail {
            ::prost::encoding::string::encode(9, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        ::prost::encoding::string::encoded_len(1, &self.endpoint_id)
            + ::prost::encoding::string::encoded_len(2, &self.provider)
            + ::prost::encoding::string::encoded_len(3, &self.request_class)
            + ::prost::encoding::int32::encoded_len(4, &i32::from(self.outcome))
            + self
                .failure_class
                .map_or(
                    0,
                    |value| ::prost::encoding::int32::encoded_len(5, &i32::from(value)),
                )
            + self
                .retryable
                .as_ref()
                .map_or(0, |value| ::prost::encoding::bool::encoded_len(6, value))
            + crate::encode::nested_len(7, &self.started_at)
            + self
                .duration_nanos
                .as_ref()
                .map_or(0, |value| ::prost::encoding::uint64::encoded_len(8, value))
            + self
                .detail
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(9, element))
    }
}
impl AcquisitionStatus {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> AcquisitionStatusBuilder {
        AcquisitionStatusBuilder::default()
    }
    /// The `endpoint_id`.
    #[must_use]
    pub fn endpoint_id(&self) -> &str {
        &self.endpoint_id
    }
    /// The `provider`.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }
    /// The `request_class`.
    #[must_use]
    pub fn request_class(&self) -> &str {
        &self.request_class
    }
    /// The `outcome`.
    #[must_use]
    pub fn outcome(&self) -> Outcome {
        self.outcome
    }
    /// The `failure_class`, when present.
    #[must_use]
    pub fn failure_class(&self) -> Option<FailureClass> {
        self.failure_class
    }
    /// The `retryable`, when present.
    #[must_use]
    pub fn retryable(&self) -> Option<bool> {
        self.retryable
    }
    /// The `started_at`.
    #[must_use]
    pub fn started_at(&self) -> &Timestamp {
        &self.started_at
    }
    /// The `duration_nanos`, when present.
    #[must_use]
    pub fn duration_nanos(&self) -> Option<u64> {
        self.duration_nanos
    }
    /// The `detail`, when present.
    #[must_use]
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.endpoint_id.is_empty() {
            return Err(Invalid::field("endpoint_id", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.endpoint_id.len(),
            limits::ACQUISITIONSTATUS_ENDPOINT_ID_MAX_LEN,
        ) {
            return Err(Invalid::field("endpoint_id", violation));
        }
        if self.provider.is_empty() {
            return Err(Invalid::field("provider", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.provider.len(),
            limits::ACQUISITIONSTATUS_PROVIDER_MAX_LEN,
        ) {
            return Err(Invalid::field("provider", violation));
        }
        if self.request_class.is_empty() {
            return Err(Invalid::field("request_class", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.request_class.len(),
            limits::ACQUISITIONSTATUS_REQUEST_CLASS_MAX_LEN,
        ) {
            return Err(Invalid::field("request_class", violation));
        }
        self.outcome.check().map_err(|violation| Invalid::field("outcome", violation))?;
        if let Some(value) = self.failure_class {
            value
                .check()
                .map_err(|violation| Invalid::field("failure_class", violation))?;
        }
        if let Some(element) = &self.detail {
            if element.is_empty() {
                return Err(Invalid::field("detail", Violation::Empty));
            }
        }
        if let Some(element) = &self.detail {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::ACQUISITIONSTATUS_DETAIL_MAX_LEN,
            ) {
                return Err(Invalid::field("detail", violation));
            }
        }
        rules::acquisition_status(self)?;
        Ok(())
    }
    /// Decodes and validates from wire bytes.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Malformed`](crate::DecodeError) when the bytes are not
    /// protobuf, [`DecodeError::Invalid`](crate::DecodeError) when they decode
    /// but break the contract.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::DecodeError> {
        let wire = <wire::AcquisitionStatus as ::prost::Message>::decode(bytes)
            .map_err(crate::DecodeError::Malformed)?;
        Self::try_from(wire).map_err(crate::DecodeError::Invalid)
    }
    /// Encodes the canonical wire form, straight from the validated
    /// representation — no intermediate wire tree, no clone. Byte-identical
    /// to prost encoding the rebuilt tree, which the tests hold it to.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(crate::encode::Emit::emitted_len(self));
        crate::encode::Emit::emit(self, &mut buf);
        buf
    }
}
/// Builds a [`AcquisitionStatus`]. Setters are infallible; [`build`](AcquisitionStatusBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct AcquisitionStatusBuilder {
    endpoint_id: Option<String>,
    provider: Option<String>,
    request_class: Option<String>,
    outcome: Option<Outcome>,
    failure_class: Option<FailureClass>,
    retryable: Option<bool>,
    started_at: Option<Timestamp>,
    duration_nanos: Option<u64>,
    detail: Option<String>,
}
impl AcquisitionStatusBuilder {
    /// Sets `endpoint_id`.
    #[must_use]
    pub fn endpoint_id(mut self, endpoint_id: impl Into<String>) -> Self {
        self.endpoint_id = Some(endpoint_id.into());
        self
    }
    /// Sets `provider`.
    #[must_use]
    pub fn provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }
    /// Sets `request_class`.
    #[must_use]
    pub fn request_class(mut self, request_class: impl Into<String>) -> Self {
        self.request_class = Some(request_class.into());
        self
    }
    /// Sets `outcome`.
    #[must_use]
    pub fn outcome(mut self, outcome: Outcome) -> Self {
        self.outcome = Some(outcome);
        self
    }
    /// Sets `failure_class`.
    #[must_use]
    pub fn failure_class(mut self, failure_class: FailureClass) -> Self {
        self.failure_class = Some(failure_class);
        self
    }
    /// Sets `retryable`.
    #[must_use]
    pub fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = Some(retryable);
        self
    }
    /// Sets `started_at`.
    #[must_use]
    pub fn started_at(mut self, started_at: Timestamp) -> Self {
        self.started_at = Some(started_at);
        self
    }
    /// Sets `duration_nanos`.
    #[must_use]
    pub fn duration_nanos(mut self, duration_nanos: u64) -> Self {
        self.duration_nanos = Some(duration_nanos);
        self
    }
    /// Sets `detail`.
    #[must_use]
    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<AcquisitionStatus, Invalid> {
        let built = AcquisitionStatus {
            endpoint_id: self
                .endpoint_id
                .ok_or_else(|| Invalid::field("endpoint_id", Violation::Absent))?,
            provider: self
                .provider
                .ok_or_else(|| Invalid::field("provider", Violation::Absent))?,
            request_class: self
                .request_class
                .ok_or_else(|| Invalid::field("request_class", Violation::Absent))?,
            outcome: self
                .outcome
                .ok_or_else(|| Invalid::field("outcome", Violation::Absent))?,
            failure_class: self.failure_class,
            retryable: self.retryable,
            started_at: self
                .started_at
                .ok_or_else(|| Invalid::field("started_at", Violation::Absent))?,
            duration_nanos: self.duration_nanos,
            detail: self.detail,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::AcquisitionStatus> for AcquisitionStatus {
    type Error = Invalid;
    fn try_from(wire: wire::AcquisitionStatus) -> Result<Self, Invalid> {
        let built = Self {
            endpoint_id: wire
                .endpoint_id
                .ok_or_else(|| Invalid::field("endpoint_id", Violation::Absent))?,
            provider: wire
                .provider
                .ok_or_else(|| Invalid::field("provider", Violation::Absent))?,
            request_class: wire
                .request_class
                .ok_or_else(|| Invalid::field("request_class", Violation::Absent))?,
            outcome: Outcome::try_from(
                    wire
                        .outcome
                        .ok_or_else(|| Invalid::field("outcome", Violation::Absent))?,
                )
                .map_err(|violation| Invalid::field("outcome", violation))?,
            failure_class: wire
                .failure_class
                .map(FailureClass::try_from)
                .transpose()
                .map_err(|violation| Invalid::field("failure_class", violation))?,
            retryable: wire.retryable,
            started_at: Timestamp::try_from(
                    wire
                        .started_at
                        .ok_or_else(|| Invalid::field("started_at", Violation::Absent))?,
                )
                .map_err(|error| error.at("started_at"))?,
            duration_nanos: wire.duration_nanos,
            detail: wire.detail,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<AcquisitionStatus> for wire::AcquisitionStatus {
    fn from(value: AcquisitionStatus) -> Self {
        Self {
            endpoint_id: Some(value.endpoint_id),
            provider: Some(value.provider),
            request_class: Some(value.request_class),
            outcome: Some(value.outcome.into()),
            failure_class: value.failure_class.map(Into::into),
            retryable: value.retryable,
            started_at: Some(value.started_at.into()),
            duration_nanos: value.duration_nanos,
            detail: value.detail,
        }
    }
}
/// Validated form of `nv.telemetry.v1.Coverage`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`CoverageBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coverage {
    completeness: Completeness,
    scope: Option<Subject>,
}
impl crate::canonical::Canonical for Coverage {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        i32::from(self.completeness)
            .cmp(&i32::from(other.completeness))
            .then_with(|| crate::canonical::cmp_option(
                self.scope.as_ref(),
                other.scope.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for Coverage {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::i32_value(state, i32::from(self.completeness));
        if let Some(element) = &self.scope {
            crate::canonical::tag(state, 2);
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for Coverage {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        ::prost::encoding::int32::encode(1, &i32::from(self.completeness), buf);
        if let Some(element) = &self.scope {
            crate::encode::nested(2, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        ::prost::encoding::int32::encoded_len(1, &i32::from(self.completeness))
            + self
                .scope
                .as_ref()
                .map_or(0, |element| crate::encode::nested_len(2, element))
    }
}
impl Coverage {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> CoverageBuilder {
        CoverageBuilder::default()
    }
    /// The `completeness`.
    #[must_use]
    pub fn completeness(&self) -> Completeness {
        self.completeness
    }
    /// The `scope`, when present.
    #[must_use]
    pub fn scope(&self) -> Option<&Subject> {
        self.scope.as_ref()
    }
    fn check(&self) -> Result<(), Invalid> {
        self.completeness
            .check()
            .map_err(|violation| Invalid::field("completeness", violation))?;
        rules::coverage(self)?;
        Ok(())
    }
}
/// Builds a [`Coverage`]. Setters are infallible; [`build`](CoverageBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct CoverageBuilder {
    completeness: Option<Completeness>,
    scope: Option<Subject>,
}
impl CoverageBuilder {
    /// Sets `completeness`.
    #[must_use]
    pub fn completeness(mut self, completeness: Completeness) -> Self {
        self.completeness = Some(completeness);
        self
    }
    /// Sets `scope`.
    #[must_use]
    pub fn scope(mut self, scope: Subject) -> Self {
        self.scope = Some(scope);
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<Coverage, Invalid> {
        let built = Coverage {
            completeness: self
                .completeness
                .ok_or_else(|| Invalid::field("completeness", Violation::Absent))?,
            scope: self.scope,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::Coverage> for Coverage {
    type Error = Invalid;
    fn try_from(wire: wire::Coverage) -> Result<Self, Invalid> {
        let built = Self {
            completeness: Completeness::try_from(
                    wire
                        .completeness
                        .ok_or_else(|| Invalid::field(
                            "completeness",
                            Violation::Absent,
                        ))?,
                )
                .map_err(|violation| Invalid::field("completeness", violation))?,
            scope: wire
                .scope
                .map(Subject::try_from)
                .transpose()
                .map_err(|error| error.at("scope"))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<Coverage> for wire::Coverage {
    fn from(value: Coverage) -> Self {
        Self {
            completeness: Some(value.completeness.into()),
            scope: value.scope.map(Into::into),
        }
    }
}
/// Validated form of `nv.telemetry.v1.EndpointContext`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`EndpointContextBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointContext {
    endpoint_id: String,
    attributes: Option<BTreeMap<String, Value>>,
}
impl crate::canonical::Canonical for EndpointContext {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.endpoint_id
            .cmp(&other.endpoint_id)
            .then_with(|| crate::canonical::cmp_option_map(
                self.attributes.as_ref(),
                other.attributes.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for EndpointContext {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::str_value(state, &self.endpoint_id);
        if let Some(map) = &self.attributes {
            crate::canonical::tag(state, 2);
            crate::canonical::map_value(state, map);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for EndpointContext {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        ::prost::encoding::string::encode(1, &self.endpoint_id, buf);
        if let Some(map) = &self.attributes {
            crate::encode::map_field(2, map, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        ::prost::encoding::string::encoded_len(1, &self.endpoint_id)
            + self
                .attributes
                .as_ref()
                .map_or(0, |map| crate::encode::map_field_len(2, map))
    }
}
impl EndpointContext {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> EndpointContextBuilder {
        EndpointContextBuilder::default()
    }
    /// The `endpoint_id`.
    #[must_use]
    pub fn endpoint_id(&self) -> &str {
        &self.endpoint_id
    }
    /// The `attributes`, when present.
    #[must_use]
    pub fn attributes(&self) -> Option<&BTreeMap<String, Value>> {
        self.attributes.as_ref()
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.endpoint_id.is_empty() {
            return Err(Invalid::field("endpoint_id", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.endpoint_id.len(),
            limits::ENDPOINTCONTEXT_ENDPOINT_ID_MAX_LEN,
        ) {
            return Err(Invalid::field("endpoint_id", violation));
        }
        if let Some(map) = &self.attributes {
            value::check_map(map, "attributes")?;
        }
        rules::endpoint_context(self)?;
        Ok(())
    }
}
/// Builds a [`EndpointContext`]. Setters are infallible; [`build`](EndpointContextBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct EndpointContextBuilder {
    endpoint_id: Option<String>,
    attributes: Option<BTreeMap<String, Value>>,
}
impl EndpointContextBuilder {
    /// Sets `endpoint_id`.
    #[must_use]
    pub fn endpoint_id(mut self, endpoint_id: impl Into<String>) -> Self {
        self.endpoint_id = Some(endpoint_id.into());
        self
    }
    /// Sets `attributes`.
    #[must_use]
    pub fn attributes(mut self, attributes: BTreeMap<String, Value>) -> Self {
        self.attributes = Some(attributes);
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<EndpointContext, Invalid> {
        let built = EndpointContext {
            endpoint_id: self
                .endpoint_id
                .ok_or_else(|| Invalid::field("endpoint_id", Violation::Absent))?,
            attributes: self.attributes,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::EndpointContext> for EndpointContext {
    type Error = Invalid;
    fn try_from(wire: wire::EndpointContext) -> Result<Self, Invalid> {
        let built = Self {
            endpoint_id: wire
                .endpoint_id
                .ok_or_else(|| Invalid::field("endpoint_id", Violation::Absent))?,
            attributes: wire
                .attributes
                .map(value::map_from_wire)
                .transpose()
                .map_err(|error| error.at("attributes"))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<EndpointContext> for wire::EndpointContext {
    fn from(value: EndpointContext) -> Self {
        Self {
            endpoint_id: Some(value.endpoint_id),
            attributes: value.attributes.map(value::map_into_wire),
        }
    }
}
/// Validated form of `nv.telemetry.v1.Inventory`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`InventoryBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inventory {
    items: Vec<InventoryItem>,
}
impl crate::canonical::Canonical for Inventory {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::cmp_slice(&self.items, &other.items)
    }
}
impl crate::canonical::Digest for Inventory {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::count(state, self.items.len());
        for element in &self.items {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for Inventory {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        for element in &self.items {
            crate::encode::nested(1, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        self.items
            .iter()
            .map(|element| crate::encode::nested_len(1, element))
            .sum::<usize>()
    }
}
impl Inventory {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> InventoryBuilder {
        InventoryBuilder::default()
    }
    /// The `items`.
    #[must_use]
    pub fn items(&self) -> &[InventoryItem] {
        &self.items
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(violation) = invalid::too_many(
            self.items.len(),
            limits::INVENTORY_ITEMS_MAX_ITEMS,
        ) {
            return Err(Invalid::field("items", violation));
        }
        for index in 1..self.items.len() {
            if self.items[index].subject == self.items[index - 1].subject {
                return Err(Invalid::element("items", index, Violation::Duplicate));
            }
        }
        rules::inventory(self)?;
        Ok(())
    }
    fn canonicalize(&mut self) {
        self.items.sort_by(crate::canonical::Canonical::canonical_cmp);
    }
}
/// Builds a [`Inventory`]. Setters are infallible; [`build`](InventoryBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct InventoryBuilder {
    items: Vec<InventoryItem>,
}
impl InventoryBuilder {
    /// Sets `items`.
    #[must_use]
    pub fn items(mut self, items: Vec<InventoryItem>) -> Self {
        self.items = items;
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<Inventory, Invalid> {
        let mut built = Inventory { items: self.items };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::Inventory> for Inventory {
    type Error = Invalid;
    fn try_from(wire: wire::Inventory) -> Result<Self, Invalid> {
        let mut built = Self {
            items: {
                let mut elements = Vec::with_capacity(wire.items.len());
                for (index, element) in wire.items.into_iter().enumerate() {
                    elements
                        .push(
                            InventoryItem::try_from(element)
                                .map_err(|error| error.at_index("items", index))?,
                        );
                }
                elements
            },
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl From<Inventory> for wire::Inventory {
    fn from(value: Inventory) -> Self {
        Self {
            items: value.items.into_iter().map(Into::into).collect(),
        }
    }
}
/// Validated form of `nv.telemetry.v1.InventoryItem`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`InventoryItemBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InventoryItem {
    subject: Subject,
    attributes: Option<BTreeMap<String, Value>>,
    source_key: Option<String>,
}
impl crate::canonical::Canonical for InventoryItem {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.subject, &other.subject)
            .then_with(|| crate::canonical::cmp_option_map(
                self.attributes.as_ref(),
                other.attributes.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.source_key.as_ref(),
                other.source_key.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for InventoryItem {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.subject, state);
        if let Some(map) = &self.attributes {
            crate::canonical::tag(state, 2);
            crate::canonical::map_value(state, map);
        }
        if let Some(element) = &self.source_key {
            crate::canonical::tag(state, 3);
            crate::canonical::str_value(state, element);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for InventoryItem {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.subject, buf);
        if let Some(map) = &self.attributes {
            crate::encode::map_field(2, map, buf);
        }
        if let Some(element) = &self.source_key {
            ::prost::encoding::string::encode(3, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.subject)
            + self
                .attributes
                .as_ref()
                .map_or(0, |map| crate::encode::map_field_len(2, map))
            + self
                .source_key
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(3, element))
    }
}
impl InventoryItem {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> InventoryItemBuilder {
        InventoryItemBuilder::default()
    }
    /// The `subject`.
    #[must_use]
    pub fn subject(&self) -> &Subject {
        &self.subject
    }
    /// The `attributes`, when present.
    #[must_use]
    pub fn attributes(&self) -> Option<&BTreeMap<String, Value>> {
        self.attributes.as_ref()
    }
    /// The `source_key`, when present.
    #[must_use]
    pub fn source_key(&self) -> Option<&str> {
        self.source_key.as_deref()
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(map) = &self.attributes {
            value::check_map(map, "attributes")?;
        }
        if let Some(element) = &self.source_key {
            if element.is_empty() {
                return Err(Invalid::field("source_key", Violation::Empty));
            }
        }
        if let Some(element) = &self.source_key {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::INVENTORYITEM_SOURCE_KEY_MAX_LEN,
            ) {
                return Err(Invalid::field("source_key", violation));
            }
        }
        rules::inventory_item(self)?;
        Ok(())
    }
}
/// Builds a [`InventoryItem`]. Setters are infallible; [`build`](InventoryItemBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct InventoryItemBuilder {
    subject: Option<Subject>,
    attributes: Option<BTreeMap<String, Value>>,
    source_key: Option<String>,
}
impl InventoryItemBuilder {
    /// Sets `subject`.
    #[must_use]
    pub fn subject(mut self, subject: Subject) -> Self {
        self.subject = Some(subject);
        self
    }
    /// Sets `attributes`.
    #[must_use]
    pub fn attributes(mut self, attributes: BTreeMap<String, Value>) -> Self {
        self.attributes = Some(attributes);
        self
    }
    /// Sets `source_key`.
    #[must_use]
    pub fn source_key(mut self, source_key: impl Into<String>) -> Self {
        self.source_key = Some(source_key.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<InventoryItem, Invalid> {
        let built = InventoryItem {
            subject: self
                .subject
                .ok_or_else(|| Invalid::field("subject", Violation::Absent))?,
            attributes: self.attributes,
            source_key: self.source_key,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::InventoryItem> for InventoryItem {
    type Error = Invalid;
    fn try_from(wire: wire::InventoryItem) -> Result<Self, Invalid> {
        let built = Self {
            subject: Subject::try_from(
                    wire
                        .subject
                        .ok_or_else(|| Invalid::field("subject", Violation::Absent))?,
                )
                .map_err(|error| error.at("subject"))?,
            attributes: wire
                .attributes
                .map(value::map_from_wire)
                .transpose()
                .map_err(|error| error.at("attributes"))?,
            source_key: wire.source_key,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<InventoryItem> for wire::InventoryItem {
    fn from(value: InventoryItem) -> Self {
        Self {
            subject: Some(value.subject.into()),
            attributes: value.attributes.map(value::map_into_wire),
            source_key: value.source_key,
        }
    }
}
/// Validated form of `nv.telemetry.v1.LogRecord`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`LogRecordBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    occurred_at: Option<Timestamp>,
    severity: Option<Severity>,
    message: String,
    subject: Option<Subject>,
    entry_id: Option<String>,
    attributes: Option<BTreeMap<String, Value>>,
}
impl crate::canonical::Canonical for LogRecord {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::cmp_option(
                self.occurred_at.as_ref(),
                other.occurred_at.as_ref(),
            )
            .then_with(|| crate::canonical::cmp_option(
                self.severity.as_ref(),
                other.severity.as_ref(),
            ))
            .then_with(|| self.message.cmp(&other.message))
            .then_with(|| crate::canonical::cmp_option(
                self.subject.as_ref(),
                other.subject.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.entry_id.as_ref(),
                other.entry_id.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option_map(
                self.attributes.as_ref(),
                other.attributes.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for LogRecord {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Some(element) = &self.occurred_at {
            crate::canonical::tag(state, 1);
            crate::canonical::Digest::digest(element, state);
        }
        if let Some(value) = self.severity {
            crate::canonical::tag(state, 2);
            crate::canonical::i32_value(state, i32::from(value));
        }
        crate::canonical::tag(state, 3);
        crate::canonical::str_value(state, &self.message);
        if let Some(element) = &self.subject {
            crate::canonical::tag(state, 4);
            crate::canonical::Digest::digest(element, state);
        }
        if let Some(element) = &self.entry_id {
            crate::canonical::tag(state, 5);
            crate::canonical::str_value(state, element);
        }
        if let Some(map) = &self.attributes {
            crate::canonical::tag(state, 6);
            crate::canonical::map_value(state, map);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for LogRecord {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        if let Some(element) = &self.occurred_at {
            crate::encode::nested(1, element, buf);
        }
        if let Some(value) = self.severity {
            ::prost::encoding::int32::encode(2, &i32::from(value), buf);
        }
        ::prost::encoding::string::encode(3, &self.message, buf);
        if let Some(element) = &self.subject {
            crate::encode::nested(4, element, buf);
        }
        if let Some(element) = &self.entry_id {
            ::prost::encoding::string::encode(5, element, buf);
        }
        if let Some(map) = &self.attributes {
            crate::encode::map_field(6, map, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        self
            .occurred_at
            .as_ref()
            .map_or(0, |element| crate::encode::nested_len(1, element))
            + self
                .severity
                .map_or(
                    0,
                    |value| ::prost::encoding::int32::encoded_len(2, &i32::from(value)),
                ) + ::prost::encoding::string::encoded_len(3, &self.message)
            + self
                .subject
                .as_ref()
                .map_or(0, |element| crate::encode::nested_len(4, element))
            + self
                .entry_id
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(5, element))
            + self
                .attributes
                .as_ref()
                .map_or(0, |map| crate::encode::map_field_len(6, map))
    }
}
impl LogRecord {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> LogRecordBuilder {
        LogRecordBuilder::default()
    }
    /// The `occurred_at`, when present.
    #[must_use]
    pub fn occurred_at(&self) -> Option<&Timestamp> {
        self.occurred_at.as_ref()
    }
    /// The `severity`, when present.
    #[must_use]
    pub fn severity(&self) -> Option<Severity> {
        self.severity
    }
    /// The `message`.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
    /// The `subject`, when present.
    #[must_use]
    pub fn subject(&self) -> Option<&Subject> {
        self.subject.as_ref()
    }
    /// The `entry_id`, when present.
    #[must_use]
    pub fn entry_id(&self) -> Option<&str> {
        self.entry_id.as_deref()
    }
    /// The `attributes`, when present.
    #[must_use]
    pub fn attributes(&self) -> Option<&BTreeMap<String, Value>> {
        self.attributes.as_ref()
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(value) = self.severity {
            value.check().map_err(|violation| Invalid::field("severity", violation))?;
        }
        if let Some(violation) = invalid::too_long(
            self.message.len(),
            limits::LOGRECORD_MESSAGE_MAX_LEN,
        ) {
            return Err(Invalid::field("message", violation));
        }
        if let Some(element) = &self.entry_id {
            if element.is_empty() {
                return Err(Invalid::field("entry_id", Violation::Empty));
            }
        }
        if let Some(element) = &self.entry_id {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::LOGRECORD_ENTRY_ID_MAX_LEN,
            ) {
                return Err(Invalid::field("entry_id", violation));
            }
        }
        if let Some(map) = &self.attributes {
            value::check_map(map, "attributes")?;
        }
        rules::log_record(self)?;
        Ok(())
    }
}
/// Builds a [`LogRecord`]. Setters are infallible; [`build`](LogRecordBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct LogRecordBuilder {
    occurred_at: Option<Timestamp>,
    severity: Option<Severity>,
    message: Option<String>,
    subject: Option<Subject>,
    entry_id: Option<String>,
    attributes: Option<BTreeMap<String, Value>>,
}
impl LogRecordBuilder {
    /// Sets `occurred_at`.
    #[must_use]
    pub fn occurred_at(mut self, occurred_at: Timestamp) -> Self {
        self.occurred_at = Some(occurred_at);
        self
    }
    /// Sets `severity`.
    #[must_use]
    pub fn severity(mut self, severity: Severity) -> Self {
        self.severity = Some(severity);
        self
    }
    /// Sets `message`.
    #[must_use]
    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
    /// Sets `subject`.
    #[must_use]
    pub fn subject(mut self, subject: Subject) -> Self {
        self.subject = Some(subject);
        self
    }
    /// Sets `entry_id`.
    #[must_use]
    pub fn entry_id(mut self, entry_id: impl Into<String>) -> Self {
        self.entry_id = Some(entry_id.into());
        self
    }
    /// Sets `attributes`.
    #[must_use]
    pub fn attributes(mut self, attributes: BTreeMap<String, Value>) -> Self {
        self.attributes = Some(attributes);
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<LogRecord, Invalid> {
        let built = LogRecord {
            occurred_at: self.occurred_at,
            severity: self.severity,
            message: self
                .message
                .ok_or_else(|| Invalid::field("message", Violation::Absent))?,
            subject: self.subject,
            entry_id: self.entry_id,
            attributes: self.attributes,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::LogRecord> for LogRecord {
    type Error = Invalid;
    fn try_from(wire: wire::LogRecord) -> Result<Self, Invalid> {
        let built = Self {
            occurred_at: wire
                .occurred_at
                .map(Timestamp::try_from)
                .transpose()
                .map_err(|error| error.at("occurred_at"))?,
            severity: wire
                .severity
                .map(Severity::try_from)
                .transpose()
                .map_err(|violation| Invalid::field("severity", violation))?,
            message: wire
                .message
                .ok_or_else(|| Invalid::field("message", Violation::Absent))?,
            subject: wire
                .subject
                .map(Subject::try_from)
                .transpose()
                .map_err(|error| error.at("subject"))?,
            entry_id: wire.entry_id,
            attributes: wire
                .attributes
                .map(value::map_from_wire)
                .transpose()
                .map_err(|error| error.at("attributes"))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<LogRecord> for wire::LogRecord {
    fn from(value: LogRecord) -> Self {
        Self {
            occurred_at: value.occurred_at.map(Into::into),
            severity: value.severity.map(Into::into),
            message: Some(value.message),
            subject: value.subject.map(Into::into),
            entry_id: value.entry_id,
            attributes: value.attributes.map(value::map_into_wire),
        }
    }
}
/// Validated form of `nv.telemetry.v1.Logs`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`LogsBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Logs {
    records: Vec<LogRecord>,
}
impl crate::canonical::Canonical for Logs {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::cmp_slice(&self.records, &other.records)
    }
}
impl crate::canonical::Digest for Logs {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::count(state, self.records.len());
        for element in &self.records {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for Logs {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        for element in &self.records {
            crate::encode::nested(1, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        self.records
            .iter()
            .map(|element| crate::encode::nested_len(1, element))
            .sum::<usize>()
    }
}
impl Logs {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> LogsBuilder {
        LogsBuilder::default()
    }
    /// The `records`.
    #[must_use]
    pub fn records(&self) -> &[LogRecord] {
        &self.records
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(violation) = invalid::too_many(
            self.records.len(),
            limits::LOGS_RECORDS_MAX_ITEMS,
        ) {
            return Err(Invalid::field("records", violation));
        }
        rules::logs(self)?;
        Ok(())
    }
    fn canonicalize(&mut self) {
        self.records.sort_by(crate::canonical::Canonical::canonical_cmp);
    }
}
/// Builds a [`Logs`]. Setters are infallible; [`build`](LogsBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct LogsBuilder {
    records: Vec<LogRecord>,
}
impl LogsBuilder {
    /// Sets `records`.
    #[must_use]
    pub fn records(mut self, records: Vec<LogRecord>) -> Self {
        self.records = records;
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<Logs, Invalid> {
        let mut built = Logs { records: self.records };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::Logs> for Logs {
    type Error = Invalid;
    fn try_from(wire: wire::Logs) -> Result<Self, Invalid> {
        let mut built = Self {
            records: {
                let mut elements = Vec::with_capacity(wire.records.len());
                for (index, element) in wire.records.into_iter().enumerate() {
                    elements
                        .push(
                            LogRecord::try_from(element)
                                .map_err(|error| error.at_index("records", index))?,
                        );
                }
                elements
            },
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl From<Logs> for wire::Logs {
    fn from(value: Logs) -> Self {
        Self {
            records: value.records.into_iter().map(Into::into).collect(),
        }
    }
}
/// The `payload` of an `nv.telemetry.v1.ObservationBatch`: exactly one case, always
/// set — the oneof is `required`, so absence is unrepresentable here.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Payload {
    /// `readings`.
    Readings(Readings),
    /// `logs`.
    Logs(Logs),
    /// `states`.
    States(States),
    /// `inventory`.
    Inventory(Inventory),
    /// `resources`.
    Resources(ResourceGraph),
}
impl Payload {
    fn arm(&self) -> u32 {
        match self {
            Payload::Readings(_) => 10,
            Payload::Logs(_) => 11,
            Payload::States(_) => 12,
            Payload::Inventory(_) => 13,
            Payload::Resources(_) => 14,
        }
    }
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        match self {
            Payload::Readings(inner) => crate::encode::nested(10, inner, buf),
            Payload::Logs(inner) => crate::encode::nested(11, inner, buf),
            Payload::States(inner) => crate::encode::nested(12, inner, buf),
            Payload::Inventory(inner) => crate::encode::nested(13, inner, buf),
            Payload::Resources(inner) => crate::encode::nested(14, inner, buf),
        }
    }
    fn emitted_len(&self) -> usize {
        match self {
            Payload::Readings(inner) => crate::encode::nested_len(10, inner),
            Payload::Logs(inner) => crate::encode::nested_len(11, inner),
            Payload::States(inner) => crate::encode::nested_len(12, inner),
            Payload::Inventory(inner) => crate::encode::nested_len(13, inner),
            Payload::Resources(inner) => crate::encode::nested_len(14, inner),
        }
    }
}
impl crate::canonical::Canonical for Payload {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Payload::Readings(left), Payload::Readings(right)) => {
                crate::canonical::Canonical::canonical_cmp(left, right)
            }
            (Payload::Logs(left), Payload::Logs(right)) => {
                crate::canonical::Canonical::canonical_cmp(left, right)
            }
            (Payload::States(left), Payload::States(right)) => {
                crate::canonical::Canonical::canonical_cmp(left, right)
            }
            (Payload::Inventory(left), Payload::Inventory(right)) => {
                crate::canonical::Canonical::canonical_cmp(left, right)
            }
            (Payload::Resources(left), Payload::Resources(right)) => {
                crate::canonical::Canonical::canonical_cmp(left, right)
            }
            _ => self.arm().cmp(&other.arm()),
        }
    }
}
impl crate::canonical::Digest for Payload {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Payload::Readings(inner) => {
                crate::canonical::tag(state, 10);
                crate::canonical::Digest::digest(inner, state);
            }
            Payload::Logs(inner) => {
                crate::canonical::tag(state, 11);
                crate::canonical::Digest::digest(inner, state);
            }
            Payload::States(inner) => {
                crate::canonical::tag(state, 12);
                crate::canonical::Digest::digest(inner, state);
            }
            Payload::Inventory(inner) => {
                crate::canonical::tag(state, 13);
                crate::canonical::Digest::digest(inner, state);
            }
            Payload::Resources(inner) => {
                crate::canonical::tag(state, 14);
                crate::canonical::Digest::digest(inner, state);
            }
        }
    }
}
/// Validated form of `nv.telemetry.v1.ObservationBatch`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ObservationBatchBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationBatch {
    endpoint: EndpointContext,
    origin: Origin,
    window: ObservationWindow,
    coverage: Coverage,
    payload: Payload,
}
impl crate::canonical::Canonical for ObservationBatch {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.endpoint, &other.endpoint)
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.origin,
                &other.origin,
            ))
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.window,
                &other.window,
            ))
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.coverage,
                &other.coverage,
            ))
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.payload,
                &other.payload,
            ))
    }
}
impl crate::canonical::Digest for ObservationBatch {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.endpoint, state);
        crate::canonical::tag(state, 2);
        crate::canonical::Digest::digest(&self.origin, state);
        crate::canonical::tag(state, 3);
        crate::canonical::Digest::digest(&self.window, state);
        crate::canonical::tag(state, 4);
        crate::canonical::Digest::digest(&self.coverage, state);
        crate::canonical::Digest::digest(&self.payload, state);
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for ObservationBatch {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.endpoint, buf);
        crate::encode::nested(2, &self.origin, buf);
        crate::encode::nested(3, &self.window, buf);
        crate::encode::nested(4, &self.coverage, buf);
        self.payload.emit(buf);
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.endpoint)
            + crate::encode::nested_len(2, &self.origin)
            + crate::encode::nested_len(3, &self.window)
            + crate::encode::nested_len(4, &self.coverage) + self.payload.emitted_len()
    }
}
impl ObservationBatch {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ObservationBatchBuilder {
        ObservationBatchBuilder::default()
    }
    /// The `endpoint`.
    #[must_use]
    pub fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }
    /// The `origin`.
    #[must_use]
    pub fn origin(&self) -> &Origin {
        &self.origin
    }
    /// The `window`.
    #[must_use]
    pub fn window(&self) -> &ObservationWindow {
        &self.window
    }
    /// The `coverage`.
    #[must_use]
    pub fn coverage(&self) -> &Coverage {
        &self.coverage
    }
    /// The `payload`.
    #[must_use]
    pub fn payload(&self) -> &Payload {
        &self.payload
    }
    fn check(&self) -> Result<(), Invalid> {
        rules::observation_batch(self)?;
        Ok(())
    }
    /// Decodes and validates from wire bytes.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Malformed`](crate::DecodeError) when the bytes are not
    /// protobuf, [`DecodeError::Invalid`](crate::DecodeError) when they decode
    /// but break the contract.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::DecodeError> {
        let wire = <wire::ObservationBatch as ::prost::Message>::decode(bytes)
            .map_err(crate::DecodeError::Malformed)?;
        Self::try_from(wire).map_err(crate::DecodeError::Invalid)
    }
    /// Encodes the canonical wire form, straight from the validated
    /// representation — no intermediate wire tree, no clone. Byte-identical
    /// to prost encoding the rebuilt tree, which the tests hold it to.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(crate::encode::Emit::emitted_len(self));
        crate::encode::Emit::emit(self, &mut buf);
        buf
    }
}
/// Builds a [`ObservationBatch`]. Setters are infallible; [`build`](ObservationBatchBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ObservationBatchBuilder {
    endpoint: Option<EndpointContext>,
    origin: Option<Origin>,
    window: Option<ObservationWindow>,
    coverage: Option<Coverage>,
    payload: Option<Payload>,
}
impl ObservationBatchBuilder {
    /// Sets `endpoint`.
    #[must_use]
    pub fn endpoint(mut self, endpoint: EndpointContext) -> Self {
        self.endpoint = Some(endpoint);
        self
    }
    /// Sets `origin`.
    #[must_use]
    pub fn origin(mut self, origin: Origin) -> Self {
        self.origin = Some(origin);
        self
    }
    /// Sets `window`.
    #[must_use]
    pub fn window(mut self, window: ObservationWindow) -> Self {
        self.window = Some(window);
        self
    }
    /// Sets `coverage`.
    #[must_use]
    pub fn coverage(mut self, coverage: Coverage) -> Self {
        self.coverage = Some(coverage);
        self
    }
    /// Sets `payload`.
    #[must_use]
    pub fn payload(mut self, payload: Payload) -> Self {
        self.payload = Some(payload);
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<ObservationBatch, Invalid> {
        let built = ObservationBatch {
            endpoint: self
                .endpoint
                .ok_or_else(|| Invalid::field("endpoint", Violation::Absent))?,
            origin: self
                .origin
                .ok_or_else(|| Invalid::field("origin", Violation::Absent))?,
            window: self
                .window
                .ok_or_else(|| Invalid::field("window", Violation::Absent))?,
            coverage: self
                .coverage
                .ok_or_else(|| Invalid::field("coverage", Violation::Absent))?,
            payload: self
                .payload
                .ok_or_else(|| Invalid::field("payload", Violation::Absent))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::ObservationBatch> for ObservationBatch {
    type Error = Invalid;
    fn try_from(wire: wire::ObservationBatch) -> Result<Self, Invalid> {
        let built = Self {
            endpoint: EndpointContext::try_from(
                    wire
                        .endpoint
                        .ok_or_else(|| Invalid::field("endpoint", Violation::Absent))?,
                )
                .map_err(|error| error.at("endpoint"))?,
            origin: Origin::try_from(
                    wire
                        .origin
                        .ok_or_else(|| Invalid::field("origin", Violation::Absent))?,
                )
                .map_err(|error| error.at("origin"))?,
            window: ObservationWindow::try_from(
                    wire
                        .window
                        .ok_or_else(|| Invalid::field("window", Violation::Absent))?,
                )
                .map_err(|error| error.at("window"))?,
            coverage: Coverage::try_from(
                    wire
                        .coverage
                        .ok_or_else(|| Invalid::field("coverage", Violation::Absent))?,
                )
                .map_err(|error| error.at("coverage"))?,
            payload: match wire
                .payload
                .ok_or_else(|| Invalid::field("payload", Violation::Absent))?
            {
                wire::observation_batch::Payload::Readings(inner) => {
                    Payload::Readings(
                        Readings::try_from(inner).map_err(|error| error.at("readings"))?,
                    )
                }
                wire::observation_batch::Payload::Logs(inner) => {
                    Payload::Logs(
                        Logs::try_from(inner).map_err(|error| error.at("logs"))?,
                    )
                }
                wire::observation_batch::Payload::States(inner) => {
                    Payload::States(
                        States::try_from(inner).map_err(|error| error.at("states"))?,
                    )
                }
                wire::observation_batch::Payload::Inventory(inner) => {
                    Payload::Inventory(
                        Inventory::try_from(inner)
                            .map_err(|error| error.at("inventory"))?,
                    )
                }
                wire::observation_batch::Payload::Resources(inner) => {
                    Payload::Resources(
                        ResourceGraph::try_from(inner)
                            .map_err(|error| error.at("resources"))?,
                    )
                }
            },
        };
        built.check()?;
        Ok(built)
    }
}
impl From<ObservationBatch> for wire::ObservationBatch {
    fn from(value: ObservationBatch) -> Self {
        Self {
            endpoint: Some(value.endpoint.into()),
            origin: Some(value.origin.into()),
            window: Some(value.window.into()),
            coverage: Some(value.coverage.into()),
            payload: Some(
                match value.payload {
                    Payload::Readings(inner) => {
                        wire::observation_batch::Payload::Readings(inner.into())
                    }
                    Payload::Logs(inner) => {
                        wire::observation_batch::Payload::Logs(inner.into())
                    }
                    Payload::States(inner) => {
                        wire::observation_batch::Payload::States(inner.into())
                    }
                    Payload::Inventory(inner) => {
                        wire::observation_batch::Payload::Inventory(inner.into())
                    }
                    Payload::Resources(inner) => {
                        wire::observation_batch::Payload::Resources(inner.into())
                    }
                },
            ),
        }
    }
}
/// Validated form of `nv.telemetry.v1.ObservationWindow`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ObservationWindowBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationWindow {
    start: Timestamp,
    end: Option<Timestamp>,
}
impl crate::canonical::Canonical for ObservationWindow {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.start, &other.start)
            .then_with(|| crate::canonical::cmp_option(
                self.end.as_ref(),
                other.end.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for ObservationWindow {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.start, state);
        if let Some(element) = &self.end {
            crate::canonical::tag(state, 2);
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for ObservationWindow {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.start, buf);
        if let Some(element) = &self.end {
            crate::encode::nested(2, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.start)
            + self
                .end
                .as_ref()
                .map_or(0, |element| crate::encode::nested_len(2, element))
    }
}
impl ObservationWindow {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ObservationWindowBuilder {
        ObservationWindowBuilder::default()
    }
    /// The `start`.
    #[must_use]
    pub fn start(&self) -> &Timestamp {
        &self.start
    }
    /// The `end`, when present.
    #[must_use]
    pub fn end(&self) -> Option<&Timestamp> {
        self.end.as_ref()
    }
    fn check(&self) -> Result<(), Invalid> {
        rules::observation_window(self)?;
        Ok(())
    }
}
/// Builds a [`ObservationWindow`]. Setters are infallible; [`build`](ObservationWindowBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ObservationWindowBuilder {
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}
impl ObservationWindowBuilder {
    /// Sets `start`.
    #[must_use]
    pub fn start(mut self, start: Timestamp) -> Self {
        self.start = Some(start);
        self
    }
    /// Sets `end`.
    #[must_use]
    pub fn end(mut self, end: Timestamp) -> Self {
        self.end = Some(end);
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<ObservationWindow, Invalid> {
        let built = ObservationWindow {
            start: self.start.ok_or_else(|| Invalid::field("start", Violation::Absent))?,
            end: self.end,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::ObservationWindow> for ObservationWindow {
    type Error = Invalid;
    fn try_from(wire: wire::ObservationWindow) -> Result<Self, Invalid> {
        let built = Self {
            start: Timestamp::try_from(
                    wire.start.ok_or_else(|| Invalid::field("start", Violation::Absent))?,
                )
                .map_err(|error| error.at("start"))?,
            end: wire
                .end
                .map(Timestamp::try_from)
                .transpose()
                .map_err(|error| error.at("end"))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<ObservationWindow> for wire::ObservationWindow {
    fn from(value: ObservationWindow) -> Self {
        Self {
            start: Some(value.start.into()),
            end: value.end.map(Into::into),
        }
    }
}
/// Validated form of `nv.telemetry.v1.ObservedResource`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ObservedResourceBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedResource {
    subject: Subject,
    source_key: String,
    source_schema: Option<String>,
    entity_tag: Option<String>,
    observed_at: Option<Timestamp>,
    properties: Option<BTreeMap<String, Value>>,
    properties_complete: bool,
    unresolved: Vec<UnresolvedReference>,
}
impl crate::canonical::Canonical for ObservedResource {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.subject, &other.subject)
            .then_with(|| self.source_key.cmp(&other.source_key))
            .then_with(|| crate::canonical::cmp_option(
                self.source_schema.as_ref(),
                other.source_schema.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option_map(
                self.properties.as_ref(),
                other.properties.as_ref(),
            ))
            .then_with(|| self.properties_complete.cmp(&other.properties_complete))
            .then_with(|| crate::canonical::cmp_slice(
                &self.unresolved,
                &other.unresolved,
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.entity_tag.as_ref(),
                other.entity_tag.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.observed_at.as_ref(),
                other.observed_at.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for ObservedResource {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.subject, state);
        crate::canonical::tag(state, 2);
        crate::canonical::str_value(state, &self.source_key);
        if let Some(element) = &self.source_schema {
            crate::canonical::tag(state, 3);
            crate::canonical::str_value(state, element);
        }
        if let Some(map) = &self.properties {
            crate::canonical::tag(state, 6);
            crate::canonical::map_value(state, map);
        }
        crate::canonical::tag(state, 7);
        crate::canonical::bool_value(state, self.properties_complete);
        crate::canonical::tag(state, 8);
        crate::canonical::count(state, self.unresolved.len());
        for element in &self.unresolved {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for ObservedResource {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.subject, buf);
        ::prost::encoding::string::encode(2, &self.source_key, buf);
        if let Some(element) = &self.source_schema {
            ::prost::encoding::string::encode(3, element, buf);
        }
        if let Some(element) = &self.entity_tag {
            ::prost::encoding::string::encode(4, element, buf);
        }
        if let Some(element) = &self.observed_at {
            crate::encode::nested(5, element, buf);
        }
        if let Some(map) = &self.properties {
            crate::encode::map_field(6, map, buf);
        }
        ::prost::encoding::bool::encode(7, &self.properties_complete, buf);
        for element in &self.unresolved {
            crate::encode::nested(8, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.subject)
            + ::prost::encoding::string::encoded_len(2, &self.source_key)
            + self
                .source_schema
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(3, element))
            + self
                .entity_tag
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(4, element))
            + self
                .observed_at
                .as_ref()
                .map_or(0, |element| crate::encode::nested_len(5, element))
            + self
                .properties
                .as_ref()
                .map_or(0, |map| crate::encode::map_field_len(6, map))
            + ::prost::encoding::bool::encoded_len(7, &self.properties_complete)
            + self
                .unresolved
                .iter()
                .map(|element| crate::encode::nested_len(8, element))
                .sum::<usize>()
    }
}
impl ObservedResource {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ObservedResourceBuilder {
        ObservedResourceBuilder::default()
    }
    /// The `subject`.
    #[must_use]
    pub fn subject(&self) -> &Subject {
        &self.subject
    }
    /// The `source_key`.
    #[must_use]
    pub fn source_key(&self) -> &str {
        &self.source_key
    }
    /// The `source_schema`, when present.
    #[must_use]
    pub fn source_schema(&self) -> Option<&str> {
        self.source_schema.as_deref()
    }
    /// The `entity_tag`, when present.
    #[must_use]
    pub fn entity_tag(&self) -> Option<&str> {
        self.entity_tag.as_deref()
    }
    /// The `observed_at`, when present.
    #[must_use]
    pub fn observed_at(&self) -> Option<&Timestamp> {
        self.observed_at.as_ref()
    }
    /// The `properties`, when present.
    #[must_use]
    pub fn properties(&self) -> Option<&BTreeMap<String, Value>> {
        self.properties.as_ref()
    }
    /// The `properties_complete`.
    #[must_use]
    pub fn properties_complete(&self) -> bool {
        self.properties_complete
    }
    /// The `unresolved`.
    #[must_use]
    pub fn unresolved(&self) -> &[UnresolvedReference] {
        &self.unresolved
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.source_key.is_empty() {
            return Err(Invalid::field("source_key", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.source_key.len(),
            limits::OBSERVEDRESOURCE_SOURCE_KEY_MAX_LEN,
        ) {
            return Err(Invalid::field("source_key", violation));
        }
        if let Some(element) = &self.source_schema {
            if element.is_empty() {
                return Err(Invalid::field("source_schema", Violation::Empty));
            }
        }
        if let Some(element) = &self.source_schema {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::OBSERVEDRESOURCE_SOURCE_SCHEMA_MAX_LEN,
            ) {
                return Err(Invalid::field("source_schema", violation));
            }
        }
        if let Some(element) = &self.entity_tag {
            if element.is_empty() {
                return Err(Invalid::field("entity_tag", Violation::Empty));
            }
        }
        if let Some(element) = &self.entity_tag {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::OBSERVEDRESOURCE_ENTITY_TAG_MAX_LEN,
            ) {
                return Err(Invalid::field("entity_tag", violation));
            }
        }
        if let Some(map) = &self.properties {
            value::check_map(map, "properties")?;
        }
        if let Some(violation) = invalid::too_many(
            self.unresolved.len(),
            limits::OBSERVEDRESOURCE_UNRESOLVED_MAX_ITEMS,
        ) {
            return Err(Invalid::field("unresolved", violation));
        }
        rules::observed_resource(self)?;
        Ok(())
    }
    fn canonicalize(&mut self) {
        self.unresolved.sort_by(crate::canonical::Canonical::canonical_cmp);
    }
}
/// Builds a [`ObservedResource`]. Setters are infallible; [`build`](ObservedResourceBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ObservedResourceBuilder {
    subject: Option<Subject>,
    source_key: Option<String>,
    source_schema: Option<String>,
    entity_tag: Option<String>,
    observed_at: Option<Timestamp>,
    properties: Option<BTreeMap<String, Value>>,
    properties_complete: Option<bool>,
    unresolved: Vec<UnresolvedReference>,
}
impl ObservedResourceBuilder {
    /// Sets `subject`.
    #[must_use]
    pub fn subject(mut self, subject: Subject) -> Self {
        self.subject = Some(subject);
        self
    }
    /// Sets `source_key`.
    #[must_use]
    pub fn source_key(mut self, source_key: impl Into<String>) -> Self {
        self.source_key = Some(source_key.into());
        self
    }
    /// Sets `source_schema`.
    #[must_use]
    pub fn source_schema(mut self, source_schema: impl Into<String>) -> Self {
        self.source_schema = Some(source_schema.into());
        self
    }
    /// Sets `entity_tag`.
    #[must_use]
    pub fn entity_tag(mut self, entity_tag: impl Into<String>) -> Self {
        self.entity_tag = Some(entity_tag.into());
        self
    }
    /// Sets `observed_at`.
    #[must_use]
    pub fn observed_at(mut self, observed_at: Timestamp) -> Self {
        self.observed_at = Some(observed_at);
        self
    }
    /// Sets `properties`.
    #[must_use]
    pub fn properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.properties = Some(properties);
        self
    }
    /// Sets `properties_complete`.
    #[must_use]
    pub fn properties_complete(mut self, properties_complete: bool) -> Self {
        self.properties_complete = Some(properties_complete);
        self
    }
    /// Sets `unresolved`.
    #[must_use]
    pub fn unresolved(mut self, unresolved: Vec<UnresolvedReference>) -> Self {
        self.unresolved = unresolved;
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<ObservedResource, Invalid> {
        let mut built = ObservedResource {
            subject: self
                .subject
                .ok_or_else(|| Invalid::field("subject", Violation::Absent))?,
            source_key: self
                .source_key
                .ok_or_else(|| Invalid::field("source_key", Violation::Absent))?,
            source_schema: self.source_schema,
            entity_tag: self.entity_tag,
            observed_at: self.observed_at,
            properties: self.properties,
            properties_complete: self
                .properties_complete
                .ok_or_else(|| Invalid::field(
                    "properties_complete",
                    Violation::Absent,
                ))?,
            unresolved: self.unresolved,
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::ObservedResource> for ObservedResource {
    type Error = Invalid;
    fn try_from(wire: wire::ObservedResource) -> Result<Self, Invalid> {
        let mut built = Self {
            subject: Subject::try_from(
                    wire
                        .subject
                        .ok_or_else(|| Invalid::field("subject", Violation::Absent))?,
                )
                .map_err(|error| error.at("subject"))?,
            source_key: wire
                .source_key
                .ok_or_else(|| Invalid::field("source_key", Violation::Absent))?,
            source_schema: wire.source_schema,
            entity_tag: wire.entity_tag,
            observed_at: wire
                .observed_at
                .map(Timestamp::try_from)
                .transpose()
                .map_err(|error| error.at("observed_at"))?,
            properties: wire
                .properties
                .map(value::map_from_wire)
                .transpose()
                .map_err(|error| error.at("properties"))?,
            properties_complete: wire
                .properties_complete
                .ok_or_else(|| Invalid::field(
                    "properties_complete",
                    Violation::Absent,
                ))?,
            unresolved: {
                let mut elements = Vec::with_capacity(wire.unresolved.len());
                for (index, element) in wire.unresolved.into_iter().enumerate() {
                    elements
                        .push(
                            UnresolvedReference::try_from(element)
                                .map_err(|error| error.at_index("unresolved", index))?,
                        );
                }
                elements
            },
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl From<ObservedResource> for wire::ObservedResource {
    fn from(value: ObservedResource) -> Self {
        Self {
            subject: Some(value.subject.into()),
            source_key: Some(value.source_key),
            source_schema: value.source_schema,
            entity_tag: value.entity_tag,
            observed_at: value.observed_at.map(Into::into),
            properties: value.properties.map(value::map_into_wire),
            properties_complete: Some(value.properties_complete),
            unresolved: value.unresolved.into_iter().map(Into::into).collect(),
        }
    }
}
/// Validated form of `nv.telemetry.v1.Origin`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`OriginBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Origin {
    provider: String,
    request_class: String,
}
impl crate::canonical::Canonical for Origin {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.provider
            .cmp(&other.provider)
            .then_with(|| self.request_class.cmp(&other.request_class))
    }
}
impl crate::canonical::Digest for Origin {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::str_value(state, &self.provider);
        crate::canonical::tag(state, 2);
        crate::canonical::str_value(state, &self.request_class);
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for Origin {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        ::prost::encoding::string::encode(1, &self.provider, buf);
        ::prost::encoding::string::encode(2, &self.request_class, buf);
    }
    fn emitted_len(&self) -> usize {
        ::prost::encoding::string::encoded_len(1, &self.provider)
            + ::prost::encoding::string::encoded_len(2, &self.request_class)
    }
}
impl Origin {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> OriginBuilder {
        OriginBuilder::default()
    }
    /// The `provider`.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }
    /// The `request_class`.
    #[must_use]
    pub fn request_class(&self) -> &str {
        &self.request_class
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.provider.is_empty() {
            return Err(Invalid::field("provider", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.provider.len(),
            limits::ORIGIN_PROVIDER_MAX_LEN,
        ) {
            return Err(Invalid::field("provider", violation));
        }
        if self.request_class.is_empty() {
            return Err(Invalid::field("request_class", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.request_class.len(),
            limits::ORIGIN_REQUEST_CLASS_MAX_LEN,
        ) {
            return Err(Invalid::field("request_class", violation));
        }
        rules::origin(self)?;
        Ok(())
    }
}
/// Builds a [`Origin`]. Setters are infallible; [`build`](OriginBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct OriginBuilder {
    provider: Option<String>,
    request_class: Option<String>,
}
impl OriginBuilder {
    /// Sets `provider`.
    #[must_use]
    pub fn provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }
    /// Sets `request_class`.
    #[must_use]
    pub fn request_class(mut self, request_class: impl Into<String>) -> Self {
        self.request_class = Some(request_class.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<Origin, Invalid> {
        let built = Origin {
            provider: self
                .provider
                .ok_or_else(|| Invalid::field("provider", Violation::Absent))?,
            request_class: self
                .request_class
                .ok_or_else(|| Invalid::field("request_class", Violation::Absent))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::Origin> for Origin {
    type Error = Invalid;
    fn try_from(wire: wire::Origin) -> Result<Self, Invalid> {
        let built = Self {
            provider: wire
                .provider
                .ok_or_else(|| Invalid::field("provider", Violation::Absent))?,
            request_class: wire
                .request_class
                .ok_or_else(|| Invalid::field("request_class", Violation::Absent))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<Origin> for wire::Origin {
    fn from(value: Origin) -> Self {
        Self {
            provider: Some(value.provider),
            request_class: Some(value.request_class),
        }
    }
}
/// Validated form of `nv.telemetry.v1.ProjectionIssue`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ProjectionIssueBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionIssue {
    path: String,
    kind: IssueKind,
    detail: Option<String>,
}
impl crate::canonical::Canonical for ProjectionIssue {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path
            .cmp(&other.path)
            .then_with(|| i32::from(self.kind).cmp(&i32::from(other.kind)))
            .then_with(|| crate::canonical::cmp_option(
                self.detail.as_ref(),
                other.detail.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for ProjectionIssue {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::str_value(state, &self.path);
        crate::canonical::tag(state, 2);
        crate::canonical::i32_value(state, i32::from(self.kind));
        if let Some(element) = &self.detail {
            crate::canonical::tag(state, 3);
            crate::canonical::str_value(state, element);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for ProjectionIssue {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        ::prost::encoding::string::encode(1, &self.path, buf);
        ::prost::encoding::int32::encode(2, &i32::from(self.kind), buf);
        if let Some(element) = &self.detail {
            ::prost::encoding::string::encode(3, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        ::prost::encoding::string::encoded_len(1, &self.path)
            + ::prost::encoding::int32::encoded_len(2, &i32::from(self.kind))
            + self
                .detail
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(3, element))
    }
}
impl ProjectionIssue {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ProjectionIssueBuilder {
        ProjectionIssueBuilder::default()
    }
    /// The `path`.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
    /// The `kind`.
    #[must_use]
    pub fn kind(&self) -> IssueKind {
        self.kind
    }
    /// The `detail`, when present.
    #[must_use]
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.path.is_empty() {
            return Err(Invalid::field("path", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.path.len(),
            limits::PROJECTIONISSUE_PATH_MAX_LEN,
        ) {
            return Err(Invalid::field("path", violation));
        }
        self.kind.check().map_err(|violation| Invalid::field("kind", violation))?;
        if let Some(element) = &self.detail {
            if element.is_empty() {
                return Err(Invalid::field("detail", Violation::Empty));
            }
        }
        if let Some(element) = &self.detail {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::PROJECTIONISSUE_DETAIL_MAX_LEN,
            ) {
                return Err(Invalid::field("detail", violation));
            }
        }
        rules::projection_issue(self)?;
        Ok(())
    }
}
/// Builds a [`ProjectionIssue`]. Setters are infallible; [`build`](ProjectionIssueBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ProjectionIssueBuilder {
    path: Option<String>,
    kind: Option<IssueKind>,
    detail: Option<String>,
}
impl ProjectionIssueBuilder {
    /// Sets `path`.
    #[must_use]
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
    /// Sets `kind`.
    #[must_use]
    pub fn kind(mut self, kind: IssueKind) -> Self {
        self.kind = Some(kind);
        self
    }
    /// Sets `detail`.
    #[must_use]
    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<ProjectionIssue, Invalid> {
        let built = ProjectionIssue {
            path: self.path.ok_or_else(|| Invalid::field("path", Violation::Absent))?,
            kind: self.kind.ok_or_else(|| Invalid::field("kind", Violation::Absent))?,
            detail: self.detail,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::ProjectionIssue> for ProjectionIssue {
    type Error = Invalid;
    fn try_from(wire: wire::ProjectionIssue) -> Result<Self, Invalid> {
        let built = Self {
            path: wire.path.ok_or_else(|| Invalid::field("path", Violation::Absent))?,
            kind: IssueKind::try_from(
                    wire.kind.ok_or_else(|| Invalid::field("kind", Violation::Absent))?,
                )
                .map_err(|violation| Invalid::field("kind", violation))?,
            detail: wire.detail,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<ProjectionIssue> for wire::ProjectionIssue {
    fn from(value: ProjectionIssue) -> Self {
        Self {
            path: Some(value.path),
            kind: Some(value.kind.into()),
            detail: value.detail,
        }
    }
}
/// Validated form of `nv.telemetry.v1.ProjectionIssues`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ProjectionIssuesBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionIssues {
    endpoint: EndpointContext,
    origin: Origin,
    at: Timestamp,
    issues: Vec<ProjectionIssue>,
}
impl crate::canonical::Canonical for ProjectionIssues {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.endpoint, &other.endpoint)
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.origin,
                &other.origin,
            ))
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.at,
                &other.at,
            ))
            .then_with(|| crate::canonical::cmp_slice(&self.issues, &other.issues))
    }
}
impl crate::canonical::Digest for ProjectionIssues {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.endpoint, state);
        crate::canonical::tag(state, 2);
        crate::canonical::Digest::digest(&self.origin, state);
        crate::canonical::tag(state, 3);
        crate::canonical::Digest::digest(&self.at, state);
        crate::canonical::tag(state, 4);
        crate::canonical::count(state, self.issues.len());
        for element in &self.issues {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for ProjectionIssues {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.endpoint, buf);
        crate::encode::nested(2, &self.origin, buf);
        crate::encode::nested(3, &self.at, buf);
        for element in &self.issues {
            crate::encode::nested(4, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.endpoint)
            + crate::encode::nested_len(2, &self.origin)
            + crate::encode::nested_len(3, &self.at)
            + self
                .issues
                .iter()
                .map(|element| crate::encode::nested_len(4, element))
                .sum::<usize>()
    }
}
impl ProjectionIssues {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ProjectionIssuesBuilder {
        ProjectionIssuesBuilder::default()
    }
    /// The `endpoint`.
    #[must_use]
    pub fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }
    /// The `origin`.
    #[must_use]
    pub fn origin(&self) -> &Origin {
        &self.origin
    }
    /// The `at`.
    #[must_use]
    pub fn at(&self) -> &Timestamp {
        &self.at
    }
    /// The `issues`.
    #[must_use]
    pub fn issues(&self) -> &[ProjectionIssue] {
        &self.issues
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(violation) = invalid::too_many(
            self.issues.len(),
            limits::PROJECTIONISSUES_ISSUES_MAX_ITEMS,
        ) {
            return Err(Invalid::field("issues", violation));
        }
        for index in 1..self.issues.len() {
            if self.issues[index].path == self.issues[index - 1].path {
                return Err(Invalid::element("issues", index, Violation::Duplicate));
            }
        }
        rules::projection_issues(self)?;
        Ok(())
    }
    fn canonicalize(&mut self) {
        self.issues.sort_by(crate::canonical::Canonical::canonical_cmp);
    }
    /// Decodes and validates from wire bytes.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Malformed`](crate::DecodeError) when the bytes are not
    /// protobuf, [`DecodeError::Invalid`](crate::DecodeError) when they decode
    /// but break the contract.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::DecodeError> {
        let wire = <wire::ProjectionIssues as ::prost::Message>::decode(bytes)
            .map_err(crate::DecodeError::Malformed)?;
        Self::try_from(wire).map_err(crate::DecodeError::Invalid)
    }
    /// Encodes the canonical wire form, straight from the validated
    /// representation — no intermediate wire tree, no clone. Byte-identical
    /// to prost encoding the rebuilt tree, which the tests hold it to.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(crate::encode::Emit::emitted_len(self));
        crate::encode::Emit::emit(self, &mut buf);
        buf
    }
}
/// Builds a [`ProjectionIssues`]. Setters are infallible; [`build`](ProjectionIssuesBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ProjectionIssuesBuilder {
    endpoint: Option<EndpointContext>,
    origin: Option<Origin>,
    at: Option<Timestamp>,
    issues: Vec<ProjectionIssue>,
}
impl ProjectionIssuesBuilder {
    /// Sets `endpoint`.
    #[must_use]
    pub fn endpoint(mut self, endpoint: EndpointContext) -> Self {
        self.endpoint = Some(endpoint);
        self
    }
    /// Sets `origin`.
    #[must_use]
    pub fn origin(mut self, origin: Origin) -> Self {
        self.origin = Some(origin);
        self
    }
    /// Sets `at`.
    #[must_use]
    pub fn at(mut self, at: Timestamp) -> Self {
        self.at = Some(at);
        self
    }
    /// Sets `issues`.
    #[must_use]
    pub fn issues(mut self, issues: Vec<ProjectionIssue>) -> Self {
        self.issues = issues;
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<ProjectionIssues, Invalid> {
        let mut built = ProjectionIssues {
            endpoint: self
                .endpoint
                .ok_or_else(|| Invalid::field("endpoint", Violation::Absent))?,
            origin: self
                .origin
                .ok_or_else(|| Invalid::field("origin", Violation::Absent))?,
            at: self.at.ok_or_else(|| Invalid::field("at", Violation::Absent))?,
            issues: self.issues,
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::ProjectionIssues> for ProjectionIssues {
    type Error = Invalid;
    fn try_from(wire: wire::ProjectionIssues) -> Result<Self, Invalid> {
        let mut built = Self {
            endpoint: EndpointContext::try_from(
                    wire
                        .endpoint
                        .ok_or_else(|| Invalid::field("endpoint", Violation::Absent))?,
                )
                .map_err(|error| error.at("endpoint"))?,
            origin: Origin::try_from(
                    wire
                        .origin
                        .ok_or_else(|| Invalid::field("origin", Violation::Absent))?,
                )
                .map_err(|error| error.at("origin"))?,
            at: Timestamp::try_from(
                    wire.at.ok_or_else(|| Invalid::field("at", Violation::Absent))?,
                )
                .map_err(|error| error.at("at"))?,
            issues: {
                let mut elements = Vec::with_capacity(wire.issues.len());
                for (index, element) in wire.issues.into_iter().enumerate() {
                    elements
                        .push(
                            ProjectionIssue::try_from(element)
                                .map_err(|error| error.at_index("issues", index))?,
                        );
                }
                elements
            },
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl From<ProjectionIssues> for wire::ProjectionIssues {
    fn from(value: ProjectionIssues) -> Self {
        Self {
            endpoint: Some(value.endpoint.into()),
            origin: Some(value.origin.into()),
            at: Some(value.at.into()),
            issues: value.issues.into_iter().map(Into::into).collect(),
        }
    }
}
/// Validated form of `nv.telemetry.v1.Reading`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ReadingBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reading {
    key: SignalKey,
    value: NumericValue,
    observed_at: Option<Timestamp>,
}
impl crate::canonical::Canonical for Reading {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.key, &other.key)
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.value,
                &other.value,
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.observed_at.as_ref(),
                other.observed_at.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for Reading {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.key, state);
        crate::canonical::tag(state, 2);
        crate::canonical::Digest::digest(&self.value, state);
        if let Some(element) = &self.observed_at {
            crate::canonical::tag(state, 3);
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for Reading {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.key, buf);
        crate::encode::nested(2, &self.value, buf);
        if let Some(element) = &self.observed_at {
            crate::encode::nested(3, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.key)
            + crate::encode::nested_len(2, &self.value)
            + self
                .observed_at
                .as_ref()
                .map_or(0, |element| crate::encode::nested_len(3, element))
    }
}
impl Reading {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ReadingBuilder {
        ReadingBuilder::default()
    }
    /// The `key`.
    #[must_use]
    pub fn key(&self) -> &SignalKey {
        &self.key
    }
    /// The `value`.
    #[must_use]
    pub fn value(&self) -> &NumericValue {
        &self.value
    }
    /// The `observed_at`, when present.
    #[must_use]
    pub fn observed_at(&self) -> Option<&Timestamp> {
        self.observed_at.as_ref()
    }
    fn check(&self) -> Result<(), Invalid> {
        rules::reading(self)?;
        Ok(())
    }
}
/// Builds a [`Reading`]. Setters are infallible; [`build`](ReadingBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ReadingBuilder {
    key: Option<SignalKey>,
    value: Option<NumericValue>,
    observed_at: Option<Timestamp>,
}
impl ReadingBuilder {
    /// Sets `key`.
    #[must_use]
    pub fn key(mut self, key: SignalKey) -> Self {
        self.key = Some(key);
        self
    }
    /// Sets `value`.
    #[must_use]
    pub fn value(mut self, value: NumericValue) -> Self {
        self.value = Some(value);
        self
    }
    /// Sets `observed_at`.
    #[must_use]
    pub fn observed_at(mut self, observed_at: Timestamp) -> Self {
        self.observed_at = Some(observed_at);
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<Reading, Invalid> {
        let built = Reading {
            key: self.key.ok_or_else(|| Invalid::field("key", Violation::Absent))?,
            value: self.value.ok_or_else(|| Invalid::field("value", Violation::Absent))?,
            observed_at: self.observed_at,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::Reading> for Reading {
    type Error = Invalid;
    fn try_from(wire: wire::Reading) -> Result<Self, Invalid> {
        let built = Self {
            key: SignalKey::try_from(
                    wire.key.ok_or_else(|| Invalid::field("key", Violation::Absent))?,
                )
                .map_err(|error| error.at("key"))?,
            value: NumericValue::try_from(
                    wire.value.ok_or_else(|| Invalid::field("value", Violation::Absent))?,
                )
                .map_err(|error| error.at("value"))?,
            observed_at: wire
                .observed_at
                .map(Timestamp::try_from)
                .transpose()
                .map_err(|error| error.at("observed_at"))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<Reading> for wire::Reading {
    fn from(value: Reading) -> Self {
        Self {
            key: Some(value.key.into()),
            value: Some(value.value.into()),
            observed_at: value.observed_at.map(Into::into),
        }
    }
}
/// Validated form of `nv.telemetry.v1.Readings`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ReadingsBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Readings {
    descriptors: Vec<SignalDescriptor>,
    samples: Vec<Reading>,
}
impl crate::canonical::Canonical for Readings {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::cmp_slice(&self.descriptors, &other.descriptors)
            .then_with(|| crate::canonical::cmp_slice(&self.samples, &other.samples))
    }
}
impl crate::canonical::Digest for Readings {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::count(state, self.descriptors.len());
        for element in &self.descriptors {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::tag(state, 2);
        crate::canonical::count(state, self.samples.len());
        for element in &self.samples {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for Readings {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        for element in &self.descriptors {
            crate::encode::nested(1, element, buf);
        }
        for element in &self.samples {
            crate::encode::nested(2, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        self
            .descriptors
            .iter()
            .map(|element| crate::encode::nested_len(1, element))
            .sum::<usize>()
            + self
                .samples
                .iter()
                .map(|element| crate::encode::nested_len(2, element))
                .sum::<usize>()
    }
}
impl Readings {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ReadingsBuilder {
        ReadingsBuilder::default()
    }
    /// The `descriptors`.
    #[must_use]
    pub fn descriptors(&self) -> &[SignalDescriptor] {
        &self.descriptors
    }
    /// The `samples`.
    #[must_use]
    pub fn samples(&self) -> &[Reading] {
        &self.samples
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(violation) = invalid::too_many(
            self.descriptors.len(),
            limits::READINGS_DESCRIPTORS_MAX_ITEMS,
        ) {
            return Err(Invalid::field("descriptors", violation));
        }
        for index in 1..self.descriptors.len() {
            if self.descriptors[index].key == self.descriptors[index - 1].key {
                return Err(Invalid::element("descriptors", index, Violation::Duplicate));
            }
        }
        if let Some(violation) = invalid::too_many(
            self.samples.len(),
            limits::READINGS_SAMPLES_MAX_ITEMS,
        ) {
            return Err(Invalid::field("samples", violation));
        }
        rules::readings(self)?;
        Ok(())
    }
    fn canonicalize(&mut self) {
        self.descriptors.sort_by(crate::canonical::Canonical::canonical_cmp);
        self.samples.sort_by(crate::canonical::Canonical::canonical_cmp);
    }
}
/// Builds a [`Readings`]. Setters are infallible; [`build`](ReadingsBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ReadingsBuilder {
    descriptors: Vec<SignalDescriptor>,
    samples: Vec<Reading>,
}
impl ReadingsBuilder {
    /// Sets `descriptors`.
    #[must_use]
    pub fn descriptors(mut self, descriptors: Vec<SignalDescriptor>) -> Self {
        self.descriptors = descriptors;
        self
    }
    /// Sets `samples`.
    #[must_use]
    pub fn samples(mut self, samples: Vec<Reading>) -> Self {
        self.samples = samples;
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<Readings, Invalid> {
        let mut built = Readings {
            descriptors: self.descriptors,
            samples: self.samples,
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::Readings> for Readings {
    type Error = Invalid;
    fn try_from(wire: wire::Readings) -> Result<Self, Invalid> {
        let mut built = Self {
            descriptors: {
                let mut elements = Vec::with_capacity(wire.descriptors.len());
                for (index, element) in wire.descriptors.into_iter().enumerate() {
                    elements
                        .push(
                            SignalDescriptor::try_from(element)
                                .map_err(|error| error.at_index("descriptors", index))?,
                        );
                }
                elements
            },
            samples: {
                let mut elements = Vec::with_capacity(wire.samples.len());
                for (index, element) in wire.samples.into_iter().enumerate() {
                    elements
                        .push(
                            Reading::try_from(element)
                                .map_err(|error| error.at_index("samples", index))?,
                        );
                }
                elements
            },
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl From<Readings> for wire::Readings {
    fn from(value: Readings) -> Self {
        Self {
            descriptors: value.descriptors.into_iter().map(Into::into).collect(),
            samples: value.samples.into_iter().map(Into::into).collect(),
        }
    }
}
/// Validated form of `nv.telemetry.v1.ResourceGraph`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ResourceGraphBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceGraph {
    resources: Vec<ObservedResource>,
    relations: Vec<ResourceRelation>,
}
impl crate::canonical::Canonical for ResourceGraph {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::cmp_slice(&self.resources, &other.resources)
            .then_with(|| crate::canonical::cmp_slice(&self.relations, &other.relations))
    }
}
impl crate::canonical::Digest for ResourceGraph {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::count(state, self.resources.len());
        for element in &self.resources {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::tag(state, 2);
        crate::canonical::count(state, self.relations.len());
        for element in &self.relations {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for ResourceGraph {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        for element in &self.resources {
            crate::encode::nested(1, element, buf);
        }
        for element in &self.relations {
            crate::encode::nested(2, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        self
            .resources
            .iter()
            .map(|element| crate::encode::nested_len(1, element))
            .sum::<usize>()
            + self
                .relations
                .iter()
                .map(|element| crate::encode::nested_len(2, element))
                .sum::<usize>()
    }
}
impl ResourceGraph {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ResourceGraphBuilder {
        ResourceGraphBuilder::default()
    }
    /// The `resources`.
    #[must_use]
    pub fn resources(&self) -> &[ObservedResource] {
        &self.resources
    }
    /// The `relations`.
    #[must_use]
    pub fn relations(&self) -> &[ResourceRelation] {
        &self.relations
    }
    /// Feeds this message's logical content into `state`.
    ///
    /// Present hash-visible fields, labeled by field number; collection
    /// metadata is skipped, transitively. Equal content produces equal
    /// bytes because construction canonicalized the representation, and
    /// the stream is injective, so distinct content cannot collide by
    /// construction of the bytes alone. Encoded wire bytes are never
    /// fed to a hash.
    ///
    /// The hasher is the caller's: whoever stores or compares hashes
    /// owns the choice of function, and the standard library's default
    /// is deliberately not stable across processes.
    pub fn content_hash<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::Digest::digest(self, state);
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(violation) = invalid::too_many(
            self.resources.len(),
            limits::RESOURCEGRAPH_RESOURCES_MAX_ITEMS,
        ) {
            return Err(Invalid::field("resources", violation));
        }
        for index in 1..self.resources.len() {
            if self.resources[index].subject == self.resources[index - 1].subject {
                return Err(Invalid::element("resources", index, Violation::Duplicate));
            }
        }
        if let Some(violation) = invalid::too_many(
            self.relations.len(),
            limits::RESOURCEGRAPH_RELATIONS_MAX_ITEMS,
        ) {
            return Err(Invalid::field("relations", violation));
        }
        for index in 1..self.relations.len() {
            if self.relations[index].source == self.relations[index - 1].source
                && self.relations[index].target == self.relations[index - 1].target
                && self.relations[index].kind == self.relations[index - 1].kind
            {
                return Err(Invalid::element("relations", index, Violation::Duplicate));
            }
        }
        rules::resource_graph(self)?;
        Ok(())
    }
    fn canonicalize(&mut self) {
        self.resources.sort_by(crate::canonical::Canonical::canonical_cmp);
        self.relations.sort_by(crate::canonical::Canonical::canonical_cmp);
    }
}
/// Builds a [`ResourceGraph`]. Setters are infallible; [`build`](ResourceGraphBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ResourceGraphBuilder {
    resources: Vec<ObservedResource>,
    relations: Vec<ResourceRelation>,
}
impl ResourceGraphBuilder {
    /// Sets `resources`.
    #[must_use]
    pub fn resources(mut self, resources: Vec<ObservedResource>) -> Self {
        self.resources = resources;
        self
    }
    /// Sets `relations`.
    #[must_use]
    pub fn relations(mut self, relations: Vec<ResourceRelation>) -> Self {
        self.relations = relations;
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<ResourceGraph, Invalid> {
        let mut built = ResourceGraph {
            resources: self.resources,
            relations: self.relations,
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::ResourceGraph> for ResourceGraph {
    type Error = Invalid;
    fn try_from(wire: wire::ResourceGraph) -> Result<Self, Invalid> {
        let mut built = Self {
            resources: {
                let mut elements = Vec::with_capacity(wire.resources.len());
                for (index, element) in wire.resources.into_iter().enumerate() {
                    elements
                        .push(
                            ObservedResource::try_from(element)
                                .map_err(|error| error.at_index("resources", index))?,
                        );
                }
                elements
            },
            relations: {
                let mut elements = Vec::with_capacity(wire.relations.len());
                for (index, element) in wire.relations.into_iter().enumerate() {
                    elements
                        .push(
                            ResourceRelation::try_from(element)
                                .map_err(|error| error.at_index("relations", index))?,
                        );
                }
                elements
            },
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl From<ResourceGraph> for wire::ResourceGraph {
    fn from(value: ResourceGraph) -> Self {
        Self {
            resources: value.resources.into_iter().map(Into::into).collect(),
            relations: value.relations.into_iter().map(Into::into).collect(),
        }
    }
}
/// Validated form of `nv.telemetry.v1.ResourceRelation`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ResourceRelationBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceRelation {
    source: Subject,
    target: Subject,
    kind: String,
}
impl crate::canonical::Canonical for ResourceRelation {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.source, &other.source)
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.target,
                &other.target,
            ))
            .then_with(|| self.kind.cmp(&other.kind))
    }
}
impl crate::canonical::Digest for ResourceRelation {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.source, state);
        crate::canonical::tag(state, 2);
        crate::canonical::Digest::digest(&self.target, state);
        crate::canonical::tag(state, 3);
        crate::canonical::str_value(state, &self.kind);
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for ResourceRelation {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.source, buf);
        crate::encode::nested(2, &self.target, buf);
        ::prost::encoding::string::encode(3, &self.kind, buf);
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.source)
            + crate::encode::nested_len(2, &self.target)
            + ::prost::encoding::string::encoded_len(3, &self.kind)
    }
}
impl ResourceRelation {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ResourceRelationBuilder {
        ResourceRelationBuilder::default()
    }
    /// The `source`.
    #[must_use]
    pub fn source(&self) -> &Subject {
        &self.source
    }
    /// The `target`.
    #[must_use]
    pub fn target(&self) -> &Subject {
        &self.target
    }
    /// The `kind`.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.kind.is_empty() {
            return Err(Invalid::field("kind", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.kind.len(),
            limits::RESOURCERELATION_KIND_MAX_LEN,
        ) {
            return Err(Invalid::field("kind", violation));
        }
        rules::resource_relation(self)?;
        Ok(())
    }
}
/// Builds a [`ResourceRelation`]. Setters are infallible; [`build`](ResourceRelationBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ResourceRelationBuilder {
    source: Option<Subject>,
    target: Option<Subject>,
    kind: Option<String>,
}
impl ResourceRelationBuilder {
    /// Sets `source`.
    #[must_use]
    pub fn source(mut self, source: Subject) -> Self {
        self.source = Some(source);
        self
    }
    /// Sets `target`.
    #[must_use]
    pub fn target(mut self, target: Subject) -> Self {
        self.target = Some(target);
        self
    }
    /// Sets `kind`.
    #[must_use]
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<ResourceRelation, Invalid> {
        let built = ResourceRelation {
            source: self
                .source
                .ok_or_else(|| Invalid::field("source", Violation::Absent))?,
            target: self
                .target
                .ok_or_else(|| Invalid::field("target", Violation::Absent))?,
            kind: self.kind.ok_or_else(|| Invalid::field("kind", Violation::Absent))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::ResourceRelation> for ResourceRelation {
    type Error = Invalid;
    fn try_from(wire: wire::ResourceRelation) -> Result<Self, Invalid> {
        let built = Self {
            source: Subject::try_from(
                    wire
                        .source
                        .ok_or_else(|| Invalid::field("source", Violation::Absent))?,
                )
                .map_err(|error| error.at("source"))?,
            target: Subject::try_from(
                    wire
                        .target
                        .ok_or_else(|| Invalid::field("target", Violation::Absent))?,
                )
                .map_err(|error| error.at("target"))?,
            kind: wire.kind.ok_or_else(|| Invalid::field("kind", Violation::Absent))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<ResourceRelation> for wire::ResourceRelation {
    fn from(value: ResourceRelation) -> Self {
        Self {
            source: Some(value.source.into()),
            target: Some(value.target.into()),
            kind: Some(value.kind),
        }
    }
}
/// Validated form of `nv.telemetry.v1.SignalDescriptor`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`SignalDescriptorBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignalDescriptor {
    key: SignalKey,
    kind: Option<String>,
    unit: Option<String>,
    range: Option<ValueRange>,
}
impl crate::canonical::Canonical for SignalDescriptor {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.key, &other.key)
            .then_with(|| crate::canonical::cmp_option(
                self.kind.as_ref(),
                other.kind.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.unit.as_ref(),
                other.unit.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.range.as_ref(),
                other.range.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for SignalDescriptor {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.key, state);
        if let Some(element) = &self.kind {
            crate::canonical::tag(state, 2);
            crate::canonical::str_value(state, element);
        }
        if let Some(element) = &self.unit {
            crate::canonical::tag(state, 3);
            crate::canonical::str_value(state, element);
        }
        if let Some(element) = &self.range {
            crate::canonical::tag(state, 4);
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for SignalDescriptor {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.key, buf);
        if let Some(element) = &self.kind {
            ::prost::encoding::string::encode(2, element, buf);
        }
        if let Some(element) = &self.unit {
            ::prost::encoding::string::encode(3, element, buf);
        }
        if let Some(element) = &self.range {
            crate::encode::nested(4, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.key)
            + self
                .kind
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(2, element))
            + self
                .unit
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(3, element))
            + self
                .range
                .as_ref()
                .map_or(0, |element| crate::encode::nested_len(4, element))
    }
}
impl SignalDescriptor {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> SignalDescriptorBuilder {
        SignalDescriptorBuilder::default()
    }
    /// The `key`.
    #[must_use]
    pub fn key(&self) -> &SignalKey {
        &self.key
    }
    /// The `kind`, when present.
    #[must_use]
    pub fn kind(&self) -> Option<&str> {
        self.kind.as_deref()
    }
    /// The `unit`, when present.
    #[must_use]
    pub fn unit(&self) -> Option<&str> {
        self.unit.as_deref()
    }
    /// The `range`, when present.
    #[must_use]
    pub fn range(&self) -> Option<&ValueRange> {
        self.range.as_ref()
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(element) = &self.kind {
            if element.is_empty() {
                return Err(Invalid::field("kind", Violation::Empty));
            }
        }
        if let Some(element) = &self.kind {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::SIGNALDESCRIPTOR_KIND_MAX_LEN,
            ) {
                return Err(Invalid::field("kind", violation));
            }
        }
        if let Some(element) = &self.unit {
            if element.is_empty() {
                return Err(Invalid::field("unit", Violation::Empty));
            }
        }
        if let Some(element) = &self.unit {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::SIGNALDESCRIPTOR_UNIT_MAX_LEN,
            ) {
                return Err(Invalid::field("unit", violation));
            }
        }
        rules::signal_descriptor(self)?;
        Ok(())
    }
}
/// Builds a [`SignalDescriptor`]. Setters are infallible; [`build`](SignalDescriptorBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct SignalDescriptorBuilder {
    key: Option<SignalKey>,
    kind: Option<String>,
    unit: Option<String>,
    range: Option<ValueRange>,
}
impl SignalDescriptorBuilder {
    /// Sets `key`.
    #[must_use]
    pub fn key(mut self, key: SignalKey) -> Self {
        self.key = Some(key);
        self
    }
    /// Sets `kind`.
    #[must_use]
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }
    /// Sets `unit`.
    #[must_use]
    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }
    /// Sets `range`.
    #[must_use]
    pub fn range(mut self, range: ValueRange) -> Self {
        self.range = Some(range);
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<SignalDescriptor, Invalid> {
        let built = SignalDescriptor {
            key: self.key.ok_or_else(|| Invalid::field("key", Violation::Absent))?,
            kind: self.kind,
            unit: self.unit,
            range: self.range,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::SignalDescriptor> for SignalDescriptor {
    type Error = Invalid;
    fn try_from(wire: wire::SignalDescriptor) -> Result<Self, Invalid> {
        let built = Self {
            key: SignalKey::try_from(
                    wire.key.ok_or_else(|| Invalid::field("key", Violation::Absent))?,
                )
                .map_err(|error| error.at("key"))?,
            kind: wire.kind,
            unit: wire.unit,
            range: wire
                .range
                .map(ValueRange::try_from)
                .transpose()
                .map_err(|error| error.at("range"))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<SignalDescriptor> for wire::SignalDescriptor {
    fn from(value: SignalDescriptor) -> Self {
        Self {
            key: Some(value.key.into()),
            kind: value.kind,
            unit: value.unit,
            range: value.range.map(Into::into),
        }
    }
}
/// Validated form of `nv.telemetry.v1.SignalKey`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`SignalKeyBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignalKey {
    subject: Subject,
    facet: Option<String>,
}
impl crate::canonical::Canonical for SignalKey {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.subject, &other.subject)
            .then_with(|| crate::canonical::cmp_option(
                self.facet.as_ref(),
                other.facet.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for SignalKey {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.subject, state);
        if let Some(element) = &self.facet {
            crate::canonical::tag(state, 2);
            crate::canonical::str_value(state, element);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for SignalKey {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.subject, buf);
        if let Some(element) = &self.facet {
            ::prost::encoding::string::encode(2, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.subject)
            + self
                .facet
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(2, element))
    }
}
impl SignalKey {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> SignalKeyBuilder {
        SignalKeyBuilder::default()
    }
    /// The `subject`.
    #[must_use]
    pub fn subject(&self) -> &Subject {
        &self.subject
    }
    /// The `facet`, when present.
    #[must_use]
    pub fn facet(&self) -> Option<&str> {
        self.facet.as_deref()
    }
    /// Feeds this message's logical content into `state`.
    ///
    /// Present hash-visible fields, labeled by field number; collection
    /// metadata is skipped, transitively. Equal content produces equal
    /// bytes because construction canonicalized the representation, and
    /// the stream is injective, so distinct content cannot collide by
    /// construction of the bytes alone. Encoded wire bytes are never
    /// fed to a hash.
    ///
    /// The hasher is the caller's: whoever stores or compares hashes
    /// owns the choice of function, and the standard library's default
    /// is deliberately not stable across processes.
    pub fn content_hash<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::Digest::digest(self, state);
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(element) = &self.facet {
            if element.is_empty() {
                return Err(Invalid::field("facet", Violation::Empty));
            }
        }
        if let Some(element) = &self.facet {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::SIGNALKEY_FACET_MAX_LEN,
            ) {
                return Err(Invalid::field("facet", violation));
            }
        }
        rules::signal_key(self)?;
        Ok(())
    }
}
/// Builds a [`SignalKey`]. Setters are infallible; [`build`](SignalKeyBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct SignalKeyBuilder {
    subject: Option<Subject>,
    facet: Option<String>,
}
impl SignalKeyBuilder {
    /// Sets `subject`.
    #[must_use]
    pub fn subject(mut self, subject: Subject) -> Self {
        self.subject = Some(subject);
        self
    }
    /// Sets `facet`.
    #[must_use]
    pub fn facet(mut self, facet: impl Into<String>) -> Self {
        self.facet = Some(facet.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<SignalKey, Invalid> {
        let built = SignalKey {
            subject: self
                .subject
                .ok_or_else(|| Invalid::field("subject", Violation::Absent))?,
            facet: self.facet,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::SignalKey> for SignalKey {
    type Error = Invalid;
    fn try_from(wire: wire::SignalKey) -> Result<Self, Invalid> {
        let built = Self {
            subject: Subject::try_from(
                    wire
                        .subject
                        .ok_or_else(|| Invalid::field("subject", Violation::Absent))?,
                )
                .map_err(|error| error.at("subject"))?,
            facet: wire.facet,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<SignalKey> for wire::SignalKey {
    fn from(value: SignalKey) -> Self {
        Self {
            subject: Some(value.subject.into()),
            facet: value.facet,
        }
    }
}
/// Validated form of `nv.telemetry.v1.StateObservation`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`StateObservationBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateObservation {
    subject: Subject,
    name: String,
    value: Value,
    observed_at: Option<Timestamp>,
    facet: Option<String>,
}
impl crate::canonical::Canonical for StateObservation {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::Canonical::canonical_cmp(&self.subject, &other.subject)
            .then_with(|| self.name.cmp(&other.name))
            .then_with(|| crate::canonical::Canonical::canonical_cmp(
                &self.value,
                &other.value,
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.observed_at.as_ref(),
                other.observed_at.as_ref(),
            ))
            .then_with(|| crate::canonical::cmp_option(
                self.facet.as_ref(),
                other.facet.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for StateObservation {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::Digest::digest(&self.subject, state);
        crate::canonical::tag(state, 2);
        crate::canonical::str_value(state, &self.name);
        crate::canonical::tag(state, 3);
        crate::canonical::Digest::digest(&self.value, state);
        if let Some(element) = &self.observed_at {
            crate::canonical::tag(state, 4);
            crate::canonical::Digest::digest(element, state);
        }
        if let Some(element) = &self.facet {
            crate::canonical::tag(state, 5);
            crate::canonical::str_value(state, element);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for StateObservation {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        crate::encode::nested(1, &self.subject, buf);
        ::prost::encoding::string::encode(2, &self.name, buf);
        crate::encode::nested(3, &self.value, buf);
        if let Some(element) = &self.observed_at {
            crate::encode::nested(4, element, buf);
        }
        if let Some(element) = &self.facet {
            ::prost::encoding::string::encode(5, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        crate::encode::nested_len(1, &self.subject)
            + ::prost::encoding::string::encoded_len(2, &self.name)
            + crate::encode::nested_len(3, &self.value)
            + self
                .observed_at
                .as_ref()
                .map_or(0, |element| crate::encode::nested_len(4, element))
            + self
                .facet
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(5, element))
    }
}
impl StateObservation {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> StateObservationBuilder {
        StateObservationBuilder::default()
    }
    /// The `subject`.
    #[must_use]
    pub fn subject(&self) -> &Subject {
        &self.subject
    }
    /// The `name`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// The `value`.
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.value
    }
    /// The `observed_at`, when present.
    #[must_use]
    pub fn observed_at(&self) -> Option<&Timestamp> {
        self.observed_at.as_ref()
    }
    /// The `facet`, when present.
    #[must_use]
    pub fn facet(&self) -> Option<&str> {
        self.facet.as_deref()
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.name.is_empty() {
            return Err(Invalid::field("name", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.name.len(),
            limits::STATEOBSERVATION_NAME_MAX_LEN,
        ) {
            return Err(Invalid::field("name", violation));
        }
        if let Some(element) = &self.facet {
            if element.is_empty() {
                return Err(Invalid::field("facet", Violation::Empty));
            }
        }
        if let Some(element) = &self.facet {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::STATEOBSERVATION_FACET_MAX_LEN,
            ) {
                return Err(Invalid::field("facet", violation));
            }
        }
        rules::state_observation(self)?;
        Ok(())
    }
}
/// Builds a [`StateObservation`]. Setters are infallible; [`build`](StateObservationBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct StateObservationBuilder {
    subject: Option<Subject>,
    name: Option<String>,
    value: Option<Value>,
    observed_at: Option<Timestamp>,
    facet: Option<String>,
}
impl StateObservationBuilder {
    /// Sets `subject`.
    #[must_use]
    pub fn subject(mut self, subject: Subject) -> Self {
        self.subject = Some(subject);
        self
    }
    /// Sets `name`.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    /// Sets `value`.
    #[must_use]
    pub fn value(mut self, value: Value) -> Self {
        self.value = Some(value);
        self
    }
    /// Sets `observed_at`.
    #[must_use]
    pub fn observed_at(mut self, observed_at: Timestamp) -> Self {
        self.observed_at = Some(observed_at);
        self
    }
    /// Sets `facet`.
    #[must_use]
    pub fn facet(mut self, facet: impl Into<String>) -> Self {
        self.facet = Some(facet.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<StateObservation, Invalid> {
        let built = StateObservation {
            subject: self
                .subject
                .ok_or_else(|| Invalid::field("subject", Violation::Absent))?,
            name: self.name.ok_or_else(|| Invalid::field("name", Violation::Absent))?,
            value: self.value.ok_or_else(|| Invalid::field("value", Violation::Absent))?,
            observed_at: self.observed_at,
            facet: self.facet,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::StateObservation> for StateObservation {
    type Error = Invalid;
    fn try_from(wire: wire::StateObservation) -> Result<Self, Invalid> {
        let built = Self {
            subject: Subject::try_from(
                    wire
                        .subject
                        .ok_or_else(|| Invalid::field("subject", Violation::Absent))?,
                )
                .map_err(|error| error.at("subject"))?,
            name: wire.name.ok_or_else(|| Invalid::field("name", Violation::Absent))?,
            value: Value::try_from(
                    wire.value.ok_or_else(|| Invalid::field("value", Violation::Absent))?,
                )
                .map_err(|error| error.at("value"))?,
            observed_at: wire
                .observed_at
                .map(Timestamp::try_from)
                .transpose()
                .map_err(|error| error.at("observed_at"))?,
            facet: wire.facet,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<StateObservation> for wire::StateObservation {
    fn from(value: StateObservation) -> Self {
        Self {
            subject: Some(value.subject.into()),
            name: Some(value.name),
            value: Some(value.value.into()),
            observed_at: value.observed_at.map(Into::into),
            facet: value.facet,
        }
    }
}
/// Validated form of `nv.telemetry.v1.States`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`StatesBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct States {
    observations: Vec<StateObservation>,
}
impl crate::canonical::Canonical for States {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::cmp_slice(&self.observations, &other.observations)
    }
}
impl crate::canonical::Digest for States {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::count(state, self.observations.len());
        for element in &self.observations {
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for States {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        for element in &self.observations {
            crate::encode::nested(1, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        self.observations
            .iter()
            .map(|element| crate::encode::nested_len(1, element))
            .sum::<usize>()
    }
}
impl States {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> StatesBuilder {
        StatesBuilder::default()
    }
    /// The `observations`.
    #[must_use]
    pub fn observations(&self) -> &[StateObservation] {
        &self.observations
    }
    fn check(&self) -> Result<(), Invalid> {
        if let Some(violation) = invalid::too_many(
            self.observations.len(),
            limits::STATES_OBSERVATIONS_MAX_ITEMS,
        ) {
            return Err(Invalid::field("observations", violation));
        }
        rules::states(self)?;
        Ok(())
    }
    fn canonicalize(&mut self) {
        self.observations.sort_by(crate::canonical::Canonical::canonical_cmp);
    }
}
/// Builds a [`States`]. Setters are infallible; [`build`](StatesBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct StatesBuilder {
    observations: Vec<StateObservation>,
}
impl StatesBuilder {
    /// Sets `observations`.
    #[must_use]
    pub fn observations(mut self, observations: Vec<StateObservation>) -> Self {
        self.observations = observations;
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<States, Invalid> {
        let mut built = States {
            observations: self.observations,
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::States> for States {
    type Error = Invalid;
    fn try_from(wire: wire::States) -> Result<Self, Invalid> {
        let mut built = Self {
            observations: {
                let mut elements = Vec::with_capacity(wire.observations.len());
                for (index, element) in wire.observations.into_iter().enumerate() {
                    elements
                        .push(
                            StateObservation::try_from(element)
                                .map_err(|error| error.at_index("observations", index))?,
                        );
                }
                elements
            },
        };
        built.canonicalize();
        built.check()?;
        Ok(built)
    }
}
impl From<States> for wire::States {
    fn from(value: States) -> Self {
        Self {
            observations: value.observations.into_iter().map(Into::into).collect(),
        }
    }
}
/// Validated form of `nv.telemetry.v1.Subject`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`SubjectBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Subject {
    kind: String,
    scope: Vec<String>,
    id: String,
}
impl crate::canonical::Canonical for Subject {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.kind
            .cmp(&other.kind)
            .then_with(|| crate::canonical::cmp_slice(&self.scope, &other.scope))
            .then_with(|| self.id.cmp(&other.id))
    }
}
impl crate::canonical::Digest for Subject {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::str_value(state, &self.kind);
        crate::canonical::tag(state, 2);
        crate::canonical::count(state, self.scope.len());
        for element in &self.scope {
            crate::canonical::str_value(state, element);
        }
        crate::canonical::tag(state, 3);
        crate::canonical::str_value(state, &self.id);
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for Subject {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        ::prost::encoding::string::encode(1, &self.kind, buf);
        ::prost::encoding::string::encode_repeated(2, &self.scope, buf);
        ::prost::encoding::string::encode(3, &self.id, buf);
    }
    fn emitted_len(&self) -> usize {
        ::prost::encoding::string::encoded_len(1, &self.kind)
            + ::prost::encoding::string::encoded_len_repeated(2, &self.scope)
            + ::prost::encoding::string::encoded_len(3, &self.id)
    }
}
impl Subject {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> SubjectBuilder {
        SubjectBuilder::default()
    }
    /// The `kind`.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }
    /// The `scope`.
    #[must_use]
    pub fn scope(&self) -> &[String] {
        &self.scope
    }
    /// The `id`.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Feeds this message's logical content into `state`.
    ///
    /// Present hash-visible fields, labeled by field number; collection
    /// metadata is skipped, transitively. Equal content produces equal
    /// bytes because construction canonicalized the representation, and
    /// the stream is injective, so distinct content cannot collide by
    /// construction of the bytes alone. Encoded wire bytes are never
    /// fed to a hash.
    ///
    /// The hasher is the caller's: whoever stores or compares hashes
    /// owns the choice of function, and the standard library's default
    /// is deliberately not stable across processes.
    pub fn content_hash<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::Digest::digest(self, state);
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.kind.is_empty() {
            return Err(Invalid::field("kind", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.kind.len(),
            limits::SUBJECT_KIND_MAX_LEN,
        ) {
            return Err(Invalid::field("kind", violation));
        }
        if let Some(violation) = invalid::too_many(
            self.scope.len(),
            limits::SUBJECT_SCOPE_MAX_ITEMS,
        ) {
            return Err(Invalid::field("scope", violation));
        }
        for (index, element) in self.scope.iter().enumerate() {
            if element.is_empty() {
                return Err(Invalid::element("scope", index, Violation::Empty));
            }
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::SUBJECT_SCOPE_MAX_LEN,
            ) {
                return Err(Invalid::element("scope", index, violation));
            }
        }
        if self.id.is_empty() {
            return Err(Invalid::field("id", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.id.len(),
            limits::SUBJECT_ID_MAX_LEN,
        ) {
            return Err(Invalid::field("id", violation));
        }
        rules::subject(self)?;
        Ok(())
    }
}
/// Builds a [`Subject`]. Setters are infallible; [`build`](SubjectBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct SubjectBuilder {
    kind: Option<String>,
    scope: Vec<String>,
    id: Option<String>,
}
impl SubjectBuilder {
    /// Sets `kind`.
    #[must_use]
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }
    /// Sets `scope`.
    #[must_use]
    pub fn scope(mut self, scope: Vec<String>) -> Self {
        self.scope = scope;
        self
    }
    /// Sets `id`.
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<Subject, Invalid> {
        let built = Subject {
            kind: self.kind.ok_or_else(|| Invalid::field("kind", Violation::Absent))?,
            scope: self.scope,
            id: self.id.ok_or_else(|| Invalid::field("id", Violation::Absent))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::Subject> for Subject {
    type Error = Invalid;
    fn try_from(wire: wire::Subject) -> Result<Self, Invalid> {
        let built = Self {
            kind: wire.kind.ok_or_else(|| Invalid::field("kind", Violation::Absent))?,
            scope: wire.scope,
            id: wire.id.ok_or_else(|| Invalid::field("id", Violation::Absent))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<Subject> for wire::Subject {
    fn from(value: Subject) -> Self {
        Self {
            kind: Some(value.kind),
            scope: value.scope,
            id: Some(value.id),
        }
    }
}
/// Validated form of `nv.telemetry.v1.UnresolvedReference`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`UnresolvedReferenceBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnresolvedReference {
    location: String,
    property: Option<String>,
}
impl crate::canonical::Canonical for UnresolvedReference {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.location
            .cmp(&other.location)
            .then_with(|| crate::canonical::cmp_option(
                self.property.as_ref(),
                other.property.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for UnresolvedReference {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::canonical::tag(state, 1);
        crate::canonical::str_value(state, &self.location);
        if let Some(element) = &self.property {
            crate::canonical::tag(state, 2);
            crate::canonical::str_value(state, element);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for UnresolvedReference {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        ::prost::encoding::string::encode(1, &self.location, buf);
        if let Some(element) = &self.property {
            ::prost::encoding::string::encode(2, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        ::prost::encoding::string::encoded_len(1, &self.location)
            + self
                .property
                .as_ref()
                .map_or(0, |element| ::prost::encoding::string::encoded_len(2, element))
    }
}
impl UnresolvedReference {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> UnresolvedReferenceBuilder {
        UnresolvedReferenceBuilder::default()
    }
    /// The `location`.
    #[must_use]
    pub fn location(&self) -> &str {
        &self.location
    }
    /// The `property`, when present.
    #[must_use]
    pub fn property(&self) -> Option<&str> {
        self.property.as_deref()
    }
    fn check(&self) -> Result<(), Invalid> {
        if self.location.is_empty() {
            return Err(Invalid::field("location", Violation::Empty));
        }
        if let Some(violation) = invalid::too_long(
            self.location.len(),
            limits::UNRESOLVEDREFERENCE_LOCATION_MAX_LEN,
        ) {
            return Err(Invalid::field("location", violation));
        }
        if let Some(element) = &self.property {
            if element.is_empty() {
                return Err(Invalid::field("property", Violation::Empty));
            }
        }
        if let Some(element) = &self.property {
            if let Some(violation) = invalid::too_long(
                element.len(),
                limits::UNRESOLVEDREFERENCE_PROPERTY_MAX_LEN,
            ) {
                return Err(Invalid::field("property", violation));
            }
        }
        rules::unresolved_reference(self)?;
        Ok(())
    }
}
/// Builds a [`UnresolvedReference`]. Setters are infallible; [`build`](UnresolvedReferenceBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct UnresolvedReferenceBuilder {
    location: Option<String>,
    property: Option<String>,
}
impl UnresolvedReferenceBuilder {
    /// Sets `location`.
    #[must_use]
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }
    /// Sets `property`.
    #[must_use]
    pub fn property(mut self, property: impl Into<String>) -> Self {
        self.property = Some(property.into());
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<UnresolvedReference, Invalid> {
        let built = UnresolvedReference {
            location: self
                .location
                .ok_or_else(|| Invalid::field("location", Violation::Absent))?,
            property: self.property,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::UnresolvedReference> for UnresolvedReference {
    type Error = Invalid;
    fn try_from(wire: wire::UnresolvedReference) -> Result<Self, Invalid> {
        let built = Self {
            location: wire
                .location
                .ok_or_else(|| Invalid::field("location", Violation::Absent))?,
            property: wire.property,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<UnresolvedReference> for wire::UnresolvedReference {
    fn from(value: UnresolvedReference) -> Self {
        Self {
            location: Some(value.location),
            property: value.property,
        }
    }
}
/// Validated form of `nv.telemetry.v1.ValueRange`; the schema carries the field semantics.
///
/// Holds its invariants for as long as it exists: built through
/// [`ValueRangeBuilder`] or decoded from the wire, both of which run the same
/// checks, including this message's cross-field rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueRange {
    min: Option<NumericValue>,
    max: Option<NumericValue>,
}
impl crate::canonical::Canonical for ValueRange {
    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        crate::canonical::cmp_option(self.min.as_ref(), other.min.as_ref())
            .then_with(|| crate::canonical::cmp_option(
                self.max.as_ref(),
                other.max.as_ref(),
            ))
    }
}
impl crate::canonical::Digest for ValueRange {
    fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Some(element) = &self.min {
            crate::canonical::tag(state, 1);
            crate::canonical::Digest::digest(element, state);
        }
        if let Some(element) = &self.max {
            crate::canonical::tag(state, 2);
            crate::canonical::Digest::digest(element, state);
        }
        crate::canonical::end(state);
    }
}
impl crate::encode::Emit for ValueRange {
    fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
        if let Some(element) = &self.min {
            crate::encode::nested(1, element, buf);
        }
        if let Some(element) = &self.max {
            crate::encode::nested(2, element, buf);
        }
    }
    fn emitted_len(&self) -> usize {
        self.min.as_ref().map_or(0, |element| crate::encode::nested_len(1, element))
            + self
                .max
                .as_ref()
                .map_or(0, |element| crate::encode::nested_len(2, element))
    }
}
impl ValueRange {
    /// A builder holding nothing yet.
    #[must_use]
    pub fn builder() -> ValueRangeBuilder {
        ValueRangeBuilder::default()
    }
    /// The `min`, when present.
    #[must_use]
    pub fn min(&self) -> Option<&NumericValue> {
        self.min.as_ref()
    }
    /// The `max`, when present.
    #[must_use]
    pub fn max(&self) -> Option<&NumericValue> {
        self.max.as_ref()
    }
    fn check(&self) -> Result<(), Invalid> {
        rules::value_range(self)?;
        Ok(())
    }
}
/// Builds a [`ValueRange`]. Setters are infallible; [`build`](ValueRangeBuilder::build)
/// validates everything at once, exactly as decoding does.
#[derive(Clone, Debug, Default)]
pub struct ValueRangeBuilder {
    min: Option<NumericValue>,
    max: Option<NumericValue>,
}
impl ValueRangeBuilder {
    /// Sets `min`.
    #[must_use]
    pub fn min(mut self, min: NumericValue) -> Self {
        self.min = Some(min);
        self
    }
    /// Sets `max`.
    #[must_use]
    pub fn max(mut self, max: NumericValue) -> Self {
        self.max = Some(max);
        self
    }
    /// Validates and builds.
    ///
    /// # Errors
    ///
    /// [`Invalid`] naming the first field that is absent or breaks its
    /// schema invariants.
    pub fn build(self) -> Result<ValueRange, Invalid> {
        let built = ValueRange {
            min: self.min,
            max: self.max,
        };
        built.check()?;
        Ok(built)
    }
}
impl TryFrom<wire::ValueRange> for ValueRange {
    type Error = Invalid;
    fn try_from(wire: wire::ValueRange) -> Result<Self, Invalid> {
        let built = Self {
            min: wire
                .min
                .map(NumericValue::try_from)
                .transpose()
                .map_err(|error| error.at("min"))?,
            max: wire
                .max
                .map(NumericValue::try_from)
                .transpose()
                .map_err(|error| error.at("max"))?,
        };
        built.check()?;
        Ok(built)
    }
}
impl From<ValueRange> for wire::ValueRange {
    fn from(value: ValueRange) -> Self {
        Self {
            min: value.min.map(Into::into),
            max: value.max.map(Into::into),
        }
    }
}
