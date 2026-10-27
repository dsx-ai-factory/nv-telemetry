// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;

use nv_telemetry_model::limits;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::Coverage;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::NumericValue;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Reading;
use nv_telemetry_model::Readings;
use nv_telemetry_model::SignalDescriptor;
use nv_telemetry_model::SignalKey;
use nv_telemetry_model::StateObservation;
use nv_telemetry_model::States;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value as ModelValue;
use nv_telemetry_model::ValueKind;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use nv_telemetry_source::SubscriptionItem;
use serde::de::DeserializeOwned;

use crate::protocol::gnmi;
use crate::protocol::gnmi::typed_value::Value;
use crate::spec::MappingKind;
use crate::spec::StateType;
use crate::NumericType;
use crate::PathElement;
use crate::StateIdentity;
use crate::SubscriptionPath;
use crate::SubscriptionSpec;

pub(super) fn project(
    spec: &SubscriptionSpec,
    notification: gnmi::Notification,
) -> SubscriptionItem {
    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    // Timestamps before the Unix epoch still need a nonnegative nanosecond remainder.
    let observed_at = Timestamp::new(
        notification.timestamp.div_euclid(NANOS_PER_SECOND),
        u32::try_from(notification.timestamp.rem_euclid(NANOS_PER_SECOND))
            .map_err(|_| AcquisitionFailure::new(AcquisitionFailureClass::Internal))?,
    )
    .map_err(|error| model_error(&error))?;

    // Readings and states have separate limits, but all chunks from a notification
    // belong to the same acquisition item.
    let readings_limit =
        limits::READINGS_SAMPLES_MAX_ITEMS.min(limits::READINGS_DESCRIPTORS_MAX_ITEMS) as usize;

    let states_limit = limits::STATES_OBSERVATIONS_MAX_ITEMS as usize;

    let mut payloads = Vec::new();
    let mut descriptors = BTreeMap::new();
    let mut samples = Vec::new();
    let mut states = Vec::new();
    let mut issues = Vec::new();
    let mut seen = BTreeSet::new();

    // gNMI uses only the final update for each path. Mark it seen before parsing
    // its value so an invalid final value also suppresses earlier updates.
    // Keep seen paths across payload splits, and enumerate before reversing to
    // preserve the original update indexes in issues.
    for (index, update) in notification.update.into_iter().enumerate().rev() {
        let path = match resolve(spec, notification.prefix.as_ref(), update.path.as_ref()) {
            Ok(path) => path,
            Err(detail) => {
                issues.push(ProjectionIssue::invalid("path", detail).at_index("update", index));
                continue;
            }
        };

        let Some(mapping) = spec
            .mappings
            .binary_search_by(|mapping| mapping.path.cmp(&path))
            .ok()
            .and_then(|index| spec.mappings.get(index))
        else {
            if spec
                .mappings
                .iter()
                .any(|mapping| path.contains(&mapping.path))
            {
                issues.push(
                    ProjectionIssue::invalid(
                        "path",
                        "update covers mapped leaves; individual leaf updates are required",
                    )
                    .at_index("update", index),
                );
            }

            continue;
        };

        if !seen.insert(&mapping.path) {
            continue;
        }

        let value = update.val.and_then(|value| value.value);

        let issue = match &mapping.kind {
            MappingKind::Reading {
                descriptor,
                numeric_type,
            } => {
                if let Some(sample) = reading(descriptor, *numeric_type, value, observed_at)? {
                    descriptors
                        .entry(descriptor.key().clone())
                        .or_insert_with(|| descriptor.clone());

                    samples.push(sample);
                    None
                } else {
                    Some("value does not match the declared type or exceeds the model limit")
                }
            }
            MappingKind::State { target, value_type } => {
                if let Some((state, unknown)) = state(target, value_type, value, observed_at)? {
                    states.push(state);
                    unknown.then_some("enum value is not listed; value was collected unchanged")
                } else {
                    Some("value does not match the declared type or exceeds the model limit")
                }
            }
        };

        if let Some(detail) = issue {
            issues.push(ProjectionIssue::invalid("val", detail).at_index("update", index));
        }

        if samples.len() == readings_limit {
            payloads.push(payload(
                std::mem::take(&mut descriptors),
                std::mem::take(&mut samples),
            )?);
        }

        if states.len() == states_limit {
            payloads.push(state_payload(std::mem::take(&mut states))?);
        }
    }

    if !samples.is_empty() {
        payloads.push(payload(descriptors, samples)?);
    }

    if !states.is_empty() {
        payloads.push(state_payload(states)?);
    }

    delete_issues(
        spec,
        notification.prefix.as_ref(),
        &notification.delete,
        &mut issues,
    );

    Ok(AcquisitionParts::new(payloads, issues))
}

/// Reports deletes that name a mapped leaf or one of its ancestors.
/// The model has no deletion observation, so deletes produce issues only.
fn delete_issues(
    spec: &SubscriptionSpec,
    prefix: Option<&gnmi::Path>,
    deletes: &[gnmi::Path],
    issues: &mut Vec<ProjectionIssue>,
) {
    for (index, delete) in deletes.iter().enumerate() {
        match resolve(spec, prefix, Some(delete)) {
            Ok(path)
                if spec
                    .mappings
                    .iter()
                    .any(|mapping| path.contains(&mapping.path)) =>
            {
                issues.push(
                    ProjectionIssue::invalid("", "mapped delete cannot remove prior observations")
                        .at_index("delete", index),
                );
            }
            Err(detail) => {
                issues.push(ProjectionIssue::invalid("path", detail).at_index("delete", index));
            }
            Ok(_) => {}
        }
    }
}

fn payload(
    descriptors: BTreeMap<SignalKey, SignalDescriptor>,
    samples: Vec<Reading>,
) -> Result<(Coverage, Payload), AcquisitionFailure> {
    let readings = Readings::builder()
        .descriptors(descriptors.into_values().collect())
        .samples(samples)
        .build()
        .map_err(|error| model_error(&error))?;

    let coverage = Coverage::builder()
        .completeness(Completeness::Partial)
        .build()
        .map_err(|error| model_error(&error))?;

    Ok((coverage, Payload::Readings(readings)))
}

fn model_error(error: &Invalid) -> AcquisitionFailure {
    AcquisitionFailure::new(AcquisitionFailureClass::Internal)
        .with_retryable(false)
        .with_detail(format!("gNMI projection model error: {error}"))
}

fn state_payload(
    observations: Vec<StateObservation>,
) -> Result<(Coverage, Payload), AcquisitionFailure> {
    let states = States::builder()
        .observations(observations)
        .build()
        .map_err(|error| model_error(&error))?;

    // A notification can omit unchanged leaves, so its state coverage is partial.
    let coverage = Coverage::builder()
        .completeness(Completeness::Partial)
        .build()
        .map_err(|error| model_error(&error))?;

    Ok((coverage, Payload::States(states)))
}

/// Returns an observation and whether its string is unlisted. A wrong type or
/// oversized string returns `Ok(None)`; model construction errors fail acquisition.
fn state(
    target: &StateIdentity,
    value_type: &StateType,
    value: Option<Value>,
    observed_at: Timestamp,
) -> Result<Option<(StateObservation, bool)>, AcquisitionFailure> {
    let value = match (value_type, value) {
        (StateType::Boolean, Some(Value::BoolVal(value))) => Some(ModelValue::bool(value)),
        (StateType::Boolean, Some(Value::JsonVal(bytes) | Value::JsonIetfVal(bytes))) => {
            serde_json::from_slice::<bool>(&bytes)
                .ok()
                .map(ModelValue::bool)
        }
        (StateType::Enum { .. }, value) => {
            let value = match value {
                Some(Value::StringVal(value)) => Some(value),
                Some(Value::JsonVal(bytes) | Value::JsonIetfVal(bytes)) => {
                    serde_json::from_slice::<String>(&bytes).ok()
                }
                _ => None,
            };

            value.and_then(|value| ModelValue::string(value).ok())
        }
        _ => None,
    };

    let Some(value) = value else {
        return Ok(None);
    };

    let unlisted = matches!((value_type, value.kind()),
        (StateType::Enum { values, .. }, ValueKind::String(value)) if !values.contains(value));

    target
        .observation(value, Some(observed_at))
        .map(|state| Some((state, unlisted)))
        .map_err(|error| model_error(&error))
}

fn resolve(
    spec: &SubscriptionSpec,
    prefix: Option<&gnmi::Path>,
    path: Option<&gnmi::Path>,
) -> Result<SubscriptionPath, &'static str> {
    let origin = prefix.map_or("", |prefix| prefix.origin.as_str());
    let target = prefix.map_or("", |prefix| prefix.target.as_str());
    let leaf_origin = path.map_or("", |path| path.origin.as_str());
    let leaf_target = path.map_or("", |path| path.target.as_str());

    if conflict(&spec.origin, origin)
        || conflict(&spec.origin, leaf_origin)
        || conflict(origin, leaf_origin)
        || conflict(&spec.target, target)
        || conflict(&spec.target, leaf_target)
        || conflict(target, leaf_target)
    {
        return Err("path origin or target conflicts with the subscription or notification prefix");
    }

    let mut elements = Vec::new();

    for part in prefix.into_iter().chain(path) {
        #[allow(deprecated)]
        if !part.element.is_empty() {
            return Err("legacy path elements are unsupported");
        }

        for element in &part.elem {
            if element.name.is_empty()
                || element
                    .key
                    .iter()
                    .any(|(key, value)| key.is_empty() || value.is_empty())
            {
                return Err("path elements and keys must be nonempty");
            }

            elements.push(PathElement {
                name: element.name.clone(),
                keys: element
                    .key
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            });
        }
    }

    Ok(SubscriptionPath::new(elements))
}

fn conflict(a: &str, b: &str) -> bool {
    !a.is_empty() && !b.is_empty() && a != b
}

fn reading(
    descriptor: &SignalDescriptor,
    kind: NumericType,
    value: Option<Value>,
    observed_at: Timestamp,
) -> Result<Option<Reading>, AcquisitionFailure> {
    let value = value.and_then(|value| match (kind, value) {
        (NumericType::Int, Value::IntVal(value)) => Some(NumericValue::Int(value)),
        (NumericType::Uint, Value::UintVal(value)) => Some(NumericValue::Uint(value)),
        (NumericType::Double, Value::DoubleVal(value)) => NumericValue::double(value).ok(),
        #[allow(deprecated)]
        (NumericType::Double, Value::FloatVal(value)) => {
            NumericValue::double(f64::from(value)).ok()
        }
        (kind, Value::JsonVal(bytes) | Value::JsonIetfVal(bytes)) => match kind {
            NumericType::Int => json_number(&bytes).map(NumericValue::Int),
            NumericType::Uint => json_number(&bytes).map(NumericValue::Uint),
            NumericType::Double => {
                json_number(&bytes).and_then(|value| NumericValue::double(value).ok())
            }
        },
        _ => None,
    });

    let Some(value) = value else {
        return Ok(None);
    };

    Reading::builder()
        .key(descriptor.key().clone())
        .value(value)
        .observed_at(observed_at)
        .build()
        .map(Some)
        .map_err(|error| model_error(&error))
}

// Parse directly into N so large i64 and u64 values never pass through f64.
fn json_number<N: DeserializeOwned + FromStr>(bytes: &[u8]) -> Option<N> {
    serde_json::from_slice(bytes)
        .ok()
        .or_else(|| serde_json::from_slice::<String>(bytes).ok()?.parse().ok())
}
