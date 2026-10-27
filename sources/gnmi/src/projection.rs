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
use nv_telemetry_model::Timestamp;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use nv_telemetry_source::SubscriptionItem;
use serde::de::DeserializeOwned;

use crate::protocol::gnmi;
use crate::protocol::gnmi::typed_value::Value;
use crate::NumericType;
use crate::PathElement;
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

    // Split at the model limits; all payloads still belong to one acquisition item.
    let chunk_limit =
        limits::READINGS_SAMPLES_MAX_ITEMS.min(limits::READINGS_DESCRIPTORS_MAX_ITEMS) as usize;

    let mut payloads = Vec::new();
    let mut descriptors = BTreeMap::new();
    let mut samples = Vec::new();
    let mut issues = Vec::new();
    let mut seen = BTreeSet::new();

    // gNMI uses only the final update for each path. Mark the path before parsing
    // its value so an invalid final value also suppresses earlier updates.
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
            continue;
        };

        if !seen.insert(&mapping.path) {
            continue;
        }

        let Some(value) = update
            .val
            .and_then(|value| value.value)
            .and_then(|value| number(mapping.numeric_type, value))
        else {
            issues.push(
                ProjectionIssue::invalid(
                    "val",
                    "value does not match the declared finite numeric type",
                )
                .at_index("update", index),
            );

            continue;
        };

        let sample = Reading::builder()
            .key(mapping.descriptor.key().clone())
            .value(value)
            .observed_at(observed_at)
            .build()
            .map_err(|error| model_error(&error))?;

        descriptors
            .entry(mapping.descriptor.key().clone())
            .or_insert_with(|| mapping.descriptor.clone());

        samples.push(sample);

        if samples.len() == chunk_limit {
            payloads.push(payload(
                std::mem::take(&mut descriptors),
                std::mem::take(&mut samples),
            )?);
        }
    }

    if !samples.is_empty() {
        payloads.push(payload(descriptors, samples)?);
    }

    for (index, delete) in notification.delete.iter().enumerate() {
        match resolve(spec, notification.prefix.as_ref(), Some(delete)) {
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

    Ok(AcquisitionParts::new(payloads, issues))
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

fn number(kind: NumericType, value: Value) -> Option<NumericValue> {
    match (kind, value) {
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
    }
}

// Parse directly into N so large i64 and u64 values never pass through f64.
fn json_number<N: DeserializeOwned + FromStr>(bytes: &[u8]) -> Option<N> {
    serde_json::from_slice(bytes)
        .ok()
        .or_else(|| serde_json::from_slice::<String>(bytes).ok()?.parse().ok())
}
