// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::btree_map::Entry;
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
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value as ModelValue;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use nv_telemetry_source::SubscriptionItem;
use serde::de::DeserializeOwned;

use crate::protocol::gnmi;
use crate::protocol::gnmi::typed_value::Value;
use crate::spec::CollectionTarget;
use crate::spec::MappingKind;
use crate::spec::StateType;
use crate::Mapping;
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

    let mut issues = Vec::new();
    let mut seen = BTreeSet::new();
    let mut candidates = BTreeMap::new();

    // Mark full concrete paths before decoding: an invalid final value suppresses
    // earlier updates for that entry, without suppressing other list entries.
    for (index, update) in notification.update.into_iter().enumerate().rev() {
        let path = match resolve(spec, notification.prefix.as_ref(), update.path.as_ref()) {
            Ok(path) => path,
            Err(detail) => {
                issues.push(ProjectionIssue::invalid("path", detail).at_index("update", index));
                continue;
            }
        };

        let Some(mapping) = spec.mapping(&path) else {
            if spec.mappings.iter().any(|mapping| mapping.covers(&path)) {
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

        if !seen.insert(path.clone()) {
            continue;
        }

        let Some(projected) = project_value(
            mapping,
            &path,
            update.val.and_then(|value| value.value),
            observed_at,
            index,
            &mut issues,
        )?
        else {
            continue;
        };

        // Resolve collisions before splitting payloads. A None entry keeps an
        // identity excluded if another path produces it.
        match candidates.entry(projected.identity()) {
            Entry::Vacant(entry) => {
                entry.insert(Some((index, projected)));
            }
            Entry::Occupied(mut entry) => {
                if let Some((previous, _)) = entry.get_mut().take() {
                    issues.push(
                        ProjectionIssue::invalid(
                            "path",
                            "different paths produce the same observation identity",
                        )
                        .at_index("update", previous),
                    );
                }

                issues.push(
                    ProjectionIssue::invalid(
                        "path",
                        "different paths produce the same observation identity",
                    )
                    .at_index("update", index),
                );
            }
        }
    }

    let payloads = split_payloads(candidates.into_values().flatten().map(|(_, value)| value))?;

    delete_issues(
        spec,
        notification.prefix.as_ref(),
        &notification.delete,
        &mut issues,
    );

    Ok(AcquisitionParts::new(payloads, issues))
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum OutputIdentity {
    Reading(SignalKey),
    State(Subject, Option<String>, String),
}

enum ProjectedObservation {
    Reading(SignalDescriptor, Reading),
    State(StateObservation),
}

impl ProjectedObservation {
    fn identity(&self) -> OutputIdentity {
        match self {
            Self::Reading(descriptor, _) => OutputIdentity::Reading(descriptor.key().clone()),
            Self::State(state) => OutputIdentity::State(
                state.subject().clone(),
                state.facet().map(str::to_owned),
                state.name().to_owned(),
            ),
        }
    }
}

fn project_value(
    mapping: &Mapping,
    path: &SubscriptionPath,
    value: Option<Value>,
    observed_at: Timestamp,
    index: usize,
    issues: &mut Vec<ProjectionIssue>,
) -> Result<Option<ProjectedObservation>, AcquisitionFailure> {
    let result = match &mapping.kind {
        MappingKind::Reading {
            descriptor,
            numeric_type,
        } => reading(descriptor, *numeric_type, value, observed_at)?
            .map(|sample| ProjectedObservation::Reading(descriptor.clone(), sample)),
        MappingKind::State { target, value_type } => {
            state(target, *value_type, value, observed_at)?.map(ProjectedObservation::State)
        }
        MappingKind::Collection {
            subject: subject_template,
            target,
        } => {
            let subject = match subject_template.subject(path) {
                Ok(subject) => subject,
                Err(detail) => {
                    issues.push(ProjectionIssue::invalid("path", detail).at_index("update", index));
                    return Ok(None);
                }
            };

            match target {
                CollectionTarget::Reading(template) => {
                    let descriptor = template
                        .descriptor(subject)
                        .map_err(|error| model_error(&error))?;

                    reading(&descriptor, template.numeric_type, value, observed_at)?
                        .map(|sample| ProjectedObservation::Reading(descriptor, sample))
                }
                CollectionTarget::State {
                    template,
                    value_type,
                } => {
                    let target = StateIdentity {
                        subject,
                        template: template.clone(),
                    };

                    state(&target, *value_type, value, observed_at)?
                        .map(ProjectedObservation::State)
                }
            }
        }
    };

    if result.is_none() {
        issues.push(
            ProjectionIssue::invalid(
                "val",
                "value does not match the declared type or exceeds the model limit",
            )
            .at_index("update", index),
        );
    }

    Ok(result)
}

fn split_payloads(
    candidates: impl Iterator<Item = ProjectedObservation>,
) -> Result<Vec<(Coverage, Payload)>, AcquisitionFailure> {
    let readings_limit =
        limits::READINGS_SAMPLES_MAX_ITEMS.min(limits::READINGS_DESCRIPTORS_MAX_ITEMS) as usize;

    let states_limit = limits::STATES_OBSERVATIONS_MAX_ITEMS as usize;

    let mut payloads = Vec::new();
    let mut descriptors = BTreeMap::new();
    let mut samples = Vec::new();
    let mut states = Vec::new();

    // Reading and state limits are independent; every chunk belongs to one item.
    for candidate in candidates {
        match candidate {
            ProjectedObservation::Reading(descriptor, sample) => {
                descriptors.insert(descriptor.key().clone(), descriptor);
                samples.push(sample);

                if samples.len() == readings_limit {
                    payloads.push(payload(
                        std::mem::take(&mut descriptors),
                        std::mem::take(&mut samples),
                    )?);
                }
            }
            ProjectedObservation::State(state) => {
                states.push(state);

                if states.len() == states_limit {
                    payloads.push(state_payload(std::mem::take(&mut states))?);
                }
            }
        }
    }

    if !samples.is_empty() {
        payloads.push(payload(descriptors, samples)?);
    }

    if !states.is_empty() {
        payloads.push(state_payload(states)?);
    }

    Ok(payloads)
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
            Ok(path) if spec.mappings.iter().any(|mapping| mapping.covers(&path)) => {
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

/// A wrong type or oversized string returns `Ok(None)`; model construction errors
/// fail acquisition.
fn state(
    target: &StateIdentity,
    value_type: StateType,
    value: Option<Value>,
    observed_at: Timestamp,
) -> Result<Option<StateObservation>, AcquisitionFailure> {
    let value = match (value_type, value) {
        (StateType::Boolean, Some(Value::BoolVal(value))) => Some(ModelValue::bool(value)),
        (StateType::Boolean, Some(Value::JsonVal(bytes) | Value::JsonIetfVal(bytes))) => {
            serde_json::from_slice::<bool>(&bytes)
                .ok()
                .map(ModelValue::bool)
        }
        (StateType::String, value) => {
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

    target
        .observation(value, Some(observed_at))
        .map(Some)
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
