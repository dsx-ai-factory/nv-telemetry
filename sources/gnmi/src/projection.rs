// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

mod json;

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
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value as ModelValue;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use serde::de::DeserializeOwned;

use crate::protocol::gnmi;
use crate::protocol::gnmi::typed_value::Value;
use crate::spec::CollectionTarget;
use crate::spec::MappingKind;
use crate::spec::MappingSpec;
use crate::spec::StateType;
use crate::Mapping;
use crate::NumericType;
use crate::ObservationIdentity;
use crate::PathElement;
use crate::ProjectedNotification;
use crate::SourceBinding;
use crate::SourcePath;
use crate::StateIdentity;
use crate::SubscriptionPath;

type ProjectionParts = (Vec<(Coverage, Payload)>, Vec<ProjectionIssue>);

pub(super) fn project(
    spec: &MappingSpec,
    notification: gnmi::Notification,
) -> Result<ProjectionParts, AcquisitionFailure> {
    let projected = project_notification(spec, notification, false)?;
    Ok((projected.payloads, projected.issues))
}

pub(super) fn project_with_sources(
    spec: &MappingSpec,
    notification: gnmi::Notification,
) -> Result<ProjectedNotification, AcquisitionFailure> {
    let projected = project_notification(spec, notification, true)?;

    Ok(ProjectedNotification {
        parts: AcquisitionParts::new(projected.payloads, projected.issues),
        bindings: projected.bindings,
        deletions: projected.deletions,
        observed_at: projected.observed_at,
    })
}

// Default Get/ONCE callers retain unbounded issues until the whole acquisition ends.
struct Projection {
    payloads: Vec<(Coverage, Payload)>,
    issues: Vec<ProjectionIssue>,
    bindings: Vec<SourceBinding>,
    deletions: Vec<SourcePath>,
    observed_at: Timestamp,
}

fn project_notification(
    spec: &MappingSpec,
    notification: gnmi::Notification,
    with_sources: bool,
) -> Result<Projection, AcquisitionFailure> {
    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    // Timestamps before the Unix epoch still need a nonnegative nanosecond remainder.
    let observed_at = Timestamp::new(
        notification.timestamp.div_euclid(NANOS_PER_SECOND),
        u32::try_from(notification.timestamp.rem_euclid(NANOS_PER_SECOND))
            .map_err(|_| AcquisitionFailure::new(AcquisitionFailureClass::Internal))?,
    )
    .map_err(|error| model_error(&error))?;

    let mut issues = Vec::new();
    let mut updates_seen = BTreeSet::new();
    let mut leaves = Vec::new();

    // Walk backward so only the final update at each original path is decoded,
    // even if that update is malformed. Expanded leaves are deduplicated later
    // because a parent object and a leaf update can address the same leaf.
    for (index, update) in notification.update.into_iter().enumerate().rev() {
        let path = match resolve(spec, notification.prefix.as_ref(), update.path.as_ref()) {
            Ok(path) => path,
            Err(detail) => {
                issues.push(ProjectionIssue::invalid("path", detail).at_index("update", index));
                continue;
            }
        };

        if spec.mapping(&path).is_none() && !spec.mappings.iter().any(|m| m.covers(&path)) {
            continue;
        }

        let context = with_sources
            .then(|| source_context(spec, notification.prefix.as_ref(), update.path.as_ref()));

        // Opt-in paths include origin/target; default acquisitions keep element/key precedence.
        if !updates_seen.insert((path.clone(), context.clone())) {
            continue;
        }

        expand_update(
            spec,
            path,
            update.val.and_then(|v| v.value),
            index,
            context,
            &mut leaves,
            &mut issues,
        );
    }

    let mut leaves_seen = BTreeSet::new();
    let mut candidates = BTreeMap::new();

    for (path, value, location, context) in leaves {
        // Claim the concrete leaf before conversion, including invalid values.
        if !leaves_seen.insert((path.clone(), context.clone())) {
            continue;
        }

        let Some(mapping) = spec.mapping(&path) else {
            issues.push(location.issue(
                "path",
                "mapped leaf cannot be resolved from the value and response path keys",
            ));

            continue;
        };

        let Some(projected) =
            project_value(mapping, &path, value, observed_at, &location, &mut issues)?
        else {
            continue;
        };

        let source = context.map(|(origin, target)| SourcePath {
            path,
            origin,
            target,
        });

        // Resolve identities only after the final contribution for each leaf.
        // A None entry keeps a collided identity excluded from later candidates.
        match candidates.entry(projected.identity()) {
            Entry::Vacant(entry) => {
                entry.insert(Some((location, projected, source)));
            }
            Entry::Occupied(mut entry) => {
                let detail = "different paths produce the same observation identity";

                if let Some((previous, _, _)) = entry.get_mut().take() {
                    issues.push(previous.issue("path", detail));
                }

                issues.push(location.issue("path", detail));
            }
        }
    }

    let mut bindings = Vec::new();

    // Bind only accepted candidates, after collision rejection and before splitting.
    let payloads = split_payloads(candidates.into_iter().filter_map(|(identity, candidate)| {
        candidate.map(|(_, observation, source)| {
            if let Some(source) = source {
                bindings.push(SourceBinding { identity, source });
            }

            observation
        })
    }))?;

    let mut deletions = Vec::new();

    project_deletes(
        spec,
        notification.prefix.as_ref(),
        &notification.delete,
        with_sources.then_some(&mut deletions),
        &mut issues,
    );

    Ok(Projection {
        payloads,
        issues,
        bindings,
        deletions,
        observed_at,
    })
}

/// Locates an original update field or a member inside its JSON value.
struct UpdateLocation {
    update: usize,

    /// `None` uses the field name; `Some("")` identifies the JSON object root.
    pointer: Option<String>,
}

impl UpdateLocation {
    fn issue(&self, field: &str, detail: &str) -> ProjectionIssue {
        let path = self
            .pointer
            .as_ref()
            .map_or_else(|| field.to_owned(), |pointer| format!("val{pointer}"));

        ProjectionIssue::invalid(path, detail).at_index("update", self.update)
    }
}

type LeafUpdate = (
    SubscriptionPath,
    Option<Value>,
    UpdateLocation,
    Option<(String, String)>,
);

fn expand_update(
    spec: &MappingSpec,
    path: SubscriptionPath,
    value: Option<Value>,
    index: usize,
    context: Option<(String, String)>,
    leaves: &mut Vec<LeafUpdate>,
    issues: &mut Vec<ProjectionIssue>,
) {
    let location = UpdateLocation {
        update: index,
        pointer: None,
    };

    let Some(Value::JsonVal(bytes) | Value::JsonIetfVal(bytes)) = &value else {
        leaves.push((path, value, location, context));
        return;
    };

    if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        leaves.push((path, value, location, context));
        return;
    }

    let ietf = matches!(value, Some(Value::JsonIetfVal(_)));
    let expanded = json::expand(spec, path, bytes, ietf).unwrap_or_else(|error| vec![Err(error)]);

    for leaf in expanded {
        match leaf {
            Ok(leaf) => leaves.push((
                leaf.path,
                leaf.value,
                UpdateLocation {
                    update: index,
                    pointer: Some(leaf.pointer),
                },
                context.clone(),
            )),
            Err(error) => {
                issues.push(
                    UpdateLocation {
                        update: index,
                        pointer: Some(error.pointer),
                    }
                    .issue("val", error.detail),
                );
            }
        }
    }
}

enum ProjectedObservation {
    Reading(SignalDescriptor, Reading),
    State(StateObservation),
}

impl ProjectedObservation {
    fn identity(&self) -> ObservationIdentity {
        match self {
            Self::Reading(descriptor, _) => ObservationIdentity::Reading(descriptor.key().clone()),
            Self::State(state) => ObservationIdentity::State {
                subject: state.subject().clone(),
                facet: state.facet().map(str::to_owned),
                name: state.name().to_owned(),
            },
        }
    }
}

fn project_value(
    mapping: &Mapping,
    path: &SubscriptionPath,
    value: Option<Value>,
    observed_at: Timestamp,
    location: &UpdateLocation,
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
                    issues.push(location.issue("path", detail));
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
        issues.push(location.issue(
            "val",
            "value does not match the declared type or exceeds the model limit",
        ));
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

/// Captures relevant deletes for opt-in consumers; existing acquisitions keep issues.
fn project_deletes(
    spec: &MappingSpec,
    prefix: Option<&gnmi::Path>,
    deletes: &[gnmi::Path],
    mut sources: Option<&mut Vec<SourcePath>>,
    issues: &mut Vec<ProjectionIssue>,
) {
    for (index, delete) in deletes.iter().enumerate() {
        match resolve(spec, prefix, Some(delete)) {
            Ok(path) if spec.mappings.iter().any(|mapping| mapping.covers(&path)) => {
                if let Some(sources) = &mut sources {
                    let (origin, target) = source_context(spec, prefix, Some(delete));

                    sources.push(SourcePath {
                        path,
                        origin,
                        target,
                    });
                } else {
                    issues.push(
                        ProjectionIssue::invalid(
                            "",
                            "mapped delete cannot remove prior observations",
                        )
                        .at_index("delete", index),
                    );
                }
            }
            Err(detail) => {
                issues.push(ProjectionIssue::invalid("path", detail).at_index("delete", index));
            }
            Ok(_) => {}
        }
    }
}

// gNMI 2.7.1 assigns openconfig when origin is unspecified. Targets have no default name.
fn source_context(
    spec: &MappingSpec,
    prefix: Option<&gnmi::Path>,
    path: Option<&gnmi::Path>,
) -> (String, String) {
    const DEFAULT_ORIGIN: &str = "openconfig";

    let origin = [
        spec.origin.as_str(),
        prefix.map_or("", |p| &p.origin),
        path.map_or("", |p| &p.origin),
    ]
    .into_iter()
    .find(|value| !value.is_empty())
    .unwrap_or(DEFAULT_ORIGIN);

    let target = [
        spec.target.as_str(),
        prefix.map_or("", |p| &p.target),
        path.map_or("", |p| &p.target),
    ]
    .into_iter()
    .find(|value| !value.is_empty())
    .unwrap_or_default();

    (origin.to_owned(), target.to_owned())
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

    // Configured mappings select leaves without establishing complete subject coverage.
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
    spec: &MappingSpec,
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
