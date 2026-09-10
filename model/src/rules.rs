// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Cross-field rules: the checks the annotation vocabulary deliberately
//! cannot express, one function per generated message.
//!
//! Every generated `check` calls its message's function here, so this file is
//! the exhaustive registry of the rules the schema states only in prose —
//! and it is exhaustive by compiler force. A new validated message does not
//! compile until the question "does it have cross-field rules?" is answered
//! here, and the answer is visible in review either way: a real rule, or a
//! deliberate `Ok(())` under a comment saying the schema states none.
//!
//! The hand-written value vocabulary enforces its own prose rules inline
//! (`Timestamp`'s nanosecond bound, `Value`'s depth), because those types are
//! their own constructors and have no generated caller to hang a hook on.

// The uniform fallible signature is the registry's whole design: generated
// `check`s call every function with `?`, so a message gaining its first rule
// changes this file and nothing else. A rule-less function wrapping `Ok(())`
// is not an unnecessary wrap; it is the recorded answer "none".
#![allow(clippy::unnecessary_wraps)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;

use crate::AcquisitionStatus;
use crate::Completeness;
use crate::Coverage;
use crate::EndpointContext;
use crate::Invalid;
use crate::Inventory;
use crate::InventoryItem;
use crate::IssueKind;
use crate::LogRecord;
use crate::Logs;
use crate::NumericValue;
use crate::ObservationBatch;
use crate::ObservationWindow;
use crate::ObservedResource;
use crate::Origin;
use crate::Outcome;
use crate::Payload;
use crate::ProjectionIssue;
use crate::ProjectionIssues;
use crate::Reading;
use crate::Readings;
use crate::ResourceGraph;
use crate::ResourceRelation;
use crate::SignalDescriptor;
use crate::SignalKey;
use crate::StateObservation;
use crate::States;
use crate::Subject;
use crate::UnresolvedReference;
use crate::ValueRange;
use crate::Violation;

/// No cross-field rules; the schema states none for `Subject`.
pub(crate) fn subject(_subject: &Subject) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `SignalKey`.
pub(crate) fn signal_key(_key: &SignalKey) -> Result<(), Invalid> {
    Ok(())
}

/// "At least one bound must be present, and min must not exceed max — both
/// wrapper rules, since the vocabulary has no cross-field constraints."
///
/// The comparison also demands one arm for both bounds: a signal's arm is
/// fixed by its source field's declared type, so a range mixing arms
/// describes no signal the contract can carry.
pub(crate) fn value_range(range: &ValueRange) -> Result<(), Invalid> {
    let ordered = match (range.min(), range.max()) {
        (None, None) => {
            return Err(Invalid::field(
                "min",
                Violation::Rule("a range carries at least one bound"),
            ));
        }
        (Some(min), Some(max)) => match (min, max) {
            (NumericValue::Double(min), NumericValue::Double(max)) => min <= max,
            (NumericValue::Int(min), NumericValue::Int(max)) => min <= max,
            (NumericValue::Uint(min), NumericValue::Uint(max)) => min <= max,
            _ => {
                return Err(Invalid::field(
                    "max",
                    Violation::Rule("both bounds take the arm the signal's declared type fixed"),
                ));
            }
        },
        _ => true,
    };
    if !ordered {
        return Err(Invalid::field(
            "max",
            Violation::Rule("the minimum must not exceed the maximum"),
        ));
    }
    Ok(())
}

/// No cross-field rules; the schema states none for `SignalDescriptor`.
pub(crate) fn signal_descriptor(_descriptor: &SignalDescriptor) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `Reading`.
pub(crate) fn reading(_reading: &Reading) -> Result<(), Invalid> {
    Ok(())
}

/// "Every key referenced by a sample must resolve here — a wrapper rule — so
/// a batch is self-describing to a consumer that arrives mid-stream or reads
/// it out of storage."
pub(crate) fn readings(readings: &Readings) -> Result<(), Invalid> {
    // Canonicalization has already sorted `descriptors`, and the key is the
    // canonical order's most significant field, so resolution is a binary
    // search over what is in hand — the set this rule once built duplicated,
    // measurably, the one the uniqueness check had just thrown away.
    for (index, sample) in readings.samples().iter().enumerate() {
        let described = readings
            .descriptors()
            .binary_search_by(|descriptor| descriptor.key().cmp(sample.key()))
            .is_ok();
        if !described {
            return Err(Invalid::element(
                "samples",
                index,
                Violation::Rule("every sample's key resolves to a descriptor in this batch"),
            ));
        }
    }
    Ok(())
}

/// No cross-field rules; the schema states none for `LogRecord`.
pub(crate) fn log_record(_record: &LogRecord) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `Logs`.
pub(crate) fn logs(_logs: &Logs) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `StateObservation`.
pub(crate) fn state_observation(_observation: &StateObservation) -> Result<(), Invalid> {
    Ok(())
}

/// A repeated (subject, name) is a series: every observation carries a
/// distinct timestamp, since canonical list position cannot preserve order.
pub(crate) fn states(states: &States) -> Result<(), Invalid> {
    // Canonicalization sorts by subject then name, making each facet contiguous.
    let observations = states.observations();
    let mut start = 0;
    for index in 1..=observations.len() {
        if index < observations.len()
            && observations[index].subject() == observations[start].subject()
            && observations[index].name() == observations[start].name()
        {
            continue;
        }
        if index - start > 1 {
            let mut instants = BTreeSet::new();
            for (offset, observation) in observations[start..index].iter().enumerate() {
                let instant = observation.observed_at().ok_or_else(|| {
                    Invalid::element(
                        "observations",
                        start + offset,
                        Violation::Rule("a repeated state facet requires observed_at"),
                    )
                })?;
                if !instants.insert(instant) {
                    return Err(Invalid::element(
                        "observations",
                        start + offset,
                        Violation::Rule("a repeated state facet requires distinct timestamps"),
                    ));
                }
            }
        }
        start = index;
    }
    Ok(())
}

/// No cross-field rules; the schema states none for `InventoryItem`.
pub(crate) fn inventory_item(_item: &InventoryItem) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `Inventory`.
pub(crate) fn inventory(_inventory: &Inventory) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `UnresolvedReference`.
pub(crate) fn unresolved_reference(_reference: &UnresolvedReference) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `ObservedResource`.
///
/// `properties_complete` speaks about `properties`, but every combination is
/// meaningful: a complete empty map is a device with no properties, and an
/// incomplete absent map is a resource whose identity alone was collected.
pub(crate) fn observed_resource(_resource: &ObservedResource) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules of its own; both ends are identities by construction.
pub(crate) fn resource_relation(_relation: &ResourceRelation) -> Result<(), Invalid> {
    Ok(())
}

/// "Must name a resource present in the graph — a wrapper rule — because the
/// source is the resource on which the relation was observed." The target may
/// be outside the graph, so partial walks retain external links.
pub(crate) fn resource_graph(graph: &ResourceGraph) -> Result<(), Invalid> {
    // `resources` is canonically sorted with the subject leading, so
    // membership is a binary search rather than a set built per validation.
    for (index, relation) in graph.relations().iter().enumerate() {
        let present = graph
            .resources()
            .binary_search_by(|resource| resource.subject().cmp(relation.source()))
            .is_ok();
        if !present {
            return Err(Invalid::element(
                "relations",
                index,
                Violation::Rule("a relation's source names a resource present in the graph"),
            ));
        }
    }
    Ok(())
}

/// No cross-field rules; the schema states none for `Coverage`.
pub(crate) fn coverage(_coverage: &Coverage) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `EndpointContext`.
pub(crate) fn endpoint_context(_endpoint: &EndpointContext) -> Result<(), Invalid> {
    Ok(())
}

/// No cross-field rules; the schema states none for `Origin`.
pub(crate) fn origin(_origin: &Origin) -> Result<(), Invalid> {
    Ok(())
}

/// "A point in time carries no end; when an end is present it must follow the
/// start — a wrapper rule — so a point has exactly one representation."
///
/// Strictly after: an end equal to the start would be a second spelling of
/// the point that omitting the end already spells.
pub(crate) fn observation_window(window: &ObservationWindow) -> Result<(), Invalid> {
    if let Some(end) = window.end() {
        if end <= window.start() {
            return Err(Invalid::field(
                "end",
                Violation::Rule("a window's end follows its start"),
            ));
        }
    }
    Ok(())
}

/// "Present exactly when the outcome is a failure — a wrapper rule."
///
/// An unrecognized outcome constrains nothing: it is a newer producer's word,
/// and what its failure class means is that producer's contract with its
/// consumers, not this build's guess.
pub(crate) fn acquisition_status(status: &AcquisitionStatus) -> Result<(), Invalid> {
    match (status.outcome(), status.failure_class()) {
        (Outcome::Failed, None) => Err(Invalid::field(
            "failure_class",
            Violation::Rule("a failure carries its class"),
        )),
        (Outcome::Succeeded, Some(_)) => Err(Invalid::field(
            "failure_class",
            Violation::Rule("a success carries no failure class"),
        )),
        _ => Ok(()),
    }
}

/// "Present exactly when the kind is invalid — a wrapper rule: silence has
/// no detail to quote."
///
/// An unrecognized kind constrains nothing: it is a newer producer's word,
/// and whether that kind quotes a detail is that producer's contract with
/// its consumers, not this build's guess.
pub(crate) fn projection_issue(issue: &ProjectionIssue) -> Result<(), Invalid> {
    match (issue.kind(), issue.detail()) {
        (IssueKind::Invalid, None) => Err(Invalid::field(
            "detail",
            Violation::Rule("an invalid value quotes its failure"),
        )),
        (IssueKind::MissingRequired, Some(_)) => Err(Invalid::field(
            "detail",
            Violation::Rule("silence has no detail to quote"),
        )),
        _ => Ok(()),
    }
}

/// "An acquisition with no issues emits no message at all — an empty
/// envelope would be a fabrication, the same one the batch doctrine
/// forbids — a wrapper rule."
pub(crate) fn projection_issues(issues: &ProjectionIssues) -> Result<(), Invalid> {
    if issues.issues().is_empty() {
        return Err(Invalid::field(
            "issues",
            Violation::Rule("no issues is no message, not an empty one"),
        ));
    }
    Ok(())
}

/// "Scope on a graph batch means reachability from the scope subject ...
/// Reachability is a wrapper rule on complete graphs."
///
/// Only complete, scoped graph payloads are constrained: a partial walk may
/// legitimately hold fragments its root cannot reach yet, and an unscoped
/// batch declares no root to reach from.
pub(crate) fn observation_batch(batch: &ObservationBatch) -> Result<(), Invalid> {
    let Payload::Resources(graph) = batch.payload() else {
        return Ok(());
    };
    if batch.coverage().completeness() != Completeness::Complete {
        return Ok(());
    }
    let Some(scope) = batch.coverage().scope() else {
        return Ok(());
    };

    let mut edges: BTreeMap<&Subject, Vec<&Subject>> = BTreeMap::new();
    for relation in graph.relations() {
        edges
            .entry(relation.source())
            .or_default()
            .push(relation.target());
    }

    let mut reachable: BTreeSet<&Subject> = BTreeSet::new();
    let mut frontier: VecDeque<&Subject> = VecDeque::from([scope]);
    while let Some(next) = frontier.pop_front() {
        if !reachable.insert(next) {
            continue;
        }
        if let Some(targets) = edges.get(next) {
            frontier.extend(targets.iter().copied());
        }
    }

    for (index, resource) in graph.resources().iter().enumerate() {
        if !reachable.contains(resource.subject()) {
            return Err(Invalid::element(
                "resources",
                index,
                Violation::Rule(
                    "every resource of a complete scoped graph is reachable from the scope subject",
                ),
            )
            .at("payload"));
        }
    }
    Ok(())
}
