// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! One line per report; with `--verbose`, one more per observation and per
//! issue.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::SystemTime;

use nv_telemetry_model::AcquisitionStatus;
use nv_telemetry_model::NumericValue;
use nv_telemetry_model::ObservationBatch;
use nv_telemetry_model::Outcome;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value;
use nv_telemetry_model::ValueKind;
use nv_telemetry_orchestration::AcquisitionReport;

pub(crate) fn report(report: &AcquisitionReport, verbose: bool) {
    let status = report.status();
    let items: usize = report.batches().iter().map(observations).sum();
    let issues = report.issues().map_or(0, |issues| issues.issues().len());
    let mut line = format!(
        "{} {} {}/{} {}",
        now(),
        status.endpoint_id(),
        status.provider(),
        status.request_class(),
        outcome(status),
    );
    if items > 0 || status.outcome() != Outcome::Failed {
        let _ = write!(line, " items={items}");
    }
    if issues > 0 {
        let _ = write!(line, " issues={issues}");
    }
    if let Some(nanos) = status.duration_nanos() {
        #[allow(clippy::cast_precision_loss)]
        let millis = nanos as f64 / 1e6;
        let _ = write!(line, " {millis:.1}ms");
    }
    println!("{line}");
    if !verbose {
        return;
    }
    for batch in report.batches() {
        batch_lines(batch);
    }
    for issue in report
        .issues()
        .into_iter()
        .flat_map(nv_telemetry_model::ProjectionIssues::issues)
    {
        println!(
            "    issue {} {:?}{}",
            issue.path(),
            issue.kind(),
            issue
                .detail()
                .map(|detail| format!(": {detail}"))
                .unwrap_or_default()
        );
    }
}

pub(crate) fn note(endpoint: &str, text: &str) {
    println!("{} {endpoint} {text}", now());
}

fn outcome(status: &AcquisitionStatus) -> String {
    let mut text = format!("{:?}", status.outcome()).to_lowercase();
    if let Some(class) = status.failure_class() {
        let _ = write!(text, " {class:?}");
    }
    if let Some(detail) = status.detail() {
        let _ = write!(text, " ({detail})");
    }
    text
}

fn observations(batch: &ObservationBatch) -> usize {
    match batch.payload() {
        Payload::Readings(readings) => readings.samples().len(),
        Payload::States(states) => states.observations().len(),
        Payload::Inventory(inventory) => inventory.items().len(),
        Payload::Logs(logs) => logs.records().len(),
        Payload::Resources(graph) => graph.resources().len(),
        _ => 0,
    }
}

fn batch_lines(batch: &ObservationBatch) {
    match batch.payload() {
        Payload::Readings(readings) => {
            let units: BTreeMap<_, _> = readings
                .descriptors()
                .iter()
                .filter_map(|descriptor| descriptor.unit().map(|unit| (descriptor.key(), unit)))
                .collect();
            for sample in readings.samples() {
                let key = sample.key();
                println!(
                    "    reading {}{} = {}{}",
                    subject(key.subject()),
                    key.facet()
                        .map(|facet| format!(" {facet}"))
                        .unwrap_or_default(),
                    numeric(sample.value()),
                    units
                        .get(key)
                        .map(|unit| format!(" {unit}"))
                        .unwrap_or_default(),
                );
            }
        }
        Payload::States(states) => {
            for state in states.observations() {
                println!(
                    "    state {} {} = {}",
                    subject(state.subject()),
                    state.name(),
                    value(state.value()),
                );
            }
        }
        Payload::Inventory(inventory) => {
            for item in inventory.items() {
                println!(
                    "    item {}{}",
                    subject(item.subject()),
                    attributes(item.attributes()),
                );
            }
        }
        Payload::Logs(logs) => {
            for record in logs.records() {
                println!(
                    "    log {} {}{}: {}",
                    record.occurred_at().map_or_else(|| "-".to_owned(), clock),
                    record
                        .severity()
                        .map_or_else(|| "-".to_owned(), |severity| format!("{severity:?}")),
                    record
                        .entry_id()
                        .map(|id| format!(" #{id}"))
                        .unwrap_or_default(),
                    record.message(),
                );
            }
        }
        Payload::Resources(graph) => {
            for resource in graph.resources() {
                println!(
                    "    resource {} {}",
                    subject(resource.subject()),
                    resource.source_key()
                );
            }
        }
        _ => {}
    }
}

fn subject(subject: &Subject) -> String {
    let mut text = subject.kind().to_owned();
    for part in subject.scope() {
        let _ = write!(text, "/{part}");
    }
    let _ = write!(text, "/{}", subject.id());
    text
}

fn attributes(attributes: Option<&BTreeMap<String, Value>>) -> String {
    attributes
        .into_iter()
        .flatten()
        .fold(String::new(), |mut text, (name, attribute)| {
            let _ = write!(text, " {name}={}", value(attribute));
            text
        })
}

fn numeric(value: &NumericValue) -> String {
    match value {
        NumericValue::Double(double) => double.to_string(),
        NumericValue::Int(int) => int.to_string(),
        NumericValue::Uint(uint) => uint.to_string(),
        other => format!("{other:?}"),
    }
}

fn value(value: &Value) -> String {
    match value.kind() {
        ValueKind::Null => "null".to_owned(),
        ValueKind::Bool(boolean) => boolean.to_string(),
        ValueKind::Int(int) => int.to_string(),
        ValueKind::Uint(uint) => uint.to_string(),
        ValueKind::Double(double) => double.to_string(),
        ValueKind::String(text) => format!("{text:?}"),
        ValueKind::Timestamp(instant) => clock(&instant),
        other => format!("{other:?}"),
    }
}

/// `HH:MM:SS.mmm` UTC.
fn clock(instant: &Timestamp) -> String {
    let seconds = instant.seconds().rem_euclid(86_400);
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60,
        instant.nanos() / 1_000_000
    )
}

fn now() -> String {
    let since = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = i64::try_from(since.as_secs()).unwrap_or(i64::MAX);
    Timestamp::new(seconds, since.subsec_nanos()).map_or_else(|_| "-".to_owned(), |now| clock(&now))
}
