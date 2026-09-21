// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from `sources/redfish/manifests/events.textpb` by `make codegen`. Do not edit.
//!
//! Deterministic, I/O-free projection from decoded source types to
//! validated observation parts plus issues. Every field is evaluated
//! before identity is decided, absence produces no output, and an
//! unusable answer produces an issue beside the parts.

// Generated code holds the line on correctness lints; the pedantic
// group is style advice for humans and is exactly where a clippy
// release breaks a checked-in file that no one edited.
#![allow(clippy::pedantic)]

/// What one `EventRecord` document projected to. The provider
/// assembles batches from these; identity failure leaves every
/// collection empty while the issues still name each fault.
#[derive(Debug)]
pub(crate) struct EventRecordParts {
    pub(crate) log_records: Vec<::nv_telemetry_model::LogRecord>,
    /// The source fields that projected to nothing, and why.
    pub(crate) issues: Vec<::nv_telemetry_source::ProjectionIssue>,
}
/// Projects one `EventRecord` document, located at the *requested*
/// URI.
///
/// # Errors
///
/// `Err` is the residual tier only — a builder refusing inputs this
/// function already triaged is a projection bug, and a bug is an
/// operational fact for the status stream rather than device data.
/// Everything a device can cause comes back as issues inside the
/// parts.
pub(crate) fn project_event_record(
    event_record: &::nv_redfish::schema::event::EventRecord,
    _location: &str,
) -> Result<EventRecordParts, ::nv_telemetry_model::Invalid> {
    let mut issues = Vec::new();
    let event_record_occurred_at = match event_record.event_timestamp {
        Some(value) => Some(crate::instant::timestamp(value)?),
        None => None,
    };
    let event_record_severity = match event_record.message_severity {
        Some(value) => {
            match value {
                ::nv_redfish::schema::resource::Health::Ok => {
                    Some(::nv_telemetry_model::Severity::Info)
                }
                ::nv_redfish::schema::resource::Health::Warning => {
                    Some(::nv_telemetry_model::Severity::Warning)
                }
                ::nv_redfish::schema::resource::Health::Critical => {
                    Some(::nv_telemetry_model::Severity::Critical)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "EventRecord.MessageSeverity",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        None => {
            match event_record.severity.clone() {
                Some(value) => {
                    match value.as_str() {
                        "OK" => Some(::nv_telemetry_model::Severity::Info),
                        "Warning" => Some(::nv_telemetry_model::Severity::Warning),
                        "Critical" => Some(::nv_telemetry_model::Severity::Critical),
                        _ => {
                            issues
                                .push(
                                    ::nv_telemetry_source::ProjectionIssue::invalid(
                                        "EventRecord.Severity",
                                        "outside the known value set",
                                    ),
                                );
                            None
                        }
                    }
                }
                None => None,
            }
        }
    };
    let event_record_message = match event_record.message.clone() {
        Some(value) => {
            if value.len()
                > ::nv_telemetry_model::limits::LOGRECORD_MESSAGE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "EventRecord.Message",
                            format!(
                                "`message`: {} bytes long, over the schema's bound of {}",
                                value.len(),
                                ::nv_telemetry_model::limits::LOGRECORD_MESSAGE_MAX_LEN
                            ),
                        ),
                    );
                None
            } else {
                Some(value)
            }
        }
        None => {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::missing(
                        "EventRecord.Message",
                    ),
                );
            None
        }
    };
    let event_record_entry_id = match event_record.event_id.clone() {
        Some(value) => {
            if value.is_empty() {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "EventRecord.EventId",
                            "`entry_id`: present but empty",
                        ),
                    );
                None
            } else if value.len()
                > ::nv_telemetry_model::limits::LOGRECORD_ENTRY_ID_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "EventRecord.EventId",
                            format!(
                                "`entry_id`: {} bytes long, over the schema's bound of {}",
                                value.len(),
                                ::nv_telemetry_model::limits::LOGRECORD_ENTRY_ID_MAX_LEN
                            ),
                        ),
                    );
                None
            } else {
                Some(value)
            }
        }
        None => None,
    };
    let mut event_record_attributes_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = {
        let value = event_record.message_id.clone();
        if value.len()
            > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
        {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "EventRecord.MessageId",
                        format!(
                            "`string_value`: {} bytes long, over the schema's bound of {}",
                            value.len(),
                            ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN
                        ),
                    ),
                );
            None
        } else {
            Some(::nv_telemetry_model::Value::string(value)?)
        }
    } {
        event_record_attributes_entries.push(("message-id".to_owned(), value));
    }
    if let Some(value) = {
        let value = event_record.event_type;
        match value {
            ::nv_redfish::schema::event::EventType::StatusChange => {
                Some(::nv_telemetry_model::Value::string("StatusChange")?)
            }
            ::nv_redfish::schema::event::EventType::ResourceUpdated => {
                Some(::nv_telemetry_model::Value::string("ResourceUpdated")?)
            }
            ::nv_redfish::schema::event::EventType::ResourceAdded => {
                Some(::nv_telemetry_model::Value::string("ResourceAdded")?)
            }
            ::nv_redfish::schema::event::EventType::ResourceRemoved => {
                Some(::nv_telemetry_model::Value::string("ResourceRemoved")?)
            }
            ::nv_redfish::schema::event::EventType::Alert => {
                Some(::nv_telemetry_model::Value::string("Alert")?)
            }
            ::nv_redfish::schema::event::EventType::MetricReport => {
                Some(::nv_telemetry_model::Value::string("MetricReport")?)
            }
            ::nv_redfish::schema::event::EventType::Other => {
                Some(::nv_telemetry_model::Value::string("Other")?)
            }
            _ => {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "EventRecord.EventType",
                            "outside the known value set",
                        ),
                    );
                None
            }
        }
    } {
        event_record_attributes_entries.push(("event-type".to_owned(), value));
    }
    if let Some(value) = {
        let value = event_record.member_id.clone();
        if value.len()
            > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
        {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "EventRecord.MemberId",
                        format!(
                            "`string_value`: {} bytes long, over the schema's bound of {}",
                            value.len(),
                            ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN
                        ),
                    ),
                );
            None
        } else {
            Some(::nv_telemetry_model::Value::string(value)?)
        }
    } {
        event_record_attributes_entries.push(("member-id".to_owned(), value));
    }
    if let Some(value) = match event_record.context.clone() {
        Some(value) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "EventRecord.Context",
                            format!(
                                "`string_value`: {} bytes long, over the schema's bound of {}",
                                value.len(),
                                ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN
                            ),
                        ),
                    );
                None
            } else {
                Some(::nv_telemetry_model::Value::string(value)?)
            }
        }
        None => None,
    } {
        event_record_attributes_entries.push(("context".to_owned(), value));
    }
    let mut log_records = Vec::new();
    if event_record_message.is_some() {
        let mut builder = ::nv_telemetry_model::LogRecord::builder();
        if let Some(value) = event_record_occurred_at {
            builder = builder.occurred_at(value);
        }
        if let Some(value) = event_record_severity {
            builder = builder.severity(value);
        }
        if let Some(value) = event_record_message {
            builder = builder.message(value);
        }
        if let Some(value) = event_record_entry_id {
            builder = builder.entry_id(value);
        }
        if !event_record_attributes_entries.is_empty() {
            builder = builder
                .attributes(event_record_attributes_entries.into_iter().collect());
        }
        log_records.push(builder.build()?);
    }
    Ok(EventRecordParts {
        log_records,
        issues,
    })
}
