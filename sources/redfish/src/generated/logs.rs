// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from `sources/redfish/manifests/logs.textpb` by `make codegen`. Do not edit.
//!
//! Deterministic, I/O-free projection from decoded source types to
//! validated observation parts plus issues. Every field is evaluated
//! before identity is decided, absence produces no output, and an
//! unusable answer produces an issue beside the parts.

// Generated code holds the line on correctness lints; the pedantic
// group is style advice for humans and is exactly where a clippy
// release breaks a checked-in file that no one edited.
#![allow(clippy::pedantic)]

/// What one `LogEntry` document projected to. The provider
/// assembles batches from these; identity failure leaves every
/// collection empty while the issues still name each fault.
#[derive(Debug)]
pub(crate) struct LogEntryParts {
    pub(crate) log_records: Vec<::nv_telemetry_model::LogRecord>,
    /// The source fields that projected to nothing, and why.
    pub(crate) issues: Vec<::nv_telemetry_source::ProjectionIssue>,
}
/// Projects one `LogEntry` document, located at the *requested*
/// URI.
///
/// # Errors
///
/// `Err` is the residual tier only — a builder refusing inputs this
/// function already triaged is a projection bug, and a bug is an
/// operational fact for the status stream rather than device data.
/// Everything a device can cause comes back as issues inside the
/// parts.
pub(crate) fn project_log_entry(
    log_entry: &::nv_redfish::schema::log_entry::LogEntry,
    _location: &str,
) -> Result<LogEntryParts, ::nv_telemetry_model::Invalid> {
    let mut issues = Vec::new();
    let log_record_occurred_at = match log_entry.created {
        Some(value) => Some(crate::instant::timestamp(value)?),
        None => None,
    };
    let log_record_severity = match log_entry.severity {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::log_entry::EventSeverity::Ok => {
                    Some(::nv_telemetry_model::Severity::Info)
                }
                ::nv_redfish::schema::log_entry::EventSeverity::Warning => {
                    Some(::nv_telemetry_model::Severity::Warning)
                }
                ::nv_redfish::schema::log_entry::EventSeverity::Critical => {
                    Some(::nv_telemetry_model::Severity::Critical)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "LogEntry.Severity",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let log_record_message = match log_entry.message.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::LOGRECORD_MESSAGE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "LogEntry.Message",
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
        _ => {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::missing("LogEntry.Message"),
                );
            None
        }
    };
    let log_record_entry_id = {
        let value = log_entry.base.id.clone();
        if value.is_empty() {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "LogEntry.Id",
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
                        "LogEntry.Id",
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
    };
    let mut log_record_attributes_entries: Vec<(String, ::nv_telemetry_model::Value)> = Vec::new();
    if let Some(value) = match log_entry.message_id.clone() {
        Some(value) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "LogEntry.MessageId",
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
        log_record_attributes_entries.push(("message-id".to_owned(), value));
    }
    if let Some(value) = {
        let value = log_entry.entry_type;
        match value {
            ::nv_redfish::schema::log_entry::LogEntryType::Event => {
                Some(::nv_telemetry_model::Value::string("Event")?)
            }
            ::nv_redfish::schema::log_entry::LogEntryType::Sel => {
                Some(::nv_telemetry_model::Value::string("SEL")?)
            }
            ::nv_redfish::schema::log_entry::LogEntryType::Oem => {
                Some(::nv_telemetry_model::Value::string("Oem")?)
            }
            ::nv_redfish::schema::log_entry::LogEntryType::Cxl => {
                Some(::nv_telemetry_model::Value::string("CXL")?)
            }
            _ => {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "LogEntry.EntryType",
                            "outside the known value set",
                        ),
                    );
                None
            }
        }
    } {
        log_record_attributes_entries.push(("entry-type".to_owned(), value));
    }
    if let Some(value) = match log_entry.entry_code.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "LogEntry.EntryCode",
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
        _ => None,
    } {
        log_record_attributes_entries.push(("entry-code".to_owned(), value));
    }
    if let Some(value) = match log_entry.sensor_type.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "LogEntry.SensorType",
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
        _ => None,
    } {
        log_record_attributes_entries.push(("sensor-type".to_owned(), value));
    }
    if let Some(value) = match log_entry.sensor_number {
        Some(Some(value)) => Some(::nv_telemetry_model::Value::int(value)),
        _ => None,
    } {
        log_record_attributes_entries.push(("sensor-number".to_owned(), value));
    }
    if let Some(value) = match log_entry.resolved {
        Some(Some(value)) => Some(::nv_telemetry_model::Value::bool(value)),
        _ => None,
    } {
        log_record_attributes_entries.push(("resolved".to_owned(), value));
    }
    if let Some(value) = match log_entry.event_timestamp {
        Some(value) => {
            Some(
                ::nv_telemetry_model::Value::timestamp(crate::instant::timestamp(value)?),
            )
        }
        None => None,
    } {
        log_record_attributes_entries.push(("event-timestamp".to_owned(), value));
    }
    if let Some(value) = match log_entry.oem_record_format.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "LogEntry.OemRecordFormat",
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
        _ => None,
    } {
        log_record_attributes_entries.push(("oem-record-format".to_owned(), value));
    }
    let mut log_records = Vec::new();
    if log_record_message.is_some() {
        let mut builder = ::nv_telemetry_model::LogRecord::builder();
        if let Some(value) = log_record_occurred_at {
            builder = builder.occurred_at(value);
        }
        if let Some(value) = log_record_severity {
            builder = builder.severity(value);
        }
        if let Some(value) = log_record_message {
            builder = builder.message(value);
        }
        if let Some(value) = log_record_entry_id {
            builder = builder.entry_id(value);
        }
        if !log_record_attributes_entries.is_empty() {
            builder = builder
                .attributes(log_record_attributes_entries.into_iter().collect());
        }
        log_records.push(builder.build()?);
    }
    Ok(LogEntryParts {
        log_records,
        issues,
    })
}
