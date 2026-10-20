// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from `sources/redfish/manifests/firmware.textpb` by `make codegen`. Do not edit.
//!
//! Deterministic, I/O-free projection from decoded source types to
//! validated observation parts plus issues. Every field is evaluated
//! before identity is decided, absence produces no output, and an
//! unusable answer produces an issue beside the parts.

// Generated code holds the line on correctness lints; the pedantic
// group is style advice for humans and is exactly where a clippy
// release breaks a checked-in file that no one edited.
#![allow(clippy::pedantic)]

/// What one `SoftwareInventory` document projected to. The provider
/// assembles batches from these; identity failure leaves every
/// collection empty while the issues still name each fault.
#[derive(Debug)]
pub(crate) struct SoftwareInventoryParts {
    pub(crate) inventory_items: Vec<::nv_telemetry_model::InventoryItem>,
    pub(crate) state_observations: Vec<::nv_telemetry_model::StateObservation>,
    /// The source fields that projected to nothing, and why.
    pub(crate) issues: Vec<::nv_telemetry_source::ProjectionIssue>,
}
/// Projects one `SoftwareInventory` document, located at the *requested*
/// URI.
///
/// # Errors
///
/// `Err` is the residual tier only — a builder refusing inputs this
/// function already triaged is a projection bug, and a bug is an
/// operational fact for the status stream rather than device data.
/// Everything a device can cause comes back as issues inside the
/// parts.
pub(crate) fn project_software_inventory(
    software_inventory: &::nv_redfish::schema::software_inventory::SoftwareInventory,
    location: &str,
) -> Result<SoftwareInventoryParts, ::nv_telemetry_model::Invalid> {
    let mut issues = Vec::new();
    let mut firmware_inventory_attributes_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = {
        let value = software_inventory.name.clone();
        if value.len()
            > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
        {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "SoftwareInventory.Name",
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
        firmware_inventory_attributes_entries.push(("name".to_owned(), value));
    }
    if let Some(value) = match software_inventory.version.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "SoftwareInventory.Version",
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
        firmware_inventory_attributes_entries.push(("version".to_owned(), value));
    }
    if let Some(value) = match software_inventory.manufacturer.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "SoftwareInventory.Manufacturer",
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
        firmware_inventory_attributes_entries.push(("manufacturer".to_owned(), value));
    }
    if let Some(value) = match software_inventory.software_id.clone() {
        Some(value) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "SoftwareInventory.SoftwareId",
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
        firmware_inventory_attributes_entries.push(("software-id".to_owned(), value));
    }
    if let Some(value) = match software_inventory.release_date {
        Some(Some(value)) => {
            Some(
                ::nv_telemetry_model::Value::timestamp(crate::instant::timestamp(value)?),
            )
        }
        _ => None,
    } {
        firmware_inventory_attributes_entries.push(("release-date".to_owned(), value));
    }
    if let Some(value) = match software_inventory.lowest_supported_version.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "SoftwareInventory.LowestSupportedVersion",
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
        firmware_inventory_attributes_entries
            .push(("lowest-supported-version".to_owned(), value));
    }
    if let Some(value) = match software_inventory.updateable {
        Some(Some(value)) => Some(::nv_telemetry_model::Value::bool(value)),
        _ => None,
    } {
        firmware_inventory_attributes_entries.push(("updateable".to_owned(), value));
    }
    let firmware_state_value = match software_inventory
        .status
        .as_ref()
        .and_then(|value| value.state)
    {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::resource::State::Enabled => {
                    Some(::nv_telemetry_model::Value::string("Enabled")?)
                }
                ::nv_redfish::schema::resource::State::Disabled => {
                    Some(::nv_telemetry_model::Value::string("Disabled")?)
                }
                ::nv_redfish::schema::resource::State::StandbyOffline => {
                    Some(::nv_telemetry_model::Value::string("StandbyOffline")?)
                }
                ::nv_redfish::schema::resource::State::StandbySpare => {
                    Some(::nv_telemetry_model::Value::string("StandbySpare")?)
                }
                ::nv_redfish::schema::resource::State::InTest => {
                    Some(::nv_telemetry_model::Value::string("InTest")?)
                }
                ::nv_redfish::schema::resource::State::Starting => {
                    Some(::nv_telemetry_model::Value::string("Starting")?)
                }
                ::nv_redfish::schema::resource::State::Absent => {
                    Some(::nv_telemetry_model::Value::string("Absent")?)
                }
                ::nv_redfish::schema::resource::State::UnavailableOffline => {
                    Some(::nv_telemetry_model::Value::string("UnavailableOffline")?)
                }
                ::nv_redfish::schema::resource::State::Deferring => {
                    Some(::nv_telemetry_model::Value::string("Deferring")?)
                }
                ::nv_redfish::schema::resource::State::Quiesced => {
                    Some(::nv_telemetry_model::Value::string("Quiesced")?)
                }
                ::nv_redfish::schema::resource::State::Updating => {
                    Some(::nv_telemetry_model::Value::string("Updating")?)
                }
                ::nv_redfish::schema::resource::State::Qualified => {
                    Some(::nv_telemetry_model::Value::string("Qualified")?)
                }
                ::nv_redfish::schema::resource::State::Degraded => {
                    Some(::nv_telemetry_model::Value::string("Degraded")?)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "SoftwareInventory.Status.State",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let firmware_health_value = match software_inventory
        .status
        .as_ref()
        .and_then(|value| value.health)
    {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::resource::Health::Ok => {
                    Some(::nv_telemetry_model::Value::string("OK")?)
                }
                ::nv_redfish::schema::resource::Health::Warning => {
                    Some(::nv_telemetry_model::Value::string("Warning")?)
                }
                ::nv_redfish::schema::resource::Health::Critical => {
                    Some(::nv_telemetry_model::Value::string("Critical")?)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "SoftwareInventory.Status.Health",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let subject_id = {
        let value = software_inventory.id.clone();
        if value.is_empty() {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "SoftwareInventory.Id",
                        "`id`: present but empty",
                    ),
                );
            None
        } else if value.len() > ::nv_telemetry_model::limits::SUBJECT_ID_MAX_LEN as usize
        {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "SoftwareInventory.Id",
                        format!(
                            "`id`: {} bytes long, over the schema's bound of {}", value
                            .len(), ::nv_telemetry_model::limits::SUBJECT_ID_MAX_LEN
                        ),
                    ),
                );
            None
        } else {
            Some(value)
        }
    };
    let subject = match (subject_id,) {
        (Some(subject_id),) => {
            match ::nv_telemetry_model::Subject::builder()
                .kind("firmware")
                .scope(vec![])
                .id(subject_id)
                .build()
            {
                Ok(subject) => Some(subject),
                Err(error) => {
                    let path = "SoftwareInventory.Id";
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                path,
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let Some(subject) = subject else {
        return Ok(SoftwareInventoryParts {
            inventory_items: Vec::new(),
            state_observations: Vec::new(),
            issues,
        });
    };
    let mut inventory_items = Vec::new();
    let mut state_observations = Vec::new();
    if !firmware_inventory_attributes_entries.is_empty() {
        let mut builder = ::nv_telemetry_model::InventoryItem::builder();
        builder = builder.subject(subject.clone());
        builder = builder.source_key(crate::uri::canonical(location));
        builder = builder
            .attributes(firmware_inventory_attributes_entries.into_iter().collect());
        inventory_items.push(builder.build()?);
    }
    if firmware_state_value.is_some() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        if let Some(value) = firmware_state_value {
            builder = builder.value(value);
        }
        builder = builder.name("state");
        state_observations.push(builder.build()?);
    }
    if firmware_health_value.is_some() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        if let Some(value) = firmware_health_value {
            builder = builder.value(value);
        }
        builder = builder.name("health");
        state_observations.push(builder.build()?);
    }
    Ok(SoftwareInventoryParts {
        inventory_items,
        state_observations,
        issues,
    })
}
