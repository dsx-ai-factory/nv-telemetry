// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from `sources/redfish/manifests/chassis.textpb` by `make codegen`. Do not edit.
//!
//! Deterministic, I/O-free projection from decoded source types to
//! validated observation parts plus issues. Every field is evaluated
//! before identity is decided, absence produces no output, and an
//! unusable answer produces an issue beside the parts.

// Generated code holds the line on correctness lints; the pedantic
// group is style advice for humans and is exactly where a clippy
// release breaks a checked-in file that no one edited.
#![allow(clippy::pedantic)]

/// What one `Chassis` document projected to. The provider
/// assembles batches from these; identity failure leaves every
/// collection empty while the issues still name each fault.
#[derive(Debug)]
pub(crate) struct ChassisParts {
    pub(crate) inventory_items: Vec<::nv_telemetry_model::InventoryItem>,
    pub(crate) state_observations: Vec<::nv_telemetry_model::StateObservation>,
    /// The source fields that projected to nothing, and why.
    pub(crate) issues: Vec<::nv_telemetry_source::ProjectionIssue>,
}
/// Projects one `Chassis` document, located at the *requested*
/// URI.
///
/// # Errors
///
/// `Err` is the residual tier only — a builder refusing inputs this
/// function already triaged is a projection bug, and a bug is an
/// operational fact for the status stream rather than device data.
/// Everything a device can cause comes back as issues inside the
/// parts.
pub(crate) fn project_chassis(
    chassis: &::nv_redfish::schema::chassis::Chassis,
    location: &str,
) -> Result<ChassisParts, ::nv_telemetry_model::Invalid> {
    let mut issues = Vec::new();
    let mut chassis_inventory_attributes_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = {
        let value = chassis.chassis_type;
        match value {
            ::nv_redfish::schema::chassis::ChassisType::Rack => {
                Some(::nv_telemetry_model::Value::string("Rack")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Blade => {
                Some(::nv_telemetry_model::Value::string("Blade")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Enclosure => {
                Some(::nv_telemetry_model::Value::string("Enclosure")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::StandAlone => {
                Some(::nv_telemetry_model::Value::string("StandAlone")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::RackMount => {
                Some(::nv_telemetry_model::Value::string("RackMount")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Card => {
                Some(::nv_telemetry_model::Value::string("Card")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Cartridge => {
                Some(::nv_telemetry_model::Value::string("Cartridge")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Row => {
                Some(::nv_telemetry_model::Value::string("Row")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Pod => {
                Some(::nv_telemetry_model::Value::string("Pod")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Expansion => {
                Some(::nv_telemetry_model::Value::string("Expansion")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Sidecar => {
                Some(::nv_telemetry_model::Value::string("Sidecar")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Zone => {
                Some(::nv_telemetry_model::Value::string("Zone")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Sled => {
                Some(::nv_telemetry_model::Value::string("Sled")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Shelf => {
                Some(::nv_telemetry_model::Value::string("Shelf")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Drawer => {
                Some(::nv_telemetry_model::Value::string("Drawer")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Module => {
                Some(::nv_telemetry_model::Value::string("Module")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Component => {
                Some(::nv_telemetry_model::Value::string("Component")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::IpBasedDrive => {
                Some(::nv_telemetry_model::Value::string("IPBasedDrive")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::RackGroup => {
                Some(::nv_telemetry_model::Value::string("RackGroup")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::StorageEnclosure => {
                Some(::nv_telemetry_model::Value::string("StorageEnclosure")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::ImmersionTank => {
                Some(::nv_telemetry_model::Value::string("ImmersionTank")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::HeatExchanger => {
                Some(::nv_telemetry_model::Value::string("HeatExchanger")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::PowerStrip => {
                Some(::nv_telemetry_model::Value::string("PowerStrip")?)
            }
            ::nv_redfish::schema::chassis::ChassisType::Other => {
                Some(::nv_telemetry_model::Value::string("Other")?)
            }
            _ => {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.ChassisType",
                            "outside the known value set",
                        ),
                    );
                None
            }
        }
    } {
        chassis_inventory_attributes_entries.push(("chassis-type".to_owned(), value));
    }
    if let Some(value) = match chassis.manufacturer.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.Manufacturer",
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
        chassis_inventory_attributes_entries.push(("manufacturer".to_owned(), value));
    }
    if let Some(value) = match chassis.model.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.Model",
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
        chassis_inventory_attributes_entries.push(("model".to_owned(), value));
    }
    if let Some(value) = match chassis.sku.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.SKU",
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
        chassis_inventory_attributes_entries.push(("sku".to_owned(), value));
    }
    if let Some(value) = match chassis.serial_number.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.SerialNumber",
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
        chassis_inventory_attributes_entries.push(("serial-number".to_owned(), value));
    }
    if let Some(value) = match chassis.part_number.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.PartNumber",
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
        chassis_inventory_attributes_entries.push(("part-number".to_owned(), value));
    }
    if let Some(value) = match chassis.asset_tag.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.AssetTag",
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
        chassis_inventory_attributes_entries.push(("asset-tag".to_owned(), value));
    }
    if let Some(value) = match chassis.spare_part_number.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.SparePartNumber",
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
        chassis_inventory_attributes_entries
            .push(("spare-part-number".to_owned(), value));
    }
    if let Some(value) = match chassis.version.clone() {
        Some(Some(value)) => {
            if value.len()
                > ::nv_telemetry_model::limits::VALUE_STRING_VALUE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Chassis.Version",
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
        chassis_inventory_attributes_entries.push(("version".to_owned(), value));
    }
    let chassis_state_value = match chassis.status.as_ref().and_then(|value| value.state)
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
                                "Chassis.Status.State",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let chassis_health_value = match chassis
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
                                "Chassis.Status.Health",
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
        let value = chassis.base.id.clone();
        if value.is_empty() {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "Chassis.Id",
                        "`id`: present but empty",
                    ),
                );
            None
        } else if value.len() > ::nv_telemetry_model::limits::SUBJECT_ID_MAX_LEN as usize
        {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "Chassis.Id",
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
                .kind("chassis")
                .scope(vec![])
                .id(subject_id)
                .build()
            {
                Ok(subject) => Some(subject),
                Err(error) => {
                    let path = "Chassis.Id";
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
        return Ok(ChassisParts {
            inventory_items: Vec::new(),
            state_observations: Vec::new(),
            issues,
        });
    };
    let mut inventory_items = Vec::new();
    let mut state_observations = Vec::new();
    if !chassis_inventory_attributes_entries.is_empty() {
        let mut builder = ::nv_telemetry_model::InventoryItem::builder();
        builder = builder.subject(subject.clone());
        builder = builder.source_key(crate::uri::canonical(location));
        builder = builder
            .attributes(chassis_inventory_attributes_entries.into_iter().collect());
        inventory_items.push(builder.build()?);
    }
    if chassis_state_value.is_some() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        if let Some(value) = chassis_state_value {
            builder = builder.value(value);
        }
        builder = builder.name("state");
        state_observations.push(builder.build()?);
    }
    if chassis_health_value.is_some() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        if let Some(value) = chassis_health_value {
            builder = builder.value(value);
        }
        builder = builder.name("health");
        state_observations.push(builder.build()?);
    }
    Ok(ChassisParts {
        inventory_items,
        state_observations,
        issues,
    })
}
