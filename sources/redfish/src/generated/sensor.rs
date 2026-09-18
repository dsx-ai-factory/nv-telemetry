// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from `sources/redfish/manifests/sensor.textpb` by `make codegen`. Do not edit.
//!
//! Deterministic, I/O-free projection from decoded source types to
//! validated observation parts plus issues. Every field is evaluated
//! before identity is decided, absence produces no output, and an
//! unusable answer produces an issue beside the parts.

// Generated code holds the line on correctness lints; the pedantic
// group is style advice for humans and is exactly where a clippy
// release breaks a checked-in file that no one edited.
#![allow(clippy::pedantic)]

/// What one `Sensor` document projected to. The provider
/// assembles batches from these; identity failure leaves every
/// collection empty while the issues still name each fault.
#[derive(Debug)]
pub(crate) struct SensorParts {
    pub(crate) signal_descriptors: Vec<::nv_telemetry_model::SignalDescriptor>,
    pub(crate) readings: Vec<::nv_telemetry_model::Reading>,
    pub(crate) state_observations: Vec<::nv_telemetry_model::StateObservation>,
    /// The source fields that projected to nothing, and why.
    pub(crate) issues: Vec<::nv_telemetry_source::ProjectionIssue>,
}
/// Projects one `Sensor` document, located at the *requested*
/// URI.
///
/// # Errors
///
/// `Err` is the residual tier only — a builder refusing inputs this
/// function already triaged is a projection bug, and a bug is an
/// operational fact for the status stream rather than device data.
/// Everything a device can cause comes back as issues inside the
/// parts.
pub(crate) fn project_sensor(
    sensor: &::nv_redfish::schema::sensor::Sensor,
    location: &str,
) -> Result<SensorParts, ::nv_telemetry_model::Invalid> {
    let mut issues = Vec::new();
    let sensor_descriptor_kind = match sensor.reading_type {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::sensor::ReadingType::Temperature => {
                    Some("temperature")
                }
                ::nv_redfish::schema::sensor::ReadingType::Humidity => Some("humidity"),
                ::nv_redfish::schema::sensor::ReadingType::Power => Some("power"),
                ::nv_redfish::schema::sensor::ReadingType::EnergykWh => Some("energy"),
                ::nv_redfish::schema::sensor::ReadingType::EnergyJoules => Some("energy"),
                ::nv_redfish::schema::sensor::ReadingType::EnergyWh => Some("energy"),
                ::nv_redfish::schema::sensor::ReadingType::ChargeAh => Some("charge"),
                ::nv_redfish::schema::sensor::ReadingType::Voltage => Some("voltage"),
                ::nv_redfish::schema::sensor::ReadingType::Current => Some("current"),
                ::nv_redfish::schema::sensor::ReadingType::Frequency => Some("frequency"),
                ::nv_redfish::schema::sensor::ReadingType::Pressure => Some("pressure"),
                ::nv_redfish::schema::sensor::ReadingType::PressurekPa => {
                    Some("pressure")
                }
                ::nv_redfish::schema::sensor::ReadingType::PressurePa => Some("pressure"),
                ::nv_redfish::schema::sensor::ReadingType::LiquidLevel => {
                    Some("liquid-level")
                }
                ::nv_redfish::schema::sensor::ReadingType::Rotational => {
                    Some("rotational")
                }
                ::nv_redfish::schema::sensor::ReadingType::AirFlow => Some("air-flow"),
                ::nv_redfish::schema::sensor::ReadingType::AirFlowCmm => Some("air-flow"),
                ::nv_redfish::schema::sensor::ReadingType::LiquidFlow => {
                    Some("liquid-flow")
                }
                ::nv_redfish::schema::sensor::ReadingType::LiquidFlowLpm => {
                    Some("liquid-flow")
                }
                ::nv_redfish::schema::sensor::ReadingType::Barometric => {
                    Some("barometric")
                }
                ::nv_redfish::schema::sensor::ReadingType::Altitude => Some("altitude"),
                ::nv_redfish::schema::sensor::ReadingType::Percent => Some("percent"),
                ::nv_redfish::schema::sensor::ReadingType::AbsoluteHumidity => {
                    Some("absolute-humidity")
                }
                ::nv_redfish::schema::sensor::ReadingType::Heat => Some("heat"),
                ::nv_redfish::schema::sensor::ReadingType::LinearPosition => {
                    Some("linear-position")
                }
                ::nv_redfish::schema::sensor::ReadingType::LinearVelocity => {
                    Some("linear-velocity")
                }
                ::nv_redfish::schema::sensor::ReadingType::LinearAcceleration => {
                    Some("linear-acceleration")
                }
                ::nv_redfish::schema::sensor::ReadingType::RotationalPosition => {
                    Some("rotational-position")
                }
                ::nv_redfish::schema::sensor::ReadingType::RotationalVelocity => {
                    Some("rotational-velocity")
                }
                ::nv_redfish::schema::sensor::ReadingType::RotationalAcceleration => {
                    Some("rotational-acceleration")
                }
                ::nv_redfish::schema::sensor::ReadingType::Valve => Some("valve"),
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.ReadingType",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let sensor_descriptor_unit = match sensor.reading_units.clone() {
        Some(Some(value)) => {
            if value.is_empty() {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Sensor.ReadingUnits",
                            "`unit`: present but empty",
                        ),
                    );
                None
            } else if value.len()
                > ::nv_telemetry_model::limits::SIGNALDESCRIPTOR_UNIT_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "Sensor.ReadingUnits",
                            format!(
                                "`unit`: {} bytes long, over the schema's bound of {}",
                                value.len(),
                                ::nv_telemetry_model::limits::SIGNALDESCRIPTOR_UNIT_MAX_LEN
                            ),
                        ),
                    );
                None
            } else {
                Some(value)
            }
        }
        _ => None,
    };
    let sensor_descriptor_range_min = match sensor.reading_range_min {
        Some(Some(value)) => {
            match ::nv_telemetry_model::NumericValue::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.ReadingRangeMin",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let sensor_descriptor_range_max = match sensor.reading_range_max {
        Some(Some(value)) => {
            match ::nv_telemetry_model::NumericValue::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.ReadingRangeMax",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let sensor_descriptor_range = if sensor_descriptor_range_min.is_some()
        || sensor_descriptor_range_max.is_some()
    {
        let mut builder = ::nv_telemetry_model::ValueRange::builder();
        if let Some(value) = sensor_descriptor_range_min {
            builder = builder.min(value);
        }
        if let Some(value) = sensor_descriptor_range_max {
            builder = builder.max(value);
        }
        match builder.build() {
            Ok(value) => Some(value),
            Err(error) => {
                let path = if error.path().starts_with("min") {
                    "Sensor.ReadingRangeMin"
                } else if error.path().starts_with("max") {
                    "Sensor.ReadingRangeMax"
                } else {
                    "Sensor.ReadingRangeMin"
                };
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
    } else {
        None
    };
    let sensor_sample_value = match sensor.reading {
        Some(Some(value)) => {
            match ::nv_telemetry_model::NumericValue::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Reading",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let sensor_state_value = match sensor.status.as_ref().and_then(|value| value.state) {
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
                                "Sensor.Status.State",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let sensor_health_value = match sensor.status.as_ref().and_then(|value| value.health)
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
                                "Sensor.Status.Health",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    };
    let mut sensor_threshold_upper_caution_value_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.upper_caution.as_ref())
        .and_then(|value| value.activation)
    {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::sensor::ThresholdActivation::Increasing => {
                    Some(::nv_telemetry_model::Value::string("Increasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Decreasing => {
                    Some(::nv_telemetry_model::Value::string("Decreasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Either => {
                    Some(::nv_telemetry_model::Value::string("Either")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Disabled => {
                    Some(::nv_telemetry_model::Value::string("Disabled")?)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.UpperCaution.Activation",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_upper_caution_value_entries
            .push(("activation".to_owned(), value));
    }
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.upper_caution.as_ref())
        .and_then(|value| value.reading)
    {
        Some(Some(value)) => {
            match ::nv_telemetry_model::Value::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.UpperCaution.Reading",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_upper_caution_value_entries.push(("reading".to_owned(), value));
    }
    let mut sensor_threshold_upper_critical_value_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.upper_critical.as_ref())
        .and_then(|value| value.activation)
    {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::sensor::ThresholdActivation::Increasing => {
                    Some(::nv_telemetry_model::Value::string("Increasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Decreasing => {
                    Some(::nv_telemetry_model::Value::string("Decreasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Either => {
                    Some(::nv_telemetry_model::Value::string("Either")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Disabled => {
                    Some(::nv_telemetry_model::Value::string("Disabled")?)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.UpperCritical.Activation",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_upper_critical_value_entries
            .push(("activation".to_owned(), value));
    }
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.upper_critical.as_ref())
        .and_then(|value| value.reading)
    {
        Some(Some(value)) => {
            match ::nv_telemetry_model::Value::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.UpperCritical.Reading",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_upper_critical_value_entries
            .push(("reading".to_owned(), value));
    }
    let mut sensor_threshold_upper_fatal_value_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.upper_fatal.as_ref())
        .and_then(|value| value.activation)
    {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::sensor::ThresholdActivation::Increasing => {
                    Some(::nv_telemetry_model::Value::string("Increasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Decreasing => {
                    Some(::nv_telemetry_model::Value::string("Decreasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Either => {
                    Some(::nv_telemetry_model::Value::string("Either")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Disabled => {
                    Some(::nv_telemetry_model::Value::string("Disabled")?)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.UpperFatal.Activation",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_upper_fatal_value_entries
            .push(("activation".to_owned(), value));
    }
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.upper_fatal.as_ref())
        .and_then(|value| value.reading)
    {
        Some(Some(value)) => {
            match ::nv_telemetry_model::Value::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.UpperFatal.Reading",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_upper_fatal_value_entries.push(("reading".to_owned(), value));
    }
    let mut sensor_threshold_lower_caution_value_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.lower_caution.as_ref())
        .and_then(|value| value.activation)
    {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::sensor::ThresholdActivation::Increasing => {
                    Some(::nv_telemetry_model::Value::string("Increasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Decreasing => {
                    Some(::nv_telemetry_model::Value::string("Decreasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Either => {
                    Some(::nv_telemetry_model::Value::string("Either")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Disabled => {
                    Some(::nv_telemetry_model::Value::string("Disabled")?)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.LowerCaution.Activation",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_lower_caution_value_entries
            .push(("activation".to_owned(), value));
    }
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.lower_caution.as_ref())
        .and_then(|value| value.reading)
    {
        Some(Some(value)) => {
            match ::nv_telemetry_model::Value::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.LowerCaution.Reading",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_lower_caution_value_entries.push(("reading".to_owned(), value));
    }
    let mut sensor_threshold_lower_critical_value_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.lower_critical.as_ref())
        .and_then(|value| value.activation)
    {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::sensor::ThresholdActivation::Increasing => {
                    Some(::nv_telemetry_model::Value::string("Increasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Decreasing => {
                    Some(::nv_telemetry_model::Value::string("Decreasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Either => {
                    Some(::nv_telemetry_model::Value::string("Either")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Disabled => {
                    Some(::nv_telemetry_model::Value::string("Disabled")?)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.LowerCritical.Activation",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_lower_critical_value_entries
            .push(("activation".to_owned(), value));
    }
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.lower_critical.as_ref())
        .and_then(|value| value.reading)
    {
        Some(Some(value)) => {
            match ::nv_telemetry_model::Value::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.LowerCritical.Reading",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_lower_critical_value_entries
            .push(("reading".to_owned(), value));
    }
    let mut sensor_threshold_lower_fatal_value_entries: Vec<
        (String, ::nv_telemetry_model::Value),
    > = Vec::new();
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.lower_fatal.as_ref())
        .and_then(|value| value.activation)
    {
        Some(Some(value)) => {
            match value {
                ::nv_redfish::schema::sensor::ThresholdActivation::Increasing => {
                    Some(::nv_telemetry_model::Value::string("Increasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Decreasing => {
                    Some(::nv_telemetry_model::Value::string("Decreasing")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Either => {
                    Some(::nv_telemetry_model::Value::string("Either")?)
                }
                ::nv_redfish::schema::sensor::ThresholdActivation::Disabled => {
                    Some(::nv_telemetry_model::Value::string("Disabled")?)
                }
                _ => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.LowerFatal.Activation",
                                "outside the known value set",
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_lower_fatal_value_entries
            .push(("activation".to_owned(), value));
    }
    if let Some(value) = match sensor
        .thresholds
        .as_ref()
        .and_then(|value| value.lower_fatal.as_ref())
        .and_then(|value| value.reading)
    {
        Some(Some(value)) => {
            match ::nv_telemetry_model::Value::double(value) {
                Ok(value) => Some(value),
                Err(error) => {
                    issues
                        .push(
                            ::nv_telemetry_source::ProjectionIssue::invalid(
                                "Sensor.Thresholds.LowerFatal.Reading",
                                error.to_string(),
                            ),
                        );
                    None
                }
            }
        }
        _ => None,
    } {
        sensor_threshold_lower_fatal_value_entries.push(("reading".to_owned(), value));
    }
    let subject_id = {
        let value = sensor.id.clone();
        if value.is_empty() {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "Sensor.Id",
                        "`id`: present but empty",
                    ),
                );
            None
        } else if value.len() > ::nv_telemetry_model::limits::SUBJECT_ID_MAX_LEN as usize
        {
            issues
                .push(
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "Sensor.Id",
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
    let scope_0 = match sensor_subject_scope_0(location) {
        Some(value) => {
            if value.is_empty() {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "@location.chassis",
                            "`scope`: present but empty",
                        ),
                    );
                None
            } else if value.len()
                > ::nv_telemetry_model::limits::SUBJECT_SCOPE_MAX_LEN as usize
            {
                issues
                    .push(
                        ::nv_telemetry_source::ProjectionIssue::invalid(
                            "@location.chassis",
                            format!(
                                "`scope`: {} bytes long, over the schema's bound of {}",
                                value.len(),
                                ::nv_telemetry_model::limits::SUBJECT_SCOPE_MAX_LEN
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
                    ::nv_telemetry_source::ProjectionIssue::invalid(
                        "@location.chassis",
                        "requested location has no chassis segment",
                    ),
                );
            None
        }
    };
    let subject = match (subject_id, scope_0) {
        (Some(subject_id), Some(scope_0)) => {
            match ::nv_telemetry_model::Subject::builder()
                .kind("sensor")
                .scope(vec![scope_0.to_owned()])
                .id(subject_id)
                .build()
            {
                Ok(subject) => Some(subject),
                Err(error) => {
                    let path = if error.path().starts_with("id") {
                        "Sensor.Id"
                    } else {
                        "@location.chassis"
                    };
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
        return Ok(SensorParts {
            signal_descriptors: Vec::new(),
            readings: Vec::new(),
            state_observations: Vec::new(),
            issues,
        });
    };
    let key = ::nv_telemetry_model::SignalKey::builder()
        .subject(subject.clone())
        .build()?;
    let mut signal_descriptors = Vec::new();
    let mut readings = Vec::new();
    let mut state_observations = Vec::new();
    let mut builder = ::nv_telemetry_model::SignalDescriptor::builder();
    builder = builder.key(key.clone());
    if let Some(value) = sensor_descriptor_kind {
        builder = builder.kind(value);
    }
    if let Some(value) = sensor_descriptor_unit {
        builder = builder.unit(value);
    }
    if let Some(value) = sensor_descriptor_range {
        builder = builder.range(value);
    }
    signal_descriptors.push(builder.build()?);
    if sensor_sample_value.is_some() {
        let mut builder = ::nv_telemetry_model::Reading::builder();
        builder = builder.key(key.clone());
        if let Some(value) = sensor_sample_value {
            builder = builder.value(value);
        }
        readings.push(builder.build()?);
    }
    if sensor_state_value.is_some() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        if let Some(value) = sensor_state_value {
            builder = builder.value(value);
        }
        builder = builder.name("state");
        state_observations.push(builder.build()?);
    }
    if sensor_health_value.is_some() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        if let Some(value) = sensor_health_value {
            builder = builder.value(value);
        }
        builder = builder.name("health");
        state_observations.push(builder.build()?);
    }
    if !sensor_threshold_upper_caution_value_entries.is_empty() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        builder = builder.name("threshold.upper-caution");
        builder = builder
            .value(
                ::nv_telemetry_model::Value::map(
                    sensor_threshold_upper_caution_value_entries,
                )?,
            );
        state_observations.push(builder.build()?);
    }
    if !sensor_threshold_upper_critical_value_entries.is_empty() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        builder = builder.name("threshold.upper-critical");
        builder = builder
            .value(
                ::nv_telemetry_model::Value::map(
                    sensor_threshold_upper_critical_value_entries,
                )?,
            );
        state_observations.push(builder.build()?);
    }
    if !sensor_threshold_upper_fatal_value_entries.is_empty() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        builder = builder.name("threshold.upper-fatal");
        builder = builder
            .value(
                ::nv_telemetry_model::Value::map(
                    sensor_threshold_upper_fatal_value_entries,
                )?,
            );
        state_observations.push(builder.build()?);
    }
    if !sensor_threshold_lower_caution_value_entries.is_empty() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        builder = builder.name("threshold.lower-caution");
        builder = builder
            .value(
                ::nv_telemetry_model::Value::map(
                    sensor_threshold_lower_caution_value_entries,
                )?,
            );
        state_observations.push(builder.build()?);
    }
    if !sensor_threshold_lower_critical_value_entries.is_empty() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        builder = builder.name("threshold.lower-critical");
        builder = builder
            .value(
                ::nv_telemetry_model::Value::map(
                    sensor_threshold_lower_critical_value_entries,
                )?,
            );
        state_observations.push(builder.build()?);
    }
    if !sensor_threshold_lower_fatal_value_entries.is_empty() {
        let mut builder = ::nv_telemetry_model::StateObservation::builder();
        builder = builder.subject(subject.clone());
        builder = builder.name("threshold.lower-fatal");
        builder = builder
            .value(
                ::nv_telemetry_model::Value::map(
                    sensor_threshold_lower_fatal_value_entries,
                )?,
            );
        state_observations.push(builder.build()?);
    }
    Ok(SensorParts {
        signal_descriptors,
        readings,
        state_observations,
        issues,
    })
}
/// Matches `/redfish/v1/Chassis/{chassis}/Sensors/{id}`, yielding `{chassis}`: the subject's
/// scope comes from the requested location, never from the
/// payload's own claim about itself.
fn sensor_subject_scope_0(location: &str) -> Option<&str> {
    let mut segments = crate::uri::canonical(location).strip_prefix('/')?.split('/');
    if segments.next()? != "redfish" {
        return None;
    }
    if segments.next()? != "v1" {
        return None;
    }
    if segments.next()? != "Chassis" {
        return None;
    }
    let captured = segments.next().filter(|segment| !segment.is_empty())?;
    if segments.next()? != "Sensors" {
        return None;
    }
    let _ = segments.next().filter(|segment| !segment.is_empty())?;
    if segments.next().is_some() {
        return None;
    }
    Some(captured)
}
