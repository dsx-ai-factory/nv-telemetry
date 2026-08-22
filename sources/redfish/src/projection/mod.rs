// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Projection: typed Redfish shapes become validated observation parts.
//!
//! The bodies are generated from `manifests/` by `make codegen`; this
//! module is the boundary the provider consumes. Two disciplines hold in
//! everything the compiler emits:
//!
//! - Every field is evaluated on every projection — no early return — so one
//!   report carries every issue, not just the first.
//! - Nothing is fabricated. A field that is absent, null, or unusable
//!   produces no observation; unusable additionally produces an issue,
//!   because the device answered and the answer cannot be represented, which
//!   is a different fact from silence.

pub(crate) use crate::generated::chassis::project_chassis;
pub(crate) use crate::generated::chassis::ChassisParts;
pub(crate) use crate::generated::sensor::project_sensor;
pub(crate) use crate::generated::sensor::SensorParts;

// The pins below plant what no fixture can: serde_json rejects non-finite
// literals before projection ever runs, so the decoded shape is built and
// the value planted — the promise held for ingress paths that make no such
// guarantee.
#[cfg(test)]
mod tests {
    use nv_redfish::schema::sensor::Sensor;
    use nv_telemetry_source::ProjectionIssue;

    use super::project_sensor;

    const URI: &str = "/redfish/v1/Chassis/1U/Sensors/CPU1Temp";

    fn decoded() -> Sensor {
        serde_json::from_str(
            r#"{
                "@odata.id": "/redfish/v1/Chassis/1U/Sensors/CPU1Temp",
                "Id": "CPU1Temp",
                "Name": "CPU 1 Temperature",
                "Reading": 47.5,
                "ReadingRangeMin": 0,
                "ReadingRangeMax": 105
            }"#,
        )
        .expect("a decodable sensor")
    }

    #[test]
    fn an_infinite_reading_is_reported_never_carried() {
        let mut sensor = decoded();
        sensor.reading = Some(Some(f64::INFINITY));

        let parts = project_sensor(&sensor, URI).expect("projection runs");
        assert!(
            parts.readings.is_empty(),
            "a non-finite sample was fabricated"
        );
        assert_eq!(parts.signal_descriptors.len(), 1, "the signal still exists");
        assert_eq!(
            parts.issues,
            [ProjectionIssue::invalid(
                "Sensor.Reading",
                "`double_value`: not a finite number"
            )]
        );
    }

    #[test]
    fn a_nonfinite_bound_is_dropped_and_reported() {
        let mut sensor = decoded();
        sensor.reading_range_min = Some(Some(f64::NAN));

        let parts = project_sensor(&sensor, URI).expect("projection runs");
        let descriptor = &parts.signal_descriptors[0];
        let range = descriptor.range().expect("the finite bound survives");
        assert!(range.min().is_none(), "a NaN bound was carried");
        assert!(range.max().is_some());
        assert_eq!(
            parts.issues,
            [ProjectionIssue::invalid(
                "Sensor.ReadingRangeMin",
                "`double_value`: not a finite number"
            )]
        );
    }
}
