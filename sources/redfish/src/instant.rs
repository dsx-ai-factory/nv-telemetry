// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What a Redfish `Edm.DateTimeOffset` denotes as a contract instant.
//!
//! The device stamps entries in whatever offset it keeps; the contract's
//! `Timestamp` is the instant itself, so equal instants written with
//! different offsets project to one value. Generated projections call
//! [`timestamp`] for every `DateTimeOffset` landing; this is the one hook
//! for instants the projection compiler expects a Redfish source crate to
//! provide, as `uri::canonical` is for locations.

use nv_redfish::core::EdmDateTimeOffset;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::Timestamp;
use time::OffsetDateTime;

/// The instant a device-stamped `DateTimeOffset` names.
///
/// # Errors
///
/// The residual tier only: `time` bounds nanoseconds below one second, so
/// the contract's own check cannot fail on what it produces.
pub(crate) fn timestamp(value: EdmDateTimeOffset) -> Result<Timestamp, Invalid> {
    let instant = OffsetDateTime::from(value);
    Timestamp::new(instant.unix_timestamp(), instant.nanosecond())
}

#[cfg(test)]
mod tests {
    use nv_redfish::core::EdmDateTimeOffset;

    use super::timestamp;

    fn parse(text: &str) -> EdmDateTimeOffset {
        text.parse().expect("RFC 3339 text parses")
    }

    #[test]
    fn the_offset_is_folded_into_the_instant() {
        let utc = timestamp(parse("2026-03-01T10:00:00Z")).expect("in range");
        let plus_two = timestamp(parse("2026-03-01T12:00:00+02:00")).expect("in range");
        assert_eq!(utc, plus_two);
        assert_eq!(utc.seconds(), 1_772_359_200);
        assert_eq!(utc.nanos(), 0);
    }

    #[test]
    fn fractional_seconds_survive_as_nanos() {
        let stamped = timestamp(parse("1970-01-01T00:00:01.250Z")).expect("in range");
        assert_eq!(stamped.seconds(), 1);
        assert_eq!(stamped.nanos(), 250_000_000);
    }

    #[test]
    fn an_unset_clock_is_the_epoch_not_a_fault() {
        let epoch = timestamp(parse("1970-01-01T00:00:00Z")).expect("in range");
        assert_eq!(epoch.seconds(), 0);
    }
}
