// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The clock orchestration stamps acquisitions with.
//!
//! Sources receive no timestamp and cannot substitute one; the instant a
//! unit is admitted comes from here. Wall-clock time names *when* an
//! acquisition happened ([`Clock::timestamp`]); monotonic time measures how
//! long it took ([`Clock::instant`]) — differencing wall-clock instants
//! would let a clock step masquerade as a duration.

use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use nv_telemetry_model::Timestamp;

/// Where acquisition instants and durations come from. Injected rather than
/// called, so a simulated run can drive the same code on a virtual
/// timeline.
pub trait Clock: Send + Sync {
    /// The wall-clock instant that stamps `at` and `started_at`.
    fn timestamp(&self) -> Timestamp;

    /// A monotonic instant for duration measurement.
    fn instant(&self) -> Instant;
}

/// The one clock a collector binds into every unit it seats.
pub(crate) type SharedClock = Arc<dyn Clock>;

/// The system clock — the workspace's one bridge from [`SystemTime`] to the
/// contract's [`Timestamp`].
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct SystemClock;

impl Clock for SystemClock {
    fn timestamp(&self) -> Timestamp {
        timestamp_from(SystemTime::now())
    }

    fn instant(&self) -> Instant {
        Instant::now()
    }
}

/// Represents any [`SystemTime`] honestly, including one before the epoch:
/// seconds go negative while nanos stay within one second, which is the
/// contract's only bound.
///
/// # Panics
///
/// Only when the instant is more than `i64::MAX` seconds from the epoch —
/// hundreds of billions of years — which no host clock can report.
fn timestamp_from(now: SystemTime) -> Timestamp {
    let (seconds, nanos) = match now.duration_since(UNIX_EPOCH) {
        Ok(since) => (
            i64::try_from(since.as_secs()).expect("a host clock within the contract's era"),
            since.subsec_nanos(),
        ),
        Err(before) => {
            let before = before.duration();
            let seconds = i64::try_from(before.as_secs()).expect("a host clock within the era");
            match before.subsec_nanos() {
                0 => (-seconds, 0),
                nanos => (-seconds - 1, 1_000_000_000 - nanos),
            }
        }
    };
    Timestamp::new(seconds, nanos).expect("subsecond nanos satisfy the contract's bound")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn the_bridge_represents_both_sides_of_the_epoch() {
        let after = timestamp_from(UNIX_EPOCH + Duration::new(7, 500));
        assert_eq!((after.seconds(), after.nanos()), (7, 500));

        let epoch = timestamp_from(UNIX_EPOCH);
        assert_eq!((epoch.seconds(), epoch.nanos()), (0, 0));

        // Half a second before the epoch: second -1, plus half a second of
        // nanos — one representation per instant, nanos always in-bound.
        let before = timestamp_from(UNIX_EPOCH - Duration::new(0, 500_000_000));
        assert_eq!((before.seconds(), before.nanos()), (-1, 500_000_000));

        let whole = timestamp_from(UNIX_EPOCH - Duration::new(3, 0));
        assert_eq!((whole.seconds(), whole.nanos()), (-3, 0));
    }
}
