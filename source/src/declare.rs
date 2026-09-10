// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Provider declarations: what one provider offers, as plain data.
//!
//! The planner plans against declarations rather than against [`Acquire`]
//! implementations — a declaration can be enumerated, compared, and
//! rejected without constructing a unit or touching a device. A provider
//! exports one declaration beside its unit type, built from the same
//! constants its [`Origin`](nv_telemetry_model::Origin) is, so the plan and
//! the wire always name the same identity.
//!
//! [`Acquire`]: crate::Acquire

/// How a provider's requests reach the device. Closed to what this build
/// can schedule; `Streamed` arrives with the gNMI milestone as an addition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AcquisitionMode {
    /// One acquisition per admission, on a cadence the plan supplies. The
    /// unit of admission is the acquisition, not the request: a single
    /// resource read is one request, a collection walk is several, and the
    /// walk holds its slot for all of them — which is why a provider that
    /// walks bounds the walk.
    Polled,
}

/// The planner's vocabulary for one provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderDeclaration {
    provider: String,
    request_class: String,
    mode: AcquisitionMode,
    cost: u64,
}

impl ProviderDeclaration {
    /// Declares a polled provider. `cost` is a relative weight per admission
    /// in the dispatcher's token units; `1` is a plain single-resource read.
    /// A unit that issues several requests per admission is metered at this
    /// one weight for all of them, so a walk declaring `1` leans on its own
    /// bound to keep the endpoint's rate honest.
    #[must_use]
    pub fn polled(
        provider: impl Into<String>,
        request_class: impl Into<String>,
        cost: u64,
    ) -> Self {
        Self {
            provider: provider.into(),
            request_class: request_class.into(),
            mode: AcquisitionMode::Polled,
            cost,
        }
    }

    /// Provider identity, as the wire `Origin` spells it.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Request class, as dispatcher lanes and breakers key it.
    #[must_use]
    pub fn request_class(&self) -> &str {
        &self.request_class
    }

    /// How requests reach the device.
    #[must_use]
    pub fn mode(&self) -> AcquisitionMode {
        self.mode
    }

    /// Relative request weight in dispatcher token units.
    #[must_use]
    pub fn cost(&self) -> u64 {
        self.cost
    }
}
