// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Planning and dispatcher vocabulary.
//!
//! Orchestration owns everything between the acquisition contract and the
//! embedder's driving loop: the clock that stamps admitted instants, the
//! failure-class policy tables, and the assembly of every
//! [`AcquisitionStatus`](nv_telemetry_model::AcquisitionStatus) and
//! [`ProjectionIssues`](nv_telemetry_model::ProjectionIssues) envelope —
//! sources never author either. One acquisition yields exactly one
//! [`AcquisitionReport`], and the type makes anything else
//! unrepresentable.
//!
//! The planner ([`plan`]) is static and deterministic: needs in, planned
//! polls out, exactly one polled declaration serving each need's request
//! class. The recipe
//! ([`endpoint_subtree`]) turns a plan into the standard admission stack —
//! endpoint admission over the endpoint breaker over the rate bucket over
//! the unit leaves. Breaker scope rides the work's channel: only
//! endpoint-scoped failures travel as errors, so the breaker samples
//! exactly the classes that indict the endpoint.
//!
//! This crate is runtime-free: leaf futures are plain boxed futures, the
//! embedder owns the dispatcher `Runtime`, its timer (`SleepUntil` is a
//! hint delivered once, never a wake-up), and all fan-out.

mod clock;
mod plan;
mod recipe;
mod report;
mod status;

pub use clock::Clock;
pub use clock::SystemClock;
pub use plan::plan;
pub use plan::Plan;
pub use plan::PlanError;
pub use plan::PlannedPoll;
pub use plan::PollNeed;
pub use recipe::endpoint_subtree;
pub use recipe::BreakerPolicy;
pub use recipe::EndpointPolicy;
pub use recipe::EndpointSubtree;
pub use recipe::PollMeta;
pub use recipe::PollUnit;
pub use recipe::RatePolicy;
pub use recipe::RecipeError;
pub use report::assemble;
pub use report::poll_future;
pub use report::AcquisitionReport;
pub use report::EndpointFault;
pub use report::TelemetryWork;
pub use status::default_retryable;
pub use status::failed_status;
pub use status::refused_status;
pub use status::success_status;
pub use status::trips_endpoint_breaker;
