// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Planning and collection.
//!
//! Orchestration owns everything between the acquisition contract and the
//! embedder's consumer: the clock that stamps admitted instants, the
//! failure-class policy tables, the dispatcher runtimes, and the assembly
//! of every [`AcquisitionStatus`](nv_telemetry_model::AcquisitionStatus)
//! and [`ProjectionIssues`](nv_telemetry_model::ProjectionIssues) envelope
//! — sources never author either. One polled acquisition or reportable
//! subscription item yields exactly one [`AcquisitionReport`], carrying
//! exactly one status. Stream synchronization controls and cancellation
//! yield none.
//!
//! The planner ([`plan`]) is static and deterministic: needs in, planned
//! polls out, exactly one polled declaration serving each need's request
//! class. The embedder binds each planned acquisition to its unit
//! ([`PollUnit`], [`StreamUnit`]) and hands one endpoint's units to the
//! [`Collector`], which seats them in the standard admission stack —
//! endpoint admission over the endpoint breaker over the rate bucket over
//! the unit leaves — on a dispatcher runtime of the endpoint's own.
//! Breaker scope rides the work's channel: only endpoint-scoped failures
//! travel as errors, so the breaker samples exactly the classes that
//! indict the endpoint.
//!
//! A stream enters the same subtree as its own connect leaf: each connect
//! attempt is one admission, charged and breaker-sampled like a poll, and
//! each reconnect is a due instant the unit's [`ReconnectPolicy`] chooses
//! and the runtime sleeps to. The stream an attempt opens is drained
//! outside the dispatcher, one report per item, at the consumer's pace.
//!
//! The collector runs on tokio: one task per endpoint, sleeping to the
//! endpoint's next due unit. Every report reaches the embedder through
//! one bounded [`Reports`] stream.

mod clock;
mod collector;
mod plan;
mod recipe;
mod report;
mod status;
mod stream;

pub use clock::Clock;
pub use clock::SystemClock;
pub use collector::Collected;
pub use collector::Collector;
pub use collector::EndpointUnits;
pub use collector::Reports;
pub use plan::plan;
pub use plan::Needs;
pub use plan::Plan;
pub use plan::PlanError;
pub use plan::PlannedPoll;
pub use plan::PlannedStream;
pub use plan::PollNeed;
pub use plan::StreamNeed;
pub use recipe::BreakerPolicy;
pub use recipe::EndpointPolicy;
pub use recipe::PollUnit;
pub use recipe::RatePolicy;
pub use recipe::RecipeError;
pub use report::AcquisitionReport;
pub use status::default_retryable;
pub use status::failed_status;
pub use status::refused_status;
pub use status::success_status;
pub use status::trips_endpoint_breaker;
pub use stream::ReconnectPolicy;
pub use stream::StreamEnd;
pub use stream::StreamUnit;

#[cfg(test)]
mod tests;
