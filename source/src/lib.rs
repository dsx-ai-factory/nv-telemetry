// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Acquisition contract shared by every protocol.
//!
//! Defines what a source implements. The planner plans against this contract
//! and protocol crates implement it, so neither depends on the other. What
//! ships today is the unit of work and its outcome vocabulary:
//!
//! - [`Acquire`] — one admitted unit of protocol work, with identity
//!   readable before it runs;
//! - [`AcquisitionParts`] — envelope-free payloads and issues returned by a
//!   collection provider;
//! - [`Acquired`] — the batches and [`ProjectionIssue`]s a success yields;
//! - [`AcquisitionFailure`] — the classified facts a failure yields;
//! - [`Capability`] — what a probe (itself just an `Acquire`) learns;
//! - [`ProviderDeclaration`] — what one provider offers, as the plain data
//!   the planner plans against;
//! - [`SubscriptionItem`] and [`stamp_item`] - a subscription's connect
//!   attempt is an [`Acquire`] whose output is a stream; [`stamp_item`]
//!   applies to one stream item the same identity-and-instant rule
//!   [`acquire()`] applies once to a whole polled request.
//!
//! A failed request emits no batch at all, and the outcome type makes the
//! alternative unrepresentable. Missing and invalid source fields are
//! different facts, and both are structured issues beside the batches, never
//! log lines and never fabricated observations.
//!
//! One rule crosses this crate without living in it:
//! [`AcquisitionStatus`](nv_telemetry_model::AcquisitionStatus) is built by
//! the orchestration layer, never by a source. A refused admission must
//! produce a status for a unit that never ran, timing is only observable
//! where the future is polled, and a source that cannot author its own
//! status cannot misreport one. Sources contribute exactly what the trait
//! exposes: identity, and the classified outcome. This holds per stream item
//! too: [`stamp_item`] takes its instant from the caller and never the
//! device's own reported timestamp.
//!
//! Streamed subscription types carry no dependency on the stage trait and
//! artifact plumbing reserved below for multi-step acquisitions such as
//! catalogs: a subscription replaces the catalog-then-poll expansion
//! entirely rather than composing with it.
//!
//! Reserved here for a later milestone: the stage trait and artifact
//! plumbing for multi-step acquisitions such as catalogs, and the projection
//! driver machinery that returns with the manifest compiler, for which
//! [`ProjectionIssue`] is the fixed anchor.

mod acquire;
mod declare;
mod issue;
mod result;
mod stream;

pub use acquire::acquire;
pub use acquire::Acquire;
pub use acquire::Capability;
pub use declare::AcquisitionMode;
pub use declare::ProviderDeclaration;
pub use issue::ProjectionIssue;
pub use issue::ProjectionIssueKind;
pub use result::Acquired;
pub use result::AcquisitionFailure;
pub use result::AcquisitionFailureClass;
pub use result::AcquisitionParts;
pub use result::OMITTED_ISSUES_LOCATOR;
pub use stream::stamp_item;
pub use stream::SubscriptionItem;
