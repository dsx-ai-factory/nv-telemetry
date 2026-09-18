// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Projection issues: what one source field failed to become.
//!
//! Missing and invalid are different facts — silence versus an answer that
//! cannot be used — and both are structured issues attached to the
//! acquisition result rather than log lines. An issue describes what is
//! *not* in any batch, which is why it rides beside the batches instead of
//! inside one: attaching issues to a batch would force fabricating an empty
//! batch just to carry them, and issues never become fabricated
//! observations.
//!
//! Paths locate the source field in the source's own schema grammar —
//! `Sensor.Reading`, `Sensors[3].Status.Health` — built outward as the issue
//! propagates, exactly as [`Invalid`](nv_telemetry_model::Invalid) builds
//! model paths. The two read alike on purpose: locating a projection issue
//! and locating a validation violation are the same skill. A fact outside
//! the schema carries an `@`-prefixed locator, such as `@location.chassis`
//! for a request-URI capture; element prefixes built by [`ProjectionIssue::at`]
//! or [`ProjectionIssue::at_index`] may precede one.
//!
//! Paths are identities: the wire envelope holds one issue per path across
//! a whole acquisition, so whoever iterates elements must prefix each
//! element's issues with [`ProjectionIssue::at`] or
//! [`ProjectionIssue::at_index`] to keep them distinct.

use std::fmt;

use nv_telemetry_model::Invalid;
use nv_telemetry_model::IssueKind;

/// Why one source field did not become an observation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProjectionIssueKind {
    /// The device did not report a field the projection requires.
    MissingRequired,
    /// The device reported the field unusably; the detail quotes the
    /// failure. An unresolvable subject scope is this kind, not a kind of
    /// its own: the location was answered and the answer cannot be used.
    Invalid {
        /// What made the reported value unusable.
        detail: String,
    },
}

/// A source field that failed to project, located by source-schema path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionIssue {
    /// Dotted path in the source's schema grammar, with indexes into
    /// repeated source fields.
    path: String,
    kind: ProjectionIssueKind,
}

impl ProjectionIssue {
    /// An issue for a field the device did not report.
    #[must_use]
    pub fn missing(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            kind: ProjectionIssueKind::MissingRequired,
        }
    }

    /// An issue for a field the device reported unusably.
    #[must_use]
    pub fn invalid(path: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            kind: ProjectionIssueKind::Invalid {
                detail: detail.into(),
            },
        }
    }

    /// Prefixes the path with the segment this issue bubbled out of.
    #[must_use]
    pub fn at(mut self, segment: &str) -> Self {
        if self.path.is_empty() {
            segment.clone_into(&mut self.path);
        } else {
            self.path = format!("{segment}.{}", self.path);
        }
        self
    }

    /// Prefixes the path with an element of a repeated source field.
    #[must_use]
    pub fn at_index(self, segment: &str, index: usize) -> Self {
        self.at(&format!("{segment}[{index}]"))
    }

    /// Dotted source-schema path to the field at fault.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// What kept the field from projecting.
    #[must_use]
    pub fn kind(&self) -> &ProjectionIssueKind {
        &self.kind
    }

    /// Converts into the wire model. The detail is prose, so overlong text
    /// is UTF-8-safely bounded to the contract limit; the path is the
    /// locator, so an empty or overlong one is rejected by the model's own
    /// validation rather than truncated into a different locator.
    ///
    /// # Errors
    ///
    /// An empty or overlong path, or an invalid-kind issue whose detail is
    /// empty.
    pub fn into_model(self) -> Result<nv_telemetry_model::ProjectionIssue, Invalid> {
        let detail_limit = nv_telemetry_model::limits::PROJECTIONISSUE_DETAIL_MAX_LEN as usize;
        let (kind, detail) = match self.kind {
            ProjectionIssueKind::MissingRequired => (IssueKind::MissingRequired, None),
            ProjectionIssueKind::Invalid { detail } => (
                IssueKind::Invalid,
                crate::result::bounded(detail, detail_limit),
            ),
        };
        let mut builder = nv_telemetry_model::ProjectionIssue::builder()
            .path(self.path)
            .kind(kind);
        if let Some(detail) = detail {
            builder = builder.detail(detail);
        }
        builder.build()
    }
}

impl fmt::Display for ProjectionIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ProjectionIssueKind::MissingRequired => {
                write!(f, "`{}`: required but not reported", self.path)
            }
            ProjectionIssueKind::Invalid { detail } => {
                write!(f, "`{}`: reported but unusable: {detail}", self.path)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_build_outward_like_model_paths() {
        let issue = ProjectionIssue::invalid("Reading", "not a finite number")
            .at_index("Sensors", 3)
            .at("Chassis");
        assert_eq!(issue.path(), "Chassis.Sensors[3].Reading");
    }

    #[test]
    fn issues_copy_onto_the_wire_with_their_kind() {
        let missing = ProjectionIssue::missing("Id").into_model().expect("valid");
        assert_eq!(missing.path(), "Id");
        assert_eq!(missing.kind(), IssueKind::MissingRequired);
        assert_eq!(missing.detail(), None);

        let invalid = ProjectionIssue::invalid("Reading", "not finite")
            .into_model()
            .expect("valid");
        assert_eq!(invalid.kind(), IssueKind::Invalid);
        assert_eq!(invalid.detail(), Some("not finite"));
    }

    #[test]
    fn overlong_detail_is_bounded_and_an_overlong_path_is_refused() {
        let path_limit = nv_telemetry_model::limits::PROJECTIONISSUE_PATH_MAX_LEN as usize;
        let detail_limit = nv_telemetry_model::limits::PROJECTIONISSUE_DETAIL_MAX_LEN as usize;

        let issue = ProjectionIssue::invalid("Reading", "d".repeat(detail_limit + 1))
            .into_model()
            .expect("bounded detail validates");
        let detail = issue.detail().expect("detail retained");
        assert_eq!(detail.len(), detail_limit);
        assert!(detail.ends_with(crate::result::DETAIL_TRUNCATION_MARKER));

        assert!(ProjectionIssue::missing("p".repeat(path_limit + 1))
            .into_model()
            .is_err());
    }

    #[test]
    fn producer_bugs_fail_loudly_instead_of_fabricating() {
        assert!(ProjectionIssue::missing("").into_model().is_err());
        assert!(ProjectionIssue::invalid("Reading", "")
            .into_model()
            .is_err());
    }
}
