// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Manifest rules: every declaration the compiler cannot honor is an error,
//! never silently degraded code — a mapping that reads as enforced while
//! doing nothing is the failure mode all of these guard.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use heck::ToSnakeCase as _;
use prost_reflect::DescriptorPool;
use prost_reflect::FieldDescriptor;
use prost_reflect::Kind;
use prost_reflect::MessageDescriptor;

use crate::is_contract_package;
use crate::options::Vocabulary;
use crate::projection::location::LocationPattern;
use crate::projection::spec;
use crate::projection::spec::AssemblySpec;
use crate::projection::spec::ConstantSpec;
use crate::projection::spec::FieldSpec;
use crate::projection::spec::ManifestSpec;
use crate::projection::spec::ProjectionSpec;
use crate::projection::spec::ScopeSpec;
use crate::projection::spec::SubjectSpec;
use crate::projection::RedfishIndex;
use crate::projection::ResolvedField;

// Enum numbers mirror manifest.proto; buf breaking guards the mirror.
const SCHEMA_INDEX: i32 = 1;
const NULL_UNSPECIFIED: i32 = 0;
const NULL_ABSENT: i32 = 1;
const NULL_INVALID: i32 = 2;
const NULL_EXPLICIT: i32 = 3;
const CARDINALITY_UNSPECIFIED: i32 = 0;
const CARDINALITY_SINGLE: i32 = 1;
const ELEMENTWISE: i32 = 2;

/// The one index this build can construct: nv-redfish's vendored DMTF bundle.
const DMTF_INDEX: &str = "nv-redfish-schema/dmtf";

/// The one target type a map assembly can build.
const VALUE: &str = "nv.telemetry.v1.Value";
const VALUE_STRING: &str = "string_value";
const VALUE_MAP: &str = "nv.telemetry.v1.Value.Map";
const VALUE_MAP_ENTRY: &str = "nv.telemetry.v1.Value.Map.Entry";

/// Contract messages that carry identity. A target path that sets one — or
/// reaches into one — is reserved: the subject declaration populates
/// identity, never a field mapping.
const SIGNAL_KEY: &str = "nv.telemetry.v1.SignalKey";
const SUBJECT_TYPE: &str = "nv.telemetry.v1.Subject";

/// A target shape the projection compiler deliberately knows how to build.
///
/// Reflection verifies these declarations against the contract, but does not
/// infer new profiles. Adding a target is an architecture decision because it
/// decides how identity lands and which builder invariants emission promises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TargetProfile {
    pub(crate) target: &'static str,
    pub(crate) identity_field: &'static str,
    pub(crate) identity: IdentityKind,
    /// A string field the compiler fills with the canonical request
    /// location — the target type's provenance. Reserved from manifests.
    pub(crate) provenance: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdentityKind {
    SignalKey,
    Subject,
}

const TARGET_PROFILES: &[TargetProfile] = &[
    TargetProfile {
        target: "nv.telemetry.v1.SignalDescriptor",
        identity_field: "key",
        identity: IdentityKind::SignalKey,
        provenance: None,
    },
    TargetProfile {
        target: "nv.telemetry.v1.Reading",
        identity_field: "key",
        identity: IdentityKind::SignalKey,
        provenance: None,
    },
    TargetProfile {
        target: "nv.telemetry.v1.StateObservation",
        identity_field: "subject",
        identity: IdentityKind::Subject,
        provenance: None,
    },
    TargetProfile {
        target: "nv.telemetry.v1.InventoryItem",
        identity_field: "subject",
        identity: IdentityKind::Subject,
        provenance: Some("source_key"),
    },
];

#[must_use]
pub(crate) fn target_profile(target: &str) -> Option<TargetProfile> {
    TARGET_PROFILES
        .iter()
        .copied()
        .find(|profile| profile.target == target)
}

/// One manifest declaration that breaks a rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    subject: String,
    reason: Reason,
}

impl Violation {
    /// The manifest file and projection at fault.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    #[must_use]
    pub fn reason(&self) -> &Reason {
        &self.reason
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.subject, self.reason)
    }
}

/// Why a manifest was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    SourceMismatch {
        declared: String,
        directory: String,
    },
    UnsupportedBackend,
    UnknownIndex {
        declared: String,
    },
    Unnamed,
    DuplicateName(String),
    ReservedManifestStem(String),
    DuplicateManifestModule(String),
    NotHonored {
        feature: &'static str,
    },
    UnknownSourceType(String),
    UnknownSourcePath(String),
    UnknownTargetType(String),
    UnvalidatedTarget(String),
    UnsupportedTargetProfile(String),
    TargetProfileDrift {
        target: String,
        detail: String,
    },
    UncoveredRequiredTarget(String),
    UnknownTargetField(String),
    ReservedTarget(String),
    DuplicateTarget(String),
    OverlappingTargets {
        outer: String,
        inner: String,
    },
    OneofConflict {
        first: String,
        second: String,
    },
    MissingSubject,
    EmptyScopeSource,
    CaptureNotInTemplate {
        template: String,
        capture: String,
    },
    NonScalarSubject {
        path: String,
        actual: String,
    },
    SubjectNeverInherited,
    AnchorConflict(String),
    MultipleAnchors,
    UndecidedNull(String),
    UnknownNullPolicy {
        path: String,
        value: i32,
    },
    UnknownCardinality {
        path: String,
        value: i32,
    },
    EmptyValueMapping(String),
    DuplicateValueMapping {
        path: String,
        from: String,
    },
    EmptySubjectKind,
    StaticValueBeyondBound {
        declaration: String,
        actual: usize,
        limit: u32,
    },
    TooManyStaticValues {
        declaration: String,
        actual: usize,
        limit: u32,
    },
    InvalidLocationTemplate {
        template: String,
        detail: String,
    },
    EmptyConstant(String),
    AssemblyTargetNotValue {
        field: String,
        actual: String,
    },
    EmptyEntryKey,
    DuplicateEntryKey(String),
    UnknownEnumValue {
        path: String,
        value: String,
    },
    EmptyKnownValue(String),
    UnresolvedPlaceholder(String),
    DuplicateMember(String),
    MembersWithoutVariation,
    ExpansionWithoutMembers,
    ReadingsPairing(String),
    ReservedProvenance(String),
    InventoryPairing(String),
    NestedAssemblyTarget(String),
}

impl fmt::Display for Reason {
    // One arm per reason; each is a diagnostic read under pressure.
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceMismatch {
                declared,
                directory,
            } => write!(
                f,
                "`source: \"{declared}\"` but the manifest lives under \
                 `sources/{directory}/`; the two must agree"
            ),
            Self::UnsupportedBackend => f.write_str(
                "only BACKEND_SCHEMA_INDEX is supported; other backends have \
                 no resolver yet",
            ),
            Self::UnknownIndex { declared } => write!(
                f,
                "index `{declared}` is not one this build can construct; the \
                 available index is `{DMTF_INDEX}`"
            ),
            Self::Unnamed => f.write_str("every projection carries a name"),
            Self::DuplicateName(name) => {
                write!(f, "a second projection named `{name}`")
            }
            Self::ReservedManifestStem(module) => write!(
                f,
                "manifest module `{module}` is owned by the generator; rename the manifest"
            ),
            Self::DuplicateManifestModule(module) => write!(
                f,
                "module `{module}` is already generated by another manifest in this crate; \
                 rename one of the files"
            ),
            Self::NotHonored { feature } => write!(
                f,
                "`{feature}` is declared but the compiler does not implement \
                 it yet; generated code would silently ignore it"
            ),
            Self::UnknownSourceType(name) => write!(
                f,
                "source type `{name}` is not in the schema index; the \
                 projection would never match anything"
            ),
            Self::UnknownSourcePath(path) => write!(
                f,
                "`{path}` resolves to no field in the schema index; it would \
                 extract nothing and report nothing"
            ),
            Self::UnknownTargetType(name) => write!(
                f,
                "target `{name}` is not a message of the contract package"
            ),
            Self::UnvalidatedTarget(name) => write!(
                f,
                "target `{name}` is not `validated`; nothing would enforce \
                 what this projection produces"
            ),
            Self::UnsupportedTargetProfile(name) => write!(
                f,
                "target `{name}` has no projection profile; the compiler \
                 supports SignalDescriptor, Reading, StateObservation, and \
                 InventoryItem"
            ),
            Self::TargetProfileDrift { target, detail } => write!(
                f,
                "target profile `{target}` no longer matches the contract: {detail}"
            ),
            Self::UncoveredRequiredTarget(field) => write!(
                f,
                "required target field `{field}` is not populated by identity, \
                 a mapping, a constant, or an assembly"
            ),
            Self::UnknownTargetField(path) => write!(
                f,
                "target field `{path}` resolves to nothing in the target \
                 message"
            ),
            Self::ReservedTarget(path) => write!(
                f,
                "`{path}` writes into the target's identity, which the \
                 subject declaration populates; identity never comes from a \
                 field mapping"
            ),
            Self::DuplicateTarget(path) => write!(
                f,
                "two declarations set `{path}`; whichever ran last would \
                 silently win"
            ),
            Self::OverlappingTargets { outer, inner } => write!(
                f,
                "`{outer}` is set whole while `{inner}` is set within it; \
                 the writes conflict"
            ),
            Self::OneofConflict { first, second } => write!(
                f,
                "`{first}` and `{second}` are cases of one oneof; setting \
                 both keeps only the last"
            ),
            Self::MissingSubject => f.write_str(
                "no subject: the projection would produce identity-less \
                 data, which joins to nothing",
            ),
            Self::EmptyScopeSource => f.write_str("a scope contributor carries no case"),
            Self::CaptureNotInTemplate { template, capture } => write!(
                f,
                "capture `{capture}` does not appear in `{template}` as \
                 `{{{capture}}}`"
            ),
            Self::NonScalarSubject { path, actual } => write!(
                f,
                "subject path `{path}` resolves to {actual}; an identity \
                 is one scalar value"
            ),
            Self::SubjectNeverInherited => f.write_str(
                "the manifest declares a subject, but every projection \
                 declares its own; the shared declaration is dead",
            ),
            Self::AnchorConflict(path) => write!(
                f,
                "`{path}` is both `anchor` and `required`: an absent anchor \
                 suppresses output silently while an absent required field \
                 reports, and one field cannot do both"
            ),
            Self::MultipleAnchors => f.write_str(
                "more than one `anchor`: the field a message exists to carry \
                 is singular",
            ),
            Self::UndecidedNull(path) => write!(
                f,
                "`{path}` is nullable at the source and declares no \
                 null_policy; what a null means is a decision, not a default"
            ),
            Self::UnknownNullPolicy { path, value } => write!(
                f,
                "`{path}` carries unknown null_policy number {value}; only declared manifest enum values are accepted"
            ),
            Self::UnknownCardinality { path, value } => write!(
                f,
                "`{path}` carries unknown cardinality number {value}; only declared manifest enum values are accepted"
            ),
            Self::EmptyValueMapping(path) => {
                write!(f, "`{path}` has a value mapping with an empty side")
            }
            Self::DuplicateValueMapping { path, from } => write!(
                f,
                "`{path}` maps `{from}` twice; whichever came last would \
                 silently win"
            ),
            Self::EmptySubjectKind => f.write_str("the subject has no kind, so it names nothing"),
            Self::StaticValueBeyondBound {
                declaration,
                actual,
                limit,
            } => write!(
                f,
                "{declaration} is {actual} bytes long, over the target's bound of {limit}"
            ),
            Self::TooManyStaticValues {
                declaration,
                actual,
                limit,
            } => write!(
                f,
                "{declaration} has {actual} items, over the target's bound of {limit}"
            ),
            Self::InvalidLocationTemplate { template, detail } => {
                write!(f, "location template `{template}` is invalid: {detail}")
            }
            Self::EmptyConstant(field) => {
                write!(f, "constant for `{field}` is empty")
            }
            Self::AssemblyTargetNotValue { field, actual } => write!(
                f,
                "map assembly targets `{field}`, which is `{actual}`; \
                 assemblies build `{VALUE}` maps"
            ),
            Self::EmptyEntryKey => f.write_str("an assembly entry without a key"),
            Self::DuplicateEntryKey(key) => {
                write!(f, "a second assembly entry keyed `{key}`")
            }
            Self::UnknownEnumValue { path, value } => write!(
                f,
                "`{path}` names `{value}`, which the source enumeration does \
                 not declare; the row would never match anything"
            ),
            Self::EmptyKnownValue(path) => write!(
                f,
                "`{path}` declares an empty known enum value; no Rust enum variant can represent it"
            ),
            Self::UnresolvedPlaceholder(text) => write!(
                f,
                "`{text}` carries a brace placeholder nothing resolves; \
                 `{{member}}` and `{{member-kebab}}` substitute only in \
                 source paths and constant values inside an `expansion`"
            ),
            Self::DuplicateMember(member) => {
                write!(f, "member `{member}` is named twice")
            }
            Self::MembersWithoutVariation => f.write_str(
                "no source path or constant in the expansion varies by \
                 member; it would emit one projection several times",
            ),
            Self::ExpansionWithoutMembers => {
                f.write_str("an expansion without members expands nothing")
            }
            Self::ReadingsPairing(detail) | Self::InventoryPairing(detail) => {
                f.write_str(detail)
            }
            Self::ReservedProvenance(path) => write!(
                f,
                "`{path}` writes the target's provenance, which the compiler \
                 populates from the requested location"
            ),
            Self::NestedAssemblyTarget(path) => write!(
                f,
                "assembly `{path}` lands inside a sub-message; an assembly \
                 builds a top-level map field"
            ),
        }
    }
}

/// Checks every manifest against the index and the contract pool.
#[must_use]
pub fn check(
    manifests: &[ManifestSpec],
    index: &RedfishIndex<'_>,
    contract: &DescriptorPool,
    vocabulary: &Vocabulary,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    // Keyed by crate: a crate's manifest files share one emitted module,
    // so a name reused across files collides just as within one.
    let mut names = BTreeSet::new();
    let mut modules = BTreeSet::new();
    for manifest in manifests {
        check_manifest(
            manifest,
            index,
            contract,
            vocabulary,
            &mut names,
            &mut modules,
            &mut violations,
        );
    }
    violations
}

fn check_manifest(
    manifest: &ManifestSpec,
    index: &RedfishIndex<'_>,
    contract: &DescriptorPool,
    vocabulary: &Vocabulary,
    names: &mut BTreeSet<(String, String)>,
    modules: &mut BTreeSet<(String, String)>,
    violations: &mut Vec<Violation>,
) {
    let file = manifest.path.display().to_string();
    let report = |violations: &mut Vec<Violation>, reason| {
        violations.push(Violation {
            subject: file.clone(),
            reason,
        });
    };

    if manifest.source != manifest.crate_source {
        report(
            violations,
            Reason::SourceMismatch {
                declared: manifest.source.clone(),
                directory: manifest.crate_source.clone(),
            },
        );
    }
    if manifest.backend != SCHEMA_INDEX {
        report(violations, Reason::UnsupportedBackend);
    }
    if manifest.index != DMTF_INDEX {
        report(
            violations,
            Reason::UnknownIndex {
                declared: manifest.index.clone(),
            },
        );
    }

    let module = manifest
        .path
        .file_stem()
        .map(|stem| stem.to_string_lossy().to_snake_case())
        .unwrap_or_default();
    if matches!(module.as_str(), "mod" | "provenance") {
        report(violations, Reason::ReservedManifestStem(module.clone()));
    }
    if !module.is_empty() && !modules.insert((manifest.crate_source.clone(), module.clone())) {
        report(violations, Reason::DuplicateManifestModule(module));
    }

    let inherited = manifest
        .projections
        .iter()
        .any(|projection| projection.subject.is_none());
    if manifest.subject.is_some() && !manifest.projections.is_empty() && !inherited {
        report(violations, Reason::SubjectNeverInherited);
    }

    for projection in &manifest.projections {
        let subject = format!("{file}: `{}`", projection.name);
        if projection.name.is_empty() {
            report(violations, Reason::Unnamed);
        } else if !names.insert((manifest.crate_source.clone(), projection.name.clone())) {
            report(violations, Reason::DuplicateName(projection.name.clone()));
        }
        Checker::check(
            projection,
            manifest.subject.as_ref(),
            &subject,
            index,
            contract,
            vocabulary,
            violations,
        );
    }

    check_payload_pairing(manifest, index, contract, vocabulary, &report, violations);
}

/// The invariants the provider relies on when it combines the generated
/// collections into payloads. Readings: all instances over a source share
/// one signal key, so more than one descriptor duplicates that key, and
/// every possible sample needs its descriptor to exist — a gated
/// descriptor beside an emitting reading would lose samples at runtime.
/// Inventory: items key by subject, and all instances over a source share
/// one subject, so a second item duplicates it.
fn check_payload_pairing(
    manifest: &ManifestSpec,
    index: &RedfishIndex<'_>,
    contract: &DescriptorPool,
    vocabulary: &Vocabulary,
    report: &impl Fn(&mut Vec<Violation>, Reason),
    violations: &mut Vec<Violation>,
) {
    let mut source_types: Vec<&str> = Vec::new();
    for projection in &manifest.projections {
        // An unknown source type already reported as the one root fault.
        if index.has_type(&projection.source_type)
            && !source_types.contains(&projection.source_type.as_str())
        {
            source_types.push(&projection.source_type);
        }
    }
    for source_type in source_types {
        let group: Vec<&ProjectionSpec> = manifest
            .projections
            .iter()
            .filter(|projection| projection.source_type == source_type)
            .collect();
        let descriptors: Vec<&&ProjectionSpec> = group
            .iter()
            .filter(|projection| projection.target_type == "nv.telemetry.v1.SignalDescriptor")
            .collect();
        let descriptor_instances: usize = descriptors
            .iter()
            .map(|projection| projection.instances().len())
            .sum();
        let reading_instances: usize = group
            .iter()
            .filter(|projection| projection.target_type == "nv.telemetry.v1.Reading")
            .map(|projection| projection.instances().len())
            .sum();
        let inventory_instances: usize = group
            .iter()
            .filter(|projection| projection.target_type == "nv.telemetry.v1.InventoryItem")
            .map(|projection| projection.instances().len())
            .sum();

        if inventory_instances > 1 {
            report(
                violations,
                Reason::InventoryPairing(format!(
                    "source `{source_type}` expands to {inventory_instances} inventory \
                     items sharing one subject; an Inventory payload keys items by \
                     subject"
                )),
            );
        }
        if descriptor_instances > 1 {
            report(
                violations,
                Reason::ReadingsPairing(format!(
                    "source `{source_type}` expands to {descriptor_instances} signal \
                     descriptors with the same key; a Readings payload requires \
                     descriptor keys to be unique"
                )),
            );
        }
        if reading_instances == 0 {
            continue;
        }
        if descriptor_instances != 1 {
            report(
                violations,
                Reason::ReadingsPairing(format!(
                    "source `{source_type}` emits readings without exactly one signal \
                     descriptor; every sample key must resolve in its Readings payload"
                )),
            );
            continue;
        }
        let gated = descriptors
            .iter()
            .any(|projection| descriptor_gated(projection, contract, vocabulary));
        if gated {
            report(
                violations,
                Reason::ReadingsPairing(format!(
                    "source `{source_type}` gates its signal descriptor while a reading \
                     can emit; every possible sample must have a descriptor"
                )),
            );
        }
    }
}

/// Whether the descriptor projection's output is conditional: an anchored or
/// required field, a contract-required landing, or an assembly (its own
/// gate) can each suppress the instance.
fn descriptor_gated(
    projection: &ProjectionSpec,
    contract: &DescriptorPool,
    vocabulary: &Vocabulary,
) -> bool {
    let target = contract.get_message_by_name(&projection.target_type);
    let expansion_fields = projection
        .expansion
        .iter()
        .flat_map(|expansion| expansion.fields.iter());
    let gated_field = projection
        .fields
        .iter()
        .chain(expansion_fields)
        .any(|field| {
            if field.anchor || field.required {
                return true;
            }
            let root = field
                .target_field
                .split('.')
                .next()
                .unwrap_or(&field.target_field);
            target
                .as_ref()
                .and_then(|target| target.get_field_by_name(root))
                .and_then(|root| vocabulary.field_invariant(&root))
                .is_some_and(|invariant| invariant.required)
        });
    let has_assemblies = !projection.map_assemblies.is_empty()
        || projection
            .expansion
            .as_ref()
            .is_some_and(|expansion| !expansion.map_assemblies.is_empty());
    gated_field || has_assemblies
}

/// What walking a path's proper prefixes established: whether any segment
/// is explicitly nullable, and whether any is a collection.
struct PrefixFacts {
    nullable: bool,
    collection: bool,
}

/// One projection's checking context: the source scope, the resolved
/// target, and the deduplicating violation sink. Expansion checks per
/// instance, but most faults do not vary by member; dedupe keeps one
/// declared fault one diagnostic. Path faults are judged only when the
/// source type itself is known, so an unknown type is one fault, not one
/// per path.
struct Checker<'a, 'b> {
    index: &'b RedfishIndex<'a>,
    source_type: &'b str,
    known: bool,
    target: Option<MessageDescriptor>,
    contract: &'b DescriptorPool,
    vocabulary: &'b Vocabulary,
    subject: &'b str,
    seen: BTreeSet<String>,
    violations: &'b mut Vec<Violation>,
}

impl<'a, 'b> Checker<'a, 'b> {
    fn check(
        projection: &'b ProjectionSpec,
        manifest_subject: Option<&SubjectSpec>,
        subject: &'b str,
        index: &'b RedfishIndex<'a>,
        contract: &'b DescriptorPool,
        vocabulary: &'b Vocabulary,
        violations: &'b mut Vec<Violation>,
    ) {
        let mut checker = Self {
            index,
            source_type: &projection.source_type,
            known: index.has_type(&projection.source_type),
            target: contract
                .get_message_by_name(&projection.target_type)
                .filter(|message| is_contract_package(message.package_name())),
            contract,
            vocabulary,
            subject,
            seen: BTreeSet::new(),
            violations,
        };
        checker.run(projection, manifest_subject, vocabulary);
    }

    fn push(&mut self, reason: Reason) {
        if self.seen.insert(reason.to_string()) {
            self.violations.push(Violation {
                subject: self.subject.to_owned(),
                reason,
            });
        }
    }

    fn resolve(&self, path: &str) -> Option<ResolvedField> {
        if self.known {
            self.index.resolve(self.source_type, path)
        } else {
            None
        }
    }

    fn run(
        &mut self,
        projection: &ProjectionSpec,
        manifest_subject: Option<&SubjectSpec>,
        vocabulary: &Vocabulary,
    ) {
        if !projection.iterate.is_empty() {
            self.push(Reason::NotHonored { feature: "iterate" });
        }
        if projection.versions > 0 {
            self.push(Reason::NotHonored {
                feature: "versions",
            });
        }
        if !self.known {
            self.push(Reason::UnknownSourceType(self.source_type.to_owned()));
        }

        let validated = self.target.as_ref().map(|message| {
            vocabulary
                .message_invariant(message)
                .is_some_and(|invariant| invariant.validated)
        });
        match validated {
            None => self.push(Reason::UnknownTargetType(projection.target_type.clone())),
            Some(false) => self.push(Reason::UnvalidatedTarget(projection.target_type.clone())),
            Some(true) => {}
        }

        self.check_target_profile(&projection.target_type);

        self.check_subject(projection.subject.as_ref().or(manifest_subject), vocabulary);

        for instance in self.expand(projection) {
            let mut anchors = 0usize;
            for field in &instance.fields {
                self.check_field(field);
                anchors += usize::from(field.anchor);
            }
            if anchors > 1 {
                self.push(Reason::MultipleAnchors);
            }

            for constant in &instance.constants {
                if constant.value.is_empty() {
                    self.push(Reason::EmptyConstant(constant.target_field.clone()));
                }
                // The literal and the target's bound are both in hand here;
                // unchecked, the fault surfaces as a builder refusal on
                // every projection, discarding the batches it rode with.
                if let Some(leaf) = self.check_target(&constant.target_field) {
                    self.check_static_text(
                        &format!("constant for `{}`", constant.target_field),
                        &constant.value,
                        &leaf,
                    );
                }
            }

            for assembly in &instance.map_assemblies {
                self.check_assembly(assembly);
            }

            self.check_targets(&instance, vocabulary);
        }
    }

    /// A projection target is an explicit compiler capability, not a shape
    /// inferred from whichever identity-typed fields reflection happens to
    /// find. Verify the small PR-2 allowlist against the live contract so a
    /// schema change fails compilation instead of changing identity behavior.
    fn check_target_profile(&mut self, target_name: &str) {
        let Some(target) = self.target.clone() else {
            return;
        };
        let Some(profile) = target_profile(target_name) else {
            self.push(Reason::UnsupportedTargetProfile(target_name.to_owned()));
            return;
        };
        let Some(identity) = target.get_field_by_name(profile.identity_field) else {
            self.push(Reason::TargetProfileDrift {
                target: target_name.to_owned(),
                detail: format!("identity field `{}` is absent", profile.identity_field),
            });
            return;
        };
        let expected = match profile.identity {
            IdentityKind::SignalKey => SIGNAL_KEY,
            IdentityKind::Subject => SUBJECT_TYPE,
        };
        let actual = match identity.kind() {
            Kind::Message(message) => message.full_name().to_owned(),
            other => format!("{other:?}"),
        };
        if actual != expected {
            self.push(Reason::TargetProfileDrift {
                target: target_name.to_owned(),
                detail: format!(
                    "identity field `{}` is `{actual}`, expected `{expected}`",
                    profile.identity_field
                ),
            });
        }

        let identity_fields: Vec<String> = target
            .fields()
            .filter_map(|field| match field.kind() {
                Kind::Message(message)
                    if message.full_name() == SIGNAL_KEY || message.full_name() == SUBJECT_TYPE =>
                {
                    Some(field.name().to_owned())
                }
                _ => None,
            })
            .collect();
        if identity_fields.as_slice() != [profile.identity_field] {
            self.push(Reason::TargetProfileDrift {
                target: target_name.to_owned(),
                detail: format!(
                    "identity fields are {identity_fields:?}, expected only `{}`",
                    profile.identity_field
                ),
            });
        }

        if let Some(provenance) = profile.provenance {
            match target
                .get_field_by_name(provenance)
                .map(|field| field.kind())
            {
                Some(Kind::String) => {}
                Some(other) => self.push(Reason::TargetProfileDrift {
                    target: target_name.to_owned(),
                    detail: format!(
                        "provenance field `{provenance}` is {other:?}, expected string"
                    ),
                }),
                None => self.push(Reason::TargetProfileDrift {
                    target: target_name.to_owned(),
                    detail: format!("provenance field `{provenance}` is absent"),
                }),
            }
        }
    }

    fn check_subject(&mut self, subject: Option<&SubjectSpec>, vocabulary: &Vocabulary) {
        let Some(spec) = subject else {
            self.push(Reason::MissingSubject);
            return;
        };
        if spec.kind.is_empty() {
            self.push(Reason::EmptySubjectKind);
        }
        if let Some(kind) = self
            .contract
            .get_message_by_name(SUBJECT_TYPE)
            .and_then(|subject| subject.get_field_by_name("kind"))
        {
            self.check_static_text("subject kind", &spec.kind, &kind);
        }
        if let Some(scope) = self
            .contract
            .get_message_by_name(SUBJECT_TYPE)
            .and_then(|subject| subject.get_field_by_name("scope"))
        {
            if let Some(limit) = vocabulary
                .field_invariant(&scope)
                .and_then(|invariant| invariant.max_items)
            {
                if spec.scope.len() > limit as usize {
                    self.push(Reason::TooManyStaticValues {
                        declaration: "subject scope".to_owned(),
                        actual: spec.scope.len(),
                        limit,
                    });
                }
            }
        }
        self.check_subject_path(&spec.id_path);
        for contributor in &spec.scope {
            match contributor {
                ScopeSpec::PayloadPath(path) => {
                    self.check_subject_path(path);
                }
                ScopeSpec::LocationTemplate { template, capture } => {
                    if capture.is_empty() || !template.contains(&format!("{{{capture}}}")) {
                        self.push(Reason::CaptureNotInTemplate {
                            template: template.clone(),
                            capture: capture.clone(),
                        });
                    }
                    if let Err(detail) = LocationPattern::parse(template, capture) {
                        self.push(Reason::InvalidLocationTemplate {
                            template: template.clone(),
                            detail,
                        });
                    }
                }
                ScopeSpec::PathKey(_) => {
                    self.push(Reason::NotHonored {
                        feature: "path_key",
                    });
                }
                ScopeSpec::Unset => self.push(Reason::EmptyScopeSource),
            }
        }
    }

    /// A subject path must name one scalar value; a collection or a
    /// structured type cannot name a resource. A path with a nullable segment
    /// cannot serve as identity either: the subject vocabulary has no null
    /// policy to declare what an explicit null anywhere along the read means,
    /// and honoring one is emission work, so declaring it is an error until
    /// then.
    fn check_subject_path(&mut self, path: &str) {
        if !self.known {
            return;
        }
        let prefixes = self.check_prefixes(path);
        match self.resolve(path) {
            // A path routed through a collection does not resolve; the
            // prefix walk named that fault already.
            None if prefixes.collection => {}
            None => self.push(Reason::UnknownSourcePath(path.to_owned())),
            Some(resolved) if resolved.collection => self.push(Reason::NonScalarSubject {
                path: path.to_owned(),
                actual: "a collection".to_owned(),
            }),
            Some(resolved) if !resolved.is_scalar() => self.push(Reason::NonScalarSubject {
                path: path.to_owned(),
                actual: format!("complex type `{}`", resolved.type_name),
            }),
            Some(resolved) if resolved.nullable || prefixes.nullable => {
                self.push(Reason::NotHonored {
                    feature: "nullable subject sources",
                });
            }
            Some(_) => {}
        }
    }

    /// The projection's instances, from the one expansion
    /// [`ProjectionSpec::instances`] defines for checking and emission
    /// alike, with the declarations only the lint judges: placeholders are
    /// meaningless outside an expansion, and any brace surviving
    /// substitution is a placeholder that failed — a misspelling would
    /// otherwise be emitted verbatim.
    fn expand(&mut self, projection: &ProjectionSpec) -> Vec<ProjectionSpec> {
        let shared = template_sites(
            &projection.fields,
            &projection.constants,
            &projection.map_assemblies,
        );
        for site in shared {
            if site.contains('{') {
                self.push(Reason::UnresolvedPlaceholder(site.to_owned()));
            }
        }
        if let Some(expansion) = &projection.expansion {
            if expansion.members.is_empty() {
                self.push(Reason::ExpansionWithoutMembers);
            }
            let mut seen = BTreeSet::new();
            for member in &expansion.members {
                if !seen.insert(member.as_str()) {
                    self.push(Reason::DuplicateMember(member.clone()));
                }
            }
            let varying = || {
                template_sites(
                    &expansion.fields,
                    &expansion.constants,
                    &expansion.map_assemblies,
                )
            };
            if !varying().any(|site| site.contains("{member")) {
                self.push(Reason::MembersWithoutVariation);
            }
            for member in &expansion.members {
                for site in varying() {
                    if spec::substitute(site, member).contains('{') {
                        self.push(Reason::UnresolvedPlaceholder(site.to_owned()));
                    }
                }
            }
        }
        projection.instances()
    }

    fn check_field(&mut self, field: &FieldSpec) {
        if field.known_values.iter().any(String::is_empty) {
            self.push(Reason::EmptyKnownValue(field.source_path.clone()));
        }
        let resolved = self.check_source(&field.source_path, field.null_policy, &field.value_map);
        if let Some(resolved) = &resolved {
            self.check_vocabulary(
                &field.source_path,
                resolved,
                &field.known_values,
                "known_values on a source that is not an enumeration",
            );
        }
        let target = self.check_target(&field.target_field);
        if let (Some(resolved), Some(target)) = (&resolved, &target) {
            self.check_enum_outputs(
                &field.source_path,
                resolved,
                &field.value_map,
                &field.known_values,
                target,
            );
        }
        if field.anchor && field.required {
            self.push(Reason::AnchorConflict(field.source_path.clone()));
        }
        match field.cardinality {
            CARDINALITY_UNSPECIFIED | CARDINALITY_SINGLE => {}
            ELEMENTWISE => self.push(Reason::NotHonored {
                feature: "CARDINALITY_ELEMENTWISE",
            }),
            value => self.push(Reason::UnknownCardinality {
                path: field.source_path.clone(),
                value,
            }),
        }
        if !field.unit.is_empty() {
            self.push(Reason::NotHonored { feature: "unit" });
        }
        if !field.unit_path.is_empty() {
            self.push(Reason::NotHonored {
                feature: "unit_path",
            });
        }
        self.check_value_map(&field.source_path, &field.value_map);
    }

    /// The checks every source path gets — resolution, collection support,
    /// a stated null policy, value-map vocabulary — shared by field
    /// mappings and assembly entries. Returns the resolution for
    /// caller-specific checks.
    fn check_source(
        &mut self,
        path: &str,
        null_policy: i32,
        value_map: &[(String, String)],
    ) -> Option<ResolvedField> {
        if !matches!(
            null_policy,
            NULL_UNSPECIFIED | NULL_ABSENT | NULL_INVALID | NULL_EXPLICIT
        ) {
            self.push(Reason::UnknownNullPolicy {
                path: path.to_owned(),
                value: null_policy,
            });
        }
        // Prefixes are walked before the whole path: a path routed through
        // a collection does not resolve at all, and reporting it as an
        // unknown path would mislabel the fault the prefix walk names.
        let prefixes = self.check_prefixes(path);
        let Some(resolved) = self.resolve(path) else {
            if self.known && !prefixes.collection {
                self.push(Reason::UnknownSourcePath(path.to_owned()));
            }
            return None;
        };
        if prefixes.nullable {
            // Reading through an explicitly nullable segment needs presence
            // tracking no generated access spells yet.
            self.push(Reason::NotHonored {
                feature: "nullable intermediate segments",
            });
        }
        if resolved.collection {
            self.push(Reason::NotHonored {
                feature: "collection-typed sources",
            });
        }
        if resolved.nullable && null_policy == NULL_UNSPECIFIED {
            self.push(Reason::UndecidedNull(path.to_owned()));
        }
        // The graph route that preserves explicit nulls does not exist yet;
        // honoring the policy is emission work, so declaring it is an error
        // until then.
        if null_policy == NULL_EXPLICIT {
            self.push(Reason::NotHonored {
                feature: "NULL_POLICY_EXPLICIT_NULL",
            });
        }
        self.check_vocabulary(
            path,
            &resolved,
            value_map.iter().map(|(from, _)| from),
            "value_map on a source that is not an enumeration",
        );
        Some(resolved)
    }

    /// Every proper prefix of a source path must be a singular segment: the
    /// leaf check alone would wave a path through a collection element,
    /// which no generated field access can spell. Nullability is gathered
    /// too, because null policy belongs to the complete read rather than
    /// only to its leaf.
    fn check_prefixes(&mut self, path: &str) -> PrefixFacts {
        let segments: Vec<&str> = path.split('.').collect();
        let mut facts = PrefixFacts {
            nullable: false,
            collection: false,
        };
        for end in 1..segments.len() {
            let prefix = segments[..end].join(".");
            if let Some(resolved) = self.resolve(&prefix) {
                facts.nullable |= resolved.nullable;
                if resolved.collection {
                    facts.collection = true;
                    self.push(Reason::NotHonored {
                        feature: "collection-typed sources",
                    });
                }
            }
        }
        facts
    }

    /// Validates `value_map` sources and `known_values` against the
    /// resolved enum's members. On a source that is not an enumeration the
    /// declarations would be consulted by nothing — a mapping that reads as
    /// enforced while doing nothing — so they are rejected as the named
    /// unimplemented feature.
    fn check_vocabulary(
        &mut self,
        path: &str,
        resolved: &ResolvedField,
        values: impl IntoIterator<Item = impl AsRef<str>>,
        feature: &'static str,
    ) {
        let Some(members) = &resolved.enum_members else {
            if values.into_iter().next().is_some() {
                self.push(Reason::NotHonored { feature });
            }
            return;
        };
        for value in values {
            let value = value.as_ref();
            if !value.is_empty() && !members.iter().any(|member| member == value) {
                self.push(Reason::UnknownEnumValue {
                    path: path.to_owned(),
                    value: value.to_owned(),
                });
            }
        }
    }

    fn check_value_map(&mut self, path: &str, value_map: &[(String, String)]) {
        let mut froms = BTreeSet::new();
        for (from, to) in value_map {
            if from.is_empty() || to.is_empty() {
                self.push(Reason::EmptyValueMapping(path.to_owned()));
            }
            if !from.is_empty() && !froms.insert(from.clone()) {
                self.push(Reason::DuplicateValueMapping {
                    path: path.to_owned(),
                    from: from.clone(),
                });
            }
        }
    }

    fn check_assembly(&mut self, assembly: &AssemblySpec) {
        // Emission sets an assembly through one top-level setter; a dotted
        // target resolves (`value.map_value` is a `Value.Map` leaf) but has
        // no such setter, so it must fail here, named, not inside emission.
        if assembly.target_field.contains('.') {
            self.push(Reason::NestedAssemblyTarget(assembly.target_field.clone()));
        } else if let Some(leaf) = self.check_target(&assembly.target_field) {
            let actual = match leaf.kind() {
                Kind::Message(message) => message.full_name().to_owned(),
                other => format!("{other:?}"),
            };
            if actual != VALUE && actual != VALUE_MAP {
                self.push(Reason::AssemblyTargetNotValue {
                    field: assembly.target_field.clone(),
                    actual,
                });
            }
        }
        if let Some(entries) = self
            .contract
            .get_message_by_name(VALUE_MAP)
            .and_then(|map| map.get_field_by_name("entries"))
            .and_then(|field| self.vocabulary.field_invariant(&field))
            .and_then(|invariant| invariant.max_items)
        {
            if assembly.entries.len() > entries as usize {
                self.push(Reason::TooManyStaticValues {
                    declaration: format!("assembly `{}`", assembly.target_field),
                    actual: assembly.entries.len(),
                    limit: entries,
                });
            }
        }
        let mut keys = BTreeSet::new();
        for entry in &assembly.entries {
            if entry.key.is_empty() {
                self.push(Reason::EmptyEntryKey);
            } else if !keys.insert(entry.key.clone()) {
                self.push(Reason::DuplicateEntryKey(entry.key.clone()));
            }
            // Keys are never substituted; a brace is a placeholder gone
            // wrong.
            if entry.key.contains('{') {
                self.push(Reason::UnresolvedPlaceholder(entry.key.clone()));
            }
            if let Some(key) = self
                .contract
                .get_message_by_name(VALUE_MAP_ENTRY)
                .and_then(|entry| entry.get_field_by_name("key"))
            {
                // The empty case has the more specific diagnostic above.
                self.check_static_text("assembly entry key", &entry.key, &key);
            }
            let resolved =
                self.check_source(&entry.source_path, entry.null_policy, &entry.value_map);
            if let (Some(resolved), Some(target)) = (
                resolved.as_ref(),
                self.contract
                    .get_message_by_name(VALUE)
                    .and_then(|value| value.get_field_by_name(VALUE_STRING)),
            ) {
                self.check_enum_outputs(
                    &entry.source_path,
                    resolved,
                    &entry.value_map,
                    &[],
                    &target,
                );
            }
            self.check_value_map(&entry.source_path, &entry.value_map);
        }
    }

    fn check_static_text(&mut self, declaration: &str, value: &str, field: &FieldDescriptor) {
        let Some(limit) = self
            .vocabulary
            .field_invariant(field)
            .and_then(|invariant| invariant.max_len)
        else {
            return;
        };
        if value.len() > limit as usize {
            self.push(Reason::StaticValueBeyondBound {
                declaration: declaration.to_owned(),
                actual: value.len(),
                limit,
            });
        }
    }

    /// Every enum spelling emission can place in a string landing is known at
    /// compile time. Prove those literals against the target rather than let a
    /// matching device value discover an over-bound manifest at runtime.
    fn check_enum_outputs(
        &mut self,
        path: &str,
        resolved: &ResolvedField,
        value_map: &[(String, String)],
        known_values: &[String],
        target: &FieldDescriptor,
    ) {
        let Some(members) = &resolved.enum_members else {
            return;
        };
        if !matches!(target.kind(), Kind::String) {
            return;
        }

        let mut outputs: Vec<&str> = value_map.iter().map(|(_, to)| to.as_str()).collect();
        for known in known_values {
            if !value_map.iter().any(|(from, _)| from == known) {
                outputs.push(known);
            }
        }
        if outputs.is_empty() {
            outputs.extend(members.iter().map(String::as_str));
        }
        for output in outputs {
            self.check_static_text(&format!("enum output for `{path}`"), output, target);
        }
    }

    /// Resolves a target path, reporting an unknown field or a repeated
    /// leaf, which no declaration can fill yet.
    fn check_target(&mut self, path: &str) -> Option<FieldDescriptor> {
        let leaf = resolve_target(self.target.as_ref()?, path);
        let Some(leaf) = leaf else {
            self.push(Reason::UnknownTargetField(path.to_owned()));
            return None;
        };
        if leaf.is_list() || leaf.is_map() {
            self.push(Reason::NotHonored {
                feature: "repeated target fields",
            });
        }
        Some(leaf)
    }

    /// Target fields must not collide: two writes to one path, a write
    /// inside a field another declaration sets whole, or two cases of one
    /// oneof.
    fn check_targets(&mut self, instance: &ProjectionSpec, vocabulary: &Vocabulary) {
        let declared: Vec<&str> = instance
            .fields
            .iter()
            .map(|field| field.target_field.as_str())
            .chain(
                instance
                    .constants
                    .iter()
                    .map(|constant| constant.target_field.as_str()),
            )
            .chain(
                instance
                    .map_assemblies
                    .iter()
                    .map(|assembly| assembly.target_field.as_str()),
            )
            .filter(|path| !path.is_empty())
            .collect();

        let identity_target = self.target.clone();
        let provenance =
            target_profile(&instance.target_type).and_then(|profile| profile.provenance);
        let mut seen = BTreeSet::new();
        for path in &declared {
            if !seen.insert(*path) {
                self.push(Reason::DuplicateTarget((*path).to_owned()));
            }
            // By spelling when the target is unknown, and by type when it is
            // known — `key` carries identity as much as `subject` does.
            if *path == "subject"
                || within("subject", path)
                || identity_target
                    .as_ref()
                    .is_some_and(|target| writes_identity(target, path))
            {
                self.push(Reason::ReservedTarget((*path).to_owned()));
            }
            if provenance.is_some_and(|provenance| *path == provenance || within(provenance, path))
            {
                self.push(Reason::ReservedProvenance((*path).to_owned()));
            }
        }
        for (index, first) in declared.iter().enumerate() {
            for second in &declared[index + 1..] {
                let (outer, inner) = if within(first, second) {
                    (first, second)
                } else if within(second, first) {
                    (second, first)
                } else {
                    continue;
                };
                self.push(Reason::OverlappingTargets {
                    outer: (*outer).to_owned(),
                    inner: (*inner).to_owned(),
                });
            }
        }

        let Some(target) = self.target.clone() else {
            return;
        };

        if let Some(profile) = target_profile(&instance.target_type) {
            for field in target.fields() {
                let required = vocabulary
                    .field_invariant(&field)
                    .is_some_and(|invariant| invariant.required);
                // Compiler-filled fields — identity and provenance — are
                // covered by construction, and provenance is also reserved,
                // so demanding manifest coverage would be a contradiction.
                if !required
                    || field.name() == profile.identity_field
                    || profile.provenance == Some(field.name())
                {
                    continue;
                }
                let covered = declared.iter().any(|path| {
                    path.split_once('.').map_or(*path, |(root, _)| root) == field.name()
                });
                if !covered {
                    self.push(Reason::UncoveredRequiredTarget(field.name().to_owned()));
                }
            }
        }

        let mut cases: BTreeMap<(String, String), &str> = BTreeMap::new();
        for path in declared {
            let Some(leaf) = resolve_target(&target, path) else {
                continue;
            };
            let Some(oneof) = leaf.containing_oneof() else {
                continue;
            };
            let parent = path.rsplit_once('.').map_or("", |(parent, _)| parent);
            let key = (parent.to_owned(), oneof.name().to_owned());
            match cases.get(&key) {
                None => {
                    cases.insert(key, path);
                }
                Some(first) if *first != path => self.push(Reason::OneofConflict {
                    first: (*first).to_owned(),
                    second: path.to_owned(),
                }),
                Some(_) => {}
            }
        }
    }
}

/// Whether `inner` addresses a field inside the field at `outer`.
fn within(outer: &str, inner: &str) -> bool {
    inner
        .strip_prefix(outer)
        .is_some_and(|rest| rest.starts_with('.'))
}

/// Whether the path sets an identity-carrying field — or reaches into one,
/// which the literal-`subject` rule alone would wave through via `key`.
fn writes_identity(target: &MessageDescriptor, path: &str) -> bool {
    let segments: Vec<&str> = path.split('.').collect();
    (1..=segments.len()).any(|end| {
        let prefix = segments[..end].join(".");
        resolve_target(target, &prefix).is_some_and(|leaf| {
            matches!(leaf.kind(), Kind::Message(message)
                if message.full_name() == SIGNAL_KEY || message.full_name() == SUBJECT_TYPE)
        })
    })
}

/// Every string a member substitutes into: source paths and constant
/// values.
fn template_sites<'a>(
    fields: &'a [FieldSpec],
    constants: &'a [ConstantSpec],
    assemblies: &'a [AssemblySpec],
) -> impl Iterator<Item = &'a str> {
    let fields = fields.iter().map(|field| field.source_path.as_str());
    let entries = assemblies
        .iter()
        .flat_map(|assembly| assembly.entries.iter())
        .map(|entry| entry.source_path.as_str());
    let constants = constants.iter().map(|constant| constant.value.as_str());
    fields.chain(entries).chain(constants)
}

/// Resolves a dotted target path such as `range.min.double_value`; oneof
/// cases are fields of their message, so one walk covers both. Descent is
/// through singular messages only — an element of a repeated field is not
/// addressable. Shared with emission, which lands setters where this walk
/// lands checks.
pub(crate) fn resolve_target(message: &MessageDescriptor, path: &str) -> Option<FieldDescriptor> {
    let mut current = message.clone();
    let mut segments = path.split('.').peekable();
    while let Some(segment) = segments.next() {
        let field = current.get_field_by_name(segment)?;
        if segments.peek().is_none() {
            return Some(field);
        }
        if field.is_list() || field.is_map() {
            return None;
        }
        let Kind::Message(next) = field.kind() else {
            return None;
        };
        current = next;
    }
    None
}
