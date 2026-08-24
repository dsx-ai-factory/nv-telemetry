// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Manifest rejections. The shipped sensor manifest is the passing case,
//! held green by `make check-codegen`; each test here pins one way a
//! declaration the compiler cannot honor fails loudly. The DMTF fold is
//! expensive, so one shared index serves every test.

use std::path::PathBuf;
use std::sync::LazyLock;

use nv_telemetry_codegen::options::Vocabulary;
use nv_telemetry_codegen::projection::check;
use nv_telemetry_codegen::projection::spec::AssemblySpec;
use nv_telemetry_codegen::projection::spec::ConstantSpec;
use nv_telemetry_codegen::projection::spec::EntrySpec;
use nv_telemetry_codegen::projection::spec::ExpansionSpec;
use nv_telemetry_codegen::projection::spec::FieldSpec;
use nv_telemetry_codegen::projection::spec::ManifestSpec;
use nv_telemetry_codegen::projection::spec::ProjectionSpec;
use nv_telemetry_codegen::projection::spec::ScopeSpec;
use nv_telemetry_codegen::projection::spec::SubjectSpec;
use nv_telemetry_codegen::projection::Bundle;
use nv_telemetry_codegen::projection::RedfishIndex;
use nv_telemetry_codegen::projection::Violation;
use prost_reflect::DescriptorPool;

const ABSENT: i32 = 1;

static POOL: LazyLock<DescriptorPool> =
    LazyLock::new(|| nv_telemetry_codegen::pool().expect("the shipped schema decodes"));
static VOCABULARY: LazyLock<Vocabulary> =
    LazyLock::new(|| Vocabulary::resolve(&POOL).expect("vocabulary resolves"));
static BUNDLE: LazyLock<Bundle> =
    LazyLock::new(|| Bundle::dmtf().expect("the vendored bundle parses"));
static INDEX: LazyLock<RedfishIndex<'static>> =
    LazyLock::new(|| BUNDLE.index().expect("the bundle indexes"));

fn violations_of(manifests: &[ManifestSpec]) -> Vec<Violation> {
    check(manifests, &INDEX, &POOL, &VOCABULARY)
}

fn emit(manifests: &[ManifestSpec]) -> Result<Vec<(PathBuf, String)>, String> {
    let checked = nv_telemetry_codegen::projection::compile(manifests, &INDEX, &POOL, &VOCABULARY)
        .map_err(|violations| {
            violations
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        })?;
    nv_telemetry_codegen::projection::emit(&checked, &INDEX, &POOL, &VOCABULARY)
}

fn rejects(spec: ManifestSpec, needle: &str) {
    let violations = violations_of(&[spec]);
    assert!(
        violations
            .iter()
            .any(|violation| violation.to_string().contains(needle)),
        "expected a violation mentioning {needle:?}, got: {violations:?}"
    );
}

/// A declaration the lint accepts but the emitter cannot honor: the error
/// is loud at `make codegen` and names the declaration.
fn emit_rejects(spec: ManifestSpec, needle: &str) {
    let error = emit(&[spec]).expect_err("the manifests should fail emission");
    assert!(
        error.contains(needle),
        "expected an emission error mentioning {needle:?}, got: {error}"
    );
}

fn passes(spec: ManifestSpec) {
    let violations = violations_of(&[spec]);
    assert!(
        violations.is_empty(),
        "unexpected violations: {violations:?}"
    );
}

fn subject() -> SubjectSpec {
    SubjectSpec {
        kind: "sensor".to_owned(),
        scope: vec![ScopeSpec::LocationTemplate {
            template: "/redfish/v1/Chassis/{chassis}/Sensors/{id}".to_owned(),
            capture: "chassis".to_owned(),
        }],
        id_path: "Id".to_owned(),
    }
}

fn field(source_path: &str, target_field: &str) -> FieldSpec {
    FieldSpec {
        source_path: source_path.to_owned(),
        target_field: target_field.to_owned(),
        required: false,
        anchor: false,
        unit: String::new(),
        unit_path: String::new(),
        known_values: Vec::new(),
        null_policy: ABSENT,
        cardinality: 0,
        value_map: Vec::new(),
    }
}

fn projection(name: &str) -> ProjectionSpec {
    ProjectionSpec {
        name: name.to_owned(),
        source_type: "Sensor".to_owned(),
        target_type: "nv.telemetry.v1.Reading".to_owned(),
        subject: Some(subject()),
        fields: vec![field("Reading", "value.double_value")],
        iterate: String::new(),
        versions: 0,
        constants: Vec::new(),
        map_assemblies: Vec::new(),
        expansion: None,
    }
}

fn descriptor(name: &str) -> ProjectionSpec {
    let mut descriptor = projection(name);
    "nv.telemetry.v1.SignalDescriptor".clone_into(&mut descriptor.target_type);
    descriptor.fields.clear();
    descriptor
}

fn state_projection(name: &str) -> ProjectionSpec {
    let mut state = projection(name);
    "nv.telemetry.v1.StateObservation".clone_into(&mut state.target_type);
    state.constants = vec![ConstantSpec {
        target_field: "name".to_owned(),
        value: name.to_owned(),
    }];
    state
}

/// An inventory-shaped projection: a top-level chassis subject (empty
/// scope) and one attributes assembly; identity and provenance are the
/// compiler's to fill.
fn inventory_projection(name: &str) -> ProjectionSpec {
    ProjectionSpec {
        source_type: "Chassis".to_owned(),
        target_type: "nv.telemetry.v1.InventoryItem".to_owned(),
        subject: Some(SubjectSpec {
            kind: "chassis".to_owned(),
            scope: Vec::new(),
            id_path: "Id".to_owned(),
        }),
        fields: Vec::new(),
        map_assemblies: vec![AssemblySpec {
            target_field: "attributes".to_owned(),
            entries: vec![EntrySpec {
                key: "manufacturer".to_owned(),
                source_path: "Manufacturer".to_owned(),
                null_policy: ABSENT,
                value_map: Vec::new(),
            }],
        }],
        ..projection(name)
    }
}

fn manifest(projections: Vec<ProjectionSpec>) -> ManifestSpec {
    ManifestSpec {
        path: manifest_path("sources/redfish/manifests/test.textpb"),
        crate_source: "redfish".to_owned(),
        source: "redfish".to_owned(),
        backend: 1,
        index: "nv-redfish-schema/dmtf".to_owned(),
        projections,
        subject: None,
    }
}

fn manifest_path(path: impl Into<PathBuf>) -> PathBuf {
    path.into()
}

/// A threshold-shaped projection: one anchored field at `path`, expanded
/// over `members`.
fn expanded(members: Vec<&str>, path: &str) -> ProjectionSpec {
    let mut threshold = field(path, "value.double_value");
    threshold.anchor = true;
    ProjectionSpec {
        target_type: "nv.telemetry.v1.StateObservation".to_owned(),
        constants: vec![ConstantSpec {
            target_field: "name".to_owned(),
            value: "threshold".to_owned(),
        }],
        fields: Vec::new(),
        expansion: Some(ExpansionSpec {
            members: members.into_iter().map(str::to_owned).collect(),
            fields: vec![threshold],
            constants: Vec::new(),
            map_assemblies: Vec::new(),
        }),
        ..projection("sample")
    }
}

// The baseline is clean, so each rejection below is its mutation's.
#[test]
fn the_baseline_manifest_is_clean() {
    passes(manifest(vec![
        descriptor("descriptor"),
        projection("sample"),
    ]));
}

#[test]
fn only_explicit_unary_target_profiles_are_accepted() {
    for target in [
        "nv.telemetry.v1.ResourceRelation",
        "nv.telemetry.v1.ValueRange",
    ] {
        let mut broken = projection("unsupported-target");
        broken.target_type = target.to_owned();
        rejects(manifest(vec![broken]), "has no projection profile");
    }
}

#[test]
fn every_required_target_field_must_be_covered() {
    let mut broken = projection("missing-value");
    broken.target_type = "nv.telemetry.v1.StateObservation".to_owned();
    broken.fields.clear();
    broken.constants = vec![ConstantSpec {
        target_field: "name".to_owned(),
        value: "state".to_owned(),
    }];
    rejects(
        manifest(vec![broken]),
        "required target field `value` is not populated",
    );
}

#[test]
fn a_required_target_field_is_an_automatic_output_gate() {
    // Sensor.Reading is nullable and NULL_POLICY_ABSENT: ordinary silence
    // must suppress the required Reading.value, never reach builder.build().
    let files = emit(&[manifest(vec![
        descriptor("descriptor"),
        projection("sample"),
    ])])
    .expect("the profiled Reading projection emits");
    let rendered = &files
        .iter()
        .find(|(path, _)| path.ends_with("test.rs"))
        .expect("the manifest module is rendered")
        .1;
    assert!(
        rendered.contains("if sample_value.is_some()"),
        "required Reading.value does not gate output:\n{rendered}"
    );
}

#[test]
fn an_unknown_source_path_is_rejected() {
    let mut broken = projection("sample");
    broken.fields[0].source_path = "Readng".to_owned();
    rejects(manifest(vec![broken]), "`Readng` resolves to no field");
}

#[test]
fn an_unknown_target_field_is_rejected() {
    let mut broken = projection("sample");
    broken.fields[0].target_field = "value.double".to_owned();
    rejects(manifest(vec![broken]), "target field `value.double`");
}

#[test]
fn anchor_and_required_cannot_meet_on_one_field() {
    let mut broken = projection("sample");
    broken.fields[0].anchor = true;
    broken.fields[0].required = true;
    rejects(manifest(vec![broken]), "both `anchor` and `required`");
}

#[test]
fn a_nullable_source_needs_a_null_policy() {
    // Reading is nullable in the DMTF schema.
    let mut broken = projection("sample");
    broken.fields[0].null_policy = 0;
    rejects(manifest(vec![broken]), "declares no null_policy");
}

#[test]
fn a_nullable_intermediate_segment_is_not_honored() {
    // `Front` is optional+nullable while `UserLabel` is optional and
    // non-nullable: an explicit null can terminate this read before its
    // leaf, and no generated access spells that state yet. Declaring the
    // path is an error until the compiler grows presence tracking.
    let mut nested = state_projection("nested-null");
    nested.source_type = "Chassis".to_owned();
    nested.subject = Some(SubjectSpec {
        kind: "chassis".to_owned(),
        scope: vec![ScopeSpec::LocationTemplate {
            template: "/redfish/v1/Chassis/{chassis}".to_owned(),
            capture: "chassis".to_owned(),
        }],
        id_path: "Id".to_owned(),
    });
    nested.fields = vec![field("Doors.Front.UserLabel", "value.string_value")];
    nested.fields[0].null_policy = 2;
    rejects(manifest(vec![nested]), "`nullable intermediate segments`");
}

#[test]
fn a_nullable_subject_prefix_is_not_honored() {
    let mut broken = state_projection("nullable-subject-prefix");
    broken.source_type = "Chassis".to_owned();
    broken.subject = Some(SubjectSpec {
        kind: "chassis".to_owned(),
        scope: vec![ScopeSpec::PayloadPath("Doors.Front.UserLabel".to_owned())],
        id_path: "Id".to_owned(),
    });
    broken.fields = vec![field("Manufacturer", "value.string_value")];
    rejects(manifest(vec![broken]), "`nullable subject sources`");
}

#[test]
fn preserving_explicit_nulls_is_not_honored_yet() {
    // The graph route that preserves explicit nulls does not exist, so the
    // declaration would read as enforced while collapsing null to absent.
    let mut broken = projection("sample");
    broken.fields[0].null_policy = 3;
    rejects(manifest(vec![broken]), "`NULL_POLICY_EXPLICIT_NULL`");
}

#[test]
fn a_capture_must_appear_in_its_template() {
    let mut broken = projection("sample");
    broken.subject = Some(SubjectSpec {
        scope: vec![ScopeSpec::LocationTemplate {
            template: "/redfish/v1/Chassis/1U".to_owned(),
            capture: "chassis".to_owned(),
        }],
        ..subject()
    });
    rejects(manifest(vec![broken]), "does not appear");
}

#[test]
fn a_nested_assembly_target_is_rejected_by_name() {
    // `value.map_value` resolves to a `Value.Map` leaf, but emission sets
    // assemblies through one top-level setter; the refusal must be a named
    // lint, never a panic inside emission.
    let mut broken = projection("sample");
    broken.map_assemblies = vec![AssemblySpec {
        target_field: "value.map_value".to_owned(),
        entries: vec![EntrySpec {
            key: "reading".to_owned(),
            source_path: "Reading".to_owned(),
            null_policy: ABSENT,
            value_map: Vec::new(),
        }],
    }];
    rejects(manifest(vec![broken]), "lands inside a sub-message");
}

#[test]
fn assemblies_build_value_maps_only() {
    let mut broken = projection("sample");
    broken.map_assemblies = vec![AssemblySpec {
        target_field: "key".to_owned(),
        entries: vec![EntrySpec {
            key: "reading".to_owned(),
            source_path: "Reading".to_owned(),
            null_policy: ABSENT,
            value_map: Vec::new(),
        }],
    }];
    rejects(manifest(vec![broken]), "assemblies build");
}

#[test]
fn an_inventory_projection_is_clean() {
    passes(manifest(vec![inventory_projection("inventory")]));
}

#[test]
fn provenance_is_filled_from_the_canonical_location() {
    // The subject has no location scope, so the parameter survives only
    // because provenance needs it — and the fill must go through
    // `canonical`, which strips query secrets before the wire.
    let files = emit(&[manifest(vec![inventory_projection("inventory")])])
        .expect("the profiled InventoryItem projection emits");
    let rendered = &files
        .iter()
        .find(|(path, _)| path.ends_with("test.rs"))
        .expect("the manifest module is rendered")
        .1;
    assert!(
        rendered.contains("source_key(crate::uri::canonical(location))"),
        "provenance is not filled from the canonical location:\n{rendered}"
    );
    assert!(
        !rendered.contains("_location"),
        "the location parameter was dropped without a scope capture:\n{rendered}"
    );
}

#[test]
fn an_attributes_assembly_lands_as_a_map() {
    let files = emit(&[manifest(vec![inventory_projection("inventory")])])
        .expect("the profiled InventoryItem projection emits");
    let rendered = &files
        .iter()
        .find(|(path, _)| path.ends_with("test.rs"))
        .expect("the manifest module is rendered")
        .1;
    assert!(
        rendered.contains(".attributes(") && !rendered.contains("Value::map"),
        "a Value.Map landing goes through the map setter, not Value::map:\n{rendered}"
    );
}

#[test]
fn source_key_is_reserved_provenance() {
    let mut broken = inventory_projection("inventory");
    broken.fields = vec![field("Manufacturer", "source_key")];
    rejects(manifest(vec![broken]), "provenance");

    let mut broken = inventory_projection("inventory");
    broken.constants = vec![ConstantSpec {
        target_field: "source_key".to_owned(),
        value: "/redfish/v1/Chassis/1U".to_owned(),
    }];
    rejects(manifest(vec![broken]), "provenance");
}

#[test]
fn one_inventory_item_per_source_type() {
    rejects(
        manifest(vec![
            inventory_projection("inventory"),
            inventory_projection("inventory-again"),
        ]),
        "inventory items sharing one subject",
    );
}

#[test]
fn iterate_is_not_honored_yet() {
    let mut broken = projection("sample");
    broken.iterate = "Members".to_owned();
    rejects(manifest(vec![broken]), "`iterate` is declared");
}

#[test]
fn an_unknown_source_type_reports_once() {
    let mut broken = projection("sample");
    broken.source_type = "Sensr".to_owned();
    let violations = violations_of(&[manifest(vec![broken])]);
    assert_eq!(
        violations.len(),
        1,
        "one root fault, no per-path echo: {violations:?}"
    );
    assert!(violations[0].to_string().contains("`Sensr`"));
}

#[test]
fn an_unknown_target_does_not_hide_a_source_fault() {
    let mut broken = projection("sample");
    broken.target_type = "nv.telemetry.v1.Nope".to_owned();
    broken.fields[0].source_path = "Readng".to_owned();
    let violations = violations_of(&[manifest(vec![broken])]);
    let text = format!("{violations:?}");
    assert!(text.contains("Nope"), "missing target fault: {text}");
    assert!(text.contains("Readng"), "missing source fault: {text}");
}

#[test]
fn a_value_mapping_from_outside_the_enum_is_rejected() {
    let mut broken = projection("sample");
    broken.fields[0].source_path = "ReadingType".to_owned();
    broken.fields[0].value_map = vec![("Temprature".to_owned(), "temperature".to_owned())];
    rejects(manifest(vec![broken]), "names `Temprature`");
}

#[test]
fn a_known_value_outside_the_enum_is_rejected() {
    let mut broken = projection("sample");
    broken.fields[0].source_path = "ReadingType".to_owned();
    broken.fields[0].known_values = vec!["Kelvinish".to_owned()];
    rejects(manifest(vec![broken]), "names `Kelvinish`");
}

#[test]
fn an_empty_known_enum_value_is_rejected_before_emission() {
    let mut broken = descriptor("descriptor");
    broken.fields = vec![field("ReadingType", "kind")];
    broken.fields[0].known_values = vec![String::new()];
    rejects(manifest(vec![broken]), "empty known enum value");
}

#[test]
fn unknown_raw_manifest_enum_numbers_are_rejected() {
    let mut null = projection("sample");
    null.fields[0].null_policy = 99;
    rejects(manifest(vec![null]), "unknown null_policy number 99");

    let mut cardinality = projection("sample");
    cardinality.fields[0].cardinality = 99;
    rejects(manifest(vec![cardinality]), "unknown cardinality number 99");

    let mut entry = state_projection("state");
    entry.fields.clear();
    entry.map_assemblies = vec![AssemblySpec {
        target_field: "value".to_owned(),
        entries: vec![EntrySpec {
            key: "reading".to_owned(),
            source_path: "Reading".to_owned(),
            null_policy: 99,
            value_map: Vec::new(),
        }],
    }];
    rejects(manifest(vec![entry]), "unknown null_policy number 99");
}

#[test]
fn a_repeated_target_leaf_is_rejected() {
    let mut broken = projection("sample");
    broken.target_type = "nv.telemetry.v1.States".to_owned();
    broken.fields[0].target_field = "observations".to_owned();
    rejects(manifest(vec![broken]), "repeated target fields");
}

#[test]
fn descent_through_a_repeated_target_is_rejected() {
    let mut broken = projection("sample");
    broken.target_type = "nv.telemetry.v1.States".to_owned();
    broken.fields[0].target_field = "observations.name".to_owned();
    rejects(
        manifest(vec![broken]),
        "`observations.name` resolves to nothing",
    );
}

#[test]
fn two_writes_to_one_target_are_rejected() {
    let mut broken = projection("sample");
    broken
        .fields
        .push(field("ReadingUnits", "value.double_value"));
    rejects(manifest(vec![broken]), "two declarations set");
}

#[test]
fn a_write_inside_a_whole_set_field_is_rejected() {
    let mut broken = projection("sample");
    broken.constants = vec![ConstantSpec {
        target_field: "value".to_owned(),
        value: "x".to_owned(),
    }];
    rejects(manifest(vec![broken]), "is set whole while");
}

#[test]
fn two_cases_of_one_oneof_are_rejected() {
    let mut broken = projection("sample");
    broken.fields.push(field("SpeedRPM", "value.int_value"));
    rejects(manifest(vec![broken]), "cases of one oneof");
}

#[test]
fn the_subject_target_is_reserved() {
    let mut broken = projection("sample");
    broken.fields[0].target_field = "subject.kind".to_owned();
    rejects(manifest(vec![broken]), "subject declaration populates");

    let mut broken = projection("sample");
    broken.constants = vec![ConstantSpec {
        target_field: "subject".to_owned(),
        value: "sensor".to_owned(),
    }];
    rejects(manifest(vec![broken]), "subject declaration populates");
}

#[test]
fn a_manifest_subject_no_projection_inherits_is_dead() {
    let mut ignored = manifest(vec![projection("sample")]);
    ignored.subject = Some(subject());
    rejects(ignored, "shared declaration is dead");
}

#[test]
fn a_subject_path_names_one_scalar() {
    let mut broken = projection("sample");
    broken.subject = Some(SubjectSpec {
        id_path: "Status".to_owned(),
        ..subject()
    });
    rejects(manifest(vec![broken]), "an identity is one scalar value");

    let mut broken = projection("sample");
    broken.subject = Some(SubjectSpec {
        id_path: "Status.Conditions".to_owned(),
        ..subject()
    });
    rejects(manifest(vec![broken]), "resolves to a collection");
}

#[test]
fn projections_inherit_the_manifest_subject() {
    let mut inherited = projection("sample");
    inherited.subject = None;
    let mut inherited_descriptor = descriptor("descriptor");
    inherited_descriptor.subject = None;
    let mut with_default = manifest(vec![inherited_descriptor, inherited]);
    with_default.subject = Some(subject());
    passes(with_default);
}

#[test]
fn a_projection_without_any_subject_is_rejected() {
    let mut orphan = projection("sample");
    orphan.subject = None;
    rejects(manifest(vec![orphan]), "no subject");
}

#[test]
fn placeholders_outside_an_expansion_are_rejected() {
    let mut broken = projection("sample");
    broken.fields[0].source_path = "Thresholds.{member}.Reading".to_owned();
    rejects(manifest(vec![broken]), "placeholder nothing resolves");
}

#[test]
fn a_misspelled_placeholder_names_its_site() {
    // The unresolved spelling both leaves a brace and fails resolution.
    rejects(
        manifest(vec![expanded(
            vec!["UpperCritical"],
            "Thresholds.{membr}.Reading",
        )]),
        "`Thresholds.{membr}.Reading`",
    );
}

#[test]
fn an_expansion_must_vary_by_member() {
    rejects(
        manifest(vec![expanded(vec!["UpperCritical"], "Thresholds.Reading")]),
        "no source path or constant",
    );
}

#[test]
fn a_duplicate_member_is_rejected() {
    rejects(
        manifest(vec![expanded(
            vec!["UpperCritical", "UpperCritical"],
            "Thresholds.{member}.Reading",
        )]),
        "named twice",
    );
}

#[test]
fn an_expansion_without_members_is_rejected() {
    rejects(
        manifest(vec![expanded(vec![], "Thresholds.{member}.Reading")]),
        "expands nothing",
    );
}

#[test]
fn expansion_feeds_real_resolution() {
    // The substituted path is what fails.
    rejects(
        manifest(vec![expanded(
            vec!["UpperBogus"],
            "Thresholds.{member}.Reading",
        )]),
        "`Thresholds.UpperBogus.Reading`",
    );
}

#[test]
fn a_fault_that_does_not_vary_is_one_diagnostic() {
    let members: Vec<&str> = vec!["M1", "M2", "M3", "M4", "M5", "M6"];
    let violations = violations_of(&[manifest(vec![expanded(
        members,
        "Thresholds.{member-keba}.Reading",
    )])]);
    let placeholders = violations
        .iter()
        .filter(|violation| violation.to_string().contains("placeholder"))
        .count();
    assert_eq!(placeholders, 1, "duplicate spam: {violations:?}");
}

#[test]
fn a_placeholder_typo_in_a_constant_is_caught() {
    // A constant is never resolved, so the leftover brace is the net.
    let mut typo = expanded(vec!["UpperCritical"], "Thresholds.{member}.Reading");
    typo.constants = Vec::new();
    typo.expansion.as_mut().expect("set above").constants = vec![ConstantSpec {
        target_field: "name".to_owned(),
        value: "threshold.{Member-kebab}".to_owned(),
    }];
    rejects(
        manifest(vec![typo]),
        "`threshold.{Member-kebab}` carries a brace",
    );
}

#[test]
fn an_entry_key_rejects_placeholders() {
    // Keys are never substituted; one would be emitted verbatim. The
    // fixture is otherwise clean, so this is its only violation.
    let mut braced = expanded(vec!["UpperCritical"], "Thresholds.{member}.Reading");
    let expansion = braced.expansion.as_mut().expect("set above");
    expansion.fields = Vec::new();
    expansion.map_assemblies = vec![AssemblySpec {
        target_field: "value".to_owned(),
        entries: vec![EntrySpec {
            key: "{member}".to_owned(),
            source_path: "Thresholds.{member}.Activation".to_owned(),
            null_policy: ABSENT,
            value_map: Vec::new(),
        }],
    }];
    let violations = violations_of(&[manifest(vec![braced])]);
    assert!(
        violations
            .iter()
            .any(|violation| violation.to_string().contains("`{member}` carries a brace")),
        "the key placeholder passed: {violations:?}"
    );
    assert_eq!(violations.len(), 1, "a noisy fixture: {violations:?}");
}

#[test]
fn projection_names_are_unique_per_crate() {
    // One emitted module per crate: a name reused across its manifest
    // files collides; another crate may reuse it freely.
    let twice = vec![
        manifest(vec![projection("sample")]),
        manifest(vec![projection("sample")]),
    ];
    let violations = violations_of(&twice);
    assert!(
        violations
            .iter()
            .any(|violation| violation.to_string().contains("a second projection named")),
        "cross-file duplicate passed: {violations:?}"
    );

    let mut other_crate = manifest(vec![projection("sample")]);
    other_crate.crate_source = "gnmi".to_owned();
    other_crate.source = "gnmi".to_owned();
    let pair = vec![manifest(vec![projection("sample")]), other_crate];
    let violations = violations_of(&pair);
    assert!(
        !violations
            .iter()
            .any(|violation| violation.to_string().contains("a second projection named")),
        "same name in another crate must not collide: {violations:?}"
    );
}

#[test]
fn assembly_entries_get_the_same_source_checks_as_fields() {
    // A collection-typed entry source is not implemented and must not pass
    // silently.
    let mut broken = projection("sample");
    broken.target_type = "nv.telemetry.v1.StateObservation".to_owned();
    broken.fields = Vec::new();
    broken.constants = vec![ConstantSpec {
        target_field: "name".to_owned(),
        value: "conditions".to_owned(),
    }];
    broken.map_assemblies = vec![AssemblySpec {
        target_field: "value".to_owned(),
        entries: vec![EntrySpec {
            key: "conditions".to_owned(),
            source_path: "Status.Conditions".to_owned(),
            null_policy: ABSENT,
            value_map: Vec::new(),
        }],
    }];
    rejects(manifest(vec![broken]), "collection-typed sources");
}

// Emission shares the checked manifests and the fold; these pins hold its
// two edges — the acceptance suite for what it produces is the corpus
// under `sources/redfish/tests/`, and the staleness gate byte-compares the
// checked-in tree.

#[test]
fn the_shipped_manifests_emit_the_checked_in_tree() {
    let root = nv_telemetry_codegen::workspace_root().expect("a workspace root above the tests");
    let manifests =
        nv_telemetry_codegen::projection::load(&root, &POOL).expect("shipped manifests load");
    assert!(!manifests.is_empty(), "the sensor manifest ships");
    let files = emit(&manifests).expect("shipped manifests emit");
    for (path, rendered) in files {
        let committed = std::fs::read_to_string(root.join(&path))
            .unwrap_or_else(|error| panic!("{} unreadable: {error}", path.display()));
        assert_eq!(
            committed,
            rendered,
            "{} is stale; run `make codegen`",
            path.display()
        );
    }
}

#[test]
fn a_conversion_the_emitter_lacks_is_an_error_never_a_silent_skip() {
    // The surface lint accepts this shape — the paths resolve and the target
    // exists — but typed lowering must reject the unsupported conversion
    // before an emitter can be obtained.
    let mut broken = projection("sample");
    broken.fields[0].source_path = "ReadingUnits".to_owned();
    emit_rejects(
        manifest(vec![descriptor("descriptor"), broken]),
        "no conversion from `Edm.String`",
    );
}

// Review-driven pins: each closes one way a lint-clean manifest could reach
// silently wrong generated code.

#[test]
fn readings_payloads_pair_descriptors_and_samples() {
    rejects(
        manifest(vec![projection("sample")]),
        "every sample key must resolve",
    );

    rejects(
        manifest(vec![descriptor("first"), descriptor("second")]),
        "2 signal descriptors with the same key",
    );

    let mut gated = descriptor("descriptor");
    gated.fields = vec![field("ReadingType", "kind")];
    gated.fields[0].anchor = true;
    rejects(
        manifest(vec![gated, projection("sample")]),
        "gates its signal descriptor",
    );
}

#[test]
fn required_nullable_source_fields_keep_explicit_null_distinct() {
    // BootOptionReference is Redfish.Required but nullable: nv-redfish emits
    // Option<String>, where None can only mean explicit JSON null.
    let mut reference = field("BootOptionReference", "value.string_value");
    reference.null_policy = 2;
    let projected = ProjectionSpec {
        name: "boot-reference".to_owned(),
        source_type: "BootOption".to_owned(),
        target_type: "nv.telemetry.v1.StateObservation".to_owned(),
        subject: Some(SubjectSpec {
            kind: "boot-option".to_owned(),
            scope: vec![ScopeSpec::LocationTemplate {
                template: "/redfish/v1/Systems/{system}/BootOptions/{id}".to_owned(),
                capture: "system".to_owned(),
            }],
            id_path: "Id".to_owned(),
        }),
        fields: vec![reference],
        iterate: String::new(),
        versions: 0,
        constants: vec![ConstantSpec {
            target_field: "name".to_owned(),
            value: "reference".to_owned(),
        }],
        map_assemblies: Vec::new(),
        expansion: None,
    };
    let files = emit(&[manifest(vec![projected])]).expect("required-nullable source emits");
    let rendered = &files
        .iter()
        .find(|(path, _)| path.ends_with("test.rs"))
        .expect("the manifest module is rendered")
        .1;
    assert!(
        rendered.contains("match boot_option.boot_option_reference.clone()")
            && rendered.contains("\"BootOption.BootOptionReference\",")
            && rendered.contains("\"explicitly null\""),
        "required-nullable None did not become explicit null:\n{rendered}"
    );
}

#[test]
fn vocabulary_on_a_non_enum_source_is_not_honored() {
    // The rewrites would be consulted by nothing: a mapping that reads as
    // enforced while doing nothing.
    let mut mapped = projection("sample");
    mapped.fields[0].source_path = "ReadingUnits".to_owned();
    mapped.fields[0].value_map = vec![("Cel".to_owned(), "celsius".to_owned())];
    rejects(
        manifest(vec![mapped]),
        "`value_map on a source that is not an enumeration`",
    );

    let mut known = projection("sample");
    known.fields[0].source_path = "ReadingUnits".to_owned();
    known.fields[0].known_values = vec!["Cel".to_owned()];
    rejects(
        manifest(vec![known]),
        "`known_values on a source that is not an enumeration`",
    );
}

#[test]
fn a_constant_over_the_targets_bound_is_rejected() {
    // Unchecked, this surfaces as a builder refusal on every projection,
    // discarding the batches and issues it rode with.
    let mut broken = projection("sample");
    broken.target_type = "nv.telemetry.v1.StateObservation".to_owned();
    broken.constants = vec![ConstantSpec {
        target_field: "name".to_owned(),
        value: "x".repeat(300),
    }];
    rejects(manifest(vec![broken]), "over the target's bound");
}

#[test]
fn static_subject_values_obey_the_subject_schema() {
    let mut overlong = projection("overlong-subject-kind");
    overlong.subject = Some(SubjectSpec {
        kind: "x".repeat(129),
        ..subject()
    });
    rejects(manifest(vec![overlong]), "subject kind is 129 bytes long");

    let contributor = ScopeSpec::LocationTemplate {
        template: "/redfish/v1/Chassis/{chassis}/Sensors/{id}".to_owned(),
        capture: "chassis".to_owned(),
    };
    let mut too_many = projection("too-many-scope-contributors");
    too_many.subject = Some(SubjectSpec {
        scope: vec![contributor; 17],
        ..subject()
    });
    rejects(manifest(vec![too_many]), "subject scope has 17 items");
}

#[test]
fn enum_outputs_obey_the_target_string_bound() {
    let mut broken = projection("overlong-enum-output");
    broken.target_type = "nv.telemetry.v1.SignalDescriptor".to_owned();
    broken.fields = vec![field("ReadingType", "kind")];
    broken.fields[0].value_map = vec![("Temperature".to_owned(), "x".repeat(129))];
    rejects(
        manifest(vec![broken]),
        "enum output for `ReadingType` is 129 bytes long",
    );
}

fn state_map_projection(entries: Vec<EntrySpec>) -> ProjectionSpec {
    ProjectionSpec {
        target_type: "nv.telemetry.v1.StateObservation".to_owned(),
        fields: Vec::new(),
        constants: vec![ConstantSpec {
            target_field: "name".to_owned(),
            value: "attributes".to_owned(),
        }],
        map_assemblies: vec![AssemblySpec {
            target_field: "value".to_owned(),
            entries,
        }],
        ..projection("state-map")
    }
}

fn map_entry(key: String) -> EntrySpec {
    EntrySpec {
        key,
        source_path: "Reading".to_owned(),
        null_policy: ABSENT,
        value_map: Vec::new(),
    }
}

#[test]
fn map_assembly_literals_obey_the_value_schema() {
    rejects(
        manifest(vec![state_map_projection(vec![map_entry("x".repeat(257))])]),
        "assembly entry key is 257 bytes long",
    );

    let entries = (0..1_025)
        .map(|index| map_entry(format!("key-{index}")))
        .collect();
    rejects(
        manifest(vec![state_map_projection(entries)]),
        "assembly `value` has 1025 items",
    );
}

#[test]
fn location_captures_are_complete_unique_segments() {
    let mut embedded = projection("embedded-capture");
    embedded.subject = Some(SubjectSpec {
        scope: vec![ScopeSpec::LocationTemplate {
            template: "/redfish/v1/Chassis/prefix-{chassis}/Sensors/{id}".to_owned(),
            capture: "chassis".to_owned(),
        }],
        ..subject()
    });
    rejects(
        manifest(vec![embedded]),
        "placeholder segment `prefix-{chassis}` must be exactly",
    );

    let mut repeated = projection("repeated-capture");
    repeated.subject = Some(SubjectSpec {
        scope: vec![ScopeSpec::LocationTemplate {
            template: "/redfish/v1/{chassis}/Chassis/{chassis}/Sensors/{id}".to_owned(),
            capture: "chassis".to_owned(),
        }],
        ..subject()
    });
    rejects(
        manifest(vec![repeated]),
        "capture `chassis` appears 2 times",
    );
}

#[test]
fn location_templates_must_already_be_canonical_resource_paths() {
    for (template, needle) in [
        (
            "/redfish/v1/Chassis/{chassis}/Sensors/{id}/",
            "no trailing separator",
        ),
        (
            "/redfish/v1/Chassis/{chassis}/Sensors/{id}?view=full",
            "query and fragment components",
        ),
        (
            "/redfish/v1/Chassis/{chassis}/Sensors/{id}#/Reading",
            "query and fragment components",
        ),
        (
            "/redfish/v1//Chassis/{chassis}/Sensors/{id}",
            "no empty segments",
        ),
    ] {
        let mut broken = projection("noncanonical-location");
        broken.subject = Some(SubjectSpec {
            scope: vec![ScopeSpec::LocationTemplate {
                template: template.to_owned(),
                capture: "chassis".to_owned(),
            }],
            ..subject()
        });
        rejects(manifest(vec![broken]), needle);
    }
}

#[test]
fn generator_owned_manifest_stems_are_rejected() {
    for stem in ["mod", "provenance"] {
        let mut broken = manifest(vec![projection(stem)]);
        broken.path = manifest_path(format!("sources/redfish/manifests/{stem}.textpb"));
        rejects(broken, "is owned by the generator");
    }
}

#[test]
fn a_stem_that_does_not_become_a_module_name_is_rejected() {
    // `3d-flow` passes the lint (not reserved, not duplicated) but snake
    // cases to `3d_flow`, which no `mod` declaration can name.
    let mut broken = manifest(vec![state_projection("sample")]);
    broken.path = manifest_path("sources/redfish/manifests/3d-flow.textpb");
    emit_rejects(broken, "rename the manifest");
}

#[test]
fn a_path_through_a_collection_is_rejected() {
    // The leaf is a plain scalar; only an intermediate is a collection, so
    // the leaf check alone would wave the path through to field access on a
    // Vec in the generated tree.
    let mut broken = projection("sample");
    broken.source_type = "Chassis".to_owned();
    broken.fields[0].source_path = "Location.Contacts.ContactName".to_owned();
    rejects(manifest(vec![broken]), "collection-typed sources");
}

#[test]
fn a_nullable_subject_source_is_not_honored() {
    // The subject vocabulary has no null policy to declare what a null
    // means, and identity must not be guessed.
    let mut broken = projection("sample");
    broken.subject = Some(SubjectSpec {
        id_path: "Reading".to_owned(),
        ..subject()
    });
    rejects(manifest(vec![broken]), "`nullable subject sources`");
}

#[test]
fn identity_reached_through_key_is_reserved() {
    // `key` carries identity as much as `subject` does; the literal-subject
    // rule alone would wave this through.
    let mut broken = projection("sample");
    broken.fields[0].target_field = "key.subject.id".to_owned();
    rejects(manifest(vec![broken]), "writes into the target's identity");
}

#[test]
fn an_anchor_inside_a_group_still_gates_output() {
    // The group build consumes the member locals, so the anchor's gate is
    // hoisted into a flag the instance condition reads: an absent anchored
    // RangeMin must suppress output even when RangeMax answered.
    let mut anchored = projection("sample");
    anchored.target_type = "nv.telemetry.v1.SignalDescriptor".to_owned();
    anchored.fields = vec![
        field("ReadingRangeMin", "range.min.double_value"),
        field("ReadingRangeMax", "range.max.double_value"),
    ];
    anchored.fields[0].anchor = true;
    let files = emit(&[manifest(vec![anchored])]).expect("a grouped anchor emits");
    let rendered = &files
        .iter()
        .find(|(path, _)| path.ends_with("test.rs"))
        .expect("the manifest's module is rendered")
        .1;
    assert!(
        rendered.contains("let sample_range_gate = sample_range_min.is_some()"),
        "the anchor's gate is not hoisted before the group build:\n{rendered}"
    );
    assert!(
        rendered.contains("if sample_range_gate"),
        "the instance does not read the hoisted gate:\n{rendered}"
    );
}

#[test]
fn a_constant_inside_a_sub_message_is_an_error() {
    let mut broken = projection("sample");
    broken.target_type = "nv.telemetry.v1.SignalDescriptor".to_owned();
    broken.fields = Vec::new();
    broken.constants = vec![ConstantSpec {
        target_field: "range.min.double_value".to_owned(),
        value: "1".to_owned(),
    }];
    emit_rejects(
        manifest(vec![broken]),
        "group-building constants is not implemented",
    );
}

#[test]
fn identical_declared_subjects_agree() {
    // Two projections stating the same identity are one derivation, however
    // the agreement was spelled; only genuine distinctness is refused.
    let mut first = state_projection("first");
    first.subject = Some(subject());
    let mut second = state_projection("second");
    second.subject = Some(subject());
    emit(&[manifest(vec![first, second])]).expect("identical subjects emit one derivation");
}

#[test]
fn distinct_subjects_for_one_source_type_are_errors() {
    let mut first = projection("first");
    first.subject = Some(subject());
    let mut second = projection("second");
    second.subject = Some(SubjectSpec {
        kind: "other-sensor".to_owned(),
        ..subject()
    });
    // Two readings-target projections would trip the pairing rule first;
    // the descriptor keeps this fixture about subjects alone.
    first.target_type = "nv.telemetry.v1.SignalDescriptor".to_owned();
    first.fields.clear();
    emit_rejects(manifest(vec![first, second]), "declare distinct subjects");
}

#[test]
fn a_projection_name_rust_cannot_spell_is_an_error_not_a_panic() {
    let broken = state_projection("3d-flow");
    emit_rejects(manifest(vec![broken]), "not a usable Rust identifier");
}

#[test]
fn two_stems_reducing_to_one_module_are_an_error() {
    let mut first = manifest(vec![projection("first")]);
    first.path = manifest_path("sources/redfish/manifests/sensor-read.textpb");
    let mut second = manifest(vec![projection("second")]);
    second.path = manifest_path("sources/redfish/manifests/sensor_read.textpb");
    let error = emit(&[first, second]).expect_err("colliding modules cannot emit");
    assert!(
        error.contains("already generated"),
        "the error names the collision: {error}"
    );
}
