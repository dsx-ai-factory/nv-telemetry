// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Covers the annotation reader and the presence lint together.
//!
//! Reading a custom option is the compiler's foundation, and getting it wrong
//! is quiet: an option that fails to resolve reads as its default, so a field
//! that exempts itself from a rule would silently stop being exempt. The
//! fixtures therefore plant known violations among fields that are exempt for
//! several different reasons, so a broken reader shows up as a wrong set of
//! violations rather than as nothing at all.

use std::path::Path;
use std::path::PathBuf;

use nv_telemetry_codegen::lint;
use nv_telemetry_codegen::options::Vocabulary;
use nv_telemetry_codegen::options::VocabularyErrorKind;
use prost_reflect::DescriptorPool;

fn pool_for(file: &str) -> DescriptorPool {
    pool_with_vocabulary(file, None)
}

/// Writes a copy of the real vocabulary with `edit` applied, and returns a
/// root that shadows the real one.
///
/// Derived at test time rather than checked in. A committed copy is a second
/// definition of the vocabulary that drifts the moment the real one gains a
/// field, and then these tests fail for a reason that has nothing to do with
/// what they are testing.
fn mutated_vocabulary(name: &str, replacements: &[(&str, &str)]) -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let real = manifest.join("../schema/proto/nv/telemetry/options/v1/annotations.proto");
    let source = std::fs::read_to_string(&real).expect("the vocabulary is readable");

    // Asserted per pattern rather than on the whole edit, so one stale
    // pattern is named instead of sailing through because a sibling still
    // matched.
    let mut edited = source;
    for (from, to) in replacements {
        assert!(
            edited.contains(from),
            "mutation `{name}`: pattern {from:?} no longer matches the vocabulary"
        );
        edited = edited.replace(from, to);
    }

    // Namespaced by process id and written via rename: CARGO_TARGET_TMPDIR is
    // one directory shared by every test binary and every concurrent cargo
    // invocation on this target dir, and a reader catching a plain write
    // mid-flight sees zero bytes, which parses as an empty file rather than
    // failing.
    let root =
        PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("{}-{name}", std::process::id()));
    let dir = root.join("nv/telemetry/options/v1");
    std::fs::create_dir_all(&dir).expect("fixture directory is writable");
    let staged = dir.join("annotations.proto.staged");
    std::fs::write(&staged, edited).expect("fixture is writable");
    std::fs::rename(&staged, dir.join("annotations.proto")).expect("fixture renames");
    root
}

/// Compiles `file`, optionally shadowing the real annotation vocabulary with a
/// deliberately wrong one. Shadowing works because the first include path that
/// resolves a name wins, which lets a fixture exercise a broken vocabulary
/// without a second copy of the contract.
fn pool_with_vocabulary(file: &str, vocabulary_root: Option<PathBuf>) -> DescriptorPool {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures = manifest.join("tests/fixtures");
    let contract = manifest.join("../schema/proto");

    let mut roots = Vec::new();
    if let Some(root) = vocabulary_root {
        roots.push(root);
    }
    roots.push(fixtures.clone());
    roots.push(contract);

    let mut compiler = protox::Compiler::new(&roots).expect("include paths resolve");
    compiler.include_imports(true);
    compiler.include_source_info(false);
    compiler
        .open_files([Path::new(file)])
        .expect("fixture schema compiles");

    // Encoded through reflection rather than through
    // prost_types::FileDescriptorSet, which has nowhere to store extensions
    // and would drop every annotation these tests are about. The schema
    // crate's build script takes the same path for the same reason.
    DescriptorPool::decode(compiler.encode_file_descriptor_set().as_slice())
        .expect("fixture descriptors load")
}

fn fixture(file: &str) -> (DescriptorPool, Vocabulary) {
    let pool = pool_for(file);
    let vocabulary = Vocabulary::resolve(&pool).expect("fixture defines the vocabulary");
    (pool, vocabulary)
}

#[test]
fn presence_lint_catches_only_the_unexcused_scalar() {
    let (pool, vocabulary) = fixture("presence.proto");

    let violations = lint::presence(&pool, &vocabulary);
    let fields: Vec<_> = violations.iter().map(lint::Violation::subject).collect();

    assert_eq!(fields, ["nv.telemetry.v1.PresenceFixture.implicit"]);
}

#[test]
fn presence_lint_reaches_nested_types_and_skips_what_it_cannot_fix() {
    let (pool, vocabulary) = fixture("edges.proto");

    let violations = lint::presence(&pool, &vocabulary);
    let fields: Vec<_> = violations.iter().map(lint::Violation::subject).collect();

    // Nested declarations are reachable, so they are linted. A scalar-valued
    // map is caught on the map field rather than on its synthetic entry: an
    // entry that carries only its key decodes the value as zero, and the entry
    // itself can be neither annotated nor made optional. A message-valued map
    // keeps absence expressible and passes. Oneof members and repeated fields
    // already distinguish absent from empty.
    assert_eq!(
        fields,
        [
            "nv.telemetry.v1.Edges.Nested.nested_implicit",
            "nv.telemetry.v1.Edges.blob",
            "nv.telemetry.v1.Edges.labels",
            "nv.telemetry.v1.Edges.misannotated",
            "nv.telemetry.v1.Edges.severity",
            "nv.telemetry.v1.Edges.wrapped",
            "nv.telemetry.v1.Edges.wrapped_by_name",
        ]
    );
}

#[test]
fn rules_other_than_presence_are_reachable() {
    let (pool, vocabulary) = fixture("rules.proto");
    let found: Vec<_> = lint::presence(&pool, &vocabulary)
        .into_iter()
        .map(|violation| (violation.subject().to_owned(), violation.reason().clone()))
        .collect();

    // Asserting the reason, not just the subject: two rules can fire on the
    // same declaration shape, so a test that checked names alone would pass
    // with the two swapped.
    assert_eq!(
        found,
        [
            // Derived from the stance columns: `zero_is_meaningful` hashes
            // unconditionally, `collection_metadata` skips hashing, and no
            // rule names the pair.
            (
                "nv.telemetry.v1.Conflicted.counter".to_owned(),
                lint::Reason::Conflicting {
                    first: "zero_is_meaningful",
                    second: "collection_metadata",
                    axis: lint::Contradiction::Hashing,
                }
            ),
            // `Nest` carries the same bound and is deliberately absent: it
            // reaches itself through `Branch`, so the bound means something.
            // Without that pair, a rule that flagged every `max_depth` would
            // pass this test.
            (
                "nv.telemetry.v1.Flat".to_owned(),
                lint::Reason::NotApplicable {
                    option: "max_depth",
                    applies_to: "messages that can contain themselves",
                }
            ),
            (
                "nv.telemetry.v1.Hashed".to_owned(),
                lint::Reason::HashableWithoutValidated
            ),
            (
                "nv.telemetry.v1.Island.tag".to_owned(),
                lint::Reason::NotApplicable {
                    option: "collection_metadata",
                    applies_to: "fields of messages a hashable message \
                                 reaches; nothing hashes this one, so there \
                                 is nothing to skip",
                }
            ),
            // Protobuf and buf both permit names Rust reserves; the two
            // paths are separate rules, so each needs its own case — a oneof
            // name becomes a field and an enum type, a field name becomes a
            // field, an accessor, and a setter.
            (
                "nv.telemetry.v1.Keyworded.match".to_owned(),
                lint::Reason::UnusableName
            ),
            (
                "nv.telemetry.v1.Keyworded.type".to_owned(),
                lint::Reason::UnusableName
            ),
            (
                "nv.telemetry.v1.Trace.tag".to_owned(),
                lint::Reason::NotApplicable {
                    option: "collection_metadata",
                    applies_to: "fields of messages a hashable message \
                                 reaches; nothing hashes this one, so there \
                                 is nothing to skip",
                }
            ),
            (
                "nv.telemetry.v1.sneaky".to_owned(),
                lint::Reason::ContractExtension
            ),
        ]
    );
}

// The expected set is exhaustive on purpose: it fails on a rule that stopped
// firing *and* on one that fired where it should not, which is what makes it
// worth more than a dozen separate assertions. Splitting it to satisfy a line
// count would trade that away, since each half would then see the other half's
// violations and have to ignore them.
#[allow(clippy::too_many_lines)]
#[test]
fn an_annotation_that_would_do_nothing_is_rejected() {
    let (pool, vocabulary) = fixture("vocabulary.proto");
    let found: Vec<_> = lint::presence(&pool, &vocabulary)
        .into_iter()
        .map(|violation| (violation.subject().to_owned(), violation.reason().clone()))
        .collect();

    let not_applicable = |option, applies_to| lint::Reason::NotApplicable { option, applies_to };
    let unique_by_applies_to_lists = not_applicable("unique_by", "repeated message fields");

    assert_eq!(
        found,
        [
            // On the element type rather than on a `unique_by` field: a oneof
            // member cannot be `required`, because a sibling arm holding the
            // value is exactly what a oneof is for.
            (
                "nv.telemetry.v1.Element.left".to_owned(),
                not_applicable(
                    "required",
                    "fields outside a oneof; a oneof states that some arm must \
                     be set with its own `required`"
                )
            ),
            // `required` on a repeated field, which is never absent — derived
            // from the option table's applicability column rather than from a
            // rule anyone wrote, and unenforced before the table existed.
            (
                "nv.telemetry.v1.Misapplied.always".to_owned(),
                not_applicable(
                    "required",
                    "fields that can be absent; a repeated field is empty \
                     rather than absent, and an implicit scalar is zero \
                     rather than absent"
                )
            ),
            // And the key rule refuses the oneof member independently, rather
            // than trusting the `required` above to have been caught.
            (
                "nv.telemetry.v1.Misapplied.armed".to_owned(),
                lint::Reason::OptionalUniqueKey("left".to_owned())
            ),
            (
                "nv.telemetry.v1.Misapplied.blank".to_owned(),
                not_applicable(
                    "non_empty",
                    "fields that may hold something, which `max_len: 0` forbids"
                )
            ),
            (
                "nv.telemetry.v1.Misapplied.count".to_owned(),
                not_applicable("non_empty", "string and bytes fields")
            ),
            (
                "nv.telemetry.v1.Misapplied.doubled".to_owned(),
                lint::Reason::DuplicateUniqueKey("id".to_owned())
            ),
            // The two zero-value contradictions are one derived rule: the
            // options' stance columns disagree, in the string spelling here
            // and the enum spelling below, and neither pair is named anywhere
            // in the compiler.
            (
                "nv.telemetry.v1.Misapplied.implicit".to_owned(),
                lint::Reason::Conflicting {
                    first: "zero_is_meaningful",
                    second: "non_empty",
                    axis: lint::Contradiction::ZeroValue,
                }
            ),
            (
                "nv.telemetry.v1.Misapplied.label".to_owned(),
                not_applicable("reject_unspecified", "enum fields")
            ),
            (
                "nv.telemetry.v1.Misapplied.level".to_owned(),
                lint::Reason::Conflicting {
                    first: "zero_is_meaningful",
                    second: "reject_unspecified",
                    axis: lint::Contradiction::ZeroValue,
                }
            ),
            (
                "nv.telemetry.v1.Misapplied.listy".to_owned(),
                lint::Reason::RepeatedUniqueKey("parts".to_owned())
            ),
            (
                "nv.telemetry.v1.Misapplied.loose".to_owned(),
                lint::Reason::OptionalUniqueKey("note".to_owned())
            ),
            (
                "nv.telemetry.v1.Misapplied.one".to_owned(),
                unique_by_applies_to_lists.clone()
            ),
            // Both keys named, in declaration order. Two violations on one
            // subject that rendered identically would say a list has a
            // problem without saying which key.
            (
                "nv.telemetry.v1.Misapplied.pair".to_owned(),
                lint::Reason::RepeatedUniqueKey("parts".to_owned())
            ),
            (
                "nv.telemetry.v1.Misapplied.pair".to_owned(),
                lint::Reason::RepeatedUniqueKey("bits".to_owned())
            ),
            (
                "nv.telemetry.v1.Misapplied.tags".to_owned(),
                unique_by_applies_to_lists
            ),
            (
                "nv.telemetry.v1.Misapplied.typo".to_owned(),
                lint::Reason::UnknownUniqueKey("idd".to_owned())
            ),
            // Every other half of the key rule passes here — present, content,
            // not repeated — and only equality is unsound.
            (
                "nv.telemetry.v1.Misapplied.unstable".to_owned(),
                lint::Reason::UnvalidatedUniqueKey("loose".to_owned())
            ),
            // Reported per annotation and in a fixed order, so the message
            // names which one to delete rather than saying the field has a
            // problem.
            (
                "nv.telemetry.v1.Misapplied.vacuous".to_owned(),
                not_applicable(
                    "unordered",
                    "fields that can hold an element, which `max_items: 0` \
                     forbids"
                )
            ),
            (
                "nv.telemetry.v1.Misapplied.vacuous".to_owned(),
                not_applicable(
                    "unique_by",
                    "fields that can hold an element, which `max_items: 0` \
                     forbids"
                )
            ),
            // Present on every element, so the presence half of the key rule
            // passes; rejected because identity must rest on content, and
            // `collection_metadata` declares the field not to be content.
            (
                "nv.telemetry.v1.Stamps.stamped".to_owned(),
                lint::Reason::MetadataUniqueKey("etag".to_owned())
            ),
        ]
    );
}

#[test]
fn a_legitimate_use_of_the_whole_vocabulary_is_permitted() {
    // The only assertion in this suite that fails when a rule is too eager
    // rather than too lax. Every other fixture plants violations, so a rule
    // that rejected far too much would leave them all passing, and the shipped
    // contract does not reach the combinations most at risk — it has no
    // `zero_is_meaningful` field, and no uniqueness key mixing a message with
    // a scalar.
    let (pool, vocabulary) = fixture("permitted.proto");

    let violations: Vec<String> = lint::presence(&pool, &vocabulary)
        .iter()
        .map(ToString::to_string)
        .collect();

    assert!(
        violations.is_empty(),
        "a rule rejected something sound: {}",
        violations.join("; ")
    );
}

#[test]
fn a_proto2_contract_file_is_rejected() {
    let (pool, vocabulary) = fixture("legacy.proto");
    let reasons: Vec<_> = lint::presence(&pool, &vocabulary)
        .into_iter()
        .map(|violation| violation.reason().clone())
        .collect();

    assert!(reasons.contains(&lint::Reason::UnsupportedSyntax));
}

#[test]
fn a_vocabulary_missing_a_field_is_an_error() {
    let pool = pool_with_vocabulary(
        "minimal.proto",
        Some(mutated_vocabulary(
            "badvocab",
            &[
                ("bool finite = 2;", "bool finiteness = 2;"),
                ("finite: true", "finiteness: true"),
            ],
        )),
    );
    let error = Vocabulary::resolve(&pool).expect_err("the vocabulary is missing `finite`");

    assert_eq!(
        error.kind(),
        VocabularyErrorKind::MissingField { field: "finite" }
    );
}

#[test]
fn a_widened_bound_is_an_error_rather_than_a_silent_zero() {
    // uint32 -> uint64 is wire-compatible, so nothing else would notice; the
    // reader would return the type's default and switch the bound off.
    let pool = pool_with_vocabulary(
        "minimal.proto",
        Some(mutated_vocabulary(
            "wrongtype",
            &[(
                "optional uint32 max_items = 5;",
                "optional uint64 max_items = 5;",
            )],
        )),
    );
    let error = Vocabulary::resolve(&pool).expect_err("max_items is unreadable as declared");

    assert_eq!(
        error.kind(),
        VocabularyErrorKind::WrongType {
            field: "max_items",
            expected: "optional uint32"
        }
    );
}

#[test]
fn a_bound_losing_explicit_presence_is_an_error() {
    // Dropping `optional` keeps the type and the number but collapses
    // `max_items: 0`, meaning "must
    // be empty", into "no bound at all". The vocabulary has had exactly this
    // bug once already.
    let pool = pool_with_vocabulary(
        "minimal.proto",
        Some(mutated_vocabulary(
            "lostpresence",
            &[("optional uint32 max_items = 5;", "uint32 max_items = 5;")],
        )),
    );
    let error = Vocabulary::resolve(&pool).expect_err("max_items can no longer express zero");

    assert_eq!(
        error.kind(),
        VocabularyErrorKind::WrongType {
            field: "max_items",
            expected: "optional uint32"
        }
    );
}

#[test]
fn a_key_list_narrowed_to_one_key_is_an_error() {
    // `repeated string` -> `optional string` is the quietest shape change in
    // the vocabulary: the reader asks for a list, gets nothing, and returns an
    // empty one — which is exactly how "no uniqueness constraint" is spelled,
    // so every collection would silently stop being checked.
    let pool = pool_with_vocabulary(
        "minimal.proto",
        Some(mutated_vocabulary(
            "narrowedkeys",
            &[
                (
                    "repeated string unique_by = 10;",
                    "optional string unique_by = 10;",
                ),
                ("unique_by: [\"id\"]", "unique_by: \"id\""),
            ],
        )),
    );
    let error = Vocabulary::resolve(&pool).expect_err("unique_by is unreadable as declared");

    assert_eq!(
        error.kind(),
        VocabularyErrorKind::WrongType {
            field: "unique_by",
            expected: "repeated string"
        }
    );
}

#[test]
fn an_option_nothing_reads_is_rejected() {
    // The mirror of the shape check: that one proves every option the compiler
    // reads is declared, this proves every option declared is read. It is the
    // quieter of the two — a renamed option is at least reported, while one
    // nobody reads produces no error anywhere and can be written across the
    // contract while generating nothing. It is also how a withdrawn option
    // comes back; EXTENSIONS.md records 52004 for exactly that reason.
    let pool = pool_with_vocabulary(
        "minimal.proto",
        Some(mutated_vocabulary(
            "unread",
            &[(
                "// Constraints on a oneof.",
                "message EnumInvariant {\n  bool closed = 1;\n}\n\n\
                 extend google.protobuf.EnumOptions {\n  \
                 EnumInvariant enum_invariant = 52004;\n}\n\n\
                 // Constraints on a oneof.",
            )],
        )),
    );
    let error = Vocabulary::resolve(&pool).expect_err("the vocabulary declares an unread option");

    assert_eq!(error.kind(), VocabularyErrorKind::NotRead);
    assert_eq!(error.name(), "nv.telemetry.options.v1.enum_invariant");
}

#[test]
fn an_unread_field_inside_an_annotation_is_rejected() {
    // The same hole as the extension check, one level down, and the likelier
    // of the two: adding a field to `FieldInvariant` is an ordinary edit,
    // adding a whole `extend` block is not. A field the shape table does not
    // name is read by nothing and recorded by nothing, so it can be written
    // across the contract and leave only schema text that reads as a rule.
    let pool = pool_with_vocabulary(
        "minimal.proto",
        Some(mutated_vocabulary(
            "unreadfield",
            &[(
                "  repeated string unique_by = 10;",
                "  repeated string unique_by = 10;\n\n  bool ignored = 11;",
            )],
        )),
    );
    let error = Vocabulary::resolve(&pool).expect_err("the annotation declares an unread field");

    assert_eq!(error.kind(), VocabularyErrorKind::NotRead);
    assert_eq!(
        error.name(),
        "nv.telemetry.options.v1.FieldInvariant.ignored"
    );
}

#[test]
fn an_unread_option_in_a_vocabulary_sub_package_is_rejected() {
    // The sub-package is the interesting case: matching the vocabulary package
    // for equality would leave one silently unchecked, which is the hole the
    // rule exists to close, moved down a level. `is_contract_package` already
    // matches by prefix for this reason.
    let root =
        PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("{}-subpkg", std::process::id()));
    let vocabulary = root.join("nv/telemetry/options/v1");
    let nested = vocabulary.join("experimental");
    std::fs::create_dir_all(&nested).expect("fixture directories are writable");

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let real = manifest.join("../schema/proto/nv/telemetry/options/v1/annotations.proto");
    let source = std::fs::read_to_string(&real).expect("the vocabulary is readable");
    let anchor = "import \"google/protobuf/descriptor.proto\";";
    assert!(
        source.contains(anchor),
        "the vocabulary no longer imports descriptor.proto"
    );
    std::fs::write(
        vocabulary.join("annotations.proto"),
        source.replace(
            anchor,
            &format!("{anchor}\nimport \"nv/telemetry/options/v1/experimental/extra.proto\";"),
        ),
    )
    .expect("fixture is writable");

    std::fs::write(
        nested.join("extra.proto"),
        "syntax = \"proto3\";\n\
         package nv.telemetry.options.v1.experimental;\n\
         import \"google/protobuf/descriptor.proto\";\n\
         message Extra {\n  bool closed = 1;\n}\n\
         extend google.protobuf.EnumOptions {\n  Extra extra = 52050;\n}\n",
    )
    .expect("fixture is writable");

    let pool = pool_with_vocabulary("minimal.proto", Some(root));
    let error = Vocabulary::resolve(&pool).expect_err("the sub-package declares an unread option");

    assert_eq!(error.kind(), VocabularyErrorKind::NotRead);
    assert_eq!(error.name(), "nv.telemetry.options.v1.experimental.extra");
}

#[test]
fn the_canary_exercises_every_option_the_compiler_reads() {
    // Belt and braces: the option table's probes already enforce this at
    // `resolve`, since a probe reads a named canary field and fails when the
    // option is not declared on it. What this adds is independence — it walks
    // the canary through the descriptor API rather than through the probes, so
    // a probe pointed at the wrong field and the canary drifting together
    // still fail here.
    let pool = nv_telemetry_codegen::pool().expect("shipped schema decodes");
    let vocabulary = Vocabulary::resolve(&pool).expect("shipped schema defines the vocabulary");
    let canary = pool
        .get_message_by_name("nv.telemetry.options.v1.Canary")
        .expect("the canary is present");

    // Nested types count: the canary's own element type carries `required`,
    // and a rule that only looked at the top level would call it uncovered.
    let messages = std::iter::once(canary.clone()).chain(canary.child_messages());
    let fields: Vec<_> = messages
        .flat_map(|message| message.fields().collect::<Vec<_>>())
        .collect();

    for option in Vocabulary::field_option_names() {
        assert!(
            fields
                .iter()
                .any(|field| vocabulary.field_option_is_set(field, option)),
            "no canary field sets `{option}`, so nothing proves its value \
             survives the encoding path"
        );
    }

    for option in Vocabulary::message_option_names() {
        assert!(
            vocabulary.message_option_is_set(&canary, option),
            "the canary does not set `{option}`"
        );
    }
}

#[test]
fn the_canary_catches_a_boolean_invariant_switched_off() {
    // Each boolean the canary asserts needs its own mutation. Without one, the
    // assertion in `check_canary` can be deleted and the whole suite stays
    // green — which is the state `the_canary_can_actually_fail` exists to
    // prevent, and it does not generalize to assertions added later.
    for (option, from, to) in [
        ("nonempty", "non_empty: true", "non_empty: false"),
        (
            "rejectunspecified",
            "reject_unspecified: true",
            "reject_unspecified: false",
        ),
    ] {
        let pool = pool_with_vocabulary(
            "minimal.proto",
            Some(mutated_vocabulary(option, &[(from, to)])),
        );
        let error =
            Vocabulary::resolve(&pool).expect_err("the canary declares the invariant switched off");

        assert!(
            matches!(error.kind(), VocabularyErrorKind::CanaryFailed { .. }),
            "`{from}` turned off in the canary went undetected"
        );
    }
}

#[test]
fn the_canary_catches_a_changed_key_list() {
    // The shape check above proves the declaration is right; this proves the
    // values survive the encoding path, which is a separate failure with the
    // same silent outcome.
    let pool = pool_with_vocabulary(
        "minimal.proto",
        Some(mutated_vocabulary(
            "changedkeys",
            &[("unique_by: [\"id\"]", "unique_by: [\"other\"]")],
        )),
    );
    let error = Vocabulary::resolve(&pool).expect_err("the canary declares different keys");

    assert!(matches!(
        error.kind(),
        VocabularyErrorKind::CanaryFailed { .. }
    ));
}

#[test]
fn the_canary_can_actually_fail() {
    // Without this, `check_canary` could be replaced with `Ok(())` and every
    // other test would still pass — leaving the one guard against silently
    // dropped option values unverified.
    let pool = pool_with_vocabulary(
        "minimal.proto",
        Some(mutated_vocabulary(
            "badcanary",
            &[("max_depth: 8", "max_depth: 9")],
        )),
    );
    let error = Vocabulary::resolve(&pool).expect_err("the canary declares different values");

    assert!(matches!(
        error.kind(),
        VocabularyErrorKind::CanaryFailed { .. }
    ));
    assert_eq!(error.name(), "nv.telemetry.options.v1.Canary");
}

#[test]
fn missing_vocabulary_is_an_error_rather_than_an_absent_annotation() {
    let pool = pool_for("bare.proto");

    let error = Vocabulary::resolve(&pool).expect_err("vocabulary is not defined by this pool");

    // Without this, a dropped import or a renamed option would read as "no
    // field is annotated", turning every invariant off without a word.
    assert_eq!(error.name(), "nv.telemetry.options.v1.field_invariant");
    assert_eq!(error.kind(), VocabularyErrorKind::NotDefined);
}

#[test]
fn field_annotations_are_read_as_typed_values() {
    let (pool, vocabulary) = fixture("presence.proto");
    let message = pool
        .get_message_by_name("nv.telemetry.v1.PresenceFixture")
        .expect("fixture message is present");

    let field = |name: &str| {
        message
            .get_field_by_name(name)
            .expect("fixture field is present")
    };

    let declared_zero = vocabulary
        .field_invariant(&field("declared_zero"))
        .expect("annotation is present");
    assert!(declared_zero.zero_is_meaningful);
    assert!(!declared_zero.finite);

    let annotated = vocabulary
        .field_invariant(&field("annotated"))
        .expect("annotation is present");
    assert!(annotated.finite);
    assert!(!annotated.zero_is_meaningful);

    // An unset bound stays None, and a bound of zero stays Some(0). Collapsing
    // the two would reproduce, inside the compiler, the presence bug the
    // compiler exists to prevent.
    assert_eq!(annotated.max_items, None);
    assert_eq!(annotated.max_len, None);

    let empty = vocabulary
        .field_invariant(&field("must_be_empty"))
        .expect("annotation is present");
    assert_eq!(empty.max_items, Some(0));

    assert_eq!(vocabulary.field_invariant(&field("explicit")), None);
}

#[test]
fn message_annotations_are_read_as_typed_values() {
    let (pool, vocabulary) = fixture("presence.proto");

    let wrapper = pool
        .get_message_by_name("nv.telemetry.v1.WrapperFixture")
        .expect("fixture message is present");
    let invariant = vocabulary
        .message_invariant(&wrapper)
        .expect("annotation is present");
    assert!(invariant.validated);
    assert!(invariant.hashable);
    assert_eq!(invariant.max_depth, Some(4));

    let plain = pool
        .get_message_by_name("nv.telemetry.v1.PresenceFixture")
        .expect("fixture message is present");
    assert_eq!(vocabulary.message_invariant(&plain), None);
}

#[test]
fn shipped_contract_passes_its_own_lint() {
    let pool = nv_telemetry_codegen::pool().expect("shipped schema decodes");
    let vocabulary = Vocabulary::resolve(&pool).expect("shipped schema defines the vocabulary");

    assert_eq!(lint::presence(&pool, &vocabulary), Vec::new());
}
