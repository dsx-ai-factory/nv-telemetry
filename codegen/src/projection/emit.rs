// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Checked emission: manifests the lint accepted become the source crate's
//! generated projection modules, in one pass.
//!
//! The division of labor is two-sided. Everything an *author* can get wrong
//! is a lint rule with its own diagnostic, and [`compile`](super::compile)
//! is the only way to obtain the receipt this module accepts — emission
//! cannot run on unchecked manifests. Everything the *compiler* has not
//! implemented yet — a conversion it lacks, a source shape it cannot spell —
//! is an `Err` from here, loud at `make codegen` and named after the
//! declaration; it is never silently degraded code.
//!
//! One module per manifest under `sources/<crate>/src/generated/`, one
//! projection function per source type, built through the same stack as the
//! model emitter: `quote!` token streams, a `syn` parse proving the assembly
//! is a Rust file, `prettyplease` rendering. Source-side names come from the
//! source generator's own case converters and field shapes from its own
//! required×nullable rule, so the emitted access code cannot drift from what
//! the source crate generated.
//!
//! The generated bodies follow the projection disciplines the corpus pins:
//! every field and every identity contributor is evaluated before identity
//! is decided, so one report carries every issue; absence produces no
//! output; an unusable answer produces an issue beside the parts; and
//! `Err` from a generated function is the residual tier only — a builder
//! refusing inputs the function already triaged is a projection bug, mapped
//! to an `Internal` acquisition failure, never device data.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::PathBuf;

use heck::ToSnakeCase as _;
use nv_redfish_csdl_compiler::compiler::TypeClass;
use nv_redfish_csdl_compiler::generator::casemungler;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use prost_reflect::DescriptorPool;
use prost_reflect::FieldDescriptor;
use prost_reflect::Kind;
use prost_reflect::MessageDescriptor;
use quote::format_ident;
use quote::quote;

use crate::options::Vocabulary;
use crate::projection::lint::resolve_target;
use crate::projection::lint::target_profile;
use crate::projection::lint::IdentityKind;
use crate::projection::lint::TargetProfile;
use crate::projection::location::LocationPattern;
use crate::projection::location::LocationSegment;
use crate::projection::spec::AssemblySpec;
use crate::projection::spec::EntrySpec;
use crate::projection::spec::FieldSpec;
use crate::projection::spec::ManifestSpec;
use crate::projection::spec::ProjectionSpec;
use crate::projection::spec::ScopeSpec;
use crate::projection::spec::SubjectSpec;
use crate::projection::Checked;
use crate::projection::RedfishIndex;
use crate::projection::Shape;
use crate::projection::Step;
use crate::provenance;
use crate::wrapper::names::constant_stem;
use crate::wrapper::names::ident;
use crate::wrapper::names::short_name;

// Null-policy numbers mirror manifest.proto; buf breaking guards the mirror.
const NULL_INVALID: i32 = 2;

/// The hand-written value vocabulary: construction is a fixed constructor
/// table, the emission counterpart of the model emitter's `HAND_WRITTEN`
/// list.
const NUMERIC_VALUE: &str = "nv.telemetry.v1.NumericValue";
const VALUE: &str = "nv.telemetry.v1.Value";
const VALUE_MAP: &str = "nv.telemetry.v1.Value.Map";
const SUBJECT: &str = "nv.telemetry.v1.Subject";

const HEADER: &str = "\
// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
";

/// Renders every generated projection file, keyed by workspace-relative
/// path: one module per manifest, plus the module root and provenance table
/// per source crate.
///
/// # Errors
///
/// A message naming the manifest and declaration when a declaration needs a
/// capability this compiler does not have, or when a renderer template
/// assembles invalid Rust.
pub fn emit(
    checked: &Checked<'_>,
    index: &RedfishIndex<'_>,
    contract: &DescriptorPool,
    vocabulary: &Vocabulary,
) -> Result<Vec<(PathBuf, String)>, String> {
    let mut crates: BTreeMap<&str, Vec<&ManifestSpec>> = BTreeMap::new();
    for manifest in checked.manifests() {
        crates
            .entry(manifest.crate_source.as_str())
            .or_default()
            .push(manifest);
    }

    let mut files = Vec::new();
    for (crate_source, manifests) in crates {
        let generated = PathBuf::from("sources")
            .join(crate_source)
            .join("src")
            .join("generated");
        let mut modules = Vec::new();
        for manifest in &manifests {
            let module = manifest
                .path
                .file_stem()
                .map(|stem| stem.to_string_lossy().to_snake_case())
                .unwrap_or_default();
            require_ident(&module, &manifest.relative_path(), || {
                format!(
                    "file stem derives the module name `{module}`, which is \
                     not a usable Rust identifier; rename the manifest"
                )
            })?;
            let rendered = Emitter {
                manifest,
                index,
                contract,
                vocabulary,
            }
            .render()?;
            files.push((generated.join(format!("{module}.rs")), rendered));
            modules.push(module);
        }
        files.push((
            generated.join("provenance.rs"),
            provenance::render(&provenance::collect(&manifests)),
        ));
        files.push((generated.join("mod.rs"), module_root(&modules)));
    }
    Ok(files)
}

/// The generated module root: written rather than hand-maintained so the
/// whole directory is generated output and the staleness gate covers it.
fn module_root(modules: &[String]) -> String {
    let mut root = String::from(HEADER);
    root.push_str(
        "\n//! Generated by `make codegen` from the manifests under `manifests/`.\n\
         //! Do not edit by hand.\n\n\
         // Formatted by the generator and byte-compared against it. rustfmt.toml\n\
         // excludes generated trees, but `ignore` is nightly-only: stable rustfmt\n\
         // would reformat and the staleness gate would then report a schema\n\
         // problem that does not exist.\n",
    );
    for module in modules {
        let _ = write!(root, "#[rustfmt::skip]\npub(crate) mod {module};\n\n");
    }
    root.push_str("#[rustfmt::skip]\npub(crate) mod provenance;\n");
    root
}

/// How a leaf converts, keyed on the resolved source type — the
/// class-plus-type-name selection the design records.
enum SourceClass {
    /// `Edm.Decimal`, or a type definition over it: an `f64` in the source
    /// crate's generated Rust.
    Decimal,
    /// `Edm.String`, or a type definition over it: a `String`.
    Text,
    /// An enumeration: a generated `Copy` enum whose members project to
    /// strings.
    Enum,
}

/// Where a target path lands once any value-vocabulary arm is consumed:
/// `range.min.double_value` evaluates into a `NumericValue` local that the
/// `min` setter receives.
struct Landing {
    /// Setter segments from the target message inward.
    setter_path: Vec<String>,
    kind: LandingKind,
}

enum LandingKind {
    /// A plain string field of a generated message.
    PlainString(FieldDescriptor),
    /// A constructor of the hand-written value vocabulary: the leaf is the
    /// named oneof arm of `NumericValue` or `Value`.
    VocabArm {
        vocabulary: String,
        arm: String,
        field: FieldDescriptor,
    },
}

impl LandingKind {
    fn description(&self) -> String {
        match self {
            Self::PlainString(field) => field.full_name().to_owned(),
            Self::VocabArm {
                vocabulary, arm, ..
            } => format!("{vocabulary}.{arm}"),
        }
    }

    fn field(&self) -> &FieldDescriptor {
        match self {
            Self::PlainString(field) | Self::VocabArm { field, .. } => field,
        }
    }
}

/// The selected conversion of one present leaf value, decided before any
/// tokens are rendered.
enum Conversion {
    Decimal {
        constructor: String,
    },
    Text {
        check: TextCheck,
        destination: TextDestination,
    },
    Enum {
        namespace: String,
        name: String,
        rows: Vec<(String, String)>,
        destination: TextDestination,
    },
}

enum TextDestination {
    Plain,
    Vocabulary(String),
}

/// The target leaf's own bounds, mirrored as pre-checks so the builder's
/// refusal stays the residual tier and the issue quotes the model's own
/// violation spelling.
struct TextCheck {
    field_name: String,
    non_empty: bool,
    max_len_constant: Option<String>,
}

/// One evaluated field mapping, ready for assembly.
struct Output {
    local: Ident,
    setter: String,
    /// Conditions under which the instance may emit: an absent `anchor` or
    /// contract-required landing suppresses output, a manifest-`required`
    /// field with the absence already reported. Grouped members hoist their
    /// gates into a flag before the group build consumes their locals.
    gates: Vec<TokenStream>,
    /// Source-prefixed issue path, for builder blame.
    issue_path: String,
}

/// One manifest's emission context.
struct Emitter<'a> {
    manifest: &'a ManifestSpec,
    index: &'a RedfishIndex<'a>,
    contract: &'a DescriptorPool,
    vocabulary: &'a Vocabulary,
}

impl Emitter<'_> {
    fn render(&self) -> Result<String, String> {
        let mut items = TokenStream::new();

        // One function per source type, in first-appearance order.
        let mut groups: Vec<(&str, Vec<&ProjectionSpec>)> = Vec::new();
        for projection in &self.manifest.projections {
            match groups
                .iter_mut()
                .find(|(source_type, _)| *source_type == projection.source_type)
            {
                Some((_, group)) => group.push(projection),
                None => groups.push((&projection.source_type, vec![projection])),
            }
        }
        for (source_type, group) in &groups {
            items.extend(self.source_type_items(source_type, group)?);
        }

        // `quote!` guarantees lexical well-formedness only; the parse proves
        // the assembled tokens are a Rust file, so a template bug fails
        // generation rather than the source crate's build.
        let parsed = syn::parse2::<syn::File>(items).map_err(|error| {
            format!(
                "{}: the projection emitter produced invalid Rust: {error}; this is a codegen template bug",
                self.manifest.relative_path()
            )
        })?;
        let body = prettyplease::unparse(&parsed);

        let mut header = String::from(HEADER);
        let _ = write!(
            header,
            "\n//! Generated from `{}` by `make codegen`. Do not edit.\n\
             //!\n\
             //! Deterministic, I/O-free projection from decoded source types to\n\
             //! validated observation parts plus issues. Every field is evaluated\n\
             //! before identity is decided, absence produces no output, and an\n\
             //! unusable answer produces an issue beside the parts.\n\n\
             // Generated code holds the line on correctness lints; the pedantic\n\
             // group is style advice for humans and is exactly where a clippy\n\
             // release breaks a checked-in file that no one edited.\n\
             #![allow(clippy::pedantic)]\n",
            self.manifest.relative_path(),
        );
        Ok(format!("{header}\n{body}"))
    }

    /// The parts struct and projection function for one source type.
    // The function template in execution order — parts, evaluations,
    // subject, assembly — and splitting it would scatter what is one shape.
    #[allow(clippy::too_many_lines)]
    fn source_type_items(
        &self,
        source_type: &str,
        group: &[&ProjectionSpec],
    ) -> Result<TokenStream, String> {
        let manifest = self.manifest.relative_path();
        require_ident(source_type, &manifest, || {
            format!("source type `{source_type}` does not name Rust items")
        })?;
        let source_namespace = self.index.entity_namespace(source_type).ok_or_else(|| {
            format!("{manifest}: source type `{source_type}` vanished from the index")
        })?;
        let parts_name = format_ident!("{source_type}Parts");
        let fn_name = format_ident!("project_{}", source_type.to_snake_case());
        let source_param = ident(&source_type.to_snake_case());

        // Parts collections: one per distinct target type, in
        // first-appearance order, named from the contract type.
        let mut target_types: Vec<&str> = Vec::new();
        for projection in group {
            if !target_types.contains(&projection.target_type.as_str()) {
                target_types.push(&projection.target_type);
            }
        }
        let mut parts_fields = Vec::new();
        let mut parts_idents = Vec::new();
        let mut collection_fields: Vec<(&str, String)> = Vec::new();
        for target_type in &target_types {
            let short = short_name(target_type);
            let field_name = format!("{}s", short.to_snake_case());
            require_ident(&field_name, &manifest, || {
                format!("target `{target_type}` derives an unusable parts field `{field_name}`")
            })?;
            let field = ident(&field_name);
            let ty = ident(&short);
            parts_fields.push(quote! {
                pub(crate) #field: Vec<::nv_telemetry_model::#ty>,
            });
            parts_idents.push(field);
            collection_fields.push((target_type, field_name));
        }

        // The effective subject; projections that inherit the manifest's
        // share one derivation, and identical declarations agree by value.
        let effective = group
            .iter()
            .map(|projection| {
                projection
                    .subject
                    .as_ref()
                    .or(self.manifest.subject.as_ref())
                    .ok_or_else(|| {
                        format!(
                            "{manifest}: `{}`: no subject survived checking",
                            projection.name
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let subject_spec = effective[0];
        if effective.iter().any(|subject| *subject != subject_spec) {
            return Err(format!(
                "{manifest}: projections over `{source_type}` declare distinct subjects; \
                 one subject derivation is required per source type"
            ));
        }

        // Every local the function body will bind. Seeded with the names
        // the template already gives meaning, because derived locals come
        // from manifest-author strings and a collision would be a silently
        // shadowing `let`, not an error.
        let mut locals: BTreeSet<String> = [
            "issues", "subject", "key", "location", "value", "error", "segments", "captured",
            "builder",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        locals.insert(source_param.to_string());
        for (_, field_name) in &collection_fields {
            locals.insert(field_name.clone());
        }

        let mut evaluations = TokenStream::new();
        let mut assemblies = TokenStream::new();
        let mut needs_key = false;
        let mut needs_provenance = false;

        for projection in group {
            let members: Vec<Option<&str>> = match &projection.expansion {
                Some(expansion) => expansion
                    .members
                    .iter()
                    .map(|member| Some(member.as_str()))
                    .collect(),
                None => vec![None],
            };
            for (instance, member) in projection.instances().into_iter().zip(members) {
                let mut prefix = projection.name.to_snake_case();
                if let Some(member) = member {
                    prefix = format!("{prefix}_{}", casemungler::to_snake(member));
                }
                let context = format!("{manifest}: `{}`", projection.name);
                let target = self
                    .contract
                    .get_message_by_name(&instance.target_type)
                    .ok_or_else(|| {
                        format!(
                            "{context}: target `{}` vanished after checking",
                            instance.target_type
                        )
                    })?;
                let profile = target_profile(&instance.target_type).ok_or_else(|| {
                    format!(
                        "{context}: target `{}` lost its projection profile",
                        instance.target_type
                    )
                })?;

                let mut pending = Vec::new();
                for field in &instance.fields {
                    let (tokens, output) = self.field_evaluation(
                        source_type,
                        &source_param,
                        &prefix,
                        &target,
                        field,
                        &mut locals,
                        &context,
                    )?;
                    evaluations.extend(tokens);
                    pending.push(output);
                }
                let outputs = build_groups(
                    &target,
                    &prefix,
                    pending,
                    &mut evaluations,
                    &mut locals,
                    &context,
                )?;

                let mut assembly_locals = Vec::new();
                for assembly in &instance.map_assemblies {
                    let (tokens, local) = self.assembly_evaluation(
                        source_type,
                        &source_param,
                        &prefix,
                        assembly,
                        &mut locals,
                        &context,
                    )?;
                    evaluations.extend(tokens);
                    assembly_locals.push((local, assembly));
                }

                let collection = collection_fields
                    .iter()
                    .find(|(target_type, _)| *target_type == instance.target_type)
                    .map(|(_, field_name)| ident(field_name))
                    .expect("the instance's target collection is planned");
                needs_key |= profile.identity == IdentityKind::SignalKey;
                needs_provenance |= profile.provenance.is_some();
                assemblies.extend(Self::instance_assembly(
                    &instance,
                    &target,
                    profile,
                    &outputs,
                    &assembly_locals,
                    &collection,
                    &context,
                )?);
            }
        }

        let (subject_tokens, helpers) =
            self.subject_derivation(source_type, &source_param, subject_spec, &mut locals)?;
        let needs_location = needs_provenance
            || subject_spec
                .scope
                .iter()
                .any(|contributor| matches!(contributor, ScopeSpec::LocationTemplate { .. }));

        let key_binding = needs_key.then(|| {
            quote! {
                let key = ::nv_telemetry_model::SignalKey::builder()
                    .subject(subject.clone())
                    .build()?;
            }
        });
        let location_param = if needs_location {
            ident("location")
        } else {
            ident("_location")
        };
        let source_ty = source_type_tokens(&source_namespace, source_type);

        let parts_doc = docs(&format!(
            "What one `{source_type}` document projected to. The provider\n\
             assembles batches from these; identity failure leaves every\n\
             collection empty while the issues still name each fault."
        ));
        let fn_doc = docs(&format!(
            "Projects one `{source_type}` document, located at the *requested*\n\
             URI.\n\
             \n\
             # Errors\n\
             \n\
             `Err` is the residual tier only — a builder refusing inputs this\n\
             function already triaged is a projection bug, and a bug is an\n\
             operational fact for the status stream rather than device data.\n\
             Everything a device can cause comes back as issues inside the\n\
             parts."
        ));

        Ok(quote! {
            #parts_doc
            #[derive(Debug)]
            pub(crate) struct #parts_name {
                #(#parts_fields)*
                /// The source fields that projected to nothing, and why.
                pub(crate) issues: Vec<::nv_telemetry_source::ProjectionIssue>,
            }

            #fn_doc
            pub(crate) fn #fn_name(
                #source_param: &#source_ty,
                #location_param: &str,
            ) -> Result<#parts_name, ::nv_telemetry_model::Invalid> {
                let mut issues = Vec::new();

                #evaluations

                #subject_tokens
                let Some(subject) = subject else {
                    return Ok(#parts_name {
                        #(#parts_idents: Vec::new(),)*
                        issues,
                    });
                };

                #key_binding
                #(let mut #parts_idents = Vec::new();)*

                #assemblies

                Ok(#parts_name {
                    #(#parts_idents,)*
                    issues,
                })
            }

            #helpers
        })
    }

    /// One field mapping's evaluation: a local holding `Option` of the
    /// converted value, issues pushed for what the device answered
    /// unusably.
    #[allow(clippy::too_many_arguments)]
    fn field_evaluation(
        &self,
        source_type: &str,
        source_param: &Ident,
        prefix: &str,
        target: &MessageDescriptor,
        field: &FieldSpec,
        locals: &mut BTreeSet<String>,
        context: &str,
    ) -> Result<(TokenStream, PendingOutput), String> {
        let steps = self
            .index
            .steps(source_type, &field.source_path)
            .map_err(|error| format!("{context}: {error}"))?;
        let issue_path = format!("{source_type}.{}", field.source_path);
        let landing = target_landing(target, &field.target_field, context)?;
        let conversion = self.conversion(
            steps.last().expect("a resolved path has a leaf"),
            &landing,
            &field.value_map,
            &field.known_values,
            context,
        )?;

        let local = claim_local(
            locals,
            &format!("{prefix}_{}", landing.setter_path.join("_").to_snake_case()),
            context,
        )?;
        let evaluation = leaf_match(
            source_param,
            &steps,
            &conversion_tokens(&conversion, &issue_path),
            field.null_policy,
            field.required,
            &issue_path,
            context,
        )?;

        // A contract-required landing gates output exactly as an anchor
        // does: ordinary absence must suppress the item, never reach the
        // builder. `required` on the manifest additionally reports it.
        let root = landing
            .setter_path
            .first()
            .expect("a checked target landing has a root field");
        let root_field = target
            .get_field_by_name(root)
            .expect("a checked target root remains present");
        let target_required = self
            .vocabulary
            .field_invariant(&root_field)
            .is_some_and(|invariant| invariant.required);
        let gates = if field.anchor || field.required || target_required {
            vec![quote! { #local.is_some() }]
        } else {
            Vec::new()
        };

        let tokens = quote! { let #local = #evaluation; };
        Ok((
            tokens,
            PendingOutput {
                local,
                setter_path: landing.setter_path,
                gates,
                issue_path,
            },
        ))
    }

    /// One map assembly's evaluation: a `Vec` of entries, one per usable
    /// source, in declaration order.
    fn assembly_evaluation(
        &self,
        source_type: &str,
        source_param: &Ident,
        prefix: &str,
        assembly: &AssemblySpec,
        locals: &mut BTreeSet<String>,
        context: &str,
    ) -> Result<(TokenStream, Ident), String> {
        let local = claim_local(
            locals,
            &format!("{prefix}_{}_entries", assembly.target_field.to_snake_case()),
            context,
        )?;
        let mut pushes = TokenStream::new();
        for entry in &assembly.entries {
            pushes.extend(self.entry_evaluation(
                source_type,
                source_param,
                &local,
                entry,
                context,
            )?);
        }
        let tokens = quote! {
            let mut #local: Vec<(String, ::nv_telemetry_model::Value)> = Vec::new();
            #pushes
        };
        Ok((tokens, local))
    }

    /// One assembly entry: evaluated exactly as a field mapping into the
    /// value vocabulary, pushed with its key when usable.
    fn entry_evaluation(
        &self,
        source_type: &str,
        source_param: &Ident,
        entries_local: &Ident,
        entry: &EntrySpec,
        context: &str,
    ) -> Result<TokenStream, String> {
        let steps = self
            .index
            .steps(source_type, &entry.source_path)
            .map_err(|error| format!("{context}: {error}"))?;
        let issue_path = format!("{source_type}.{}", entry.source_path);
        let leaf = steps.last().expect("a resolved path has a leaf");

        // Entries build the contract's `Value`; the lint pinned the
        // assembly target to it, so the landing is fixed by the source.
        let arm = match Self::source_class(leaf, context)? {
            SourceClass::Decimal => "double_value",
            SourceClass::Text | SourceClass::Enum => "string_value",
        };
        let field = self
            .contract
            .get_message_by_name(VALUE)
            .and_then(|message| message.get_field_by_name(arm))
            .ok_or_else(|| format!("{context}: contract `{VALUE}.{arm}` vanished"))?;
        let landing = Landing {
            setter_path: Vec::new(),
            kind: LandingKind::VocabArm {
                vocabulary: VALUE.to_owned(),
                arm: arm.to_owned(),
                field,
            },
        };
        let conversion = self.conversion(leaf, &landing, &entry.value_map, &[], context)?;
        let evaluation = leaf_match(
            source_param,
            &steps,
            &conversion_tokens(&conversion, &issue_path),
            entry.null_policy,
            false,
            &issue_path,
            context,
        )?;
        let key = &entry.key;
        Ok(quote! {
            if let Some(value) = #evaluation {
                #entries_local.push((#key.to_owned(), value));
            }
        })
    }

    /// Selects the conversion of one present leaf value; a pairing this
    /// compiler cannot honor is an error naming the declaration.
    fn conversion(
        &self,
        leaf: &Step,
        landing: &Landing,
        value_map: &[(String, String)],
        known_values: &[String],
        context: &str,
    ) -> Result<Conversion, String> {
        let class = Self::source_class(leaf, context)?;
        match (&landing.kind, class) {
            (
                LandingKind::VocabArm {
                    vocabulary, arm, ..
                },
                SourceClass::Decimal,
            ) if arm == "double_value" => Ok(Conversion::Decimal {
                constructor: vocabulary.clone(),
            }),
            (kind, SourceClass::Text) => Ok(Conversion::Text {
                check: self.text_check(kind.field()),
                destination: text_destination(kind, leaf, context)?,
            }),
            (kind, SourceClass::Enum) => {
                let destination = text_destination(kind, leaf, context)?;
                let members = leaf.enum_members.as_ref().ok_or_else(|| {
                    format!(
                        "{context}: `{}.{}` lost its members",
                        leaf.namespace, leaf.name
                    )
                })?;
                let mut rows = value_map.to_vec();
                for known in known_values {
                    if !rows.iter().any(|(from, _)| from == known) {
                        rows.push((known.clone(), known.clone()));
                    }
                }
                if rows.is_empty() {
                    rows.extend(
                        members
                            .iter()
                            .map(|member| (member.clone(), member.clone())),
                    );
                }
                Ok(Conversion::Enum {
                    namespace: leaf.namespace.clone(),
                    name: leaf.name.clone(),
                    rows,
                    destination,
                })
            }
            (kind, SourceClass::Decimal) => Err(format!(
                "{context}: no conversion from `{}.{}` into `{}`",
                leaf.namespace,
                leaf.name,
                kind.description()
            )),
        }
    }

    fn source_class(leaf: &Step, context: &str) -> Result<SourceClass, String> {
        let primitive = match leaf.class {
            TypeClass::SimpleType => leaf.name.as_str(),
            TypeClass::TypeDefinition => leaf.underlying.as_deref().ok_or_else(|| {
                format!(
                    "{context}: `{}.{}` has no underlying primitive",
                    leaf.namespace, leaf.name
                )
            })?,
            TypeClass::EnumType => return Ok(SourceClass::Enum),
            TypeClass::ComplexType => {
                return Err(format!(
                    "{context}: `{}` is a complex type; a mapping extracts one scalar",
                    leaf.property
                ));
            }
        };
        if leaf.is_text() {
            return Ok(SourceClass::Text);
        }
        match primitive {
            "Decimal" => Ok(SourceClass::Decimal),
            other => Err(format!(
                "{context}: no conversion from `Edm.{other}` is implemented; \
                 extending the compiler is required"
            )),
        }
    }

    /// The target leaf's own bounds, from the contract vocabulary.
    fn text_check(&self, field: &FieldDescriptor) -> TextCheck {
        let invariant = self.vocabulary.field_invariant(field);
        TextCheck {
            field_name: field.name().to_owned(),
            non_empty: invariant
                .as_ref()
                .is_some_and(|invariant| invariant.non_empty),
            max_len_constant: invariant
                .and_then(|invariant| invariant.max_len)
                .map(|_| format!("{}_MAX_LEN", constant_stem(field.full_name()))),
        }
    }

    /// One projection instance's assembly: gate on anchors, contract-required
    /// landings, and assemblies; build the target; push it into its parts
    /// collection.
    #[allow(clippy::too_many_arguments)]
    fn instance_assembly(
        instance: &ProjectionSpec,
        target: &MessageDescriptor,
        profile: TargetProfile,
        outputs: &[Output],
        assemblies: &[(Ident, &AssemblySpec)],
        collection: &Ident,
        context: &str,
    ) -> Result<TokenStream, String> {
        let target_model = ident(&short_name(target.full_name()));
        let identity_setter = ident(profile.identity_field);
        let identity = match profile.identity {
            IdentityKind::SignalKey => quote! { builder = builder.#identity_setter(key.clone()); },
            IdentityKind::Subject => {
                quote! { builder = builder.#identity_setter(subject.clone()); }
            }
        };
        let provenance = profile.provenance.map(|field| {
            let setter = ident(field);
            quote! { builder = builder.#setter(crate::uri::canonical(location)); }
        });
        let output_setters = outputs.iter().map(|output| {
            let local = &output.local;
            let setter = ident(&output.setter);
            quote! {
                if let Some(value) = #local {
                    builder = builder.#setter(value);
                }
            }
        });
        let constants = instance
            .constants
            .iter()
            .map(|constant| {
                let landing = target_landing(target, &constant.target_field, context)?;
                let [setter] = landing.setter_path.as_slice() else {
                    return Err(format!(
                        "{context}: constant for `{}` lands inside `{}`; \
                         group-building constants is not implemented",
                        constant.target_field, landing.setter_path[0]
                    ));
                };
                let setter = ident(setter);
                let literal = &constant.value;
                let value = match constant_destination(&landing.kind) {
                    Some(TextDestination::Plain) => quote! { #literal },
                    Some(TextDestination::Vocabulary(vocabulary)) => {
                        let constructor = vocab_type(&vocabulary);
                        quote! { #constructor::string(#literal)? }
                    }
                    None => {
                        return Err(format!(
                            "{context}: a constant cannot populate `{}`",
                            landing.kind.description()
                        ));
                    }
                };
                Ok(quote! { builder = builder.#setter(#value); })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let assembly_setters = assemblies.iter().map(|(local, assembly)| {
            let setter = ident(&assembly.target_field);
            let lands_on_map = target.get_field_by_name(&assembly.target_field).is_some_and(
                |field| matches!(field.kind(), Kind::Message(message) if message.full_name() == VALUE_MAP),
            );
            if lands_on_map {
                quote! { builder = builder.#setter(#local.into_iter().collect()); }
            } else {
                quote! {
                    builder = builder.#setter(::nv_telemetry_model::Value::map(#local)?);
                }
            }
        });

        let body = quote! {
            let mut builder = ::nv_telemetry_model::#target_model::builder();
            #identity
            #provenance
            #(#output_setters)*
            #(#constants)*
            #(#assembly_setters)*
            #collection.push(builder.build()?);
        };
        let conditions = outputs
            .iter()
            .flat_map(|output| output.gates.iter().cloned())
            .chain(assemblies.iter().map(|(local, _)| {
                quote! { !#local.is_empty() }
            }))
            .collect::<Vec<_>>();
        Ok(if conditions.is_empty() {
            body
        } else {
            quote! {
                if #(#conditions)&&* {
                    #body
                }
            }
        })
    }

    /// The subject derivation: every contributor and the id evaluate into
    /// locals first — so one report carries every identity fault — then one
    /// match builds the subject, with builder blame mapped to what fed it.
    #[allow(clippy::too_many_lines)]
    fn subject_derivation(
        &self,
        source_type: &str,
        source_param: &Ident,
        subject: &SubjectSpec,
        locals: &mut BTreeSet<String>,
    ) -> Result<(TokenStream, TokenStream), String> {
        let manifest = self.manifest.relative_path();
        let context = format!("{manifest}: subject");
        let subject_message = self
            .contract
            .get_message_by_name(SUBJECT)
            .ok_or_else(|| format!("{context}: contract Subject vanished"))?;
        let id_field = subject_message
            .get_field_by_name("id")
            .ok_or_else(|| format!("{context}: contract Subject.id vanished"))?;
        let scope_field = subject_message
            .get_field_by_name("scope")
            .ok_or_else(|| format!("{context}: contract Subject.scope vanished"))?;

        // The id: a required non-nullable scalar reads directly; anything
        // optional needs triage no manifest has asked for yet.
        let id_steps = self
            .index
            .steps(source_type, &subject.id_path)
            .map_err(|error| format!("{context}: {error}"))?;
        let id_leaf = id_steps.last().expect("a resolved path has a leaf");
        if id_steps.len() != 1 || id_leaf.shape() != Shape::Bare {
            return Err(format!(
                "{context}: id path `{}` is not a required scalar on the source type itself; \
                 optional identity is not implemented",
                subject.id_path
            ));
        }
        if !matches!(Self::source_class(id_leaf, &context)?, SourceClass::Text) {
            return Err(format!(
                "{context}: id path `{}` is not a string",
                subject.id_path
            ));
        }
        let id_local = claim_local(locals, "subject_id", &context)?;
        let id_blame = format!("{source_type}.{}", subject.id_path);
        let id_place = place(id_leaf.hops, &source_field_ident(&id_leaf.property));
        let id_conversion = checked_text(
            &self.text_check(&id_field),
            &id_blame,
            quote! { Some(value) },
        );
        let mut evaluations = quote! {
            let #id_local = {
                let value = #source_param #id_place.clone();
                #id_conversion
            };
        };

        let mut helpers = TokenStream::new();
        let mut scope_locals = Vec::new();
        let mut blames = Vec::new();
        for (position, contributor) in subject.scope.iter().enumerate() {
            let local = claim_local(locals, &format!("scope_{position}"), &context)?;
            match contributor {
                ScopeSpec::PayloadPath(path) => {
                    let steps = self
                        .index
                        .steps(source_type, path)
                        .map_err(|error| format!("{context}: {error}"))?;
                    let leaf = steps.last().expect("a resolved path has a leaf");
                    if !matches!(Self::source_class(leaf, &context)?, SourceClass::Text) {
                        return Err(format!("{context}: scope path `{path}` is not a string"));
                    }
                    let blame = format!("{source_type}.{path}");
                    let conversion = checked_text(
                        &self.text_check(&scope_field),
                        &blame,
                        quote! { Some(value) },
                    );
                    // Identity the payload fails to state is missing, not
                    // guessable; the projection reports and emits nothing.
                    let evaluation =
                        leaf_match(source_param, &steps, &conversion, 0, true, &blame, &context)?;
                    evaluations.extend(quote! { let #local = #evaluation; });
                    scope_locals.push(local);
                    blames.push(blame);
                }
                ScopeSpec::LocationTemplate { template, capture } => {
                    let pattern = LocationPattern::parse(template, capture).map_err(|detail| {
                        format!(
                            "{context}: location template survived checking but could not \
                             be typed: {detail}"
                        )
                    })?;
                    let helper = ident(&format!(
                        "{}_subject_scope_{position}",
                        source_type.to_snake_case()
                    ));
                    helpers.extend(location_helper(&helper, &pattern));
                    // The fact at fault is a URI capture, not a schema field;
                    // `@location.{capture}` names it without colliding with
                    // the schema's PascalCase properties. A requested URI is
                    // untrusted and may carry query secrets, so detail stays
                    // static rather than copying the request into diagnostics.
                    let locator = format!("@location.{capture}");
                    let detail = format!("requested location has no {capture} segment");
                    let conversion = checked_text(
                        &self.text_check(&scope_field),
                        &locator,
                        quote! { Some(value) },
                    );
                    evaluations.extend(quote! {
                        let #local = match #helper(location) {
                            Some(value) => #conversion,
                            None => {
                                issues.push(::nv_telemetry_source::ProjectionIssue::invalid(
                                    #locator,
                                    #detail,
                                ));
                                None
                            }
                        };
                    });
                    scope_locals.push(local);
                    blames.push(locator);
                }
                ScopeSpec::PathKey(_) | ScopeSpec::Unset => {
                    return Err(format!(
                        "{context}: a scope contributor survived checking that the \
                         compiler cannot honor"
                    ));
                }
            }
        }

        let kind = &subject.kind;
        // Without scope contributors every builder fault blames the id;
        // an `if` with identical arms would not survive the generated
        // tree's own lints.
        let blame = if blames.is_empty() {
            quote! { let path = #id_blame; }
        } else {
            let scope_blame = blame_chain(&blames, &id_blame);
            quote! {
                let path = if error.path().starts_with("id") {
                    #id_blame
                } else {
                    #scope_blame
                };
            }
        };
        let derivation = quote! {
            match (#id_local, #(#scope_locals,)*) {
                (Some(#id_local), #(Some(#scope_locals),)*) => {
                    match ::nv_telemetry_model::Subject::builder()
                        .kind(#kind)
                        .scope(vec![#(#scope_locals.to_owned()),*])
                        .id(#id_local)
                        .build()
                    {
                        Ok(subject) => Some(subject),
                        Err(error) => {
                            #blame
                            issues.push(::nv_telemetry_source::ProjectionIssue::invalid(
                                path,
                                error.to_string(),
                            ));
                            None
                        }
                    }
                }
                _ => None,
            }
        };
        Ok((
            quote! {
                #evaluations
                let subject = #derivation;
            },
            helpers,
        ))
    }
}

/// Builds sub-messages for outputs whose setter paths share a head segment —
/// `range.min` and `range.max` become one `range` — inline in the evaluation
/// phase, so the build's issue lands in declaration order. Recurses for
/// deeper nesting; the returned outputs all carry single-segment setters.
fn build_groups(
    target: &MessageDescriptor,
    prefix: &str,
    pending: Vec<PendingOutput>,
    evaluations: &mut TokenStream,
    locals: &mut BTreeSet<String>,
    context: &str,
) -> Result<Vec<Output>, String> {
    let mut heads = Vec::new();
    for output in &pending {
        if output.setter_path.len() > 1 && !heads.contains(&output.setter_path[0]) {
            heads.push(output.setter_path[0].clone());
        }
    }
    if heads.is_empty() {
        return Ok(pending
            .into_iter()
            .map(PendingOutput::into_output)
            .collect());
    }

    let mut flat = Vec::new();
    let mut grouped: BTreeMap<String, Vec<PendingOutput>> = BTreeMap::new();
    for output in pending {
        if output.setter_path.len() > 1 {
            grouped
                .entry(output.setter_path[0].clone())
                .or_default()
                .push(output);
        } else {
            flat.push(output.into_output());
        }
    }

    // `heads` keeps declaration order; the map only buckets.
    for head in heads {
        let mut members = grouped.remove(&head).expect("the head was collected");
        let field = target
            .get_field_by_name(&head)
            .ok_or_else(|| format!("{context}: target field `{head}` vanished after checking"))?;
        let Kind::Message(message) = field.kind() else {
            return Err(format!(
                "{context}: `{head}` is not a message; nothing can be built within it"
            ));
        };
        for member in &mut members {
            member.setter_path.remove(0);
        }
        let sub_prefix = format!("{prefix}_{}", head.to_snake_case());
        let members = build_groups(&message, &sub_prefix, members, evaluations, locals, context)?;

        let group_local = claim_local(locals, &sub_prefix, context)?;
        let member_locals: Vec<&Ident> = members.iter().map(|member| &member.local).collect();
        let setters: Vec<Ident> = members.iter().map(|member| ident(&member.setter)).collect();
        let issue_path = members[0].issue_path.clone();
        let blame = group_blame_chain(&members, &issue_path);

        // The group build consumes the member locals, so any gates they
        // carry are hoisted into a flag first; the instance's condition
        // reads the flag, and an absent anchor keeps suppressing output
        // from inside a group.
        let member_gates: Vec<TokenStream> = members
            .iter()
            .flat_map(|member| member.gates.iter().cloned())
            .collect();
        let gates = if member_gates.is_empty() {
            Vec::new()
        } else {
            let hoisted = claim_local(locals, &format!("{sub_prefix}_gate"), context)?;
            evaluations.extend(quote! {
                let #hoisted = #(#member_gates)&&*;
            });
            vec![quote! { #hoisted }]
        };

        let model_type = ident(&short_name(message.full_name()));
        evaluations.extend(quote! {
            let #group_local = if #(#member_locals.is_some())||* {
                let mut builder = ::nv_telemetry_model::#model_type::builder();
                #(if let Some(value) = #member_locals {
                    builder = builder.#setters(value);
                })*
                match builder.build() {
                    Ok(value) => Some(value),
                    Err(error) => {
                        let path = #blame;
                        issues.push(::nv_telemetry_source::ProjectionIssue::invalid(
                            path,
                            error.to_string(),
                        ));
                        None
                    }
                }
            } else {
                None
            };
        });
        flat.push(Output {
            local: group_local,
            setter: head,
            gates,
            issue_path,
        });
    }
    Ok(flat)
}

/// A field mapping between evaluation and grouping: the setter path still
/// carries every segment.
struct PendingOutput {
    local: Ident,
    setter_path: Vec<String>,
    gates: Vec<TokenStream>,
    issue_path: String,
}

impl PendingOutput {
    fn into_output(self) -> Output {
        Output {
            local: self.local,
            setter: self.setter_path[0].clone(),
            gates: self.gates,
            issue_path: self.issue_path,
        }
    }
}

/// Renders the selected conversion as an expression over a present `value`.
fn conversion_tokens(conversion: &Conversion, issue_path: &str) -> TokenStream {
    match conversion {
        Conversion::Decimal { constructor } => {
            let constructor = vocab_type(constructor);
            quote! {
                match #constructor::double(value) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        issues.push(::nv_telemetry_source::ProjectionIssue::invalid(
                            #issue_path,
                            error.to_string(),
                        ));
                        None
                    }
                }
            }
        }
        Conversion::Text { check, destination } => {
            let value = quote! { value };
            let accept = text_accept(destination, &value);
            checked_text(check, issue_path, accept)
        }
        Conversion::Enum {
            namespace,
            name,
            rows,
            destination,
        } => {
            let enum_type = source_type_tokens(namespace, name);
            let arms = rows.iter().map(|(from, to)| {
                let variant = escaped_ident(&casemungler::to_camel(from));
                let value = quote! { #to };
                let output = text_accept(destination, &value);
                quote! { #enum_type::#variant => #output, }
            });
            quote! {
                match value {
                    #(#arms)*
                    _ => {
                        issues.push(::nv_telemetry_source::ProjectionIssue::invalid(
                            #issue_path,
                            "outside the known value set",
                        ));
                        None
                    }
                }
            }
        }
    }
}

fn text_accept(destination: &TextDestination, value: &TokenStream) -> TokenStream {
    match destination {
        TextDestination::Plain => quote! { Some(#value) },
        TextDestination::Vocabulary(vocabulary) => {
            let constructor = vocab_type(vocabulary);
            quote! { Some(#constructor::string(#value)?) }
        }
    }
}

/// Verbatim device text, bounded by the target's own invariants: the checks
/// mirror the model's violations — same conditions, same spelling — so the
/// issue reads exactly as the builder would refuse.
fn checked_text(check: &TextCheck, issue_path: &str, accept: TokenStream) -> TokenStream {
    let mut arms = TokenStream::new();
    if check.non_empty {
        let detail = format!("`{}`: present but empty", check.field_name);
        arms.extend(quote! {
            if value.is_empty() {
                issues.push(::nv_telemetry_source::ProjectionIssue::invalid(
                    #issue_path,
                    #detail,
                ));
                None
            } else
        });
    }
    if let Some(constant) = &check.max_len_constant {
        let constant = ident(constant);
        let detail = format!(
            "`{}`: {{}} bytes long, over the schema's bound of {{}}",
            check.field_name
        );
        arms.extend(quote! {
            if value.len() > ::nv_telemetry_model::limits::#constant as usize {
                issues.push(::nv_telemetry_source::ProjectionIssue::invalid(
                    #issue_path,
                    format!(#detail, value.len(), ::nv_telemetry_model::limits::#constant),
                ));
                None
            } else
        });
    }
    if arms.is_empty() {
        accept
    } else {
        quote! { #arms { #accept } }
    }
}

fn location_helper(name: &Ident, pattern: &LocationPattern) -> TokenStream {
    let checks = pattern.segments().iter().map(|segment| match segment {
        LocationSegment::Capture => quote! {
            let captured = segments.next().filter(|segment| !segment.is_empty())?;
        },
        LocationSegment::Wildcard => quote! {
            let _ = segments.next().filter(|segment| !segment.is_empty())?;
        },
        LocationSegment::Literal(segment) => quote! {
            if segments.next()? != #segment {
                return None;
            }
        },
    });
    let template = pattern.template();
    let capture = pattern.capture();
    let doc = docs(&format!(
        "Matches `{template}`, yielding `{{{capture}}}`: the subject's\n\
         scope comes from the requested location, never from the\n\
         payload's own claim about itself."
    ));
    quote! {
        #doc
        fn #name(location: &str) -> Option<&str> {
            let mut segments = crate::uri::canonical(location).strip_prefix('/')?.split('/');
            #(#checks)*
            if segments.next().is_some() {
                return None;
            }
            Some(captured)
        }
    }
}

/// The evaluation of one leaf: source access composed with presence shape,
/// then the checked conversion over a present `value`.
///
/// The composition supports the shapes the shipped schemas exercise: any
/// chain of presence-only segments, and a leaf of any shape — including a
/// required-nullable leaf read directly off the source type, whose `None`
/// can only mean explicit null. A nullable *intermediate* segment is
/// rejected by the lint (`nullable intermediate segments`), and a
/// required-nullable leaf below an optional prefix would conflate absence
/// with null, so both are errors here rather than silently collapsed reads.
fn leaf_match(
    source_param: &Ident,
    steps: &[Step],
    conversion: &TokenStream,
    null_policy: i32,
    required: bool,
    issue_path: &str,
    context: &str,
) -> Result<TokenStream, String> {
    let (leaf, prefix_steps) = steps.split_last().expect("a resolved path has a leaf");

    let mut expression = quote! { #source_param };
    let mut optional = false;
    for step in prefix_steps {
        let step_place = place(step.hops, &source_field_ident(&step.property));
        expression = match (optional, step.shape()) {
            (_, Shape::Nullable | Shape::RequiredNullable) => {
                return Err(format!(
                    "{context}: `{issue_path}` reads through a nullable segment \
                     `{}`, which survived checking; extending the compiler is required",
                    step.property
                ));
            }
            (false, Shape::Bare) => quote! { #expression #step_place },
            (false, Shape::Optional) => {
                optional = true;
                quote! { #expression #step_place.as_ref() }
            }
            (true, Shape::Bare) => quote! { #expression.map(|value| &value #step_place) },
            (true, Shape::Optional) => {
                quote! { #expression.and_then(|value| value #step_place.as_ref()) }
            }
        };
    }

    let leaf_place = place(leaf.hops, &source_field_ident(&leaf.property));
    let extract = if leaf.is_text() {
        quote! { #leaf_place.clone() }
    } else {
        quote! { #leaf_place }
    };
    let (leaf_expr, effective) = match (optional, leaf.shape()) {
        (false, shape) => (quote! { #expression #extract }, shape),
        (true, Shape::Bare) => (
            quote! { #expression.map(|value| value #extract) },
            Shape::Optional,
        ),
        (true, Shape::RequiredNullable) => {
            return Err(format!(
                "{context}: `{issue_path}` is a required-nullable leaf below an \
                 optional segment; absence and explicit null would conflate — \
                 extending the compiler is required"
            ));
        }
        (true, shape) => (
            quote! { #expression.and_then(|value| value #extract) },
            shape,
        ),
    };

    let absent = if required {
        quote! {
            {
                issues.push(::nv_telemetry_source::ProjectionIssue::missing(#issue_path));
                None
            }
        }
    } else {
        quote! { None }
    };
    let null_invalid = quote! {
        {
            issues.push(::nv_telemetry_source::ProjectionIssue::invalid(
                #issue_path,
                "explicitly null",
            ));
            None
        }
    };
    Ok(match effective {
        Shape::Bare => quote! {
            {
                let value = #leaf_expr;
                #conversion
            }
        },
        // A required-nullable leaf is `Option<T>` where `None` can only be
        // an explicit JSON null the device sent.
        Shape::RequiredNullable if null_policy == NULL_INVALID => quote! {
            match #leaf_expr {
                Some(value) => #conversion,
                None => #null_invalid,
            }
        },
        Shape::Optional | Shape::RequiredNullable => quote! {
            match #leaf_expr {
                Some(value) => #conversion,
                None => #absent,
            }
        },
        Shape::Nullable if null_policy == NULL_INVALID => quote! {
            match #leaf_expr {
                Some(Some(value)) => #conversion,
                Some(None) => #null_invalid,
                None => #absent,
            }
        },
        Shape::Nullable => quote! {
            match #leaf_expr {
                Some(Some(value)) => #conversion,
                _ => #absent,
            }
        },
    })
}

/// The builder-error blame chain over identity contributors: `scope[i]`
/// faults map to what fed each contributor, anything else to the first
/// contributor's blame.
fn blame_chain(blames: &[String], fallback: &str) -> TokenStream {
    match blames {
        [] => quote! { #fallback },
        [only] => quote! { #only },
        many => {
            let arms = many.iter().enumerate().map(|(position, blame)| {
                let prefix = format!("scope[{position}]");
                quote! {
                    if error.path().starts_with(#prefix) {
                        #blame
                    } else
                }
            });
            let first = &many[0];
            quote! { #(#arms)* { #first } }
        }
    }
}

/// Maps a grouped builder's target-relative error path back to the source
/// read that populated that setter. The fallback is explicit for genuinely
/// cross-group rules whose path names no member.
fn group_blame_chain(members: &[Output], fallback: &str) -> TokenStream {
    let arms = members.iter().map(|member| {
        let setter = &member.setter;
        let blame = &member.issue_path;
        quote! {
            if error.path().starts_with(#setter) {
                #blame
            } else
        }
    });
    quote! { #(#arms)* { #fallback } }
}

/// Where a target path lands, with any value-vocabulary arm consumed.
fn target_landing(
    target: &MessageDescriptor,
    target_field: &str,
    context: &str,
) -> Result<Landing, String> {
    let leaf = resolve_target(target, target_field).ok_or_else(|| {
        format!("{context}: target field `{target_field}` vanished after checking")
    })?;
    let segments = target_field.split('.').collect::<Vec<_>>();
    let parent = leaf.parent_message();
    if matches!(parent.full_name(), NUMERIC_VALUE | VALUE) {
        if segments.len() < 2 {
            return Err(format!(
                "{context}: `{target_field}` names the value vocabulary itself"
            ));
        }
        return Ok(Landing {
            setter_path: segments[..segments.len() - 1]
                .iter()
                .map(|segment| (*segment).to_owned())
                .collect(),
            kind: LandingKind::VocabArm {
                vocabulary: parent.full_name().to_owned(),
                arm: segments[segments.len() - 1].to_owned(),
                field: leaf,
            },
        });
    }
    match leaf.kind() {
        Kind::String => Ok(Landing {
            setter_path: segments
                .iter()
                .map(|segment| (*segment).to_owned())
                .collect(),
            kind: LandingKind::PlainString(leaf),
        }),
        other => Err(format!(
            "{context}: no conversion lands on `{target_field}` ({other:?}); \
             extending the compiler is required"
        )),
    }
}

/// Text landings only: a plain string field, or the string arm of the value
/// vocabulary. `None` is a landing text cannot populate.
fn constant_destination(kind: &LandingKind) -> Option<TextDestination> {
    match kind {
        LandingKind::PlainString(_) => Some(TextDestination::Plain),
        LandingKind::VocabArm {
            vocabulary, arm, ..
        } if arm == "string_value" => Some(TextDestination::Vocabulary(vocabulary.clone())),
        LandingKind::VocabArm { .. } => None,
    }
}

fn text_destination(
    kind: &LandingKind,
    leaf: &Step,
    context: &str,
) -> Result<TextDestination, String> {
    constant_destination(kind).ok_or_else(|| {
        format!(
            "{context}: no conversion from `{}.{}` into `{}`",
            leaf.namespace,
            leaf.name,
            kind.description()
        )
    })
}

fn source_type_tokens(namespace: &str, name: &str) -> TokenStream {
    let module = escaped_ident(&casemungler::to_snake(namespace));
    let name = escaped_ident(name);
    quote! { ::nv_redfish::schema::#module::#name }
}

fn place(hops: usize, field: &Ident) -> TokenStream {
    let bases = std::iter::repeat_n(quote! { .base }, hops).collect::<TokenStream>();
    quote! { #bases.#field }
}

fn source_field_ident(property: &str) -> Ident {
    escaped_ident(&casemungler::to_snake(property))
}

fn vocab_type(full_name: &str) -> TokenStream {
    let short = ident(&short_name(full_name));
    quote! { ::nv_telemetry_model::#short }
}

fn docs(text: &str) -> TokenStream {
    crate::wrapper::names::docs(&text.split('\n').map(str::trim_end).collect::<Vec<_>>())
}

/// Claims a derived local name, refusing a duplicate or a name Rust cannot
/// spell — both come from manifest-author strings, and either would
/// otherwise surface as a silently shadowing `let` or a panic instead of an
/// error naming the declaration.
fn claim_local(locals: &mut BTreeSet<String>, name: &str, context: &str) -> Result<Ident, String> {
    require_ident(name, context, || {
        format!(
            "derives the local name `{name}`, which is not a usable Rust \
             identifier; rename the projection"
        )
    })?;
    if !locals.insert(name.to_owned()) {
        return Err(format!(
            "{context}: derives the local name `{name}` twice; rename a \
             projection so generated locals stay distinct"
        ));
    }
    Ok(ident(name))
}

fn require_ident(name: &str, subject: &str, detail: impl FnOnce() -> String) -> Result<(), String> {
    if syn::parse_str::<Ident>(name).is_err() {
        return Err(format!("{subject}: {}", detail()));
    }
    Ok(())
}

/// An identifier as the source generator escapes it: path keywords gain an
/// underscore, other keywords become raw identifiers.
fn escaped_ident(name: &str) -> Ident {
    if matches!(name, "crate" | "self" | "super" | "Self") {
        return format_ident!("{name}_");
    }
    syn::parse_str::<Ident>(name)
        .unwrap_or_else(|_| Ident::new_raw(name, proc_macro2::Span::call_site()))
}
