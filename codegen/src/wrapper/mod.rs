// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Validated model generation.
//!
//! The validated model is owned types: conversion consumes the wire message
//! field by field, and encoding rebuilds it. The alternative — newtypes over
//! the wire structs — cannot reshape, and reshaping is where the model's
//! value is: a required oneof becomes an enum rather than an `Option` of one,
//! a `required` string becomes `String` rather than `Option<String>`, and
//! absence is unrepresentable after conversion instead of checked at each
//! use.
//!
//! Construction is builder-only. A `new(required...)` constructor reads
//! better until the first message whose rules constrain only optional fields
//! — a `ValueRange` needs at least one of two optional bounds, so its `new()`
//! would always fail and the type would have no path to a valid value. A
//! builder with infallible setters and a fallible `build` handles every
//! message the same way, and `build` runs exactly the checks decoding runs,
//! so a built message and a decoded one cannot disagree about validity.
//!
//! The value vocabulary — `Value`, `NumericValue`, `Timestamp` — is
//! deliberately not generated. Its validated shapes are nothing like its wire
//! shapes (a recursive enum, a sorted map), and the [`limits`] module exists
//! so that hand-written code enforces the schema's bounds rather than a copy
//! of them.
//!
//! Cross-field rules the annotation vocabulary cannot express live in the
//! hand-written `rules` module, one function per generated message, and every
//! generated `check` calls its function. The registry is exhaustive by
//! compiler force: a new message does not compile until someone answers the
//! question "does it have cross-field rules?", in a file whose diff shows the
//! answer.
//!
//! Emission builds `quote!` token streams, parses them into a file with
//! `syn`, and renders with `prettyplease`. Tokens rather than text because
//! Rust-emitting-Rust through `format!` doubles every brace and can fuse
//! adjacent tokens; the parse stays because `quote!` guarantees only lexical
//! well-formedness, and a template assembling tokens in a syntactically
//! impossible order must fail generation with a codegen error, not the model
//! crate's build. The file header is prepended verbatim: token streams have
//! no position for plain `//` comments, and the header's license and lint
//! rationale are exactly that. The `limits` module stays a text emitter — a
//! flat list of constants earns no tokens.
//!
//! Names the model cannot use as Rust identifiers — keyword field names, a
//! legal and common thing in protobuf — are rejected by the schema lint
//! before emission, so `format_ident!` here never meets one.

mod limits;
pub(crate) mod names;
mod plans;

use std::collections::BTreeSet;
use std::fmt::Write as _;

use proc_macro2::Ident;
use proc_macro2::Literal;
use proc_macro2::TokenStream;
use prost_reflect::DescriptorPool;
use prost_reflect::Kind;
use prost_reflect::MessageDescriptor;
use quote::format_ident;
use quote::quote;

pub use self::limits::limits;
use self::names::docs;
use self::names::ident;
use self::names::short_name;
use self::names::snake;
use self::names::variant_name;
use self::plans::plan_field;
use self::plans::plan_oneof;
use self::plans::Plan;
use crate::is_contract_package;
use crate::options::Vocabulary;

/// Messages whose validated forms are hand-written in `model/src/value.rs`.
const HAND_WRITTEN: &[&str] = &[
    "nv.telemetry.v1.Null",
    "nv.telemetry.v1.Timestamp",
    "nv.telemetry.v1.NumericValue",
    "nv.telemetry.v1.Value",
    "nv.telemetry.v1.Value.List",
    "nv.telemetry.v1.Value.Map",
    "nv.telemetry.v1.Value.Map.Entry",
];

/// Types the hand-written vocabulary provides. Referenced by their short
/// names — the generated module imports them from the crate root.
fn hand_written_type(full_name: &str) -> Option<&'static str> {
    match full_name {
        "nv.telemetry.v1.Timestamp" => Some("Timestamp"),
        "nv.telemetry.v1.NumericValue" => Some("NumericValue"),
        "nv.telemetry.v1.Value" => Some("Value"),
        _ => None,
    }
}

const MODEL_HEADER: &str = "\
// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from `nv.telemetry.v1` by `make codegen`. Do not edit.
//!
//! The validated model. Every type here upholds the schema's invariants for
//! as long as it exists: construction is a builder whose `build` validates,
//! decoding is `TryFrom` over the wire type running the same `check`, and the
//! fields are private so no path around either exists. Encoding rebuilds the
//! wire form, which is also where canonicalization shows: what comes out is
//! the validated representation, not the bytes that arrived.
//!
//! Absence is reshaped away where the schema says so. A `required` field is a
//! plain value, a required oneof is an enum, and an enum field cannot carry
//! the unspecified value — a value this build does not recognize decodes as
//! `Unrecognized`, because a newer producer naming something real is not an
//! error.

// Generated code holds the line on correctness lints; the pedantic group is
// style advice for humans and is exactly where a clippy release breaks a
// checked-in file that no one edited.
#![allow(clippy::pedantic, dead_code)]

use std::collections::BTreeMap;

use crate::invalid;
use crate::rules;
use crate::value;
";

/// Types the header imports from the crate root. One list renders the `use`
/// lines and seeds the claim registry, so an import the module depends on and
/// a name the registry protects cannot drift apart — they are one row. The
/// header itself carries only module imports, which no derived type name can
/// collide with.
const PRELUDE_TYPES: &[&str] = &["Invalid", "NumericValue", "Timestamp", "Value", "Violation"];

/// Names in scope without an import: the Rust prelude the generated code
/// spells unqualified, and the std collections the header and the uniqueness
/// fallback bring in. Claimed for the same reason the imports are.
const AMBIENT_NAMES: &[&str] = &["BTreeMap", "BTreeSet", "String", "Vec", "Option", "Result"];

/// Renders the validated model for every generated contract message.
///
/// # Errors
///
/// Returns a description naming the declaration if the contract contains a
/// shape this generator does not know how to reshape — which is a request to
/// extend the generator, not a schema error.
pub fn model(pool: &DescriptorPool, vocabulary: &Vocabulary) -> Result<String, String> {
    let mut items = TokenStream::new();

    let ordered = ord_needed(pool, vocabulary);
    let roots = root_messages(pool);

    // Every type name the module will contain, seeded with the names its
    // header already gives meaning. A derived name landing on one of these —
    // a oneof called `value` becomes `pub enum Value` beside the imported
    // `Value` — is parseable, so without this it fails in the model crate's
    // build, blamed on a checked-in file. The seeds are the same constants
    // the header renders from, so a new import is claimed by construction.
    let mut claimed: BTreeSet<String> = PRELUDE_TYPES
        .iter()
        .chain(AMBIENT_NAMES)
        .map(|&name| name.to_owned())
        .collect();

    let mut enums: Vec<_> = pool
        .all_enums()
        .filter(|declared| is_contract_package(declared.package_name()))
        .collect();
    enums.sort_by(|left, right| left.full_name().cmp(right.full_name()));
    for declared in enums {
        items.extend(enum_items(&declared, &mut claimed)?);
    }

    let mut messages: Vec<_> = pool
        .all_messages()
        .filter(|message| {
            is_contract_package(message.package_name())
                && !message.is_map_entry()
                && !HAND_WRITTEN.contains(&message.full_name())
        })
        .collect();
    messages.sort_by(|left, right| left.full_name().cmp(right.full_name()));

    for message in &messages {
        items.extend(message_items(
            message,
            vocabulary,
            ordered.contains(message.full_name()),
            roots.contains(message.full_name()),
            &mut claimed,
        )?);
    }

    // `quote!` guarantees lexical well-formedness only; this is what proves
    // the assembled tokens are a Rust file. A stream that does not parse is a
    // bug in this module, and it must be reported here — as a generation
    // failure naming the parse error — rather than downstream as a broken
    // checked-in file.
    let parsed = syn::parse2::<syn::File>(items).map_err(|error| {
        format!(
            "the model emitter produced invalid Rust: {error}; this is a \
             codegen template bug, not a schema problem"
        )
    })?;

    let body = prettyplease::unparse(&parsed);
    let mut header = String::from(MODEL_HEADER);
    for name in PRELUDE_TYPES {
        let _ = writeln!(header, "use crate::{name};");
    }
    header.push_str("\nuse super::limits;\nuse super::wire;\n");
    // The uniqueness fallback is the one consumer of BTreeSet, and today's
    // contract never takes it: every unique collection is unordered with its
    // keys leading the canonical order, so the adjacent scan wins everywhere.
    // Import on demand rather than carry an allow for an import that is
    // usually dead.
    if body.contains("BTreeSet") {
        header.push_str("use std::collections::BTreeSet;\n");
    }
    Ok(format!("{header}\n{body}"))
}

/// Generated messages needing `Ord` and `Hash`: every message type a
/// `unique_by` key resolves to, transitively, so uniqueness checks can hold
/// keys in a `BTreeSet` and consumers can use identities as map keys.
fn ord_needed(pool: &DescriptorPool, vocabulary: &Vocabulary) -> BTreeSet<String> {
    let mut needed = BTreeSet::new();
    for message in pool.all_messages() {
        if !is_contract_package(message.package_name()) {
            continue;
        }
        for field in message.fields() {
            let Some(invariant) = vocabulary.field_invariant(&field) else {
                continue;
            };
            if invariant.unique_by.is_empty() {
                continue;
            }
            let Kind::Message(element) = field.kind() else {
                continue;
            };
            for key in &invariant.unique_by {
                if let Some(member) = element.get_field_by_name(key) {
                    if let Kind::Message(key_type) = member.kind() {
                        collect_ord(&key_type, &mut needed);
                    }
                }
            }
        }
    }
    needed
}

fn collect_ord(message: &MessageDescriptor, needed: &mut BTreeSet<String>) {
    if !needed.insert(message.full_name().to_owned()) {
        return;
    }
    for field in message.fields() {
        if let Kind::Message(nested) = field.kind() {
            collect_ord(&nested, needed);
        }
    }
}

/// Messages nothing else in the contract holds: the boundary types, which get
/// `decode` and `encode_to_vec` because bytes are how they arrive and leave.
fn root_messages(pool: &DescriptorPool) -> BTreeSet<String> {
    let mut referenced = BTreeSet::new();
    for message in pool.all_messages() {
        if !is_contract_package(message.package_name()) {
            continue;
        }
        for field in message.fields() {
            if let Kind::Message(nested) = field.kind() {
                referenced.insert(nested.full_name().to_owned());
            }
        }
    }
    pool.all_messages()
        .filter(|message| {
            is_contract_package(message.package_name())
                && !message.is_map_entry()
                && !referenced.contains(message.full_name())
                && !HAND_WRITTEN.contains(&message.full_name())
        })
        .map(|message| message.full_name().to_owned())
        .collect()
}

/// Claims a derived type name, refusing one the module already uses.
fn claim(claimed: &mut BTreeSet<String>, name: &str, source: &str) -> Result<(), String> {
    if claimed.insert(name.to_owned()) {
        Ok(())
    } else {
        Err(format!(
            "{source} derives the type name `{name}`, which the generated \
             module already uses; rename it in the schema"
        ))
    }
}

fn enum_items(
    declared: &prost_reflect::EnumDescriptor,
    claimed: &mut BTreeSet<String>,
) -> Result<TokenStream, String> {
    let full = declared.full_name();
    let short = short_name(full);
    claim(claimed, &short, &format!("enum `{full}`"))?;
    let name = ident(&short);

    let doc = docs(&[
        format!("Validated form of `{full}`."),
        String::new(),
        "The unspecified value is unrepresentable: conversion rejects it, because".to_owned(),
        format!("every `{full}` field in the contract declares `reject_unspecified`. A value"),
        format!("newer than this build decodes as [`{short}::Unrecognized`] instead of"),
        "failing, so additive schema evolution does not break older consumers.".to_owned(),
    ]);
    let unrecognized_doc = docs(&[
        "A value newer than this build. Interpreting it is the consumer's",
        "decision; re-encoding preserves it.",
    ]);

    // Variant names are derived, so two schema names can reduce to one Rust
    // name — `X_A_B` and `X_A__B` both become `AB` — and the generator itself
    // claims `Unrecognized`. A duplicate variant is still parseable Rust, so
    // without this check it would fail in the model crate's build, attributed
    // to a checked-in file rather than to the schema name that caused it.
    let mut taken = BTreeSet::from(["Unrecognized".to_owned()]);
    let mut arms = Vec::new();
    for entry in declared.values() {
        if entry.number() == 0 {
            continue;
        }
        let arm = variant_name(&short, entry.name());
        // Deriving can produce something Rust cannot spell — stripping the
        // enum prefix from `FORM_FACTOR_2U` leaves a variant starting with a
        // digit — and `format_ident!` would panic on it, a generator crash
        // where a schema error is owed.
        if syn::parse_str::<Ident>(&arm).is_err() {
            return Err(format!(
                "enum `{full}` value `{}` derives the variant name `{arm}`, \
                 which is not a usable Rust identifier; rename the value in \
                 the schema",
                entry.name()
            ));
        }
        if !taken.insert(arm.clone()) {
            return Err(format!(
                "enum `{full}` value `{}` becomes the variant `{arm}`, which \
                 another value — or the generator's own `Unrecognized` arm — \
                 already uses; rename the value in the schema",
                entry.name()
            ));
        }
        arms.push((
            ident(&arm),
            Literal::i32_unsuffixed(entry.number()),
            entry.name().to_owned(),
        ));
    }

    let variants = arms.iter().map(|(arm, _, value_name)| {
        let doc = docs(&[format!("`{value_name}`.")]);
        quote! { #doc #arm, }
    });
    let decode_arms = arms.iter().map(|(arm, number, _)| {
        quote! { #number => Ok(Self::#arm), }
    });
    let encode_arms = arms.iter().map(|(arm, number, _)| {
        quote! { #name::#arm => #number, }
    });
    let check = enum_check(&name);

    Ok(quote! {
        #doc
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[non_exhaustive]
        pub enum #name {
            #(#variants)*
            #unrecognized_doc
            Unrecognized(i32),
        }

        impl TryFrom<i32> for #name {
            type Error = Violation;

            fn try_from(value: i32) -> Result<Self, Violation> {
                match value {
                    0 => Err(Violation::Unspecified),
                    #(#decode_arms)*
                    other => Ok(Self::Unrecognized(other)),
                }
            }
        }

        impl From<#name> for i32 {
            fn from(value: #name) -> Self {
                match value {
                    #(#encode_arms)*
                    #name::Unrecognized(other) => other,
                }
            }
        }

        impl crate::canonical::Canonical for #name {
            fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
                i32::from(*self).cmp(&i32::from(*other))
            }
        }

        #check
    })
}

/// The honesty check every enum-typed field runs on build and decode alike.
fn enum_check(name: &Ident) -> TokenStream {
    quote! {
        impl #name {
            /// Rejects an `Unrecognized` that aliases the unspecified value
            /// or a variant this build recognizes: its bytes would not
            /// round-trip — 0 is refused by decode outright, and an alias
            /// silently comes back as the recognized variant instead of
            /// itself.
            fn check(self) -> Result<(), Violation> {
                match self {
                    Self::Unrecognized(value) => match Self::try_from(value) {
                        Ok(Self::Unrecognized(_)) => Ok(()),
                        Ok(_) => Err(Violation::Rule(
                            "an unrecognized value must not alias a recognized one",
                        )),
                        Err(violation) => Err(violation),
                    },
                    _ => Ok(()),
                }
            }
        }
    }
}

// The message template in execution order — struct, impl, builder, TryFrom,
// From — and splitting it would scatter what is one shape.
#[allow(clippy::too_many_lines)]
fn message_items(
    message: &MessageDescriptor,
    vocabulary: &Vocabulary,
    ordered: bool,
    root: bool,
    claimed: &mut BTreeSet<String>,
) -> Result<TokenStream, String> {
    let full = message.full_name();
    let short = short_name(full);
    claim(claimed, &short, &format!("message `{full}`"))?;
    claim(
        claimed,
        &format!("{short}Builder"),
        &format!("message `{full}`'s builder"),
    )?;
    let name = ident(&short);
    let builder = format_ident!("{short}Builder");
    // `Match` is a legal, styled message name whose registry function would
    // be `fn match`; `syn` rejects keywords as identifiers, so this is also
    // where that surfaces as a schema error rather than a parse failure
    // blamed on the templates.
    let rules_name = snake(&short);
    if syn::parse_str::<Ident>(&rules_name).is_err() {
        return Err(format!(
            "message `{full}`'s cross-field rules function would be named \
             `{rules_name}`, which Rust reserves; rename the message in the \
             schema"
        ));
    }
    let rules_fn = ident(&rules_name);

    let mut items = TokenStream::new();

    let mut plans = Vec::new();
    for field in message.fields() {
        if field
            .containing_oneof()
            .is_some_and(|oneof| !oneof.is_synthetic())
        {
            continue; // Planned with the oneof.
        }
        plans.push(plan_field(&field, vocabulary)?);
    }
    for oneof in message.oneofs().filter(|oneof| !oneof.is_synthetic()) {
        let (payload_enum, plan) = plan_oneof(&oneof, vocabulary, claimed)?;
        items.extend(payload_enum);
        plans.push(plan);
    }

    // Ordered types *derive* `Ord`, and its agreement with `canonical_cmp` —
    // which the rules module's binary searches are sound by — is pinned by
    // the model's public-order test rather than by construction. The
    // by-construction alternative was tried and measured: any `Ord` written
    // in terms of the canonical comparators, delegated or inlined, cost 2.5%
    // of graph decoding on the pinned CI toolchain by perturbing inlining
    // across the crate. If the pin ever fires, a schema reordered fields
    // against their numbers; fix the schema, or switch the rules to
    // `canonical_cmp` and accept the cost knowingly.
    let derive = if ordered {
        quote! { #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)] }
    } else {
        quote! { #[derive(Clone, Debug, PartialEq, Eq)] }
    };

    let struct_doc = docs(&[
        format!("Validated form of `{full}`; the schema carries the field semantics."),
        String::new(),
        "Holds its invariants for as long as it exists: built through".to_owned(),
        format!("[`{short}Builder`] or decoded from the wire, both of which run the same"),
        "checks, including this message's cross-field rules.".to_owned(),
    ]);
    let builder_doc = docs(&[
        format!("Builds a [`{short}`]. Setters are infallible; [`build`]({short}Builder::build)"),
        "validates everything at once, exactly as decoding does.".to_owned(),
    ]);
    let builder_fn_doc = docs(&["A builder holding nothing yet."]);
    let build_doc = docs(&[
        "Validates and builds.",
        "",
        "# Errors",
        "",
        "[`Invalid`] naming the first field that is absent or breaks its",
        "schema invariants.",
    ]);

    let codec = root.then(|| {
        let decode_doc = docs(&[
            "Decodes and validates from wire bytes.",
            "",
            "# Errors",
            "",
            "[`DecodeError::Malformed`](crate::DecodeError) when the bytes are not",
            "protobuf, [`DecodeError::Invalid`](crate::DecodeError) when they decode",
            "but break the contract.",
        ]);
        let encode_doc = docs(&[
            "Encodes the canonical wire form, straight from the validated".to_owned(),
            "representation — no intermediate wire tree, no clone. Byte-identical".to_owned(),
            "to prost encoding the rebuilt tree, which the tests hold it to.".to_owned(),
        ]);
        quote! {
            #decode_doc
            pub fn decode(bytes: &[u8]) -> Result<Self, crate::DecodeError> {
                let wire = <wire::#name as ::prost::Message>::decode(bytes)
                    .map_err(crate::DecodeError::Malformed)?;
                Self::try_from(wire).map_err(crate::DecodeError::Invalid)
            }

            #encode_doc
            #[must_use]
            pub fn encode_to_vec(&self) -> Vec<u8> {
                let mut buf = Vec::with_capacity(crate::encode::Emit::emitted_len(self));
                crate::encode::Emit::emit(self, &mut buf);
                buf
            }
        }
    });

    // Canonical order walks fields by number — hash-visible first, metadata
    // as the tiebreak — never by declaration order.
    let mut ordered_plans: Vec<&Plan> = plans.iter().collect();
    ordered_plans.sort_by_key(|plan| (plan.metadata, plan.number));
    let cmp_chain = {
        let mut comparisons = ordered_plans.iter().map(|plan| &plan.cmp);
        comparisons.next().map_or_else(
            || quote! { std::cmp::Ordering::Equal },
            |first| {
                let rest = comparisons;
                quote! { #first #(.then_with(|| #rest))* }
            },
        )
    };
    let digests = ordered_plans
        .iter()
        .filter(|plan| !plan.metadata)
        .map(|plan| &plan.digest);
    // Wire order: field numbers ascending, metadata included in place —
    // encoding is fidelity where hashing is content — matching prost's own
    // emission of the rebuilt tree byte for byte.
    let mut wire_plans: Vec<&Plan> = plans.iter().collect();
    wire_plans.sort_by_key(|plan| plan.number);
    let emits = wire_plans.iter().map(|plan| &plan.emit);
    let emit_len_body = {
        let mut lengths = wire_plans.iter().map(|plan| &plan.emit_len);
        lengths.next().map_or_else(
            || quote! { 0 },
            |first| {
                let rest = lengths;
                quote! { #first #(+ #rest)* }
            },
        )
    };
    let emit_impl = quote! {
        impl crate::encode::Emit for #name {
            fn emit(&self, buf: &mut impl ::prost::bytes::BufMut) {
                #(#emits)*
            }

            fn emitted_len(&self) -> usize {
                #emit_len_body
            }
        }
    };

    let canonical_impls = quote! {
        impl crate::canonical::Canonical for #name {
            fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
                #cmp_chain
            }
        }

        impl crate::canonical::Digest for #name {
            fn digest<H: std::hash::Hasher>(&self, state: &mut H) {
                #(#digests)*
                crate::canonical::end(state);
            }
        }
    };

    // Canonicalization sorts what the schema calls `unordered`, so equal
    // content is one representation: encode emits it, hashing counts on it,
    // and the uniqueness scan reads neighbors instead of building sets.
    let sortable: Vec<&Ident> = plans
        .iter()
        .filter(|plan| plan.sortable)
        .map(|plan| &plan.ident)
        .collect();
    let canonicalize = (!sortable.is_empty()).then(|| {
        quote! {
            fn canonicalize(&mut self) {
                #(self.#sortable.sort_by(crate::canonical::Canonical::canonical_cmp);)*
            }
        }
    });
    let canonicalize_call = canonicalize
        .is_some()
        .then(|| quote! { built.canonicalize(); });
    let built_binding = if canonicalize.is_some() {
        quote! { let mut built }
    } else {
        quote! { let built }
    };

    let content_hash = vocabulary
        .message_invariant(message)
        .is_some_and(|invariant| invariant.hashable)
        .then(|| {
            let hash_doc = docs(&[
                "Feeds this message's logical content into `state`.".to_owned(),
                String::new(),
                "Present hash-visible fields, labeled by field number; collection".to_owned(),
                "metadata is skipped, transitively. Equal content produces equal".to_owned(),
                "bytes because construction canonicalized the representation, and".to_owned(),
                "the stream is injective, so distinct content cannot collide by".to_owned(),
                "construction of the bytes alone. Encoded wire bytes are never".to_owned(),
                "fed to a hash.".to_owned(),
                String::new(),
                "The hasher is the caller's: whoever stores or compares hashes".to_owned(),
                "owns the choice of function, and the standard library's default".to_owned(),
                "is deliberately not stable across processes.".to_owned(),
            ]);
            quote! {
                #hash_doc
                pub fn content_hash<H: std::hash::Hasher>(&self, state: &mut H) {
                    crate::canonical::Digest::digest(self, state);
                }
            }
        });

    let idents: Vec<&Ident> = plans.iter().map(|plan| &plan.ident).collect();
    let decl_tys: Vec<&TokenStream> = plans.iter().map(|plan| &plan.decl_ty).collect();
    let builder_tys: Vec<&TokenStream> = plans.iter().map(|plan| &plan.builder_ty).collect();
    let setters = plans.iter().map(|plan| &plan.setter);
    let build_inits = plans.iter().map(|plan| &plan.build_init);
    let from_wires = plans.iter().map(|plan| &plan.from_wire);
    let into_wires = plans.iter().map(|plan| &plan.into_wire);
    let checks = plans.iter().map(|plan| &plan.checks);
    let accessors = plans.iter().map(|plan| &plan.accessor);

    items.extend(quote! {
        #struct_doc
        #derive
        pub struct #name {
            #(#idents: #decl_tys,)*
        }


        #canonical_impls

        #emit_impl

        impl #name {
            #builder_fn_doc
            #[must_use]
            pub fn builder() -> #builder {
                #builder::default()
            }

            #(#accessors)*

            #content_hash

            fn check(&self) -> Result<(), Invalid> {
                #(#checks)*
                rules::#rules_fn(self)?;
                Ok(())
            }

            #canonicalize

            #codec
        }

        #builder_doc
        #[derive(Clone, Debug, Default)]
        pub struct #builder {
            #(#idents: #builder_tys,)*
        }

        impl #builder {
            #(#setters)*

            #build_doc
            pub fn build(self) -> Result<#name, Invalid> {
                #built_binding = #name {
                    #(#idents: #build_inits,)*
                };
                #canonicalize_call
                built.check()?;
                Ok(built)
            }
        }

        impl TryFrom<wire::#name> for #name {
            type Error = Invalid;

            fn try_from(wire: wire::#name) -> Result<Self, Invalid> {
                #built_binding = Self {
                    #(#idents: #from_wires,)*
                };
                #canonicalize_call
                built.check()?;
                Ok(built)
            }
        }

        impl From<#name> for wire::#name {
            fn from(value: #name) -> Self {
                Self {
                    #(#idents: #into_wires,)*
                }
            }
        }
    });

    Ok(items)
}
