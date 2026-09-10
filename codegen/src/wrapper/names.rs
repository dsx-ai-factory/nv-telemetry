// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Naming: how schema declarations become Rust identifiers.
//!
//! Everything that turns a protobuf name into a Rust one lives here, so the
//! rules are one place — including the rule that the case conversions
//! delegate to `heck`, because prost-build's do, and some of these names must
//! land on modules prost-build generated.

use heck::ToShoutySnakeCase as _;
use heck::ToSnakeCase as _;
use heck::ToUpperCamelCase as _;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

/// `#[doc = " line"]` attributes from plain text lines, the token spelling of
/// `///` comments; prettyplease renders them back as `///`. Crate-visible:
/// the projection emitter renders docs through the same spelling.
pub(crate) fn docs<S: AsRef<str>>(lines: &[S]) -> TokenStream {
    let attrs = lines.iter().map(|line| {
        let line = line.as_ref();
        let text = if line.is_empty() {
            String::new()
        } else {
            format!(" {line}")
        };
        quote! { #[doc = #text] }
    });
    quote! { #(#attrs)* }
}

pub(crate) fn ident(name: &str) -> Ident {
    format_ident!("{name}")
}

/// `nv.telemetry.v1.Value.Map.Entry.key` -> `VALUE_MAP_ENTRY_KEY`.
///
/// Crate-visible because the projection emitter references the same
/// constants the limits module renders; one naming rule, two consumers.
pub(crate) fn constant_stem(full_name: &str) -> String {
    full_name
        .strip_prefix(&format!("{}.", crate::CONTRACT_PACKAGE))
        .unwrap_or(full_name)
        .replace('.', "_")
        .to_ascii_uppercase()
}

/// `131072` -> `131_072`, matching what the workspace lints expect of a
/// literal a human reads.
pub(super) fn separated(bound: u32) -> String {
    let digits = bound.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    let leading = digits.len() % 3;
    for (position, digit) in digits.chars().enumerate() {
        if position != 0 && position % 3 == leading % 3 {
            out.push('_');
        }
        out.push(digit);
    }
    out
}

/// `nv.telemetry.v1.AcquisitionStatus.FailureClass` -> `FailureClass`.
pub(crate) fn short_name(full_name: &str) -> String {
    full_name.rsplit('.').next().unwrap_or(full_name).to_owned()
}

/// `payload` -> `Payload`, `readings` -> `Readings`.
pub(super) fn camel(snake_name: &str) -> String {
    snake_name.to_upper_camel_case()
}

/// `SignalKey` -> `signal_key`, exactly as prost-build names modules.
///
/// This one must match prost-build precisely, because it names the `wire::`
/// module a oneof's enum lives in: `GPUState` is `gpu_state` under heck and
/// `g_p_u_state` under a letter-by-letter split, and the difference surfaces
/// as a checked-in file that does not compile.
pub(super) fn snake(camel_name: &str) -> String {
    camel_name.to_snake_case()
}

/// `FailureClass` -> `FAILURE_CLASS`.
fn screaming(camel_name: &str) -> String {
    camel_name.to_shouty_snake_case()
}

/// The model variant of a contract enum value: `FAILURE_CLASS_CONNECTIVITY`
/// of `FailureClass` -> `Connectivity`. The one derivation, so the projection
/// emitter names exactly the variant the model emitter generated.
pub(crate) fn variant_name(enum_short_name: &str, value_name: &str) -> String {
    let prefix = format!("{}_", screaming(enum_short_name));
    camel(
        &value_name
            .strip_prefix(prefix.as_str())
            .unwrap_or(value_name)
            .to_ascii_lowercase(),
    )
}
