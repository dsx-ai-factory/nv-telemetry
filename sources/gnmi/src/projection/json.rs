// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Extracts mapped scalar leaves from JSON objects using received path keys.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;

use nv_telemetry_model::limits;
use serde::de::Error as _;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::Deserializer as _;
use serde_json::value::RawValue;

use crate::protocol::gnmi::typed_value::Value;
use crate::spec::MappingSpec;
use crate::PathElement;
use crate::SubscriptionPath;

/// A mapped value and its JSON Pointer. `None` marks an object or array as invalid
/// for a scalar mapping, so an earlier valid update cannot replace that value.
pub(super) struct Leaf {
    pub(super) path: SubscriptionPath,
    pub(super) value: Option<Value>,
    pub(super) pointer: String,
}

pub(super) struct InvalidJson {
    pub(super) pointer: String,
    pub(super) detail: &'static str,
}

/// Extracts leaves at a resolved response path, retaining its received list keys.
/// Invalid syntax, duplicate names, or overlong issue paths discard all leaves.
pub(super) fn expand(
    spec: &MappingSpec,
    mut path: SubscriptionPath,
    bytes: &[u8],
) -> Result<Vec<Leaf>, InvalidJson> {
    let raw = serde_json::from_slice::<&RawValue>(bytes).map_err(|_| InvalidJson {
        pointer: String::new(),
        detail: "malformed JSON object",
    })?;

    let mut extractor = Extractor {
        spec,
        pointer_limit: (limits::PROJECTIONISSUE_PATH_MAX_LEN as usize).saturating_sub(
            format!("notification[{}].update[{}].val", usize::MAX, usize::MAX).len(),
        ),
        leaves: Vec::new(),
    };

    extractor.walk(&mut path, raw, &mut String::new())?;

    // No leaves escape when a later member fails object validation.
    Ok(extractor.leaves)
}

struct Extractor<'a> {
    spec: &'a MappingSpec,
    pointer_limit: usize,
    leaves: Vec<Leaf>,
}

impl Extractor<'_> {
    fn walk(
        &mut self,
        path: &mut SubscriptionPath,
        raw: &RawValue,
        pointer: &mut String,
    ) -> Result<(), InvalidJson> {
        let is_object = raw.get().starts_with('{');
        let mapped = self.spec.mapping(path).is_some();

        // An object at a scalar mapping is invalid there, but its descendants
        // can still match other mappings. Arrays remain opaque.
        if mapped || (!is_object && self.spec.mappings.iter().any(|m| m.covers(path))) {
            let value = (!is_object && !raw.get().starts_with('['))
                .then(|| Value::JsonVal(raw.get().as_bytes().to_vec()));

            self.leaves.push(Leaf {
                path: path.clone(),
                value,
                pointer: pointer.clone(),
            });
        }

        if !is_object {
            return Ok(());
        }

        let mut failure = None;
        let mut decoder = serde_json::Deserializer::from_str(raw.get());

        // Validate unmapped object branches too; a duplicate rejects the whole update.
        let fields = decoder
            .deserialize_map(Object {
                failure: &mut failure,
            })
            .map_err(|_| InvalidJson {
                pointer: pointer.clone(),
                detail: failure.unwrap_or("malformed JSON object"),
            })?;

        for (name, value) in fields {
            let previous = pointer.len();

            pointer.push('/');
            pointer.push_str(&name.replace('~', "~0").replace('/', "~1"));

            // Reserve both indexes even for Subscribe, so the same locator is
            // valid after Get prefixes it with a notification index.
            if pointer.len() > self.pointer_limit {
                return Err(InvalidJson {
                    pointer: String::new(),
                    detail: "JSON member path exceeds the projection issue path limit",
                });
            }

            path.0.push(PathElement::new(name));
            self.walk(path, value, pointer)?;
            path.0.pop();
            pointer.truncate(previous);
        }

        Ok(())
    }
}

// Borrow scalar tokens so integer values use the same parser as leaf updates.
// The visitor also rejects duplicate names before a map can replace them.
struct Object<'a> {
    failure: &'a mut Option<&'static str>,
}

impl<'de> Visitor<'de> for Object<'_> {
    type Value = BTreeMap<String, &'de RawValue>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object with unique member names")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut fields = BTreeMap::new();

        while let Some(name) = map.next_key::<String>()? {
            let Entry::Vacant(entry) = fields.entry(name) else {
                *self.failure = Some("JSON object has duplicate member names");
                return Err(M::Error::custom("JSON object has duplicate member names"));
            };

            entry.insert(map.next_value()?);
        }

        Ok(fields)
    }
}
