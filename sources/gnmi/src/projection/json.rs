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

/// Extracts leaves and member issues, retaining received path names and list keys.
/// Invalid syntax, duplicate names in traversed objects, or overlong issue paths
/// discard all leaves from the update.
pub(super) fn expand(
    spec: &MappingSpec,
    mut path: SubscriptionPath,
    bytes: &[u8],
    ietf: bool,
) -> Result<Vec<Result<Leaf, InvalidJson>>, InvalidJson> {
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
        json_start: ietf.then_some(path.0.len()),
    };

    extractor.walk(&mut path, raw, &mut String::new())?;

    // No leaves escape when a later member fails object validation.
    Ok(extractor.leaves)
}

struct Extractor<'a> {
    spec: &'a MappingSpec,
    pointer_limit: usize,
    leaves: Vec<Result<Leaf, InvalidJson>>,

    /// First path element supplied by a `JSON_IETF` member.
    json_start: Option<usize>,
}

impl Extractor<'_> {
    fn walk(
        &mut self,
        path: &mut SubscriptionPath,
        mut raw: &RawValue,
        pointer: &mut String,
    ) -> Result<(), InvalidJson> {
        // Keep parent iterators and path lengths on the heap so object nesting
        // does not consume a call-stack frame for each member.
        let mut parents = Vec::new();

        loop {
            let is_object = raw.get().starts_with('{');
            let mapped = self.spec.mapping(path).is_some();
            let relevant = self.spec.mappings.iter().any(|m| m.covers(path));

            let mismatch = !relevant
                && self
                    .json_start
                    .is_some_and(|start| qualification_mismatch(self.spec, path, start));

            // Reserve both indexes for locations that may produce an issue,
            // including Get's notification index. Unrelated members need no locator.
            if (relevant || mismatch) && pointer.len() > self.pointer_limit {
                return Err(InvalidJson {
                    pointer: String::new(),
                    detail: "JSON member path exceeds the projection issue path limit",
                });
            }

            // An object at a scalar mapping is invalid there, but its descendants
            // can still match other mappings. Arrays remain opaque.
            if mapped || (!is_object && relevant) {
                let value = (!is_object && !raw.get().starts_with('['))
                    .then(|| Value::JsonVal(raw.get().as_bytes().to_vec()));

                self.leaves.push(Ok(Leaf {
                    path: path.clone(),
                    value,
                    pointer: pointer.clone(),
                }));
            } else if mismatch {
                self.leaves.push(Err(InvalidJson {
                    pointer: pointer.clone(),
                    detail: "module-qualified JSON_IETF member does not match the configured literal path",
                }));
            }

            if is_object && relevant {
                let mut failure = None;
                let mut decoder = serde_json::Deserializer::from_str(raw.get());

                // Only traverse branches that can reach a literal mapping. A
                // qualified-only branch reports its mismatch above and stops here.
                let fields = decoder
                    .deserialize_map(Object {
                        failure: &mut failure,
                    })
                    .map_err(|_| InvalidJson {
                        pointer: pointer.clone(),
                        detail: failure.unwrap_or("malformed JSON object"),
                    })?;

                parents.push((fields.into_iter(), path.0.len(), pointer.len()));
            }

            // Restore each parent's path before advancing to a sibling or
            // returning to the next outer object.
            let (name, value) = loop {
                let Some((members, path_len, pointer_len)) = parents.last_mut() else {
                    return Ok(());
                };

                path.0.truncate(*path_len);
                pointer.truncate(*pointer_len);

                if let Some(member) = members.next() {
                    break member;
                }

                parents.pop();
            };

            pointer.push('/');
            pointer.push_str(&name.replace('~', "~0").replace('/', "~1"));

            path.0.push(PathElement::new(name));
            raw = value;
        }
    }
}

// Detect paths that would match only after removing a JSON_IETF module qualifier.
fn qualification_mismatch(spec: &MappingSpec, path: &SubscriptionPath, start: usize) -> bool {
    spec.mappings.iter().any(|mapping| {
        if path.0.len() > mapping.path.0.len()
            || !path.0.iter().zip(&mapping.path.0).enumerate().all(
                |(index, (received, configured))| {
                    let name = if index >= start && !configured.name.contains(':') {
                        received
                            .name
                            .split_once(':')
                            .map_or(received.name.as_str(), |(_, name)| name)
                    } else {
                        &received.name
                    };

                    name == configured.name
                },
            )
        {
            return false;
        }

        let mut candidate = path.clone();

        for (received, configured) in candidate.0.iter_mut().zip(&mapping.path.0).skip(start) {
            received.name.clone_from(&configured.name);
        }

        mapping.covers(&candidate)
    })
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
