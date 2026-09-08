// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The Redfish backend's schema index: a thin view over nv-redfish's
//! `SchemaQuery`.
//!
//! The query answers what a path *is* — type, nullability, cardinality,
//! vocabulary — which is everything the lint judges, and per segment what
//! the source crate's generated Rust *does* with it: which type in the
//! collapsed base chain declares it, whether generation gave it
//! presence-only or nullable shape, and a type definition's underlying
//! primitive. [`RedfishIndex::steps`] adapts that walk into the emitter's
//! [`Step`] rows; the leaf view and the segment view are one walk inside
//! the query, so the two cannot disagree.

use std::fmt;
use std::fs;

use nv_redfish_csdl_compiler::compiler::SchemaBundle;
use nv_redfish_csdl_compiler::compiler::TypeClass;
use nv_redfish_csdl_compiler::edmx::Edmx;
use nv_redfish_csdl_compiler::query::SchemaQuery;

/// Why the CSDL bundle could not become an index.
#[derive(Debug)]
pub enum IndexError {
    Io(String, std::io::Error),
    Parse(String, String),
    Build(String),
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(path, error) => write!(f, "{path}: {error}"),
            Self::Parse(path, error) => write!(f, "{path}: not valid CSDL: {error}"),
            Self::Build(error) => write!(f, "the CSDL bundle does not compile: {error}"),
        }
    }
}

impl std::error::Error for IndexError {}

/// One resolved source field: what the lint type-checks against and the
/// emitter picks conversions from.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResolvedField {
    /// As CSDL spells it: `Edm.Decimal`, or a reference like `Resource.Health`.
    pub type_name: String,
    pub class: TypeClass,
    pub nullable: bool,
    pub collection: bool,
    /// Member names when the type is an enum: the closed vocabulary
    /// `value_map` and `known_values` are validated against.
    pub enum_members: Option<Vec<String>>,
}

impl ResolvedField {
    /// Whether one value of this field is a single scalar — a primitive,
    /// a type definition, or an enum, as opposed to a structured type.
    #[must_use]
    pub fn is_scalar(&self) -> bool {
        self.class != TypeClass::ComplexType
    }
}

/// One segment of a resolved source path, as the source crate's generated
/// Rust presents it. The last step is the leaf the mapping extracts.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Step {
    /// The segment, in CSDL spelling.
    pub property: String,
    /// `base` hops from the holding type to the declaring type in the
    /// optimizer-collapsed chain — the count of `.base` accesses generation
    /// puts in front of the field.
    pub hops: usize,
    /// `Redfish.Required` on the property; with `nullable`, decides the
    /// generated field's `Option` nesting.
    pub required: bool,
    pub nullable: bool,
    pub collection: bool,
    pub class: TypeClass,
    /// Namespace of the property's type after the fold: `Sensor`, `Edm`.
    pub namespace: String,
    /// Type name within that namespace: `ReadingType`, `Decimal`.
    pub name: String,
    /// For a type definition, the `Edm` primitive underneath, which
    /// conversion selection keys on.
    pub underlying: Option<String>,
    /// Member names when the type is an enum.
    pub enum_members: Option<Vec<String>>,
}

impl Step {
    /// How generation shapes the field, from the same rule the source
    /// crate's own struct generator applies.
    #[must_use]
    pub fn shape(&self) -> Shape {
        match (self.required, self.nullable) {
            (true, false) => Shape::Bare,
            (true, true) => Shape::RequiredNullable,
            (false, true) => Shape::Nullable,
            (false, false) => Shape::Optional,
        }
    }

    /// Whether the leaf is text in the generated Rust — `Edm.String` or a
    /// type definition over it, cloned rather than copied. One
    /// classification for conversion selection and field access alike, so
    /// the two cannot diverge.
    #[must_use]
    pub fn is_text(&self) -> bool {
        match self.class {
            TypeClass::SimpleType => self.name == "String",
            TypeClass::TypeDefinition => self.underlying.as_deref() == Some("String"),
            TypeClass::EnumType | TypeClass::ComplexType => false,
        }
    }
}

/// The `Option` nesting generation gives a singular field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// `T`: required and non-nullable.
    Bare,
    /// `Option<T>`: presence only.
    Optional,
    /// `Option<T>`: required on the wire, where `None` is explicit null.
    RequiredNullable,
    /// `Option<Option<T>>`: outer absence, inner explicit null.
    Nullable,
}

/// The parsed bundle; [`RedfishIndex`] borrows from it.
pub struct Bundle {
    bundle: SchemaBundle,
    documents: usize,
}

impl fmt::Debug for Bundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bundle({} documents)", self.documents)
    }
}

impl Bundle {
    /// Parses the vendored bundle nv-redfish ships — Redfish and Swordfish
    /// both, because DMTF types reference Swordfish ones (`Volume` names
    /// `FeaturesRegistry`).
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] naming the file that failed to read or parse.
    pub fn dmtf() -> Result<Self, IndexError> {
        let mut documents = Vec::new();
        let paths = nv_redfish_schema::glob_redfish_xml()
            .into_iter()
            .chain(nv_redfish_schema::glob_swordfish_xml());
        for path in paths {
            let content =
                fs::read_to_string(&path).map_err(|error| IndexError::Io(path.clone(), error))?;
            let document = Edmx::parse(&content)
                .map_err(|error| IndexError::Parse(path.clone(), format!("{error:?}")))?;
            documents.push(document);
        }
        // Every document is a root: the query resolves across the whole
        // vendored set, nothing is compiled.
        let count = documents.len();
        Ok(Self {
            bundle: SchemaBundle::new(documents, Vec::new()),
            documents: count,
        })
    }

    /// Builds the queryable index over the parsed documents.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Build`] if the bundle does not compile.
    ///
    /// # Panics
    ///
    /// Only if the compile thread cannot be spawned or itself panics.
    pub fn index(&self) -> Result<RedfishIndex<'_>, IndexError> {
        // The compiler's descent is deep enough to overflow a default test
        // stack in debug builds; nv-redfish's own build runs it the same
        // way. Scoped so the query can borrow the bundle.
        let query = std::thread::scope(|scope| {
            std::thread::Builder::new()
                .stack_size(64 * 1024 * 1024)
                .spawn_scoped(scope, || {
                    SchemaQuery::build(&self.bundle)
                        .map_err(|error| IndexError::Build(format!("{error:?}")))
                })
                .expect("the compile thread spawns")
                .join()
                .expect("the compile thread does not panic")
        })?;
        Ok(RedfishIndex { query })
    }
}

/// Dotted source paths resolve to typed, nullability-aware fields.
pub struct RedfishIndex<'a> {
    query: SchemaQuery<'a>,
}

impl fmt::Debug for RedfishIndex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RedfishIndex")
    }
}

impl RedfishIndex<'_> {
    #[must_use]
    pub fn has_type(&self, name: &str) -> bool {
        self.query.has_entity(name)
    }

    /// Resolves a dotted path such as `Thresholds.UpperCritical.Reading`
    /// against the entity type named by `source_type`.
    #[must_use]
    pub fn resolve(&self, source_type: &str, path: &str) -> Option<ResolvedField> {
        let property = self.query.resolve(source_type, path)?;
        Some(ResolvedField {
            type_name: property.type_name.to_string(),
            class: property.class,
            nullable: property.nullable,
            collection: property.collection,
            enum_members: property.enum_members.map(|members| {
                members
                    .iter()
                    .map(|member| member.inner().to_string())
                    .collect()
            }),
        })
    }

    /// The namespace of the entity the query picked for `name` — `Sensor`
    /// for `Sensor` — which spells the module its generated type lives in.
    #[must_use]
    pub fn entity_namespace(&self, name: &str) -> Option<String> {
        self.query
            .entity(name)
            .map(|qname| qname.namespace.to_string())
    }

    /// Resolves a path into per-segment emission facts.
    ///
    /// # Errors
    ///
    /// A message naming the path when it does not resolve — the lint runs
    /// first, so that is an emitter bug.
    pub fn steps(&self, source_type: &str, path: &str) -> Result<Vec<Step>, String> {
        let steps = self
            .query
            .steps(source_type, path)
            .ok_or_else(|| format!("`{source_type}.{path}` does not resolve"))?;
        Ok(path
            .split('.')
            .zip(steps)
            .map(|(segment, step)| Step {
                property: segment.to_owned(),
                hops: step.hops,
                required: step.required,
                nullable: step.nullable,
                collection: step.collection,
                class: step.class,
                namespace: step.type_name.namespace.to_string(),
                name: step.type_name.name.inner().clone(),
                underlying: step
                    .underlying
                    .map(|underlying| underlying.name.inner().clone()),
                enum_members: step.enum_members.map(|members| {
                    members
                        .iter()
                        .map(|member| member.inner().to_string())
                        .collect()
                }),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::Bundle;
    use super::Shape;
    use super::TypeClass;

    #[test]
    fn the_worked_examples_paths_resolve_with_their_types() {
        let bundle = Bundle::dmtf().expect("the vendored bundle parses");
        let index = bundle.index().expect("the bundle compiles");

        assert!(index.has_type("Sensor"));
        assert!(!index.has_type("Sensr"));

        // The sample's anchor: a nullable decimal, because a sensor that
        // cannot answer reports null.
        let reading = index
            .resolve("Sensor", "Reading")
            .expect("Reading resolves");
        assert_eq!(reading.type_name, "Edm.Decimal");
        assert!(reading.nullable);
        assert!(!reading.collection);
        assert!(reading.enum_members.is_none());

        // A base-chain property: Id lives on Resource, not Sensor. Its
        // typedef is still a scalar; a complex type is not.
        let id = index
            .resolve("Sensor", "Id")
            .expect("Id resolves via the base chain");
        assert_eq!(id.type_name, "Resource.Id");
        assert!(id.is_scalar());
        let status = index.resolve("Sensor", "Status").expect("Status resolves");
        assert!(!status.is_scalar());

        // Through complex types, with the closed vocabularies attached.
        let health = index
            .resolve("Sensor", "Status.Health")
            .expect("Status.Health resolves");
        let members = health.enum_members.expect("Health is an enum");
        assert!(members.iter().any(|member| member == "OK"));

        let activation = index
            .resolve("Sensor", "Thresholds.UpperCritical.Activation")
            .expect("the threshold activation resolves");
        let members = activation.enum_members.expect("Activation is an enum");
        assert!(members.iter().any(|member| member == "Disabled"));

        let kind = index
            .resolve("Sensor", "ReadingType")
            .expect("ReadingType resolves");
        let members = kind.enum_members.expect("ReadingType is an enum");
        assert!(members.iter().any(|member| member == "AirFlowCMM"));

        // Unknown paths and descent through scalars resolve to nothing.
        assert!(index.resolve("Sensor", "ReadingTypo").is_none());
        assert!(index.resolve("Sensor", "Reading.Deeper").is_none());
    }

    #[test]
    fn steps_carry_what_field_access_generation_needs() {
        let bundle = Bundle::dmtf().expect("the vendored bundle parses");
        let index = bundle.index().expect("the bundle compiles");

        // Id: one base hop up the collapsed chain, required and
        // non-nullable — generation makes it a bare field — and a typedef
        // whose underlying primitive conversion selection keys on.
        let steps = index.steps("Sensor", "Id").expect("Id resolves");
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].hops, 1);
        assert_eq!(steps[0].shape(), Shape::Bare);
        assert_eq!(steps[0].class, TypeClass::TypeDefinition);
        assert_eq!(steps[0].underlying.as_deref(), Some("String"));

        // A nullable leaf under two presence-only complex segments, each
        // declared on its own type: no hops, single-`Option` shapes.
        let steps = index
            .steps("Sensor", "Thresholds.UpperCritical.Reading")
            .expect("the threshold reading resolves");
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].shape(), Shape::Optional);
        assert_eq!(steps[0].hops, 0);
        assert_eq!(steps[1].shape(), Shape::Optional);
        assert_eq!(steps[1].hops, 0);
        assert_eq!(steps[2].shape(), Shape::Nullable);
        assert_eq!(steps[2].namespace, "Edm");
        assert_eq!(steps[2].name, "Decimal");

        // Nullability can live above a non-nullable leaf. A path-wide null
        // policy check must therefore inspect every step, not only the query's
        // leaf result.
        let steps = index
            .steps("Chassis", "Doors.Front.UserLabel")
            .expect("the nested door label resolves");
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].shape(), Shape::Optional);
        assert_eq!(steps[1].shape(), Shape::Nullable);
        assert_eq!(steps[2].shape(), Shape::Optional);

        // The steps view refuses what the leaf view refuses.
        assert!(index.steps("Sensor", "ReadingTypo").is_err());
    }
}
