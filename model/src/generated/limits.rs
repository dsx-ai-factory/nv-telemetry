// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from `nv.telemetry.v1` by `make codegen`. Do not edit.
//!
//! Every numeric bound the schema declares, one constant per annotation, so
//! validators — generated and hand-written alike — enforce the schema's
//! number rather than a copy of it. A bound changed in the schema reaches
//! every check by regeneration; a field renamed in the schema breaks its
//! hand-written consumers at compile time instead of leaving them checking a
//! bound that no longer exists.

// The whole contract's bounds are emitted whether or not anything consumes
// them yet, exactly as the wire types are.
#![allow(dead_code)]

/// `nv.telemetry.v1.AcquisitionStatus.detail` `max_len`, in bytes.
pub const ACQUISITIONSTATUS_DETAIL_MAX_LEN: u32 = 4_096;

/// `nv.telemetry.v1.AcquisitionStatus.endpoint_id` `max_len`, in bytes.
pub const ACQUISITIONSTATUS_ENDPOINT_ID_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.AcquisitionStatus.provider` `max_len`, in bytes.
pub const ACQUISITIONSTATUS_PROVIDER_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.AcquisitionStatus.request_class` `max_len`, in bytes.
pub const ACQUISITIONSTATUS_REQUEST_CLASS_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.EndpointContext.endpoint_id` `max_len`, in bytes.
pub const ENDPOINTCONTEXT_ENDPOINT_ID_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.InventoryItem.source_key` `max_len`, in bytes.
pub const INVENTORYITEM_SOURCE_KEY_MAX_LEN: u32 = 1_024;

/// `nv.telemetry.v1.Inventory.items` `max_items`.
pub const INVENTORY_ITEMS_MAX_ITEMS: u32 = 65_536;

/// `nv.telemetry.v1.LogRecord.entry_id` `max_len`, in bytes.
pub const LOGRECORD_ENTRY_ID_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.LogRecord.message` `max_len`, in bytes.
pub const LOGRECORD_MESSAGE_MAX_LEN: u32 = 65_536;

/// `nv.telemetry.v1.Logs.records` `max_items`.
pub const LOGS_RECORDS_MAX_ITEMS: u32 = 65_536;

/// `nv.telemetry.v1.ObservedResource.entity_tag` `max_len`, in bytes.
pub const OBSERVEDRESOURCE_ENTITY_TAG_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.ObservedResource.source_key` `max_len`, in bytes.
pub const OBSERVEDRESOURCE_SOURCE_KEY_MAX_LEN: u32 = 1_024;

/// `nv.telemetry.v1.ObservedResource.source_schema` `max_len`, in bytes.
pub const OBSERVEDRESOURCE_SOURCE_SCHEMA_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.ObservedResource.unresolved` `max_items`.
pub const OBSERVEDRESOURCE_UNRESOLVED_MAX_ITEMS: u32 = 1_024;

/// `nv.telemetry.v1.Origin.provider` `max_len`, in bytes.
pub const ORIGIN_PROVIDER_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.Origin.request_class` `max_len`, in bytes.
pub const ORIGIN_REQUEST_CLASS_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.ProjectionIssues.issues` `max_items`.
pub const PROJECTIONISSUES_ISSUES_MAX_ITEMS: u32 = 65_536;

/// `nv.telemetry.v1.ProjectionIssue.detail` `max_len`, in bytes.
pub const PROJECTIONISSUE_DETAIL_MAX_LEN: u32 = 4_096;

/// `nv.telemetry.v1.ProjectionIssue.path` `max_len`, in bytes.
pub const PROJECTIONISSUE_PATH_MAX_LEN: u32 = 1_024;

/// `nv.telemetry.v1.Readings.descriptors` `max_items`.
pub const READINGS_DESCRIPTORS_MAX_ITEMS: u32 = 65_536;

/// `nv.telemetry.v1.Readings.samples` `max_items`.
pub const READINGS_SAMPLES_MAX_ITEMS: u32 = 65_536;

/// `nv.telemetry.v1.ResourceGraph.relations` `max_items`.
pub const RESOURCEGRAPH_RELATIONS_MAX_ITEMS: u32 = 131_072;

/// `nv.telemetry.v1.ResourceGraph.resources` `max_items`.
pub const RESOURCEGRAPH_RESOURCES_MAX_ITEMS: u32 = 65_536;

/// `nv.telemetry.v1.ResourceRelation.kind` `max_len`, in bytes.
pub const RESOURCERELATION_KIND_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.SignalDescriptor.kind` `max_len`, in bytes.
pub const SIGNALDESCRIPTOR_KIND_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.SignalDescriptor.unit` `max_len`, in bytes.
pub const SIGNALDESCRIPTOR_UNIT_MAX_LEN: u32 = 64;

/// `nv.telemetry.v1.SignalKey.facet` `max_len`, in bytes.
pub const SIGNALKEY_FACET_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.StateObservation.facet` `max_len`, in bytes.
pub const STATEOBSERVATION_FACET_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.StateObservation.name` `max_len`, in bytes.
pub const STATEOBSERVATION_NAME_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.States.observations` `max_items`.
pub const STATES_OBSERVATIONS_MAX_ITEMS: u32 = 65_536;

/// `nv.telemetry.v1.Subject.id` `max_len`, in bytes.
pub const SUBJECT_ID_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.Subject.kind` `max_len`, in bytes.
pub const SUBJECT_KIND_MAX_LEN: u32 = 128;

/// `nv.telemetry.v1.Subject.scope` `max_items`.
pub const SUBJECT_SCOPE_MAX_ITEMS: u32 = 16;

/// `nv.telemetry.v1.Subject.scope` `max_len`, in bytes.
pub const SUBJECT_SCOPE_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.UnresolvedReference.location` `max_len`, in bytes.
pub const UNRESOLVEDREFERENCE_LOCATION_MAX_LEN: u32 = 1_024;

/// `nv.telemetry.v1.UnresolvedReference.property` `max_len`, in bytes.
pub const UNRESOLVEDREFERENCE_PROPERTY_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.Value.bytes_value` `max_len`, in bytes.
pub const VALUE_BYTES_VALUE_MAX_LEN: u32 = 4_096;

/// `nv.telemetry.v1.Value.List.values` `max_items`.
pub const VALUE_LIST_VALUES_MAX_ITEMS: u32 = 1_024;

/// `nv.telemetry.v1.Value.Map.entries` `max_items`.
pub const VALUE_MAP_ENTRIES_MAX_ITEMS: u32 = 1_024;

/// `nv.telemetry.v1.Value.Map.Entry.key` `max_len`, in bytes.
pub const VALUE_MAP_ENTRY_KEY_MAX_LEN: u32 = 256;

/// `nv.telemetry.v1.Value` `max_depth`, in logical levels.
pub const VALUE_MAX_DEPTH: u32 = 16;

/// `nv.telemetry.v1.Value.string_value` `max_len`, in bytes.
pub const VALUE_STRING_VALUE_MAX_LEN: u32 = 4_096;
