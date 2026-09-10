// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Location grammar: what a Redfish URI denotes before identity is derived.
//!
//! The subject's scope comes from the *requested* location, never from the
//! payload's `@odata.id` — the plan named the URI, and the payload's claim
//! about itself is provenance, not identity. Generated projections match
//! their manifest's location template over [`canonical`]'s output; this is
//! the one hook the projection compiler expects a Redfish source crate to
//! provide.

/// Reduces a Redfish URI to the resource it denotes.
///
/// A metric report names a sensor's reading with a property fragment
/// (`.../CPU0Temp#/Reading`) while the sensor resource names itself without
/// one. Query options select a representation of that same resource. Both
/// query and fragment, followed by any trailing separator, are therefore
/// dropped before the URI is used as an identity.
pub(crate) fn canonical(uri: &str) -> &str {
    let path = uri.split_once('#').map_or(uri, |(path, _)| path);
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() && path.starts_with('/') {
        "/"
    } else {
        trimmed
    }
}

/// The namespaced owner of a log service at a supported Redfish location.
/// Keep the collection name as well as its local id: Systems/1 and
/// Managers/1 are different owners on the same endpoint.
pub(crate) fn log_service_owner(uri: &str) -> Option<(&str, &str)> {
    let segments: Vec<_> = canonical(uri).split('/').collect();
    match segments.as_slice() {
        ["", "redfish", "v1", kind @ ("Systems" | "Managers" | "Chassis"), owner, "LogServices", service]
            if !owner.is_empty() && !service.is_empty() =>
        {
            Some((kind, owner))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::canonical;
    use super::log_service_owner;

    #[test]
    fn a_log_service_owner_keeps_its_namespace() {
        assert_eq!(
            log_service_owner("/redfish/v1/Systems/1/LogServices/SEL"),
            Some(("Systems", "1"))
        );
        assert_eq!(
            log_service_owner("/redfish/v1/Managers/1/LogServices/SEL?token=x"),
            Some(("Managers", "1"))
        );
        assert_eq!(
            log_service_owner("/redfish/v1/Chassis/1/LogServices/SEL"),
            Some(("Chassis", "1"))
        );
        for path in [
            "/LogServices/SEL",
            "/redfish/v1/Odd/SEL",
            "/redfish/v1/Systems//LogServices/SEL",
            "/redfish/v1/Systems/1/LogServices/SEL/Entries",
        ] {
            assert_eq!(log_service_owner(path), None);
        }
    }

    #[test]
    fn a_property_fragment_names_the_same_resource_as_the_bare_uri() {
        assert_eq!(
            canonical("/redfish/v1/Chassis/1/Sensors/CPU0Temp#/Reading"),
            "/redfish/v1/Chassis/1/Sensors/CPU0Temp"
        );
        assert_eq!(
            canonical("/redfish/v1/Chassis/1/Sensors/CPU0Temp/"),
            "/redfish/v1/Chassis/1/Sensors/CPU0Temp"
        );
        assert_eq!(
            canonical("/redfish/v1/Chassis/1/Sensors/CPU0Temp/?$select=Reading#/Reading"),
            "/redfish/v1/Chassis/1/Sensors/CPU0Temp"
        );
        assert_eq!(canonical("/?$select=Id"), "/");
    }
}
