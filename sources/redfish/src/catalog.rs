// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Bounded discovery of modern chassis sensor locations.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use nv_redfish::core::EntityTypeRef;
use nv_redfish::core::ODataETag;
use nv_redfish::core::ODataId;
use nv_redfish::Bmc;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::Origin;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::ProjectionIssue;
use serde::Deserialize;
use url::Url;

use crate::failure::ClassifyError;
use crate::provider::with_deadline;
use crate::provider::Page;
use crate::provider::WalkBudget;
use crate::provider::TRUNCATED_WALK_LOCATOR;
use crate::transport::RedfishService;
use crate::transport::RedfishTransport;

/// Sensor locations discovered during 1 bounded traversal.
///
/// Locations retain discovery order and contain endpoint-relative paths and
/// queries, ready for [`crate::SensorRead`]. Completeness describes traversal of
/// advertised modern Sensors collections, not a physical inventory snapshot.
/// The caller owns refresh, selection, scheduling, and reporting.
/// See the [crate example](crate#sensor-discovery) for direct consumption.
#[derive(Clone, PartialEq, Eq)]
pub struct SensorCatalog {
    locations: Vec<String>,
    issues: Vec<ProjectionIssue>,
    completeness: Completeness,
}

impl SensorCatalog {
    /// Unique sensor locations, in discovery order. Discovery does not GET
    /// these resources or validate their readings.
    #[must_use]
    pub fn locations(&self) -> &[String] {
        &self.locations
    }

    /// Resource-local failures and traversal limits, without reporting envelopes.
    #[must_use]
    pub fn issues(&self) -> &[ProjectionIssue] {
        &self.issues
    }

    /// Whether every advertised modern sensor collection was traversed.
    #[must_use]
    pub fn completeness(&self) -> Completeness {
        self.completeness
    }
}

impl fmt::Debug for SensorCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SensorCatalog")
            .field("locations", &self.locations.len())
            .field("issues", &self.issues.len())
            .field("completeness", &self.completeness)
            .finish()
    }
}

/// Discovers modern Sensors resources through 1 endpoint's shared service.
///
/// Every acquisition starts a fresh traversal under the supplied graph-wide
/// budget. The outer deadline includes platform classification and cancels
/// unfinished protocol work. References resolve against the containing resource;
/// absolute authorities are discarded. References with fragments or resolved paths
/// beginning with `//` are rejected.
/// Dropping the acquisition future cancels discovery;
/// no background tasks or catalog cache are created.
/// See the [crate example](crate#sensor-discovery) for construction and execution.
pub struct SensorCatalogRead<T> {
    endpoint: EndpointContext,
    origin: Origin,
    service: Arc<RedfishService<T>>,
    budget: WalkBudget,
}

impl<T> SensorCatalogRead<T> {
    /// Provider identity used by admission and reporting.
    pub const PROVIDER: &'static str = "redfish.sensor.catalog";

    /// Request class used by admission and reporting.
    pub const REQUEST_CLASS: &'static str = "sensor-catalog";

    /// Creates discovery work with explicit member and elapsed-time limits.
    /// Listed chassis and sensor entries spend the same budget before deduplication.
    ///
    /// # Errors
    ///
    /// Returns an error if the provider constants violate `Origin` bounds.
    pub fn new(
        endpoint: EndpointContext,
        service: Arc<RedfishService<T>>,
        budget: WalkBudget,
    ) -> Result<Self, Invalid> {
        let origin = Origin::builder()
            .provider(Self::PROVIDER)
            .request_class(Self::REQUEST_CLASS)
            .build()?;

        Ok(Self {
            endpoint,
            origin,
            service,
            budget,
        })
    }
}

impl<T> fmt::Debug for SensorCatalogRead<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SensorCatalogRead")
            .field("endpoint_id", &self.endpoint.endpoint_id())
            .field("provider", &self.origin.provider())
            .field("request_class", &self.origin.request_class())
            .finish_non_exhaustive()
    }
}

impl<T: RedfishTransport> Acquire for SensorCatalogRead<T> {
    type Output = SensorCatalog;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<SensorCatalog, AcquisitionFailure> {
        let mut walk = CatalogWalk {
            budget: self.budget,
            started: Instant::now(),
            visited: 0,
            stopped: false,
            catalog: SensorCatalog {
                locations: Vec::new(),
                issues: Vec::new(),
                completeness: Completeness::Complete,
            },
        };

        let acquisition = async {
            if walk.exhausted() {
                return Ok(walk.catalog);
            }

            let bmc = self.service.compat().await?;

            if walk.exhausted() {
                return Ok(walk.catalog);
            }

            let root_location = ODataId::service_root();

            let root: Arc<CatalogRoot> = bmc
                .get(&root_location)
                .await
                .map_err(|error| error.classify())?;

            let chassis = root.chassis.as_ref().ok_or_else(|| {
                AcquisitionFailure::new(AcquisitionFailureClass::Unsupported)
                    .with_retryable(false)
                    .with_detail("service root has no Chassis navigation")
            })?;

            let Some(chassis_location) =
                walk.resolve(&root_location.to_string(), &chassis.odata_id, "Chassis")
            else {
                return Ok(walk.catalog);
            };

            let chassis_locations = walk.list(&bmc, chassis_location, "Chassis").await?;

            let mut collections = HashSet::new();
            let mut sensors = HashSet::new();

            for (location, locator) in chassis_locations {
                if walk.exhausted() {
                    break;
                }

                let chassis: Arc<CatalogChassis> =
                    match bmc.get(&ODataId::from(location.clone())).await {
                        Ok(chassis) => chassis,
                        Err(error) => {
                            walk.failed(&locator, error.classify())?;
                            continue;
                        }
                    };

                let Some(navigation) = chassis.sensors.as_ref() else {
                    continue;
                };

                let Some(collection) = walk.resolve(
                    &location,
                    &navigation.odata_id,
                    &format!("{locator}.Sensors"),
                ) else {
                    continue;
                };

                if !collections.insert(collection.clone()) {
                    continue;
                }

                let locations = walk
                    .list(&bmc, collection, &format!("{locator}.Sensors"))
                    .await?;

                walk.catalog.locations.extend(
                    locations
                        .into_iter()
                        .map(|(location, _)| location)
                        .filter(|location| sensors.insert(location.clone())),
                );
            }

            Ok(walk.catalog)
        };

        with_deadline(self.budget.deadline(), acquisition).await
    }
}

// Only IDs are decoded from navigation and listing members. Expanded readings
// and unused navigation cannot invalidate an otherwise usable discovery link.
#[derive(Deserialize)]
struct SensorLink {
    #[serde(rename = "@odata.id")]
    odata_id: ODataId,
}

impl EntityTypeRef for SensorLink {
    fn odata_id(&self) -> &ODataId {
        &self.odata_id
    }

    fn etag(&self) -> Option<&ODataETag> {
        None
    }
}

#[derive(Deserialize)]
struct CatalogRoot {
    #[serde(rename = "@odata.id")]
    odata_id: ODataId,
    #[serde(rename = "Chassis")]
    chassis: Option<SensorLink>,
}

impl EntityTypeRef for CatalogRoot {
    fn odata_id(&self) -> &ODataId {
        &self.odata_id
    }

    fn etag(&self) -> Option<&ODataETag> {
        None
    }
}

#[derive(Deserialize)]
struct CatalogChassis {
    #[serde(rename = "@odata.id")]
    odata_id: ODataId,
    #[serde(rename = "Sensors")]
    sensors: Option<SensorLink>,
}

impl EntityTypeRef for CatalogChassis {
    fn odata_id(&self) -> &ODataId {
        &self.odata_id
    }

    fn etag(&self) -> Option<&ODataETag> {
        None
    }
}

/// State shared by both collection levels for 1 acquisition only.
struct CatalogWalk {
    budget: WalkBudget,
    started: Instant,
    visited: usize,
    stopped: bool,
    catalog: SensorCatalog,
}

impl CatalogWalk {
    /// Check before protocol work as well as before each listed entry.
    fn exhausted(&mut self) -> bool {
        if self.stopped {
            return true;
        }

        let Some(reason) = self.budget.exhausted(self.visited, self.started.elapsed()) else {
            return false;
        };

        self.stopped = true;
        self.catalog.completeness = Completeness::Partial;

        self.catalog.issues.push(ProjectionIssue::invalid(
            TRUNCATED_WALK_LOCATOR,
            reason.as_str(),
        ));

        true
    }

    /// All links are resolved against the resource that contained them, then
    /// bound to the caller's endpoint. Fragments and non-HTTP schemes cannot
    /// name GET resources. Leading `//` paths could change the transport's request
    /// authority. Diagnostics never include advertised references.
    fn resolve(&mut self, containing: &str, reference: &ODataId, locator: &str) -> Option<String> {
        let reference = reference.to_string();

        let resolved = Url::parse("http://redfish.invalid")
            .and_then(|endpoint| endpoint.join(containing))
            .and_then(|base| base.join(&reference));

        if let Ok(url) = resolved {
            if matches!(url.scheme(), "http" | "https")
                && url.fragment().is_none()
                && !url.path().starts_with("//")
                && !reference.trim().is_empty()
            {
                let mut location = url.path().to_owned();

                if let Some(query) = url.query() {
                    location.push('?');
                    location.push_str(query);
                }

                return Some(location);
            }
        }

        self.catalog.completeness = Completeness::Partial;

        self.catalog.issues.push(ProjectionIssue::invalid(
            locator,
            "unusable resource reference",
        ));

        None
    }

    /// Device answers stay local; endpoint and internal faults discard the artifact.
    fn failed(
        &mut self,
        locator: &str,
        failure: AcquisitionFailure,
    ) -> Result<(), AcquisitionFailure> {
        if !failure.class().is_device_answer() {
            return Err(failure);
        }

        self.catalog.completeness = Completeness::Partial;

        self.catalog.issues.push(ProjectionIssue::invalid(
            locator,
            format!(
                "resource not read ({:?}): {}",
                failure.class(),
                failure.detail().unwrap_or("no detail")
            ),
        ));

        Ok(())
    }

    /// Both chassis and sensor listings share paging, counting, and failure policy.
    /// Locators retain original page/member positions across deduplication.
    async fn list<B: Bmc>(
        &mut self,
        bmc: &B,
        location: String,
        locator: &str,
    ) -> Result<Vec<(String, String)>, AcquisitionFailure>
    where
        B::Error: ClassifyError,
    {
        let mut next = Some(location);
        let mut pages = HashSet::new();
        let mut members = HashSet::new();
        let mut locations = Vec::new();
        let mut listed = 0_u64;
        let mut count = 0_u64;
        let mut page_index = 0;
        let mut terminal = false;

        while let Some(location) = next.take() {
            if self.exhausted() {
                break;
            }

            let page_locator = format!("{locator}.Pages[{page_index}]");

            if !pages.insert(location.clone()) {
                self.failed(
                    &page_locator,
                    AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                        .with_detail("repeated collection page"),
                )?;

                break;
            }

            let page: Arc<Page<SensorLink>> = match bmc.get(&ODataId::from(location.clone())).await
            {
                Ok(page) => page,
                Err(error) => {
                    self.failed(&page_locator, error.classify())?;
                    break;
                }
            };

            count = count.max(page.count.unwrap_or(0));

            if page.members.is_empty() && (page_index > 0 || page.next_link.is_some()) {
                self.failed(
                    &page_locator,
                    AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                        .with_detail("empty continuation page"),
                )?;

                break;
            }

            for (index, member) in page.members.iter().enumerate() {
                if self.exhausted() {
                    break;
                }

                self.visited += 1;
                listed = listed.saturating_add(1);

                let member_locator = format!("{page_locator}.Members[{index}]");

                let Some(member) = self.resolve(&location, member.id(), &member_locator) else {
                    continue;
                };

                if members.insert(member.clone()) {
                    locations.push((member, member_locator));
                }
            }

            terminal = page.next_link.is_none();

            next = page.next_link.as_ref().and_then(|reference| {
                self.resolve(
                    &location,
                    &ODataId::from(reference.clone()),
                    &format!("{page_locator}.Members@odata.nextLink"),
                )
            });

            page_index += 1;
        }

        if terminal && listed < count && !self.stopped {
            self.failed(
                &format!("{locator}.Members@odata.count"),
                AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                    .with_detail("listing shorter than advertised count"),
            )?;
        }

        Ok(locations)
    }
}
