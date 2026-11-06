// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A Redfish endpoint: what it has, found by following its links, and the
//! units that read it.
//!
//! The library reads targets it is given and has no catalog yet, so
//! discovery is the embedder's: it runs once, and a sensor that appears on a
//! live BMC later is not read.

use std::collections::BTreeSet;
use std::sync::Arc;

use nv_redfish::bmc_http::reqwest::Client;
use nv_redfish::bmc_http::reqwest::ClientParams;
use nv_redfish::bmc_http::BmcCredentials;
use nv_redfish::bmc_http::CacheSettings;
use nv_redfish::bmc_http::HttpBmc;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_orchestration::plan;
use nv_telemetry_orchestration::EndpointUnits;
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamUnit;
use nv_telemetry_probe::BmcTransport;
use nv_telemetry_redfish::ChassisRead;
use nv_telemetry_redfish::EventStream;
use nv_telemetry_redfish::FirmwareRead;
use nv_telemetry_redfish::LogRead;
use nv_telemetry_redfish::RedfishService;
use nv_telemetry_redfish::RedfishTransport;
use nv_telemetry_redfish::SensorRead;
use nv_telemetry_redfish::TransportFault;
use serde_json::Value;
use url::Url;

use crate::config::Defaults;
use crate::config::RedfishEndpoint;
use crate::console;
use crate::endpoint_context;

type Transport = BmcTransport<HttpBmc<Client>>;

/// The collections that hold log services.
const LOG_OWNERS: [&str; 3] = ["Managers", "Systems", "Chassis"];

pub(crate) struct Redfish<'a> {
    config: &'a RedfishEndpoint,
    defaults: &'a Defaults,
    endpoint: EndpointContext,
    /// What the units read through; discovery shares its client, so both
    /// share one connection pool and `ETag` cache.
    service: Arc<RedfishService<Transport>>,
    discovery: Transport,
}

/// What discovery found.
#[derive(Default)]
struct Targets {
    chassis: BTreeSet<String>,
    sensors: BTreeSet<String>,
    update_service: Option<String>,
    log_services: BTreeSet<String>,
    events: bool,
}

impl<'a> Redfish<'a> {
    pub(crate) fn new(config: &'a RedfishEndpoint, defaults: &'a Defaults) -> Result<Self, String> {
        let endpoint = endpoint_context(&config.id, &config.labels)?;
        let base = Url::parse(&config.url).map_err(|error| format!("url: {error}"))?;
        let client = if config.insecure {
            Client::with_params(ClientParams::new().accept_invalid_certs(true))
        } else {
            Client::new()
        }
        .map_err(|error| format!("http client: {error}"))?;
        let password = config
            .password_env
            .as_deref()
            .map(|name| std::env::var(name).map_err(|_| format!("{name} is not set")))
            .transpose()?;
        let credentials = BmcCredentials::username_password(config.username.clone(), password);
        let bmc = Arc::new(HttpBmc::new(
            client,
            base,
            credentials,
            CacheSettings::default(),
        ));
        Ok(Self {
            config,
            defaults,
            endpoint,
            service: Arc::new(RedfishService::new(BmcTransport::new(Arc::clone(&bmc)))),
            discovery: BmcTransport::new(bmc),
        })
    }

    pub(crate) fn id(&self) -> &str {
        &self.config.id
    }

    /// Discovers the endpoint and binds a unit to every planned read.
    pub(crate) async fn units(&self) -> Result<(EndpointUnits, String), String> {
        let targets = self.discover().await?;
        let summary = format!(
            "{} chassis, {} sensors, {} log services, firmware {}, events {}",
            targets.chassis.len(),
            targets.sensors.len(),
            targets.log_services.len(),
            yes(targets.update_service.is_some()),
            yes(targets.events),
        );

        let intervals = &self.config.intervals;
        let defaults = &self.defaults.intervals;
        let poll = |class: &str, target: &String, cadence| {
            PollNeed::new(self.endpoint.clone(), class, target.clone(), cadence)
        };
        let polls = targets
            .sensors
            .iter()
            .map(|target| {
                poll(
                    SensorRead::<()>::REQUEST_CLASS,
                    target,
                    intervals.sensors(defaults),
                )
            })
            .chain(targets.chassis.iter().map(|target| {
                poll(
                    ChassisRead::<()>::REQUEST_CLASS,
                    target,
                    intervals.chassis(defaults),
                )
            }))
            .chain(targets.log_services.iter().map(|target| {
                poll(
                    LogRead::<()>::REQUEST_CLASS,
                    target,
                    intervals.logs(defaults),
                )
            }))
            .chain(targets.update_service.iter().map(|target| {
                poll(
                    FirmwareRead::<()>::REQUEST_CLASS,
                    target,
                    intervals.firmware(defaults),
                )
            }));
        let streams = targets
            .events
            .then(|| StreamNeed::new(self.endpoint.clone(), EventStream::<()>::REQUEST_CLASS));
        let plan = plan(
            Needs::default().with_polls(polls).with_streams(streams),
            &[
                SensorRead::<()>::declaration(),
                ChassisRead::<()>::declaration(),
                LogRead::<()>::declaration(),
                FirmwareRead::<()>::declaration(),
                EventStream::<()>::declaration(),
            ],
        )
        .map_err(|error| format!("plan: {error}"))?;

        let service = &self.service;
        let polls = plan.polls().iter().map(|planned| {
            let endpoint = planned.endpoint().clone();
            let target = planned.target();
            let unit = Arc::clone(service);
            match planned.origin().request_class() {
                SensorRead::<Transport>::REQUEST_CLASS => PollUnit::new(
                    planned.clone(),
                    Arc::new(SensorRead::new(endpoint, target, unit)),
                ),
                ChassisRead::<Transport>::REQUEST_CLASS => PollUnit::new(
                    planned.clone(),
                    Arc::new(ChassisRead::new(endpoint, target, unit)),
                ),
                LogRead::<Transport>::REQUEST_CLASS => PollUnit::new(
                    planned.clone(),
                    Arc::new(LogRead::new(endpoint, target, unit)),
                ),
                FirmwareRead::<Transport>::REQUEST_CLASS => PollUnit::new(
                    planned.clone(),
                    Arc::new(FirmwareRead::new(endpoint, target, unit)),
                ),
                _ => unreachable!("the plan selects only declared providers"),
            }
        });
        let policy = self.config.policy.over(&self.defaults.policy);
        let streams = plan.streams().iter().map(|planned| {
            let unit = EventStream::new(planned.endpoint().clone(), Arc::clone(service));
            StreamUnit::new(planned.clone(), Arc::new(unit), policy.reconnect())
        });
        let units = EndpointUnits::new(policy.endpoint())
            .with_polls(polls.collect::<Vec<_>>())
            .with_streams(streams.collect::<Vec<_>>());
        Ok((units, summary))
    }

    /// Follows the service root's links to every target the enabled
    /// domains read. Links only: no reading is taken here.
    ///
    /// A document the service refuses is skipped, as BMCs leave parts of
    /// the tree unimplemented; an unreachable service fails discovery, so
    /// it is retried.
    async fn discover(&self) -> Result<Targets, String> {
        let domains = &self.config.domains;
        let root = self
            .get("/redfish/v1")
            .await?
            .ok_or("the service root was refused")?;
        let mut targets = Targets::default();

        if domains.chassis || domains.sensors {
            for chassis in self.members(link(&root, "Chassis")).await? {
                if domains.sensors {
                    if let Some(document) = self.get(&chassis).await? {
                        targets
                            .sensors
                            .extend(self.members(link(&document, "Sensors")).await?);
                    }
                }
                if domains.chassis {
                    targets.chassis.insert(chassis);
                }
            }
        }
        if domains.logs {
            for owners in LOG_OWNERS {
                for owner in self.members(link(&root, owners)).await? {
                    if let Some(document) = self.get(&owner).await? {
                        targets
                            .log_services
                            .extend(self.members(link(&document, "LogServices")).await?);
                    }
                }
            }
        }
        if domains.firmware {
            targets.update_service = link(&root, "UpdateService");
        }
        if let Some(service) = link(&root, "EventService").filter(|_| domains.events) {
            if let Some(document) = self.get(&service).await? {
                targets.events = document.get("ServerSentEventUri").is_some();
            }
        }
        Ok(targets)
    }

    /// The document at `uri`; `None` when the service answered but would
    /// not serve it.
    async fn get(&self, uri: &str) -> Result<Option<Value>, String> {
        match self.discovery.get(uri).await {
            Ok(document) => Ok(Some(document)),
            Err(fault @ (TransportFault::Status(_) | TransportFault::Body)) => {
                console::note(self.id(), &format!("discovery skipped {uri}: {fault:?}"));
                Ok(None)
            }
            Err(fault) => Err(format!("GET {uri}: {fault:?}")),
        }
    }

    /// A collection's members across every page it serves; none when the
    /// link is absent.
    async fn members(&self, collection: Option<String>) -> Result<Vec<String>, String> {
        let mut members = Vec::new();
        let mut page = collection;
        while let Some(uri) = page {
            let Some(document) = self.get(&uri).await? else {
                break;
            };
            if let Some(listed) = document.get("Members").and_then(Value::as_array) {
                members.extend(listed.iter().filter_map(|member| {
                    member
                        .get("@odata.id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                }));
            }
            page = document
                .get("Members@odata.nextLink")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        Ok(members)
    }
}

/// The `@odata.id` a navigation property points at.
fn link(document: &Value, property: &str) -> Option<String> {
    document
        .get(property)?
        .get("@odata.id")?
        .as_str()
        .map(str::to_owned)
}

fn yes(present: bool) -> &'static str {
    if present {
        "yes"
    } else {
        "no"
    }
}
