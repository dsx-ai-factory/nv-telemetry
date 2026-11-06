// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A gNMI endpoint: one Subscribe call for its streamed leaves, one Get
//! poll per configured snapshot.

use std::sync::Arc;

use nv_telemetry_gnmi::ConfiguredGet;
use nv_telemetry_gnmi::ConfiguredSubscription;
use nv_telemetry_gnmi::Encoding as GnmiEncoding;
use nv_telemetry_gnmi::GetSpec;
use nv_telemetry_gnmi::IdentityValue;
use nv_telemetry_gnmi::Mapping;
use nv_telemetry_gnmi::NumericType;
use nv_telemetry_gnmi::PathElement;
use nv_telemetry_gnmi::ReadingTemplate;
use nv_telemetry_gnmi::StateIdentity;
use nv_telemetry_gnmi::StateTemplate;
use nv_telemetry_gnmi::SubjectTemplate;
use nv_telemetry_gnmi::SubscriptionPath;
use nv_telemetry_gnmi::SubscriptionRequest;
use nv_telemetry_gnmi::SubscriptionSpec;
use nv_telemetry_model::SignalDescriptor;
use nv_telemetry_model::SignalKey;
use nv_telemetry_model::Subject;
use nv_telemetry_orchestration::plan;
use nv_telemetry_orchestration::EndpointUnits;
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamUnit;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::Request;
use tonic::Status;

use crate::config::Defaults;
use crate::config::Encoding;
use crate::config::GnmiEndpoint;
use crate::config::Leaf;
use crate::config::Mode;
use crate::config::ValueType;
use crate::endpoint_context;

type Transport = InterceptedService<Channel, Credentials>;

/// The `username` and `password` metadata every call carries, when the
/// endpoint has credentials.
#[derive(Clone)]
pub(crate) struct Credentials(Option<(AsciiMetadataValue, AsciiMetadataValue)>);

impl Interceptor for Credentials {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some((username, password)) = &self.0 {
            let metadata = request.metadata_mut();
            metadata.insert("username", username.clone());
            metadata.insert("password", password.clone());
        }
        Ok(request)
    }
}

/// The endpoint's units. The channel connects on first use, so a target
/// that is down shows up as failed acquisitions, not as a startup error.
pub(crate) fn units(config: &GnmiEndpoint, defaults: &Defaults) -> Result<EndpointUnits, String> {
    let endpoint = endpoint_context(&config.id, &config.labels)?;
    let transport = transport(config)?;

    let stream = (!config.subscribe.is_empty()).then(|| {
        StreamNeed::new(
            endpoint.clone(),
            ConfiguredSubscription::<()>::REQUEST_CLASS,
        )
    });
    let polls = config.get.iter().map(|get| {
        PollNeed::new(
            endpoint.clone(),
            ConfiguredGet::<()>::REQUEST_CLASS,
            get.name.clone(),
            get.interval,
        )
    });
    let plan = plan(
        Needs::default().with_polls(polls).with_streams(stream),
        &[
            ConfiguredSubscription::<()>::declaration(),
            ConfiguredGet::<()>::declaration(),
        ],
    )
    .map_err(|error| format!("plan: {error}"))?;

    let policy = config.policy.over(&defaults.policy);
    let mut streams = Vec::new();
    for planned in plan.streams() {
        let spec = subscription(config)?;
        let unit = ConfiguredSubscription::new(endpoint.clone(), transport.clone(), spec)
            .map_err(|error| format!("subscription: {error}"))?;
        streams.push(StreamUnit::new(
            planned.clone(),
            Arc::new(unit),
            policy.reconnect(),
        ));
    }
    let mut polls = Vec::new();
    for planned in plan.polls() {
        let get = config
            .get
            .iter()
            .find(|get| get.name == planned.target())
            .expect("every planned Get is a configured one");
        let leaves = get
            .leaves
            .iter()
            .map(|leaf| Ok((parse_path(&leaf.path)?, leaf)))
            .collect::<Result<Vec<_>, String>>()
            .map_err(|error| format!("get `{}`: {error}", get.name))?;
        let paths = leaves.iter().map(|(path, _)| path.path.clone()).collect();
        let mappings = leaves
            .iter()
            .map(|(path, leaf)| mapping(path, leaf))
            .collect::<Result<_, _>>()
            .map_err(|error| format!("get `{}`: {error}", get.name))?;
        let mut spec = GetSpec::new(paths, mappings)
            .map_err(|error| format!("get `{}`: {error:?}", get.name))?;
        if let Some(origin) = &config.origin {
            spec = spec.with_origin(origin);
        }
        if let Some(target) = &config.target {
            spec = spec.with_target(target);
        }
        let unit = ConfiguredGet::new(endpoint.clone(), transport.clone(), spec)
            .map_err(|error| format!("get `{}`: {error}", get.name))?;
        polls.push(PollUnit::new(planned.clone(), Arc::new(unit)));
    }
    Ok(EndpointUnits::new(policy.endpoint())
        .with_polls(polls)
        .with_streams(streams))
}

fn transport(config: &GnmiEndpoint) -> Result<Transport, String> {
    let mut endpoint = Endpoint::from_shared(config.address.clone())
        .map_err(|error| format!("address: {error}"))?;
    if config.address.starts_with("https://") {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|error| format!("tls: {error}"))?;
    }
    let credentials = match &config.username {
        None => None,
        Some(username) => {
            let password = match &config.password_env {
                Some(name) => std::env::var(name).map_err(|_| format!("{name} is not set"))?,
                None => String::new(),
            };
            let value = |text: &str| {
                AsciiMetadataValue::try_from(text).map_err(|error| format!("credentials: {error}"))
            };
            Some((value(username)?, value(&password)?))
        }
    };
    Ok(InterceptedService::new(
        endpoint.connect_lazy(),
        Credentials(credentials),
    ))
}

fn subscription(config: &GnmiEndpoint) -> Result<SubscriptionSpec, String> {
    let mut requests = Vec::new();
    let mut mappings = Vec::new();
    for leaf in &config.subscribe {
        let path = parse_path(&leaf.path)?;
        let request = match leaf.mode {
            Mode::TargetDefined => SubscriptionRequest::target_defined(path.path.clone()),
            Mode::OnChange => SubscriptionRequest::on_change(path.path.clone()),
            Mode::Sample => {
                let interval = leaf
                    .interval
                    .ok_or_else(|| format!("`{}`: sample mode needs an interval", leaf.path))?;
                SubscriptionRequest::sample(path.path.clone(), interval)
            }
        };
        let request = match leaf.heartbeat {
            Some(heartbeat) => request.with_heartbeat_interval(heartbeat),
            None => request,
        };
        requests.push(request.with_suppress_redundant(leaf.suppress_redundant));
        mappings.push(mapping(&path, leaf)?);
    }
    let mut spec = SubscriptionSpec::from_requests(requests, mappings)
        .map_err(|error| format!("subscription: {error:?}"))?
        .with_encoding(match config.encoding {
            Encoding::Json => GnmiEncoding::Json,
            Encoding::JsonIetf => GnmiEncoding::JsonIetf,
        });
    if let Some(origin) = &config.origin {
        spec = spec.with_origin(origin);
    }
    if let Some(target) = &config.target {
        spec = spec.with_target(target);
    }
    Ok(spec)
}

/// A wildcard path maps every entry, each named by its key; a path
/// without one maps the one leaf to the configured subject.
fn mapping(path: &ParsedPath, leaf: &Leaf) -> Result<Mapping, String> {
    let Some((element, key)) = &path.wildcard else {
        return single(path, leaf);
    };
    let subject = SubjectTemplate::new(
        leaf.subject.clone(),
        IdentityValue::path_key(*element, key.clone()),
    );
    let reading = |numeric| {
        let template = ReadingTemplate::new(numeric).with_facet(leaf.facet.clone());
        let template = match &leaf.unit {
            Some(unit) => template.with_unit(unit.clone()),
            None => template,
        };
        Mapping::collection_reading(path.path.clone(), subject.clone(), template)
    };
    let state = StateTemplate::new(leaf.facet.clone());
    Ok(match leaf.value {
        ValueType::Uint => reading(NumericType::Uint),
        ValueType::Int => reading(NumericType::Int),
        ValueType::Double => reading(NumericType::Double),
        ValueType::String => Mapping::collection_string_state(path.path.clone(), subject, state),
        ValueType::Bool => Mapping::collection_boolean_state(path.path.clone(), subject, state),
    })
}

fn single(path: &ParsedPath, leaf: &Leaf) -> Result<Mapping, String> {
    let invalid = |error: nv_telemetry_model::Invalid| format!("`{}`: {error}", leaf.path);
    let id = leaf.subject_id.as_deref().ok_or_else(|| {
        format!(
            "`{}`: a path without `[key=*]` needs a subject_id",
            leaf.path
        )
    })?;
    let subject = Subject::builder()
        .kind(leaf.subject.clone())
        .id(id)
        .build()
        .map_err(invalid)?;
    let reading = |numeric| {
        let key = SignalKey::builder()
            .subject(subject.clone())
            .facet(leaf.facet.clone())
            .build()
            .map_err(invalid)?;
        let descriptor = SignalDescriptor::builder().key(key);
        let descriptor = match &leaf.unit {
            Some(unit) => descriptor.unit(unit.clone()),
            None => descriptor,
        };
        Ok(Mapping::new(
            path.path.clone(),
            descriptor.build().map_err(invalid)?,
            numeric,
        ))
    };
    let state = StateIdentity::new(subject.clone(), leaf.facet.clone());
    match leaf.value {
        ValueType::Uint => reading(NumericType::Uint),
        ValueType::Int => reading(NumericType::Int),
        ValueType::Double => reading(NumericType::Double),
        ValueType::String => Ok(Mapping::string_state(path.path.clone(), state)),
        ValueType::Bool => Ok(Mapping::boolean_state(path.path.clone(), state)),
    }
}

struct ParsedPath {
    path: SubscriptionPath,
    /// The element and key whose value names the subject.
    wildcard: Option<(usize, String)>,
}

/// `interfaces/interface[name=*]/state/counters/in-octets`.
fn parse_path(text: &str) -> Result<ParsedPath, String> {
    let mut elements = Vec::new();
    let mut wildcard = None;
    for (index, segment) in text.split('/').filter(|s| !s.is_empty()).enumerate() {
        let (name, mut selectors) = segment.split_once('[').unwrap_or((segment, ""));
        let mut element = PathElement::new(name);
        while !selectors.is_empty() {
            let (selector, rest) = selectors
                .split_once(']')
                .ok_or_else(|| format!("`{text}`: unclosed `[`"))?;
            let (key, value) = selector
                .split_once('=')
                .ok_or_else(|| format!("`{text}`: `[{selector}]` is not `[key=value]`"))?;
            if value == "*" {
                if wildcard.replace((index, key.to_owned())).is_some() {
                    return Err(format!("`{text}`: at most one `[key=*]`"));
                }
            } else {
                element = element.with_key(key, value);
            }
            selectors = rest.strip_prefix('[').unwrap_or(rest);
        }
        elements.push(element);
    }
    Ok(ParsedPath {
        path: SubscriptionPath::new(elements),
        wildcard,
    })
}
