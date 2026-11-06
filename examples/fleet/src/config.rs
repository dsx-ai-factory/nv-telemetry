// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The fleet file: what to acquire, from which endpoints, how often.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::RatePolicy;
use nv_telemetry_orchestration::ReconnectPolicy;
use serde::Deserialize;
use serde::Deserializer;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fleet {
    #[serde(default)]
    pub(crate) defaults: Defaults,
    #[serde(default)]
    pub(crate) redfish: Vec<RedfishEndpoint>,
    #[serde(default)]
    pub(crate) gnmi: Vec<GnmiEndpoint>,
}

impl Fleet {
    pub(crate) fn read(path: &str) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
        toml::from_str(&text).map_err(|error| format!("{path}: {error}"))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Defaults {
    #[serde(default)]
    pub(crate) policy: Policy,
    #[serde(default)]
    pub(crate) intervals: Intervals,
}

/// One endpoint's admission and reconnect settings; unset fields keep the
/// library's defaults.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    max_in_flight: Option<NonZeroU32>,
    burst: Option<u64>,
    /// Requests per `per`.
    refill: Option<u64>,
    #[serde(default, deserialize_with = "duration")]
    per: Option<Duration>,
    #[serde(default, deserialize_with = "duration")]
    first_retry: Option<Duration>,
    #[serde(default, deserialize_with = "duration")]
    max_retry: Option<Duration>,
}

impl Policy {
    /// `self` with every field it leaves unset taken from `defaults`.
    pub(crate) fn over(&self, defaults: &Self) -> Self {
        Self {
            max_in_flight: self.max_in_flight.or(defaults.max_in_flight),
            burst: self.burst.or(defaults.burst),
            refill: self.refill.or(defaults.refill),
            per: self.per.or(defaults.per),
            first_retry: self.first_retry.or(defaults.first_retry),
            max_retry: self.max_retry.or(defaults.max_retry),
        }
    }

    pub(crate) fn endpoint(&self) -> EndpointPolicy {
        let mut rate = RatePolicy::default();
        if let Some(burst) = self.burst {
            rate = rate.with_burst(burst);
        }
        if let Some(refill) = self.refill {
            rate = rate.with_refill(refill, self.per.unwrap_or(Duration::from_secs(1)));
        }
        let mut policy = EndpointPolicy::default().with_rate(rate);
        if let Some(max_in_flight) = self.max_in_flight {
            policy = policy.with_max_in_flight(max_in_flight);
        }
        policy
    }

    pub(crate) fn reconnect(&self) -> ReconnectPolicy {
        let mut policy = ReconnectPolicy::default();
        if let Some(first_retry) = self.first_retry {
            policy = policy.with_first_retry(first_retry);
        }
        if let Some(max_retry) = self.max_retry {
            policy = policy.with_max_retry(max_retry);
        }
        policy
    }
}

/// How often each Redfish domain is read.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intervals {
    #[serde(default, deserialize_with = "duration")]
    sensors: Option<Duration>,
    #[serde(default, deserialize_with = "duration")]
    chassis: Option<Duration>,
    #[serde(default, deserialize_with = "duration")]
    firmware: Option<Duration>,
    #[serde(default, deserialize_with = "duration")]
    logs: Option<Duration>,
}

impl Intervals {
    pub(crate) fn sensors(&self, defaults: &Self) -> Duration {
        self.sensors
            .or(defaults.sensors)
            .unwrap_or(Duration::from_secs(30))
    }

    pub(crate) fn chassis(&self, defaults: &Self) -> Duration {
        self.chassis
            .or(defaults.chassis)
            .unwrap_or(Duration::from_mins(5))
    }

    pub(crate) fn firmware(&self, defaults: &Self) -> Duration {
        self.firmware
            .or(defaults.firmware)
            .unwrap_or(Duration::from_mins(30))
    }

    pub(crate) fn logs(&self, defaults: &Self) -> Duration {
        self.logs
            .or(defaults.logs)
            .unwrap_or(Duration::from_mins(1))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RedfishEndpoint {
    pub(crate) id: String,
    pub(crate) url: String,
    pub(crate) username: String,
    /// The environment variable holding the password.
    pub(crate) password_env: Option<String>,
    /// Accept a self-signed certificate.
    #[serde(default)]
    pub(crate) insecure: bool,
    #[serde(default)]
    pub(crate) labels: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) domains: Domains,
    #[serde(default)]
    pub(crate) intervals: Intervals,
    #[serde(default)]
    pub(crate) policy: Policy,
}

/// The Redfish domains to acquire; all of them unless switched off.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct Domains {
    pub(crate) sensors: bool,
    pub(crate) chassis: bool,
    pub(crate) firmware: bool,
    pub(crate) logs: bool,
    pub(crate) events: bool,
}

impl Default for Domains {
    fn default() -> Self {
        Self {
            sensors: true,
            chassis: true,
            firmware: true,
            logs: true,
            events: true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GnmiEndpoint {
    pub(crate) id: String,
    /// `http://` connects in plaintext, `https://` over TLS with the
    /// system's roots.
    pub(crate) address: String,
    pub(crate) username: Option<String>,
    pub(crate) password_env: Option<String>,
    #[serde(default)]
    pub(crate) encoding: Encoding,
    pub(crate) origin: Option<String>,
    pub(crate) target: Option<String>,
    #[serde(default)]
    pub(crate) labels: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) policy: Policy,
    /// Leaves streamed over one Subscribe call.
    #[serde(default)]
    pub(crate) subscribe: Vec<Leaf>,
    /// Snapshots, each read by its own Get call on its own interval.
    #[serde(default)]
    pub(crate) get: Vec<Get>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Encoding {
    #[default]
    Json,
    JsonIetf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Get {
    pub(crate) name: String,
    #[serde(deserialize_with = "required_duration")]
    pub(crate) interval: Duration,
    pub(crate) leaves: Vec<Leaf>,
}

/// One leaf and what it becomes.
///
/// `path` is slash-separated, with `[key=value]` selecting one list entry
/// and `[key=*]` every entry. The subject's id is the wildcard key's value
/// in each received path, or `subject_id` when the path has no wildcard.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Leaf {
    pub(crate) path: String,
    /// Subscribe only: `sample` (with `interval`), `on_change`, or
    /// `target_defined`, the default.
    #[serde(default)]
    pub(crate) mode: Mode,
    #[serde(default, deserialize_with = "duration")]
    pub(crate) interval: Option<Duration>,
    #[serde(default, deserialize_with = "duration")]
    pub(crate) heartbeat: Option<Duration>,
    #[serde(default)]
    pub(crate) suppress_redundant: bool,
    /// The subject kind, e.g. `interface`.
    pub(crate) subject: String,
    pub(crate) subject_id: Option<String>,
    /// The reading's facet, or the state's name.
    pub(crate) facet: String,
    pub(crate) unit: Option<String>,
    #[serde(rename = "type")]
    pub(crate) value: ValueType,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Mode {
    #[default]
    TargetDefined,
    Sample,
    OnChange,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ValueType {
    Uint,
    Int,
    Double,
    String,
    Bool,
}

fn duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Duration>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|text| parse_duration(&text).map_err(serde::de::Error::custom))
        .transpose()
}

fn required_duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    parse_duration(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

/// `500ms`, `30s`, `5m`, `2h`.
fn parse_duration(text: &str) -> Result<Duration, String> {
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("`{text}` needs a unit: ms, s, m, or h"))?;
    let (amount, unit) = text.split_at(split);
    let amount: u64 = amount
        .parse()
        .map_err(|_| format!("`{text}` is not a duration"))?;
    match unit {
        "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => Ok(Duration::from_mins(amount)),
        "h" => Ok(Duration::from_hours(amount)),
        _ => Err(format!("`{text}`: the unit is one of ms, s, m, h")),
    }
}
