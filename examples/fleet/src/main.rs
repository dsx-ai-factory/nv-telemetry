// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Example daemon: acquires a fleet of Redfish and gNMI endpoints and
//! prints what it collects.
//!
//! The embedder's whole job, end to end: read the fleet file, discover what
//! each Redfish endpoint has, plan and bind its units, hand every endpoint
//! to one collector, and consume the reports until Ctrl-C, then let every
//! endpoint retire.

// A command-line tool reports on stdout and stderr; the workspace lint that
// keeps printing out of library code does not apply to this target.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod config;
mod console;
mod gnmi;
mod redfish;

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::time::Duration;

use futures_util::StreamExt as _;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Value;
use nv_telemetry_orchestration::Collected;
use nv_telemetry_orchestration::Collector;
use nv_telemetry_orchestration::SystemClock;
use tokio::task::JoinSet;

use crate::config::Fleet;
use crate::redfish::Redfish;

const USAGE: &str = "usage: nv-telemetry-fleet --config <fleet.toml> [--verbose]";

/// Discovery retries back off from this to `DISCOVERY_MAX`.
const DISCOVERY_FIRST: Duration = Duration::from_secs(5);
const DISCOVERY_MAX: Duration = Duration::from_mins(5);

fn main() -> ExitCode {
    let mut config = None;
    let mut verbose = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = args.next(),
            "--verbose" => verbose = true,
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::FAILURE;
            }
        }
    }
    let Some(config) = config else {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    };
    let fleet = match Fleet::read(&config) {
        Ok(fleet) => fleet,
        Err(error) => {
            eprintln!("nv-telemetry-fleet: {error}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("nv-telemetry-fleet: runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run(fleet, verbose));
    ExitCode::SUCCESS
}

async fn run(fleet: Fleet, verbose: bool) {
    let fleet: &'static Fleet = Box::leak(Box::new(fleet));
    let (collector, mut reports) = Collector::new(SystemClock::default());

    // gNMI endpoints need no discovery: their leaves are configured.
    for config in &fleet.gnmi {
        match gnmi::units(config, &fleet.defaults) {
            Ok(units) => add(&collector, &config.id, units, "configured"),
            Err(error) => console::note(&config.id, &format!("skipped: {error}")),
        }
    }
    // A Redfish endpoint joins once discovery succeeds; until then it is
    // retried, so an unreachable BMC does not hold up the rest.
    let mut discoveries = JoinSet::new();
    for config in &fleet.redfish {
        let redfish = match Redfish::new(config, &fleet.defaults) {
            Ok(redfish) => redfish,
            Err(error) => {
                console::note(&config.id, &format!("skipped: {error}"));
                continue;
            }
        };
        let collector = collector.clone();
        discoveries.spawn(async move {
            let mut backoff = DISCOVERY_FIRST;
            loop {
                match redfish.units().await {
                    Ok((units, found)) => {
                        add(&collector, redfish.id(), units, &found);
                        return;
                    }
                    Err(error) => {
                        console::note(
                            redfish.id(),
                            &format!("discovery failed, retrying in {backoff:?}: {error}"),
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(DISCOVERY_MAX);
                    }
                }
            }
        });
    }

    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    let mut stopping = false;
    loop {
        tokio::select! {
            collected = reports.next() => match collected {
                Some(Collected::Report(report)) => console::report(&report, verbose),
                Some(Collected::Retired(endpoint)) => {
                    console::note(endpoint.endpoint_id(), "retired");
                }
                // Every endpoint has retired after the shutdown.
                None => break,
            },
            _ = &mut interrupt, if !stopping => {
                stopping = true;
                println!("stopping: delivering what is in flight");
                discoveries.abort_all();
                collector.shutdown();
            }
        }
    }
}

fn add(
    collector: &Collector,
    id: &str,
    units: nv_telemetry_orchestration::EndpointUnits,
    found: &str,
) {
    match collector.add(units) {
        Ok(()) => console::note(id, &format!("collecting: {found}")),
        Err(error) => console::note(id, &format!("refused: {error}")),
    }
}

pub(crate) fn endpoint_context(
    id: &str,
    labels: &BTreeMap<String, String>,
) -> Result<EndpointContext, String> {
    let mut builder = EndpointContext::builder().endpoint_id(id);
    if !labels.is_empty() {
        let attributes = labels
            .iter()
            .map(|(name, label)| Ok((name.clone(), Value::string(label.clone())?)))
            .collect::<Result<_, nv_telemetry_model::Invalid>>()
            .map_err(|error| format!("labels: {error}"))?;
        builder = builder.attributes(attributes);
    }
    builder
        .build()
        .map_err(|error| format!("endpoint: {error}"))
}
