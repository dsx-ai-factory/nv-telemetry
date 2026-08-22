// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Demo embedder: polls one Redfish endpoint's sensors and chassis on a
//! cadence and
//! prints the three output streams — batches, statuses, and issues — one
//! tagged line each.
//!
//! This binary is the embedder role the architecture assigns outside the
//! library: it owns the endpoint list, the driving loop, and the timer.
//! `SleepUntil` is a hint delivered once, so the loop retains the latest
//! deadline and races the runtime against a sleep — the canonical driver
//! shape.

// A command-line tool reports on stdout and stderr; the workspace lint that
// keeps printing out of library code does not apply to this target.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use nv_redfish::bmc_http::reqwest::Client;
use nv_redfish::bmc_http::reqwest::ClientParams;
use nv_redfish::bmc_http::BmcCredentials;
use nv_redfish::bmc_http::CacheSettings;
use nv_redfish::bmc_http::HttpBmc;
use nv_redfish_bmc_mock::Expect;
use nv_redfish_dispatcher::ClockConfig;
use nv_redfish_dispatcher::Runtime;
use nv_redfish_dispatcher::RuntimeConfig;
use nv_redfish_dispatcher::RuntimeOutput;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_orchestration::endpoint_subtree;
use nv_telemetry_orchestration::plan;
use nv_telemetry_orchestration::AcquisitionReport;
use nv_telemetry_orchestration::EndpointFault;
use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::PollMeta;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::SystemClock;
use nv_telemetry_redfish::ChassisRead;
use nv_telemetry_redfish::SensorRead;
use url::Url;

const USAGE: &str = "\
usage: nv-telemetry-probe --mode mock|http --endpoint-id <id>
           [--sensor <odata-id> ...] [--chassis <odata-id> ...]
           [--cadence-ms <ms>] [--count <n>] [--base-url <url>] [--insecure]

  mock    poll the in-process BMC mock, replaying fixtures/
  http    poll a live Redfish service at --base-url; credentials come
          from PROBE_USERNAME and PROBE_PASSWORD; --insecure accepts
          self-signed BMC certificates

Prints one tagged line per stream item: batch, issues, status.
";

struct Args {
    mode: Mode,
    endpoint_id: String,
    sensors: Vec<String>,
    chassis: Vec<String>,
    cadence: Duration,
    count: usize,
    base_url: Option<String>,
    insecure: bool,
}

#[derive(PartialEq, Eq)]
enum Mode {
    Mock,
    Http,
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("nv-telemetry-probe: {error}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds");
    match runtime.block_on(run(&args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("nv-telemetry-probe: {error}");
            ExitCode::FAILURE
        }
    }
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut mode = None;
    let mut endpoint_id = None;
    let mut sensors = Vec::new();
    let mut chassis = Vec::new();
    let mut cadence = Duration::from_secs(5);
    let mut count = 10;
    let mut base_url = None;
    let mut insecure = false;

    while let Some(flag) = args.next() {
        let mut value = |flag: &str| args.next().ok_or(format!("`{flag}` needs a value"));
        match flag.as_str() {
            "--mode" => {
                mode = Some(match value("--mode")?.as_str() {
                    "mock" => Mode::Mock,
                    "http" => Mode::Http,
                    other => return Err(format!("unknown mode `{other}`")),
                });
            }
            "--endpoint-id" => endpoint_id = Some(value("--endpoint-id")?),
            "--sensor" => sensors.push(value("--sensor")?),
            "--chassis" => chassis.push(value("--chassis")?),
            "--cadence-ms" => {
                let ms = value("--cadence-ms")?
                    .parse()
                    .map_err(|_| "`--cadence-ms` needs milliseconds".to_owned())?;
                cadence = Duration::from_millis(ms);
            }
            "--count" => {
                count = value("--count")?
                    .parse()
                    .map_err(|_| "`--count` needs a number".to_owned())?;
            }
            "--base-url" => base_url = Some(value("--base-url")?),
            "--insecure" => insecure = true,
            other => return Err(format!("unknown argument `{other}`")),
        }
    }

    let mode = mode.ok_or("`--mode` is required")?;
    if mode == Mode::Http && base_url.is_none() {
        return Err("http mode needs `--base-url`".to_owned());
    }
    if sensors.is_empty() && chassis.is_empty() {
        return Err("at least one `--sensor` or `--chassis` is required".to_owned());
    }
    Ok(Args {
        mode,
        endpoint_id: endpoint_id.ok_or("`--endpoint-id` is required")?,
        sensors,
        chassis,
        cadence,
        count,
        base_url,
        insecure,
    })
}

async fn run(args: &Args) -> Result<(), String> {
    if args.count == 0 {
        return Ok(());
    }
    let endpoint = EndpointContext::builder()
        .endpoint_id(&args.endpoint_id)
        .build()
        .map_err(|error| format!("endpoint id: {error}"))?;

    let clock = SystemClock::default();
    let needs = args
        .sensors
        .iter()
        .map(|sensor| {
            PollNeed::new(
                endpoint.clone(),
                SensorRead::<()>::REQUEST_CLASS,
                sensor.clone(),
                args.cadence,
            )
        })
        .chain(args.chassis.iter().map(|chassis| {
            PollNeed::new(
                endpoint.clone(),
                ChassisRead::<()>::REQUEST_CLASS,
                chassis.clone(),
                args.cadence,
            )
        }))
        .collect();
    let plan = plan(
        needs,
        &[
            SensorRead::<()>::declaration(),
            ChassisRead::<()>::declaration(),
        ],
    )
    .map_err(|error| format!("plan: {error}"))?;

    match args.mode {
        Mode::Mock => {
            let bmc = Arc::new(nv_redfish_bmc_mock::Bmc::<nv_redfish_bmc_mock::Error>::default());
            let sensor_fixture = include_str!("../fixtures/sensor.json");
            let chassis_fixture = include_str!("../fixtures/chassis.json");
            // Mock expectations are one-shot AND strict-FIFO, so priming
            // follows dispatch order: the ring visits targets in needs
            // order each round. Target-major priming would mismatch the
            // moment there are two targets.
            for _ in 0..args.count {
                for sensor in &args.sensors {
                    bmc.expect(Expect::get(sensor, sensor_fixture));
                }
                for chassis in &args.chassis {
                    bmc.expect(Expect::get(chassis, chassis_fixture));
                }
            }
            let units = units(&plan, &bmc, clock);
            drive(&endpoint, units, clock, args).await
        }
        Mode::Http => {
            let base = args.base_url.as_deref().expect("checked at parse time");
            let base = Url::parse(base).map_err(|error| format!("base url: {error}"))?;
            let client = if args.insecure {
                Client::with_params(ClientParams::new().accept_invalid_certs(true))
            } else {
                Client::new()
            }
            .map_err(|error| format!("http client: {error}"))?;
            let credentials = credentials_from_env()?;
            let bmc = Arc::new(HttpBmc::new(
                client,
                base,
                credentials,
                CacheSettings::default(),
            ));
            let units = units(&plan, &bmc, clock);
            drive(&endpoint, units, clock, args).await
        }
    }
}

fn credentials_from_env() -> Result<BmcCredentials, String> {
    let username =
        std::env::var("PROBE_USERNAME").map_err(|_| "PROBE_USERNAME is not set".to_owned())?;
    let password = std::env::var("PROBE_PASSWORD").ok();
    Ok(BmcCredentials::username_password(username, password))
}

/// The class-dispatch point: static wiring from each planned poll's
/// request class to the provider that declared it — the embedder's role
/// until provider registries exist.
fn units<B>(
    plan: &nv_telemetry_orchestration::Plan,
    bmc: &Arc<B>,
    clock: SystemClock,
) -> Vec<PollUnit>
where
    B: nv_redfish::Bmc + Send + Sync + 'static,
    B::Error: nv_telemetry_redfish::ClassifyError,
{
    plan.polls()
        .iter()
        .map(|planned| {
            let endpoint = planned.endpoint().clone();
            let target = planned.target().to_owned().into();
            if planned.origin().request_class() == SensorRead::<B>::REQUEST_CLASS {
                let unit = SensorRead::new(endpoint, target, Arc::clone(bmc));
                PollUnit::new(planned.clone(), Arc::new(unit), &clock)
            } else if planned.origin().request_class() == ChassisRead::<B>::REQUEST_CLASS {
                let unit = ChassisRead::new(endpoint, target, Arc::clone(bmc));
                PollUnit::new(planned.clone(), Arc::new(unit), &clock)
            } else {
                unreachable!("the plan selects only declared providers")
            }
        })
        .collect()
}

async fn drive(
    endpoint: &EndpointContext,
    units: Vec<PollUnit>,
    clock: SystemClock,
    args: &Args,
) -> Result<(), String> {
    let subtree = endpoint_subtree(&EndpointPolicy::default(), &clock, units)
        .map_err(|error| format!("recipe: {error}"))?;

    let mut runtime: Runtime<AcquisitionReport, EndpointFault, PollMeta> = Runtime::new(
        RuntimeConfig {
            global_max_in_flight: std::num::NonZeroUsize::MIN,
            clock: ClockConfig::Wallclock,
        },
        subtree,
    );
    let handle = runtime.handle();

    println!(
        "polling {} target(s) on `{}` every {:?}, {} report(s)",
        args.sensors.len() + args.chassis.len(),
        endpoint.endpoint_id(),
        args.cadence,
        args.count
    );

    let mut remaining = args.count;
    let mut deadline = None;
    loop {
        let output = if let Some(at) = deadline {
            tokio::select! {
                output = runtime.next() => output,
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(at)) => {
                    deadline = None;
                    continue;
                }
            }
        } else {
            runtime.next().await
        };

        match output {
            RuntimeOutput::SleepUntil(at) => deadline = Some(at),
            RuntimeOutput::Work { result, .. } => {
                match result {
                    Ok(reports) => {
                        for report in reports {
                            let (batches, issues, status) = report.into_parts();
                            for batch in batches {
                                println!("batch: {batch:?}");
                            }
                            if let Some(issues) = issues {
                                println!("issues: {issues:?}");
                            }
                            println!("status: {status:?}");
                            remaining = remaining.saturating_sub(1);
                        }
                    }
                    Err(fault) => {
                        println!("status: {:?}", fault.into_status());
                        remaining = remaining.saturating_sub(1);
                    }
                }
                if remaining == 0 {
                    handle.graceful_shutdown();
                }
            }
            RuntimeOutput::Shutdown => break,
            RuntimeOutput::Runtime(_) => {}
        }
    }
    Ok(())
}
