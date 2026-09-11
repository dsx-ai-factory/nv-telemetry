// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Demo embedder: polls one Redfish endpoint's sensors, chassis, and log
//! services on a cadence and prints the three output streams — batches,
//! statuses, and issues — one tagged line each.
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
use nv_telemetry_model::Outcome;
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
use nv_telemetry_redfish::LogRead;
use nv_telemetry_redfish::SensorRead;
use url::Url;

const USAGE: &str = "\
usage: nv-telemetry-probe --mode mock|http --endpoint-id <id>
           [--sensor <odata-id> ...] [--chassis <odata-id> ...]
           [--log-service <odata-id> ...]
           [--cadence-ms <ms>] [--count <n>] [--base-url <url>] [--insecure]
           [--strict]

  mock    poll the in-process BMC mock, replaying fixtures/
  http    poll a live Redfish service at --base-url; credentials come
          from PROBE_USERNAME and PROBE_PASSWORD; --insecure accepts
          self-signed BMC certificates

Prints one tagged line per stream item: batch, issues, status.
--strict exits 1 after the requested reports if any acquisition failed or
reported projection issues; requires --count greater than zero. Without it,
reported acquisition failures and issues do not change the exit status.
";

/// The mock log fixture's own entries collection and member: what the
/// replayed service document links to, whatever `--log-service` named.
const LOG_ENTRIES: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries";
const LOG_ENTRY: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries/1";

struct Args {
    mode: Mode,
    endpoint_id: String,
    sensors: Vec<String>,
    chassis: Vec<String>,
    log_services: Vec<String>,
    cadence: Duration,
    count: usize,
    base_url: Option<String>,
    insecure: bool,
    strict: bool,
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
    let mut log_services = Vec::new();
    let mut cadence = Duration::from_secs(5);
    let mut count = 10;
    let mut base_url = None;
    let mut insecure = false;
    let mut strict = false;

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
            "--log-service" => log_services.push(value("--log-service")?),
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
            "--strict" => strict = true,
            other => return Err(format!("unknown argument `{other}`")),
        }
    }

    let mode = mode.ok_or("`--mode` is required")?;
    if mode == Mode::Http && base_url.is_none() {
        return Err("http mode needs `--base-url`".to_owned());
    }
    if strict && count == 0 {
        return Err("`--strict` requires `--count` greater than zero".to_owned());
    }
    if sensors.is_empty() && chassis.is_empty() && log_services.is_empty() {
        return Err(
            "at least one `--sensor`, `--chassis`, or `--log-service` is required".to_owned(),
        );
    }
    Ok(Args {
        mode,
        endpoint_id: endpoint_id.ok_or("`--endpoint-id` is required")?,
        sensors,
        chassis,
        log_services,
        cadence,
        count,
        base_url,
        insecure,
        strict,
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
        .chain(args.log_services.iter().map(|service| {
            PollNeed::new(
                endpoint.clone(),
                LogRead::<()>::REQUEST_CLASS,
                service.clone(),
                args.cadence,
            )
        }))
        .collect();
    let plan = plan(
        needs,
        &[
            SensorRead::<()>::declaration(),
            ChassisRead::<()>::declaration(),
            LogRead::<()>::declaration(),
        ],
    )
    .map_err(|error| format!("plan: {error}"))?;

    match args.mode {
        Mode::Mock => {
            let bmc = Arc::new(nv_redfish_bmc_mock::Bmc::<nv_redfish_bmc_mock::Error>::default());
            let sensor_fixture = include_str!("../fixtures/sensor.json");
            let chassis_fixture = include_str!("../fixtures/chassis.json");
            let log_service_fixture = include_str!("../fixtures/log-service.json");
            let log_entries_fixture = include_str!("../fixtures/log-entries.json");
            let log_entry_fixture = include_str!("../fixtures/log-entry.json");
            let service_root_fixture = include_str!("../fixtures/service-root.json");
            // Mock expectations are one-shot AND strict-FIFO, so priming
            // follows dispatch order: the ring visits targets in needs
            // order each round, and a log read asks three times — the
            // service, its entries collection, then each member — plus the
            // service root on its second round, to learn whether the device
            // filters. The collection and entry URIs are the fixture's own.
            for round in 0..args.count {
                for sensor in &args.sensors {
                    bmc.expect(Expect::get(sensor, sensor_fixture));
                }
                for chassis in &args.chassis {
                    bmc.expect(Expect::get(chassis, chassis_fixture));
                }
                for service in &args.log_services {
                    if round == 1 {
                        bmc.expect(Expect::get("/redfish/v1", service_root_fixture));
                    }
                    bmc.expect(Expect::get(service, log_service_fixture));
                    bmc.expect(Expect::get(LOG_ENTRIES, log_entries_fixture));
                    bmc.expect(Expect::get(LOG_ENTRY, log_entry_fixture));
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
            } else if planned.origin().request_class() == LogRead::<B>::REQUEST_CLASS {
                let unit = LogRead::new(endpoint, target, Arc::clone(bmc));
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
        args.sensors.len() + args.chassis.len() + args.log_services.len(),
        endpoint.endpoint_id(),
        args.cadence,
        args.count
    );

    let mut remaining = args.count;
    let mut failures = 0usize;
    let mut issue_reports = 0usize;
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
                            failures += usize::from(status.outcome() == Outcome::Failed);
                            for batch in batches {
                                println!("batch: {batch:?}");
                            }
                            if let Some(issues) = issues {
                                issue_reports += 1;
                                println!("issues: {issues:?}");
                            }
                            println!("status: {status:?}");
                            remaining = remaining.saturating_sub(1);
                        }
                    }
                    Err(fault) => {
                        failures += 1;
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
    if args.strict && (failures > 0 || issue_reports > 0) {
        return Err(format!(
            "strict check failed: {failures} failed acquisition(s), {issue_reports} report(s) with projection issues"
        ));
    }
    Ok(())
}
