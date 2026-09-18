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

use futures_util::StreamExt as _;
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
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::PollMeta;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::ReconnectPolicy;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamReports;
use nv_telemetry_orchestration::StreamUnit;
use nv_telemetry_orchestration::SystemClock;
use nv_telemetry_redfish::ChassisRead;
use nv_telemetry_redfish::ClassifyError;
use nv_telemetry_redfish::EventStream;
use nv_telemetry_redfish::LogRead;
use nv_telemetry_redfish::SensorRead;
use url::Url;

const USAGE: &str = "\
usage: nv-telemetry-probe --mode mock|http --endpoint-id <id>
           [--sensor <odata-id> ...] [--chassis <odata-id> ...]
           [--log-service <odata-id> ...] [--event-stream]
           [--cadence-ms <ms>] [--count <n>] [--base-url <url>] [--insecure]
           [--strict]

  mock    poll the in-process BMC mock, replaying fixtures/
  http    poll a live Redfish service at --base-url; credentials come
          from PROBE_USERNAME and PROBE_PASSWORD; --insecure accepts
          self-signed BMC certificates

--event-stream (http only) also plans the endpoint's server-sent event
stream; each Event payload is one report. The dispatcher reopens a stream
that ends after a backoff that doubles while instances deliver nothing,
staggered per endpoint, unless the failure that ended it was not retryable.

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
    event_stream: bool,
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
    let mut event_stream = false;
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
            "--event-stream" => event_stream = true,
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
    if event_stream && mode == Mode::Mock {
        // The mock's expectations are strict-FIFO, and a stream's connect
        // attempt shares the ring with the polls, so its requests cannot be
        // primed in a known order.
        return Err("`--event-stream` needs `--mode http`".to_owned());
    }
    if sensors.is_empty() && chassis.is_empty() && log_services.is_empty() && !event_stream {
        return Err(
            "at least one `--sensor`, `--chassis`, `--log-service`, or `--event-stream` is required"
                .to_owned(),
        );
    }
    Ok(Args {
        mode,
        endpoint_id: endpoint_id.ok_or("`--endpoint-id` is required")?,
        sensors,
        chassis,
        log_services,
        event_stream,
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
        }));
    // The stream is planned like the polls: the embedder seats what the
    // plan resolved and nothing else.
    let stream_need = args
        .event_stream
        .then(|| StreamNeed::new(endpoint.clone(), EventStream::<()>::REQUEST_CLASS));
    let plan = plan(
        Needs::default().with_polls(needs).with_streams(stream_need),
        &[
            SensorRead::<()>::declaration(),
            ChassisRead::<()>::declaration(),
            LogRead::<()>::declaration(),
            EventStream::<()>::declaration(),
        ],
    )
    .map_err(|error| format!("plan: {error}"))?;

    match args.mode {
        Mode::Mock => {
            let bmc = Arc::new(nv_redfish_bmc_mock::Bmc::<nv_redfish_bmc_mock::Error>::default());
            prime_mock(&bmc, args);
            let units = units(&plan, &bmc, clock);
            let streams = streams(&plan, &bmc, clock);
            drive(&endpoint, units, streams, clock, args).await
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
            let streams = streams(&plan, &bmc, clock);
            drive(&endpoint, units, streams, clock, args).await
        }
    }
}

/// Mock expectations are one-shot AND strict-FIFO, so priming follows
/// dispatch order: the ring visits targets in needs order each round, and a
/// log read asks three times — the service, its entries collection, then
/// each member — plus the service root on its second round, to learn
/// whether the device filters. The collection and entry URIs are the
/// fixture's own.
fn prime_mock(bmc: &nv_redfish_bmc_mock::Bmc<nv_redfish_bmc_mock::Error>, args: &Args) {
    let sensor_fixture = include_str!("../fixtures/sensor.json");
    let chassis_fixture = include_str!("../fixtures/chassis.json");
    let log_service_fixture = include_str!("../fixtures/log-service.json");
    let log_entries_fixture = include_str!("../fixtures/log-entries.json");
    let log_entry_fixture = include_str!("../fixtures/log-entry.json");
    let service_root_fixture = include_str!("../fixtures/service-root.json");
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
    B::Error: ClassifyError,
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

/// The streamed half of the class dispatch: each planned stream to the
/// provider that declared its class, paired with the reports the driving
/// loop pulls.
fn streams<B>(
    plan: &nv_telemetry_orchestration::Plan,
    bmc: &Arc<B>,
    clock: SystemClock,
) -> Vec<(StreamUnit, StreamReports)>
where
    B: nv_redfish::Bmc + Send + Sync + 'static,
    B::Error: ClassifyError + 'static,
{
    plan.streams()
        .iter()
        .map(|planned| {
            if planned.origin().request_class() == EventStream::<B>::REQUEST_CLASS {
                let unit = EventStream::new(planned.endpoint().clone(), Arc::clone(bmc));
                StreamUnit::new(
                    planned.clone(),
                    Arc::new(unit),
                    ReconnectPolicy::default(),
                    clock,
                )
            } else {
                unreachable!("the plan selects only declared providers")
            }
        })
        .collect()
}

/// What the driving loop counts across every report, polled or streamed.
#[derive(Default)]
struct Tally {
    reports: usize,
    failures: usize,
    issue_reports: usize,
}

impl Tally {
    fn record(&mut self, result: Result<Vec<AcquisitionReport>, EndpointFault>) {
        match result {
            Ok(reports) => {
                for report in reports {
                    let (batches, issues, status) = report.into_parts();
                    self.failures += usize::from(status.outcome() == Outcome::Failed);
                    for batch in batches {
                        println!("batch: {batch:?}");
                    }
                    if let Some(issues) = issues {
                        self.issue_reports += 1;
                        println!("issues: {issues:?}");
                    }
                    println!("status: {status:?}");
                    self.reports += 1;
                }
            }
            Err(fault) => {
                self.failures += 1;
                println!("status: {:?}", fault.into_status());
                self.reports += 1;
            }
        }
    }
}

async fn drive(
    endpoint: &EndpointContext,
    units: Vec<PollUnit>,
    streams: Vec<(StreamUnit, StreamReports)>,
    clock: SystemClock,
    args: &Args,
) -> Result<(), String> {
    let polled = !units.is_empty();
    let (streams, reports): (Vec<StreamUnit>, Vec<StreamReports>) = streams.into_iter().unzip();
    let subtree = endpoint_subtree(&EndpointPolicy::default(), &clock, units, streams)
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
        "polling {} target(s) on `{}` every {:?}{}, {} report(s)",
        args.sensors.len() + args.chassis.len() + args.log_services.len(),
        endpoint.endpoint_id(),
        args.cadence,
        if reports.is_empty() {
            ""
        } else {
            " plus its event stream"
        },
        args.count
    );

    // The streams' reports join the runtime's outputs. The runtime itself
    // schedules every connect and reconnect; the reports end once policy
    // has stopped the last stream.
    let mut streaming = !reports.is_empty();
    let mut reports = futures_util::stream::select_all(reports);

    let mut tally = Tally::default();
    let mut deadline = None;
    loop {
        // Three wake-ups: the runtime spoke, a stream reported or the last
        // one ended, or the runtime's sleep hint came due.
        tokio::select! {
            output = runtime.next() => match output {
                RuntimeOutput::SleepUntil(at) => deadline = Some(at),
                RuntimeOutput::Work { result, .. } => tally.record(result),
                RuntimeOutput::Shutdown => break,
                RuntimeOutput::Runtime(_) => {}
            },
            report = reports.next(), if streaming => {
                if let Some(report) = report {
                    tally.record(report.map(|report| vec![report]));
                } else {
                    // With no polls beside the ended streams, nothing more
                    // can report.
                    streaming = false;
                    if !polled {
                        handle.graceful_shutdown();
                    }
                }
            }
            () = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await,
                    None => std::future::pending().await,
                }
            } => deadline = None,
        }
        if tally.reports >= args.count {
            handle.graceful_shutdown();
        }
    }
    if args.strict && (tally.failures > 0 || tally.issue_reports > 0) {
        return Err(format!(
            "strict check failed: {} failed acquisition(s), {} report(s) with projection issues",
            tally.failures, tally.issue_reports
        ));
    }
    Ok(())
}
