// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::time::Duration;

use futures_util::StreamExt;
use nv_telemetry_gnmi::ConfiguredGet;
use nv_telemetry_gnmi::ConfiguredSubscription;
use nv_telemetry_model::FailureClass;
use nv_telemetry_model::NumericValue;
use nv_telemetry_model::Outcome;
use nv_telemetry_model::Payload;
use nv_telemetry_orchestration::AcquisitionReport;
use nv_telemetry_orchestration::Collected;
use nv_telemetry_orchestration::Collector;
use nv_telemetry_orchestration::Reports;
use nv_telemetry_orchestration::SystemClock;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time::timeout;
use tonic::codegen::http;
use tonic::codegen::Bytes;

use super::units;
use crate::config::Defaults;
use crate::config::GnmiEndpoint;

const ENDPOINT_ID: &str = "switch-test";
const GET_CLASS: &str = ConfiguredGet::<()>::REQUEST_CLASS;
const SUBSCRIBE_CLASS: &str = ConfiguredSubscription::<()>::REQUEST_CLASS;
const TEST_LIMIT: Duration = Duration::from_secs(35);

#[tokio::test]
async fn get_without_final_status_times_out_and_later_poll_and_subscription_succeed() {
    let (listener, _collector, mut reports) = collect(true).await;
    let first_get = Notify::new();

    with_mock(listener, &first_get, async {
        first_get.notified().await;

        let initial = next_report(&mut reports).await;

        assert_reading(&initial, SUBSCRIBE_CLASS, 7);

        let expired = next_report(&mut reports).await;

        assert_timeout(&expired);

        let first = next_report(&mut reports).await;
        let second = next_report(&mut reports).await;

        // The poll and stream have independent report order after the timeout.
        let (get, subscription) = if first.status().request_class() == GET_CLASS {
            (first, second)
        } else {
            (second, first)
        };

        assert_reading(&get, GET_CLASS, 11);
        assert_reading(&subscription, SUBSCRIBE_CLASS, 13);
    })
    .await;
}

#[tokio::test]
async fn shutdown_reports_get_timeout_before_retiring_endpoint() {
    let (listener, collector, mut reports) = collect(false).await;
    let first_get = Notify::new();

    with_mock(listener, &first_get, async {
        first_get.notified().await;

        collector.shutdown();

        let expired = next_report(&mut reports).await;

        assert_timeout(&expired);

        let Some(Collected::Retired(endpoint)) = reports.next().await else {
            panic!("expected the gNMI endpoint to retire");
        };

        assert_eq!(endpoint.endpoint_id(), ENDPOINT_ID);
        assert!(reports.next().await.is_none());
    })
    .await;
}

#[tokio::test]
async fn rejects_invalid_leaf_in_later_get_with_same_name() {
    let config: GnmiEndpoint = toml::from_str(
        r#"
id = "switch-a"
address = "http://127.0.0.1:9339"

[[get]]
name = "system"
interval = "10s"

[[get.leaves]]
path = "system/state/hostname"
subject = "system"
subject_id = "switch-a"
facet = "hostname"
type = "string"

[[get]]
name = "system"
interval = "20s"

[[get.leaves]]
path = "system/state/domain-name"
subject = "system"
facet = "domain-name"
type = "string"
"#,
    )
    .expect("the endpoint configuration must parse");

    let result = units(&config, &Defaults::default());

    let error = result.expect_err("the second Get leaf requires a subject_id");

    assert_eq!(
        error,
        "get `system`: `system/state/domain-name`: a path without `[key=*]` needs a subject_id"
    );
}

async fn collect(subscribe: bool) -> (TcpListener, Collector, Reports) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind loopback listener");

    let address = listener.local_addr().expect("listener address");

    let mut config: GnmiEndpoint = toml::from_str(&format!(
        r#"
        id = "{ENDPOINT_ID}"
        address = "http://{address}"
        subscribe = [{{path = "x", subject = "sensor", subject_id = "1", facet = "count", type = "uint"}}]
        [[get]]
        name = "snapshot"
        interval = "1s"
        leaves = [{{path = "x", subject = "sensor", subject_id = "1", facet = "count", type = "uint"}}]
        "#,
    ))
    .expect("gNMI endpoint config");

    if !subscribe {
        config.subscribe.clear();
    }

    let endpoint_units = units(&config, &Defaults::default()).expect("gNMI endpoint units");
    let (collector, reports) = Collector::new(SystemClock::default());

    collector
        .add(endpoint_units)
        .expect("collect gNMI endpoint");

    (listener, collector, reports)
}

async fn with_mock(listener: TcpListener, first_get: &Notify, test: impl Future<Output = ()>) {
    let run = async {
        tokio::pin!(test);

        tokio::select! {
            () = serve(listener, first_get) => test.await,
            () = &mut test => {}
        }
    };

    timeout(TEST_LIMIT, run)
        .await
        .expect("gNMI fixture timed out");
}

async fn serve(listener: TcpListener, first_get: &Notify) {
    let (socket, _) = listener.accept().await.expect("accept gNMI connection");

    let mut connection = h2::server::handshake(socket)
        .await
        .expect("HTTP/2 handshake");

    let mut stalled = None;
    let mut subscription: Option<h2::SendStream<Bytes>> = None;

    while let Some(request) = connection.accept().await {
        let (request, mut respond) = request.expect("accept gNMI request");

        let headers = http::Response::builder()
            .header("content-type", "application/grpc")
            .body(())
            .expect("gRPC headers");

        let mut stream = respond.send_response(headers, false).expect("send headers");

        match request.uri().path() {
            "/gnmi.gNMI/Get" if stalled.is_none() => {
                stream
                    .send_data(reading_frame(5), false)
                    .expect("send Get message");

                // Keep the first response open after its message. Tonic still needs final status.
                stalled = Some(stream);
                first_get.notify_one();
            }
            "/gnmi.gNMI/Get" => {
                stream
                    .send_data(reading_frame(11), false)
                    .expect("send Get message");

                let mut trailers = http::HeaderMap::new();
                trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                stream.send_trailers(trailers).expect("finish Get response");

                if let Some(subscription) = &mut subscription {
                    subscription
                        .send_data(reading_frame(13), false)
                        .expect("send later update on original subscription");
                }
            }
            "/gnmi.gNMI/Subscribe" => {
                assert!(subscription.is_none(), "Subscribe opened a second RPC");

                stream
                    .send_data(reading_frame(7), false)
                    .expect("send initial subscription update");

                subscription = Some(stream);
            }
            path => panic!("unexpected gNMI request {path}"),
        }
    }
}

fn reading_frame(value: u8) -> Bytes {
    // GetResponse.notification and SubscribeResponse.update both use field 1.
    // The gRPC frame carries a timestamped Notification with uint_val at /x.
    Bytes::copy_from_slice(&[
        0, 0, 0, 0, 17, // Uncompressed gRPC message with a 17-byte body.
        0x0a, 15, // Field 1 contains a Notification.
        0x08, 1, // Nanosecond timestamp.
        0x22, 11, // Update.
        0x0a, 5, 0x1a, 3, 0x0a, 1, b'x', // Update path.
        0x1a, 2, 0x18, value, // Update typed value.
    ])
}

async fn next_report(reports: &mut Reports) -> AcquisitionReport {
    let Some(Collected::Report(report)) = reports.next().await else {
        panic!("expected an acquisition report");
    };

    report
}

fn assert_timeout(report: &AcquisitionReport) {
    let status = report.status();
    let duration = Duration::from_nanos(status.duration_nanos().expect("Get duration"));

    assert_eq!(status.endpoint_id(), ENDPOINT_ID);
    assert_eq!(status.provider(), ConfiguredGet::<()>::PROVIDER);
    assert_eq!(status.request_class(), GET_CLASS);
    assert_eq!(status.outcome(), Outcome::Failed);
    assert_eq!(status.failure_class(), Some(FailureClass::Timeout));
    assert_eq!(status.retryable(), Some(true));
    assert!(duration >= Duration::from_secs(30));
    assert!(report.batches().is_empty());
    assert!(report.issues().is_none());
}

fn assert_reading(report: &AcquisitionReport, request_class: &str, value: u64) {
    let [batch] = report.batches() else {
        panic!("expected 1 projected batch");
    };

    let Payload::Readings(readings) = batch.payload() else {
        panic!("expected projected readings");
    };

    let [reading] = readings.samples() else {
        panic!("expected 1 projected reading");
    };

    assert_eq!(report.status().outcome(), Outcome::Succeeded);
    assert_eq!(report.status().request_class(), request_class);
    assert_eq!(reading.value(), &NumericValue::Uint(value));
}
