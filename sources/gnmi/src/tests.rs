// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use futures_util::FutureExt;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use http_body_util::Full;
use nv_redfish_dispatcher::Scheduler;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::NumericValue;
use nv_telemetry_model::Payload;
use nv_telemetry_model::SignalDescriptor;
use nv_telemetry_model::SignalKey;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_orchestration::Clock;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailureClass as Class;
use prost::Message;
use tonic::codegen::http;
use tonic::codegen::Bytes;
use tonic::codegen::Service;
use tonic::Code;

use super::*;
use crate::protocol::gnmi as wire;
use crate::protocol::gnmi::typed_value::Value;

#[derive(Default)]
struct Script {
    responses: Vec<wire::SubscribeResponse>,
    status: Option<Code>,
    opening_error: bool,
    hold_connect: bool,
    hold_open: bool,
}

#[derive(Clone)]
struct Mock {
    script: Arc<Script>,
    requests: Arc<Mutex<Vec<wire::SubscribeRequest>>>,
    dropped: Arc<AtomicUsize>,
}

impl Mock {
    fn subscription(&self) -> ConfiguredSubscription<Self> {
        ConfiguredSubscription::new(endpoint(), self.clone(), spec(NumericType::Uint))
            .expect("subscription")
    }

    fn new(script: Script) -> Self {
        Self {
            script: Arc::new(script),
            requests: Arc::default(),
            dropped: Arc::default(),
        }
    }
}

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Service<http::Request<tonic::body::Body>> for Mock {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let mock = self.clone();

        // Keep the probe in the pending request or response body to test cancellation.
        let probe = DropProbe(Arc::clone(&self.dropped));

        Box::pin(async move {
            assert_eq!(request.uri().path(), "/gnmi.gNMI/Subscribe");

            let bytes = request
                .into_body()
                .collect()
                .await
                .expect("request body")
                .to_bytes();

            assert_eq!(bytes[0], 0, "uncompressed gRPC request");
            let length = u32::from_be_bytes(bytes[1..5].try_into().expect("frame length")) as usize;
            assert_eq!(length + 5, bytes.len(), "one SubscribeRequest");

            let request =
                wire::SubscribeRequest::decode(&bytes[5..]).expect("valid request protobuf");

            mock.requests.lock().expect("request lock").push(request);

            if mock.script.hold_connect {
                let _probe = probe;
                return std::future::pending().await;
            }

            let mut bytes = Vec::new();

            // Each gRPC message starts with a compression flag and a four-byte length.
            for response in &mock.script.responses {
                let message = response.encode_to_vec();
                bytes.push(0);

                bytes.extend_from_slice(
                    &u32::try_from(message.len())
                        .expect("small fixture")
                        .to_be_bytes(),
                );

                bytes.extend_from_slice(&message);
            }

            let mut trailers = http::HeaderMap::new();

            trailers.insert(
                "grpc-status",
                (mock.script.status.unwrap_or(Code::Ok) as i32)
                    .to_string()
                    .parse()
                    .expect("status header"),
            );

            let opening_error = mock.script.opening_error;
            let status = mock.script.status.unwrap_or(Code::Ok);

            let body = Full::new(Bytes::from(bytes)).with_trailers(async move {
                let _probe = probe;

                if mock.script.hold_open {
                    std::future::pending::<()>().await;
                }

                Some(Ok::<_, std::convert::Infallible>(trailers))
            });

            let mut response = http::Response::new(tonic::body::Body::new(body));

            response.headers_mut().insert(
                "content-type",
                "application/grpc".parse().expect("content type"),
            );

            if opening_error {
                response.headers_mut().insert(
                    "grpc-status",
                    (status as i32).to_string().parse().expect("status header"),
                );
            }

            Ok(response)
        })
    }
}

#[derive(Clone)]
struct FixedClock;

impl Clock for FixedClock {
    fn timestamp(&self) -> Timestamp {
        Timestamp::new(99, 0).expect("valid timestamp")
    }

    fn instant(&self) -> Instant {
        Instant::now()
    }
}

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("switch-a")
        .build()
        .expect("valid endpoint")
}

fn path(names: &[&str]) -> SubscriptionPath {
    SubscriptionPath::new(names.iter().map(|name| PathElement::new(*name)).collect())
}

fn wire_path(names: &[&str]) -> wire::Path {
    wire::Path {
        elem: names
            .iter()
            .map(|name| wire::PathElem {
                name: (*name).into(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn descriptor(id: &str) -> SignalDescriptor {
    let subject = Subject::builder()
        .kind("sensor")
        .id(id)
        .build()
        .expect("valid subject");

    let key = SignalKey::builder()
        .subject(subject)
        .build()
        .expect("valid key");

    SignalDescriptor::builder()
        .key(key)
        .unit("1")
        .build()
        .expect("valid descriptor")
}

fn spec(kind: NumericType) -> SubscriptionSpec {
    let leaf = path(&["leaf"]);
    SubscriptionSpec::new(
        vec![leaf.clone()],
        vec![Mapping::new(leaf, descriptor("a"), kind)],
    )
    .expect("valid specification")
}

fn update(value: Value) -> wire::Update {
    wire::Update {
        path: Some(wire_path(&["leaf"])),
        val: Some(wire::TypedValue { value: Some(value) }),
        ..Default::default()
    }
}

fn notification(updates: Vec<wire::Update>) -> wire::SubscribeResponse {
    wire::SubscribeResponse {
        response: Some(wire::subscribe_response::Response::Update(
            wire::Notification {
                timestamp: 7_000_000_123,
                update: updates,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

#[test]
fn configuration_rejects_empty_ambiguous_and_unrequested_paths() {
    let valid = path(&["leaf"]);
    let mapping = Mapping::new(valid.clone(), descriptor("a"), NumericType::Uint);

    let cases = [
        (vec![], vec![mapping.clone()]),
        (vec![valid.clone()], vec![]),
        (vec![path(&[])], vec![mapping.clone()]),
        (vec![path(&[""])], vec![mapping.clone()]),
        (vec![valid.clone()], vec![mapping.clone(), mapping.clone()]),
        (vec![path(&["other"])], vec![mapping.clone()]),
        (
            vec![valid.clone()],
            vec![Mapping::new(path(&[]), descriptor("a"), NumericType::Uint)],
        ),
        (
            vec![valid.clone(), path(&["other"])],
            vec![
                mapping,
                Mapping::new(path(&["other"]), descriptor("a"), NumericType::Uint),
            ],
        ),
    ];

    for (requests, mappings) in cases {
        assert!(SubscriptionSpec::new(requests, mappings).is_err());
    }

    let keyed = SubscriptionPath::new(vec![
        PathElement::new("interface").with_key("name", "Ethernet1"),
        PathElement::new("counter"),
    ]);

    assert!(SubscriptionSpec::new(
        vec![path(&["interface"])],
        vec![Mapping::new(keyed, descriptor("a"), NumericType::Uint)]
    )
    .is_ok());

    assert!(SubscriptionSpec::new(
        vec![path(&["inter"])],
        vec![Mapping::new(
            path(&["interface", "counter"]),
            descriptor("a"),
            NumericType::Uint
        )]
    )
    .is_err());
}

#[test]
#[allow(deprecated)]
fn declared_numeric_types_preserve_precision_and_reject_incompatible_values() {
    use NumericType::*;
    use Value::*;

    let cases = [
        (Int, IntVal(i64::MIN), Some(NumericValue::Int(i64::MIN))),
        (
            Int,
            JsonVal(b"9223372036854775807".to_vec()),
            Some(NumericValue::Int(i64::MAX)),
        ),
        (
            Int,
            JsonIetfVal(br#""-9223372036854775808""#.to_vec()),
            Some(NumericValue::Int(i64::MIN)),
        ),
        (Uint, UintVal(u64::MAX), Some(NumericValue::Uint(u64::MAX))),
        (
            Uint,
            JsonVal(b"18446744073709551615".to_vec()),
            Some(NumericValue::Uint(u64::MAX)),
        ),
        (
            Uint,
            JsonIetfVal(br#""9007199254740993""#.to_vec()),
            Some(NumericValue::Uint(9_007_199_254_740_993)),
        ),
        (
            Double,
            DoubleVal(1.5),
            Some(NumericValue::double(1.5).expect("finite")),
        ),
        (
            Double,
            FloatVal(1.5),
            Some(NumericValue::double(1.5).expect("finite")),
        ),
        (
            Double,
            JsonVal(br#""1.5e2""#.to_vec()),
            Some(NumericValue::double(150.0).expect("finite")),
        ),
        (Int, JsonVal(b"1.5".to_vec()), None),
        (Int, JsonVal(b"9223372036854775808".to_vec()), None),
        (Uint, JsonVal(b"18446744073709551616".to_vec()), None),
        (Uint, JsonVal(b"-1".to_vec()), None),
        (Uint, IntVal(1), None),
        (Int, UintVal(1), None),
        (Int, DoubleVal(1.0), None),
        (Double, UintVal(1), None),
        (Double, IntVal(1), None),
        (Double, DoubleVal(f64::INFINITY), None),
        (Double, FloatVal(f32::NAN), None),
        (Double, JsonVal(br#""NaN""#.to_vec()), None),
        (Double, JsonVal(b"{}".to_vec()), None),
        (Double, JsonVal(b"[]".to_vec()), None),
        (Double, JsonVal(b"broken".to_vec()), None),
        (Double, BoolVal(true), None),
        (
            Double,
            DecimalVal(wire::Decimal64 {
                digits: 15,
                precision: 1,
            }),
            None,
        ),
    ];

    for (kind, value, expected) in cases {
        let parts = projection::project(
            &spec(kind),
            wire::Notification {
                update: vec![update(value.clone())],
                ..Default::default()
            },
        )
        .expect("notification projects");

        if let Some(expected) = expected {
            let Payload::Readings(readings) = &parts.payloads()[0].1 else {
                panic!("numeric payload")
            };

            assert_eq!(*readings.samples()[0].value(), expected, "{value:?}");
            assert_eq!(readings.descriptors(), &[descriptor("a")]);
            assert!(parts.issues().is_empty(), "{value:?}");
        } else {
            assert!(parts.payloads().is_empty(), "{value:?}");
            assert_eq!(parts.issues().len(), 1, "{value:?}");
            assert!(parts.issues()[0].path().contains("[0]"));
        }
    }
}

#[test]
fn duplicate_updates_project_only_the_final_value_and_keep_original_issue_indexes() {
    let mapping = path(&["parent", "leaf"]);

    let spec = SubscriptionSpec::new(
        vec![path(&["parent"])],
        vec![Mapping::new(mapping, descriptor("a"), NumericType::Uint)],
    )
    .expect("parent subscription");

    let cases = [
        (
            "valid then valid",
            [Some(Value::UintVal(9)), Some(Value::UintVal(3))],
            Some(3),
        ),
        (
            "valid then invalid",
            [Some(Value::UintVal(9)), Some(Value::BoolVal(true))],
            None,
        ),
        (
            "invalid then valid",
            [Some(Value::BoolVal(true)), Some(Value::UintVal(3))],
            Some(3),
        ),
        (
            "invalid then invalid",
            [Some(Value::BoolVal(true)), Some(Value::BoolVal(false))],
            None,
        ),
        ("valid then missing", [Some(Value::UintVal(9)), None], None),
    ];

    for (name, values, expected) in cases {
        let notification = wire::Notification {
            prefix: Some(wire_path(&["parent"])),
            update: values
                .into_iter()
                .map(|value| wire::Update {
                    path: Some(wire_path(&["leaf"])),
                    val: value.map(|value| wire::TypedValue { value: Some(value) }),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };

        let parts = projection::project(&spec, notification).expect("duplicate update projection");

        if let Some(expected) = expected {
            assert_eq!(parts.payloads().len(), 1, "{name}");

            let Payload::Readings(readings) = &parts.payloads()[0].1 else {
                panic!("numeric payload")
            };

            assert_eq!(readings.samples().len(), 1, "{name}");

            assert_eq!(
                *readings.samples()[0].value(),
                NumericValue::Uint(expected),
                "{name}"
            );

            assert!(parts.issues().is_empty(), "{name}");
        } else {
            assert!(parts.payloads().is_empty(), "{name}");
            assert_eq!(parts.issues().len(), 1, "{name}");
            assert_eq!(parts.issues()[0].path(), "update[1].val", "{name}");
        }
    }
}

#[test]
fn projection_retains_valid_siblings_and_sparse_notifications_have_no_missing_issues() {
    let spec = SubscriptionSpec::new(
        vec![path(&["leaf"]), path(&["invalid"])],
        vec![
            Mapping::new(path(&["leaf"]), descriptor("a"), NumericType::Uint),
            Mapping::new(path(&["invalid"]), descriptor("b"), NumericType::Uint),
        ],
    )
    .expect("distinct mapped siblings");

    let mut invalid = update(Value::BoolVal(true));
    invalid.path = Some(wire_path(&["invalid"]));

    let mut unmapped = update(Value::BoolVal(true));
    unmapped.path = Some(wire_path(&["other"]));

    let notification = wire::Notification {
        timestamp: -1,
        update: vec![update(Value::UintVal(42)), invalid, unmapped],
        delete: vec![wire_path(&["leaf"])],
        ..Default::default()
    };

    let parts = projection::project(&spec, notification).expect("partial projection");

    assert_eq!(parts.issues().len(), 2);
    assert_ne!(parts.issues()[0].path(), parts.issues()[1].path());
    assert_eq!(parts.payloads()[0].0.completeness(), Completeness::Partial);

    let Payload::Readings(readings) = &parts.payloads()[0].1 else {
        panic!("numeric payload")
    };

    assert_eq!(readings.samples().len(), 1);
    assert_eq!(*readings.samples()[0].value(), NumericValue::Uint(42));

    assert_eq!(
        readings.samples()[0].observed_at(),
        Some(&Timestamp::new(-1, 999_999_999).expect("valid timestamp"))
    );

    let sparse =
        projection::project(&spec, wire::Notification::default()).expect("empty notification");

    assert!(sparse.payloads().is_empty());
    assert!(sparse.issues().is_empty());
}

#[test]
fn prefixes_keys_and_context_resolve_structurally_and_parent_deletes_match() {
    let keyed = SubscriptionPath::new(vec![
        PathElement::new("interface").with_key("name", "Ethernet1"),
        PathElement::new("counter"),
    ]);

    let spec = SubscriptionSpec::new(
        vec![path(&["interface"])],
        vec![Mapping::new(keyed, descriptor("a"), NumericType::Uint)],
    )
    .expect("parent request")
    .with_origin("openconfig")
    .with_target("switch-a");

    let mut prefix = wire_path(&["interface"]);
    prefix.elem[0].key.insert("name".into(), "Ethernet1".into());
    prefix.origin = "openconfig".into();
    prefix.target = "switch-a".into();
    let mut leaf = update(Value::UintVal(9));
    leaf.path = Some(wire_path(&["counter"]));
    let mut wrong_key = leaf.clone();
    wrong_key.path.as_mut().expect("path").target = "different-target".into();

    let parts = projection::project(
        &spec,
        wire::Notification {
            prefix: Some(prefix.clone()),
            update: vec![leaf, wrong_key],
            delete: vec![wire_path(&[])],
            ..Default::default()
        },
    )
    .expect("partial projection");

    assert_eq!(parts.payloads().len(), 1);
    assert_eq!(parts.issues().len(), 2);

    prefix.elem[0].key.insert("name".into(), "Ethernet2".into());

    let absent = projection::project(
        &spec,
        wire::Notification {
            prefix: Some(prefix),
            update: vec![wire::Update {
                path: Some(wire_path(&["counter"])),
                val: Some(wire::TypedValue {
                    value: Some(Value::UintVal(8)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .expect("unmapped key");

    assert!(absent.payloads().is_empty());
    assert!(absent.issues().is_empty());
}

#[test]
fn large_notifications_split_payloads_and_bound_issues_without_losing_descriptors() {
    let spec = SubscriptionSpec::new(
        vec![path(&["root"])],
        (0..65_537)
            .map(|index| {
                let name = format!("leaf-{index}");
                Mapping::new(path(&["root", &name]), descriptor(&name), NumericType::Uint)
            })
            .collect(),
    )
    .expect("distinct mapped signals below a parent request");

    let notification = wire::Notification {
        prefix: Some(wire_path(&["root"])),
        update: (0..65_537)
            .map(|index| wire::Update {
                path: Some(wire_path(&[&format!("leaf-{index}")])),
                val: Some(wire::TypedValue {
                    value: Some(Value::UintVal(5)),
                }),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };

    let parts = projection::project(&spec, notification.clone()).expect("large notification");

    assert_eq!(parts.payloads().len(), 2);
    let mut total = 0;

    for (_, payload) in parts.payloads() {
        let Payload::Readings(readings) = payload else {
            panic!("numeric payload")
        };

        let keys: BTreeSet<_> = readings
            .descriptors()
            .iter()
            .map(SignalDescriptor::key)
            .collect();

        assert!(readings.samples().len() <= 65_536);
        assert!(readings.descriptors().len() <= 65_536);
        assert!(readings
            .samples()
            .iter()
            .all(|sample| keys.contains(sample.key())));

        total += readings.samples().len();
    }

    assert_eq!(total, 65_537);

    let mut invalid = notification;

    for update in &mut invalid.update {
        update.val = Some(wire::TypedValue {
            value: Some(Value::BoolVal(true)),
        });
    }

    let parts = projection::project(&spec, invalid).expect("issue-only notification");

    assert_eq!(
        parts.issues().len(),
        nv_telemetry_model::limits::PROJECTIONISSUES_ISSUES_MAX_ITEMS as usize
    );

    assert_eq!(
        parts.issues().last().expect("omission issue").path(),
        nv_telemetry_source::OMITTED_ISSUES_LOCATOR
    );
}

#[tokio::test]
async fn subscribe_request_sets_paths_context_modes_and_explicit_encoding() {
    for encoding in [None, Some(Encoding::Json), Some(Encoding::JsonIetf)] {
        let mock = Mock::new(Script::default());
        let requests = Arc::clone(&mock.requests);

        let keyed = SubscriptionPath::new(vec![
            PathElement::new("interface").with_key("name", "Ethernet1")
        ]);

        let spec = SubscriptionSpec::new(
            vec![keyed.clone(), path(&["other"])],
            vec![Mapping::new(keyed, descriptor("a"), NumericType::Uint)],
        )
        .expect("multiple requests")
        .with_origin("openconfig")
        .with_target("switch-a");

        let spec = if let Some(encoding) = encoding {
            spec.with_encoding(encoding)
        } else {
            spec
        };

        let unit = ConfiguredSubscription::new(endpoint(), mock, spec).expect("subscription");

        let stream = unit.perform().await.expect("opens");
        drop(stream);

        let requests = requests.lock().expect("request lock");

        let Some(wire::subscribe_request::Request::Subscribe(list)) = &requests[0].request else {
            panic!("Subscribe list")
        };

        let prefix = list.prefix.as_ref().expect("context prefix");

        assert_eq!(
            (&*prefix.origin, &*prefix.target),
            ("openconfig", "switch-a")
        );

        assert!(prefix.elem.is_empty());
        assert_eq!(list.mode, wire::subscription_list::Mode::Stream as i32);
        assert!(!list.updates_only);
        assert!(!list.allow_aggregation);

        assert_eq!(
            list.encoding,
            if encoding.unwrap_or(Encoding::Json) == Encoding::Json {
                wire::Encoding::Json as i32
            } else {
                wire::Encoding::JsonIetf as i32
            }
        );

        assert_eq!(list.subscription.len(), 2);

        assert_eq!(
            list.subscription[0].path.as_ref().expect("path").elem[0].key["name"],
            "Ethernet1"
        );

        assert_eq!(
            list.subscription[1].path.as_ref().expect("path").elem[0].name,
            "other"
        );

        assert!(list
            .subscription
            .iter()
            .all(|subscription| subscription.mode == wire::SubscriptionMode::TargetDefined as i32));
    }
}

#[tokio::test]
async fn sync_controls_are_suppressed_and_stream_closure_is_reported_once() {
    let mut unmapped = update(Value::UintVal(8));
    unmapped.path = Some(wire_path(&["other"]));

    let mock = Mock::new(Script {
        responses: vec![
            wire::SubscribeResponse {
                response: Some(wire::subscribe_response::Response::SyncResponse(true)),
                ..Default::default()
            },
            notification(vec![]),
            notification(vec![unmapped]),
            notification(vec![update(Value::BoolVal(true))]),
            notification(vec![update(Value::UintVal(3))]),
        ],
        ..Default::default()
    });

    let unit = mock.subscription();

    let mut stream = unit.perform().await.expect("opens");

    for (payloads, issues) in [(0, 0), (0, 0), (0, 1), (1, 0)] {
        let item = stream
            .next()
            .await
            .expect("one item per notification")
            .expect("successful projection");

        assert_eq!(item.payloads().len(), payloads);
        assert_eq!(item.issues().len(), issues);
    }

    let failure = stream
        .next()
        .await
        .expect("closure failure")
        .expect_err("unexpected STREAM closure");

    assert_eq!(failure.class(), Class::Protocol);
    assert_eq!(failure.retryable(), Some(true));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn opening_and_terminal_statuses_share_failure_classification_and_retry_hints() {
    for (code, class, retryable) in [
        (Code::Unavailable, Class::Connectivity, true),
        (Code::DeadlineExceeded, Class::Timeout, true),
        (Code::Unauthenticated, Class::Authentication, false),
        (Code::PermissionDenied, Class::Authentication, false),
        (Code::Unimplemented, Class::Unsupported, false),
        (Code::InvalidArgument, Class::Unsupported, false),
        (Code::NotFound, Class::Unsupported, false),
        (Code::AlreadyExists, Class::Unsupported, false),
        (Code::FailedPrecondition, Class::Unsupported, false),
        (Code::OutOfRange, Class::Protocol, false),
        (Code::Cancelled, Class::Device, true),
        (Code::Unknown, Class::Device, true),
        (Code::Aborted, Class::Device, true),
        (Code::ResourceExhausted, Class::Device, true),
        (Code::Internal, Class::Protocol, false),
        (Code::DataLoss, Class::Protocol, false),
    ] {
        for opening_error in [true, false] {
            let mock = Mock::new(Script {
                status: Some(code),
                opening_error,
                ..Default::default()
            });

            let unit = mock.subscription();

            let failure = match unit.perform().await {
                Err(failure) => {
                    assert!(opening_error);
                    failure
                }
                Ok(mut stream) => {
                    assert!(!opening_error);

                    let failure = stream
                        .next()
                        .await
                        .expect("terminal failure")
                        .expect_err("status fails");

                    assert!(stream.next().await.is_none());
                    failure
                }
            };

            assert_eq!(failure.class(), class, "{code:?}");
            assert_eq!(failure.retryable(), Some(retryable), "{code:?}");
        }
    }

    for response in [
        wire::SubscribeResponse::default(),
        notification(vec![update(Value::JsonVal(vec![
            b'0';
            4 * 1024 * 1024 + 1
        ]))]),
    ] {
        let mock = Mock::new(Script {
            responses: vec![response],
            ..Default::default()
        });

        let unit = mock.subscription();

        let mut stream = unit.perform().await.expect("opens");

        let failure = stream
            .next()
            .await
            .expect("terminal response failure")
            .expect_err("missing response variant or decoding limit");

        assert_eq!(failure.class(), Class::Protocol);
        assert_eq!(failure.retryable(), Some(false));
        assert!(stream.next().await.is_none());
    }
}

#[tokio::test]
async fn dropping_connect_or_live_stream_cancels_and_acquisitions_are_independent() {
    let mock = Mock::new(Script {
        hold_connect: true,
        ..Default::default()
    });

    let dropped = Arc::clone(&mock.dropped);

    let unit = mock.subscription();

    let mut connecting = Box::pin(unit.perform());

    assert!(futures_util::poll!(&mut connecting).is_pending());
    drop(connecting);

    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let mock = Mock::new(Script {
        responses: vec![notification(vec![update(Value::UintVal(3))])],
        hold_open: true,
        ..Default::default()
    });

    let dropped = Arc::clone(&mock.dropped);
    let requests = Arc::clone(&mock.requests);

    let unit = mock.subscription();

    let mut first = unit.perform().await.expect("first opens");
    let mut second = unit.perform().await.expect("second opens independently");

    drop(unit);

    first
        .next()
        .await
        .expect("first item")
        .expect("first projects");

    second
        .next()
        .await
        .expect("second item")
        .expect("second projects");

    assert!(first.next().now_or_never().is_none());
    drop(first);
    drop(second);

    assert_eq!(requests.lock().expect("request lock").len(), 2);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn stream_unit_reports_notification_identity_and_distinct_collector_timestamp() {
    fn require_owned<A: Acquire + Send + Sync + 'static>(_: &A)
    where
        A::Output: Stream<Item = SubscriptionItem> + Send + 'static,
    {
    }

    let mock = Mock::new(Script {
        responses: vec![notification(vec![update(Value::UintVal(3))])],
        ..Default::default()
    });

    let unit = Arc::new(mock.subscription());

    require_owned(unit.as_ref());

    let plan = nv_telemetry_orchestration::plan(
        nv_telemetry_orchestration::Needs::default().with_streams([
            nv_telemetry_orchestration::StreamNeed::new(
                endpoint(),
                ConfiguredSubscription::<Mock>::REQUEST_CLASS,
            ),
        ]),
        &[ConfiguredSubscription::<Mock>::declaration()],
    )
    .expect("provider declaration");

    let (mut leaf, mut reports) = nv_telemetry_orchestration::StreamUnit::new(
        plan.streams()[0].clone(),
        unit,
        nv_telemetry_orchestration::ReconnectPolicy::default(),
        FixedClock,
    );

    assert!(leaf.update_ready(Instant::now()).ready);
    let work = leaf.take_next().expect("connect work");
    let connected = work.payload.await.expect("connect report");

    let report = reports
        .next()
        .await
        .expect("notification report")
        .expect("successful report");

    assert!(connected.is_empty());
    assert_eq!(report.status().provider(), "gnmi.configured");
    assert_eq!(report.status().request_class(), "configured-subscribe");
    assert_eq!(report.batches()[0].endpoint(), &endpoint());
    assert_eq!(report.batches()[0].window().start().seconds(), 99);

    let Payload::Readings(readings) = report.batches()[0].payload() else {
        panic!("numeric payload")
    };

    assert_eq!(
        readings.samples()[0].observed_at(),
        Some(&Timestamp::new(7, 123).expect("device timestamp"))
    );
}
