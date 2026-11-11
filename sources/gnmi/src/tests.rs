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
use std::time::Duration;
use std::time::Instant;

use futures_util::FutureExt;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use http_body_util::Full;
use nv_telemetry_model::Accumulation;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::FailureClass;
use nv_telemetry_model::NumericValue;
use nv_telemetry_model::Outcome;
use nv_telemetry_model::Payload;
use nv_telemetry_model::SignalDescriptor;
use nv_telemetry_model::SignalKey;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value as ModelValue;
use nv_telemetry_orchestration::AcquisitionReport;
use nv_telemetry_orchestration::Clock;
use nv_telemetry_orchestration::Collected;
use nv_telemetry_orchestration::Collector;
use nv_telemetry_orchestration::EndpointPolicy;
use nv_telemetry_orchestration::EndpointUnits;
use nv_telemetry_orchestration::Needs;
use nv_telemetry_orchestration::PollNeed;
use nv_telemetry_orchestration::PollUnit;
use nv_telemetry_orchestration::ReconnectPolicy;
use nv_telemetry_orchestration::Reports;
use nv_telemetry_orchestration::StreamNeed;
use nv_telemetry_orchestration::StreamUnit;
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
    get_responses: Vec<wire::GetResponse>,
    advance_clock: Option<(GetClock, Duration)>,
    status: Option<Code>,
    opening_error: bool,
    hold_connect: bool,
    hold_open: bool,
}

#[derive(Clone)]
struct Mock {
    script: Arc<Script>,
    requests: Arc<Mutex<Vec<wire::SubscribeRequest>>>,
    get_requests: Arc<Mutex<Vec<wire::GetRequest>>>,
    dropped: Arc<AtomicUsize>,
}

impl Mock {
    fn subscription(&self) -> ConfiguredSubscription<Self> {
        ConfiguredSubscription::new(endpoint(), self.clone(), spec(NumericType::Uint))
            .expect("subscription")
    }

    fn get(&self, spec: GetSpec) -> ConfiguredGet<Self> {
        ConfiguredGet::new(endpoint(), self.clone(), spec).expect("configured Get")
    }

    fn once(&self, spec: OnceSpec) -> ConfiguredOnce<Self> {
        ConfiguredOnce::new(endpoint(), self.clone(), spec).expect("configured ONCE")
    }

    fn new(script: Script) -> Self {
        Self {
            script: Arc::new(script),
            requests: Arc::default(),
            get_requests: Arc::default(),
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
            let is_get = request.uri().path() == "/gnmi.gNMI/Get";

            assert!(is_get || request.uri().path() == "/gnmi.gNMI/Subscribe");

            let bytes = request
                .into_body()
                .collect()
                .await
                .expect("request body")
                .to_bytes();

            assert_eq!(bytes[0], 0, "uncompressed gRPC request");
            let length = u32::from_be_bytes(bytes[1..5].try_into().expect("frame length")) as usize;
            assert_eq!(length + 5, bytes.len(), "one request");

            let messages = if is_get {
                let request = wire::GetRequest::decode(&bytes[5..]).expect("Get request");
                let mut requests = mock.get_requests.lock().expect("request lock");
                let index = requests.len();

                requests.push(request);

                let response = mock
                    .script
                    .get_responses
                    .get(index)
                    .or_else(|| mock.script.get_responses.last())
                    .cloned()
                    .unwrap_or_default();

                vec![response.encode_to_vec()]
            } else {
                let request =
                    wire::SubscribeRequest::decode(&bytes[5..]).expect("Subscribe request");

                mock.requests.lock().expect("request lock").push(request);
                mock.script
                    .responses
                    .iter()
                    .map(Message::encode_to_vec)
                    .collect()
            };

            if let Some((clock, elapsed)) = &mock.script.advance_clock {
                clock.advance(*elapsed);
            }

            if mock.script.hold_connect {
                let _probe = probe;
                return std::future::pending().await;
            }

            let mut bytes = Vec::new();

            // Each gRPC message starts with a compression flag and a four-byte length.
            for message in messages {
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

            trailers.insert("grpc-message", "target-secret".parse().expect("error text"));

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
                response
                    .headers_mut()
                    .insert("grpc-message", "target-secret".parse().expect("error text"));

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

/// Collects one stream unit on the fixed clock.
fn collect(stream: StreamUnit) -> (Collector, Reports) {
    let (collector, reports) = Collector::new(FixedClock);
    collector
        .add(EndpointUnits::new(EndpointPolicy::default()).with_streams([stream]))
        .expect("a stream forms an endpoint");
    (collector, reports)
}

async fn next_report(reports: &mut Reports) -> AcquisitionReport {
    match reports.next().await {
        Some(Collected::Report(report)) => report,
        Some(Collected::Retired(_)) => panic!("a report is due, not the retirement"),
        None => panic!("a report is due, but the reports ended"),
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

    for subject in [
        SubjectTemplate::new("interface", IdentityValue::literal("fixed")),
        SubjectTemplate::new("interface", IdentityValue::path_key(1, "name")),
        SubjectTemplate::new("", IdentityValue::path_key(0, "name")),
    ] {
        assert!(GetSpec::new(
            vec![valid.clone()],
            vec![Mapping::collection_boolean_state(
                valid.clone(),
                subject,
                StateTemplate::new("enabled"),
            )]
        )
        .is_err());
    }

    for (requests, mappings) in cases {
        assert!(GetSpec::new(requests.clone(), mappings.clone()).is_err());
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
fn state_configuration_validates_name_and_facet_byte_bounds() {
    use nv_telemetry_model::limits;

    let subject = descriptor("a").key().subject().clone();
    let n = limits::STATEOBSERVATION_NAME_MAX_LEN as usize;
    let f = limits::STATEOBSERVATION_FACET_MAX_LEN as usize;
    let multibyte = "\u{00e9}";

    let cases = [
        ("status".into(), None, true),
        (String::new(), None, false),
        ("x".repeat(n), None, true),
        ("x".repeat(n + 1), None, false),
        (multibyte.repeat(n / 2), None, true),
        (multibyte.repeat(n / 2 + 1), None, false),
        ("status".into(), Some(String::new()), false),
        ("status".into(), Some("x".repeat(f)), true),
        ("status".into(), Some("x".repeat(f + 1)), false),
        ("status".into(), Some(multibyte.repeat(f / 2)), true),
        ("status".into(), Some(multibyte.repeat(f / 2 + 1)), false),
    ];

    for (index, (name, facet, expected)) in cases.into_iter().enumerate() {
        let mut identity = StateIdentity::new(subject.clone(), &name);

        if let Some(facet) = facet {
            identity = identity.with_facet(facet);
        }

        let mappings = [
            Mapping::string_state(path(&["leaf"]), identity.clone()),
            Mapping::boolean_state(path(&["leaf"]), identity),
        ];

        for mapping in mappings {
            assert_eq!(
                SubscriptionSpec::new(vec![path(&["leaf"])], vec![mapping]).is_ok(),
                expected,
                "case {index}"
            );
        }
    }
}

#[test]
fn declared_state_types_preserve_native_and_json_scalars() {
    let subject = descriptor("a").key().subject().clone();
    let up = ModelValue::string("openconfig-platform-types:ACTIVE").expect("state string");
    let lower = ModelValue::string("up").expect("state string");
    let empty = ModelValue::string("").expect("empty value is valid");
    let yes = ModelValue::bool(true);
    let no = ModelValue::bool(false);

    let cases = [
        (
            true,
            Value::StringVal("openconfig-platform-types:ACTIVE".into()),
            &up,
        ),
        (true, Value::JsonVal(br#""up""#.to_vec()), &lower),
        (
            true,
            Value::JsonIetfVal(br#""openconfig-platform-types:ACTIVE""#.to_vec()),
            &up,
        ),
        (true, Value::StringVal(String::new()), &empty),
        (false, Value::BoolVal(true), &yes),
        (false, Value::BoolVal(false), &no),
        (false, Value::JsonVal(b"false".to_vec()), &no),
        (false, Value::JsonIetfVal(b"true".to_vec()), &yes),
    ];

    for (is_string, value, expected) in cases {
        let (name, facet) = if is_string {
            ("status", Some("link".to_owned()))
        } else {
            ("enabled", None)
        };

        let mut identity = StateIdentity::new(subject.clone(), name);

        if let Some(facet) = &facet {
            identity = identity.with_facet(facet);
        }

        let mapping = if is_string {
            Mapping::string_state(path(&["leaf"]), identity)
        } else {
            Mapping::boolean_state(path(&["leaf"]), identity)
        };

        let spec = SubscriptionSpec::new(vec![path(&["leaf"])], vec![mapping])
            .expect("state-only subscription");

        let parts = projection::project(
            &spec.projection,
            wire::Notification {
                timestamp: -1,
                update: vec![update(value)],
                ..Default::default()
            },
        )
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .expect("state projects");

        assert_eq!(parts.payloads().len(), 1);
        assert_eq!(parts.payloads()[0].0.completeness(), Completeness::Partial);
        assert!(parts.issues().is_empty());

        let Payload::States(states) = &parts.payloads()[0].1 else {
            panic!("state payload")
        };

        let observation = &states.observations()[0];

        assert_eq!(states.observations().len(), 1);
        assert_eq!(observation.subject(), &subject);
        assert_eq!(observation.name(), name);
        assert_eq!(observation.facet(), facet.as_deref());
        assert_eq!(observation.value(), expected);

        assert_eq!(
            observation.observed_at(),
            Some(&Timestamp::new(-1, 999_999_999).expect("device timestamp"))
        );
    }
}

#[test]
fn unsupported_ancestor_values_report_issues() {
    let keyed = SubscriptionPath::new(vec![
        PathElement::new("interface").with_key("name", "eth0"),
        PathElement::new("enabled"),
    ]);

    let spec = SubscriptionSpec::new(
        vec![path(&["interface"])],
        vec![Mapping::boolean_state(
            keyed,
            StateIdentity::new(descriptor("a").key().subject().clone(), "enabled"),
        )],
    )
    .expect("keyed leaf mapping");

    let mut matching = wire_path(&["interface"]);
    matching.elem[0].key.insert("name".into(), "eth0".into());
    let mut unrelated = matching.clone();
    unrelated.elem[0].key.insert("name".into(), "eth1".into());

    for parent in [wire_path(&[]), wire_path(&["interface"]), matching] {
        for value in [
            Value::JsonVal(b"[]".to_vec()),
            Value::JsonIetfVal(b"[]".to_vec()),
            Value::BoolVal(false),
        ] {
            let parts = projection::project(
                &spec.projection,
                wire::Notification {
                    prefix: Some(parent.clone()),
                    update: vec![wire::Update {
                        path: Some(wire_path(&[])),
                        val: Some(wire::TypedValue { value: Some(value) }),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
            .expect("ancestor update issue");

            assert!(parts.payloads().is_empty());
            assert_eq!(parts.issues().len(), 1);
            assert_eq!(parts.issues()[0].path(), "update[0].path");
        }
    }

    let parts = projection::project(
        &spec.projection,
        wire::Notification {
            prefix: Some(unrelated),
            update: vec![update(Value::JsonVal(b"{}".to_vec()))],
            ..Default::default()
        },
    )
    .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
    .expect("unrelated update is ignored");

    assert!(parts.payloads().is_empty());
    assert!(parts.issues().is_empty());
}

#[test]
fn state_decode_failures_keep_valid_siblings_and_original_issue_indexes() {
    let subject = descriptor("a").key().subject().clone();

    let cases = [
        (
            Mapping::string_state(
                path(&["invalid"]),
                StateIdentity::new(subject.clone(), "status"),
            ),
            vec![
                Value::BoolVal(true),
                Value::StringVal("x".repeat(4097)),
                Value::JsonVal(serde_json::to_vec(&"x".repeat(4097)).expect("JSON string")),
                Value::JsonIetfVal(serde_json::to_vec(&"x".repeat(4097)).expect("JSON string")),
            ],
        ),
        (
            Mapping::boolean_state(path(&["invalid"]), StateIdentity::new(subject, "enabled")),
            vec![
                Value::StringVal("true".into()),
                Value::JsonVal(br#""false""#.to_vec()),
                Value::JsonIetfVal(br#""true""#.to_vec()),
            ],
        ),
    ];

    for (mapping, mismatches) in cases {
        let spec = SubscriptionSpec::new(
            vec![path(&["leaf"]), path(&["invalid"])],
            vec![
                Mapping::new(path(&["leaf"]), descriptor("a"), NumericType::Uint),
                mapping,
            ],
        )
        .expect("mixed subscription");

        let mut values = vec![
            None,
            Some(Value::IntVal(1)),
            Some(Value::UintVal(0)),
            Some(Value::DoubleVal(1.0)),
            Some(Value::BytesVal(b"UP".to_vec())),
        ];

        values.extend(mismatches.into_iter().map(Some));

        for json in [b"null".as_slice(), b"{}", b"[]", b"broken", b"1"] {
            values.push(Some(Value::JsonVal(json.to_vec())));
            values.push(Some(Value::JsonIetfVal(json.to_vec())));
        }

        for value in values {
            let invalid = wire::Update {
                path: Some(wire_path(&["invalid"])),
                val: Some(wire::TypedValue { value }),
                ..Default::default()
            };

            let parts = projection::project(
                &spec.projection,
                wire::Notification {
                    update: vec![update(Value::UintVal(42)), invalid],
                    ..Default::default()
                },
            )
            .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
            .expect("partial notification");

            assert_eq!(parts.issues().len(), 1);
            assert_eq!(parts.issues()[0].path(), "update[1].val");

            assert_eq!(
                parts.issues()[0].kind(),
                &nv_telemetry_source::ProjectionIssueKind::Invalid {
                    detail: "value does not match the declared type or exceeds the model limit"
                        .into(),
                }
            );

            assert_eq!(parts.payloads().len(), 1);

            let Payload::Readings(readings) = &parts.payloads()[0].1 else {
                panic!("valid numeric sibling")
            };

            assert_eq!(*readings.samples()[0].value(), NumericValue::Uint(42));
        }
    }
}

#[test]
fn state_configuration_separates_reading_keys_from_state_identities() {
    let subject = descriptor("a").key().subject().clone();

    let state = Mapping::boolean_state(
        path(&["root", "enabled"]),
        StateIdentity::new(subject.clone(), "enabled"),
    );

    assert!(SubscriptionSpec::new(vec![path(&["root"])], vec![state.clone()]).is_ok());

    assert!(SubscriptionSpec::new(
        vec![path(&["root"])],
        vec![
            state.clone(),
            Mapping::new(
                path(&["root", "counter"]),
                descriptor("a"),
                NumericType::Uint
            )
        ]
    )
    .is_ok());

    let cases = [
        ("status", subject.clone(), "enabled", None, false),
        ("status", subject.clone(), "status", None, true),
        (
            "status",
            subject.clone(),
            "enabled",
            Some("admin".to_owned()),
            true,
        ),
        (
            "status",
            descriptor("b").key().subject().clone(),
            "enabled",
            None,
            true,
        ),
        ("enabled", subject.clone(), "status", None, false),
    ];

    for (leaf, subject, name, facet, expected) in cases {
        let mut identity = StateIdentity::new(subject, name);

        if let Some(facet) = facet {
            identity = identity.with_facet(facet);
        }

        let other = Mapping::string_state(path(&["root", leaf]), identity);

        assert_eq!(
            SubscriptionSpec::new(vec![path(&["root"])], vec![state.clone(), other]).is_ok(),
            expected,
            "{leaf}/{name}"
        );
    }

    for state_leaf in ["enabled", "outside"] {
        let numeric = Mapping::new(path(&["enabled"]), descriptor("a"), NumericType::Uint);

        let state = Mapping::boolean_state(
            path(&[state_leaf]),
            StateIdentity::new(subject.clone(), "enabled"),
        );

        assert!(SubscriptionSpec::new(vec![path(&["enabled"])], vec![numeric, state]).is_err());
    }
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
        for enabled in [false, true] {
            let mapping = if enabled {
                Mapping::numeric_with_text(path(&["leaf"]), descriptor("a"), kind)
            } else {
                Mapping::new(path(&["leaf"]), descriptor("a"), kind)
            };

            let spec = SubscriptionSpec::new(vec![path(&["leaf"])], vec![mapping]).unwrap();

            let parts = projection::project(
                &spec.projection,
                wire::Notification {
                    update: vec![update(value.clone())],
                    ..Default::default()
                },
            )
            .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
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
}

#[test]
fn numeric_text_requires_opt_in_and_preserves_the_declared_type() {
    use NumericType::*;
    use NumericValue::Int as Signed;
    use NumericValue::Uint as Unsigned;

    let fractional = NumericValue::double(-1.25).unwrap();
    let exponent = NumericValue::double(150.0).unwrap();
    let rounded = NumericValue::double(9_007_199_254_740_992.0).unwrap();

    let cases = [
        (Int, "-9223372036854775808", Some(Signed(i64::MIN))),
        (Int, "9223372036854775807", Some(Signed(i64::MAX))),
        (Int, "+0042", Some(Signed(42))),
        (Int, "-0", Some(Signed(0))),
        (Int, "9223372036854775808", None),
        (Int, "-9223372036854775809", None),
        (Int, "1.0", None),
        (Int, "1e2", None),
        (Uint, "18446744073709551615", Some(Unsigned(u64::MAX))),
        (
            Uint,
            "9007199254740993",
            Some(Unsigned(9_007_199_254_740_993)),
        ),
        (Uint, "+0042", Some(Unsigned(42))),
        (Uint, "0", Some(Unsigned(0))),
        (Uint, "18446744073709551616", None),
        (Uint, "-1", None),
        (Uint, "-0", None),
        (Uint, "1.0", None),
        (Uint, "1e2", None),
        (Double, "-1.25", Some(fractional)),
        (Double, "+001.5e2", Some(exponent)),
        (Double, "9007199254740993", Some(rounded)),
        (Double, "NaN", None),
        (Double, "inf", None),
        (Double, "-inf", None),
        (Double, "Infinity", None),
        (Double, "1e999", None),
    ];

    let invalid = [Int, Uint, Double].into_iter().flat_map(|kind| {
        [
            "",
            " 42",
            "42 ",
            "42\n",
            "42ms",
            "0x2a",
            "1_000",
            "target-secret",
        ]
        .into_iter()
        .map(move |text| (kind, text, None))
    });

    for (kind, text, expected) in cases.into_iter().chain(invalid) {
        for (enabled, ascii) in [(false, false), (false, true), (true, false), (true, true)] {
            let mapping = if enabled {
                Mapping::numeric_with_text(path(&["leaf"]), descriptor("a"), kind)
            } else {
                Mapping::new(path(&["leaf"]), descriptor("a"), kind)
            };

            let spec = SubscriptionSpec::new(vec![path(&["leaf"])], vec![mapping]).unwrap();

            let value = if ascii {
                Value::AsciiVal(text.into())
            } else {
                Value::StringVal(text.into())
            };

            let (payloads, issues) = projection::project(
                &spec.projection,
                wire::Notification {
                    update: vec![update(value)],
                    ..Default::default()
                },
            )
            .unwrap();

            if let Some(expected) = expected.filter(|_| enabled) {
                let Payload::Readings(readings) = &payloads[0].1 else {
                    panic!("readings")
                };

                assert_eq!(
                    *readings.samples()[0].value(),
                    expected,
                    "{kind:?}: {text:?}"
                );

                assert_eq!(readings.descriptors(), &[descriptor("a")]);
                assert!(issues.is_empty());
            } else {
                assert!(payloads.is_empty(), "{kind:?}: {text:?}");

                let detail = if enabled {
                    format!("text is not a valid {kind:?} value")
                } else {
                    "value does not match the declared type or exceeds the model limit".into()
                };

                assert_eq!(
                    issues,
                    vec![nv_telemetry_source::ProjectionIssue::invalid(
                        "update[0].val",
                        detail,
                    )]
                );
            }
        }
    }
}

#[tokio::test]
async fn numeric_text_projects_consistently_through_stream_get_once_and_source_bindings() {
    let requests = vec![path(&["leaf"]), path(&["entry"]), path(&["bad"])];

    let mappings = vec![
        Mapping::numeric_with_text(path(&["leaf"]), descriptor("a"), NumericType::Uint),
        Mapping::numeric_with_text(path(&["bad"]), descriptor("bad"), NumericType::Int),
        Mapping::collection_reading(
            path(&["entry", "count"]),
            SubjectTemplate::new("sensor", IdentityValue::path_key(0, "name")),
            ReadingTemplate::new(NumericType::Double).with_text_parsing(),
        ),
    ];

    let mut collection_path = wire_path(&["entry", "count"]);

    collection_path.elem[0]
        .key
        .insert("name".into(), "b".into());

    let notification = snapshot(
        7_000_000_123,
        vec![
            update(Value::StringVal(u64::MAX.to_string())),
            wire::Update {
                path: Some(collection_path),
                ..update(Value::AsciiVal("-2.5e1".into()))
            },
            wire::Update {
                path: Some(wire_path(&["bad"])),
                ..update(Value::StringVal("target-secret".into()))
            },
        ],
    );

    let mut script = once_script(vec![notification.clone()]);

    script.get_responses = vec![wire::GetResponse {
        notification: vec![notification],
        ..Default::default()
    }];

    let mock = Mock::new(script);

    let source = ConfiguredSubscription::new(
        endpoint(),
        mock.clone(),
        SubscriptionSpec::new(requests.clone(), mappings.clone()).unwrap(),
    )
    .unwrap();

    let mut stream = source.perform().await.unwrap();

    let parts = stream.next().await.unwrap().unwrap();

    let mut sourced = source.subscribe_with_sources().await.unwrap();

    let sourced = sourced.next().await.unwrap().unwrap();

    let get = mock
        .get(GetSpec::new(requests.clone(), mappings.clone()).unwrap())
        .perform()
        .await
        .unwrap();

    let once = mock
        .once(OnceSpec::new(requests, mappings).unwrap())
        .perform()
        .await
        .unwrap();

    assert_eq!(get, once);
    assert_eq!(get.payloads(), parts.payloads());
    assert_eq!(get.issues().len(), 1);

    assert_eq!(
        get.issues()[0],
        parts.issues()[0].clone().at_index("notification", 0)
    );

    assert_eq!(sourced.parts(), &parts);
    assert_eq!(parts.issues().len(), 1);
    assert_eq!(sourced.bindings().len(), 2);

    let Payload::Readings(readings) = &parts.payloads()[0].1 else {
        panic!("readings")
    };

    let actual: std::collections::BTreeMap<_, _> = readings
        .samples()
        .iter()
        .map(|reading| (reading.key().subject().id(), *reading.value()))
        .collect();

    assert_eq!(
        actual,
        std::collections::BTreeMap::from([
            ("a", NumericValue::Uint(u64::MAX)),
            ("b", NumericValue::double(-25.0).unwrap()),
        ])
    );

    for binding in sourced.bindings() {
        let ObservationIdentity::Reading(key) = binding.identity() else {
            panic!("reading identity")
        };

        assert!(readings
            .samples()
            .iter()
            .any(|reading| reading.key() == key));

        assert_eq!(
            binding.source().elements().last().unwrap().name(),
            if key.subject().id() == "a" {
                "leaf"
            } else {
                "count"
            }
        );
    }

    assert!(readings
        .samples()
        .iter()
        .all(|reading| reading.observed_at() == Some(&Timestamp::new(7, 123).unwrap())));
}

#[test]
fn numeric_text_keeps_final_values_and_excludes_collisions_from_source_bindings() {
    let spec = SubscriptionSpec::new(
        vec![path(&["entry"])],
        vec![Mapping::collection_reading(
            path(&["entry", "count"]),
            SubjectTemplate::new("sensor", IdentityValue::path_key(0, "name")),
            ReadingTemplate::new(NumericType::Uint).with_text_parsing(),
        )],
    )
    .unwrap();

    let make = |name: &str, source: &str, value| {
        let mut path = wire_path(&["entry", "count"]);

        path.elem[0].key.extend([
            ("name".into(), name.into()),
            ("source".into(), source.into()),
        ]);

        wire::Update {
            path: Some(path),
            ..update(value)
        }
    };

    let notification = snapshot(
        1,
        vec![
            make("stale", "one", Value::StringVal("1".into())),
            make("stale", "one", Value::AsciiVal("invalid".into())),
            make("collision", "one", Value::StringVal("2".into())),
            make("collision", "two", Value::AsciiVal("3".into())),
            make("good", "one", Value::AsciiVal("4".into())),
            make("good", "one", Value::StringVal("5".into())),
            make("good", "two", Value::StringVal("invalid".into())),
        ],
    );

    let projected = projection::project_with_sources(&spec.projection, notification).unwrap();

    let Payload::Readings(readings) = &projected.parts().payloads()[0].1 else {
        panic!("readings")
    };

    assert_eq!(readings.samples().len(), 1);
    assert_eq!(readings.samples()[0].key().subject().id(), "good");
    assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(5));
    assert_eq!(projected.bindings().len(), 1);

    assert_eq!(
        projected.bindings()[0].source().elements()[0].keys()["source"],
        "one"
    );

    let issues: BTreeSet<_> = projected
        .parts()
        .issues()
        .iter()
        .map(nv_telemetry_source::ProjectionIssue::path)
        .collect();

    assert_eq!(
        issues,
        BTreeSet::from([
            "update[1].val",
            "update[2].path",
            "update[3].path",
            "update[6].val",
        ])
    );
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

        let parts = projection::project(&spec.projection, notification)
            .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
            .expect("duplicate update projection");

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
fn state_duplicate_updates_use_only_the_final_value_at_the_original_index() {
    let mapping = Mapping::string_state(
        path(&["parent", "leaf"]),
        StateIdentity::new(descriptor("a").key().subject().clone(), "status"),
    );

    let spec =
        SubscriptionSpec::new(vec![path(&["parent"])], vec![mapping]).expect("state specification");

    let up = Some(Value::StringVal("UP".into()));
    let invalid = Some(Value::BoolVal(true));

    let cases = [
        (
            up.clone(),
            Some(Value::StringVal("DOWN".into())),
            Some("DOWN"),
        ),
        (invalid.clone(), up.clone(), Some("UP")),
        (invalid, Some(Value::BoolVal(false)), None),
        (up, None, None),
    ];

    for (first, last, expected) in cases {
        let updates = [first, last]
            .into_iter()
            .map(|value| wire::Update {
                path: Some(wire_path(&["leaf"])),
                val: value.map(|value| wire::TypedValue { value: Some(value) }),
                ..Default::default()
            })
            .collect();

        let parts = projection::project(
            &spec.projection,
            wire::Notification {
                prefix: Some(wire_path(&["parent"])),
                update: updates,
                ..Default::default()
            },
        )
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .expect("duplicate state projection");

        if let Some(expected) = expected {
            assert_eq!(parts.payloads().len(), 1);
            assert!(parts.issues().is_empty());

            let Payload::States(states) = &parts.payloads()[0].1 else {
                panic!("state payload")
            };

            assert_eq!(states.observations().len(), 1);

            assert_eq!(
                states.observations()[0].value(),
                &ModelValue::string(expected).expect("state string")
            );
        } else {
            assert!(parts.payloads().is_empty());
            assert_eq!(parts.issues().len(), 1);
            assert_eq!(parts.issues()[0].path(), "update[1].val");
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

    let parts = projection::project(&spec.projection, notification)
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .expect("partial projection");

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

    let sparse = projection::project(&spec.projection, wire::Notification::default())
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .expect("empty notification");

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
        vec![Mapping::boolean_state(
            keyed,
            StateIdentity::new(descriptor("a").key().subject().clone(), "enabled"),
        )],
    )
    .expect("parent request")
    .with_origin("openconfig")
    .with_target("switch-a");

    let mut prefix = wire_path(&["interface"]);
    prefix.elem[0].key.insert("name".into(), "Ethernet1".into());
    prefix.origin = "openconfig".into();
    prefix.target = "switch-a".into();
    let mut leaf = update(Value::BoolVal(false));
    leaf.path = Some(wire_path(&["counter"]));
    let mut wrong_key = leaf.clone();
    wrong_key.path.as_mut().expect("path").target = "different-target".into();

    let parts = projection::project(
        &spec.projection,
        wire::Notification {
            prefix: Some(prefix.clone()),
            update: vec![leaf, wrong_key],
            delete: vec![wire_path(&[])],
            ..Default::default()
        },
    )
    .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
    .expect("partial projection");

    assert_eq!(parts.payloads().len(), 1);
    assert_eq!(parts.issues().len(), 2);

    let Payload::States(states) = &parts.payloads()[0].1 else {
        panic!("state payload")
    };

    assert_eq!(states.observations()[0].value(), &ModelValue::bool(false));
    assert!(parts
        .issues()
        .iter()
        .any(|issue| issue.path() == "delete[0]"));

    prefix.elem[0].key.insert("name".into(), "Ethernet2".into());

    let absent = projection::project(
        &spec.projection,
        wire::Notification {
            prefix: Some(prefix),
            update: vec![wire::Update {
                path: Some(wire_path(&["counter"])),
                val: Some(wire::TypedValue {
                    value: Some(Value::BoolVal(true)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
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

    let parts = projection::project(&spec.projection, notification.clone())
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .expect("large notification");

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

    let parts = projection::project(&spec.projection, invalid)
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .expect("issue-only notification");

    assert_eq!(
        parts.issues().len(),
        nv_telemetry_model::limits::PROJECTIONISSUES_ISSUES_MAX_ITEMS as usize
    );

    assert_eq!(
        parts.issues().last().expect("omission issue").path(),
        nv_telemetry_source::OMITTED_ISSUES_LOCATOR
    );
}

#[test]
fn large_state_notifications_split_payloads_and_bound_issues() {
    let count = nv_telemetry_model::limits::STATES_OBSERVATIONS_MAX_ITEMS as usize + 1;

    let spec = SubscriptionSpec::new(
        vec![path(&["root"])],
        (0..count)
            .map(|index| {
                let name = format!("leaf-{index}");
                Mapping::boolean_state(
                    path(&["root", &name]),
                    StateIdentity::new(descriptor(&name).key().subject().clone(), "enabled"),
                )
            })
            .collect(),
    )
    .expect("distinct state identities");

    let mut notification = wire::Notification {
        prefix: Some(wire_path(&["root"])),
        update: (0..count)
            .map(|index| wire::Update {
                path: Some(wire_path(&[&format!("leaf-{index}")])),
                val: Some(wire::TypedValue {
                    value: Some(Value::BoolVal(false)),
                }),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };

    let parts = projection::project(&spec.projection, notification.clone())
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .expect("large state notification");

    assert_eq!(parts.payloads().len(), 2);
    assert!(parts.issues().is_empty());

    let mut total = 0;

    for (coverage, payload) in parts.payloads() {
        let Payload::States(states) = payload else {
            panic!("state payload")
        };

        assert_eq!(coverage.completeness(), Completeness::Partial);
        assert!(states.observations().len() < count);
        assert!(states
            .observations()
            .iter()
            .all(|observation| observation.value() == &ModelValue::bool(false)));

        total += states.observations().len();
    }

    assert_eq!(total, count);

    for update in &mut notification.update {
        update.val = Some(wire::TypedValue {
            value: Some(Value::StringVal("false".into())),
        });
    }

    let invalid = projection::project(&spec.projection, notification)
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .expect("bounded state issues");

    assert!(invalid.payloads().is_empty());

    assert_eq!(
        invalid.issues().len(),
        nv_telemetry_model::limits::PROJECTIONISSUES_ISSUES_MAX_ITEMS as usize
    );

    assert_eq!(
        invalid.issues().last().expect("omission issue").path(),
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

        let mut expected_keyed = wire_path(&["interface"]);
        expected_keyed.elem[0]
            .key
            .insert("name".into(), "Ethernet1".into());

        let expected = wire::SubscribeRequest {
            request: Some(wire::subscribe_request::Request::Subscribe(
                wire::SubscriptionList {
                    prefix: Some(wire::Path {
                        origin: "openconfig".into(),
                        target: "switch-a".into(),
                        ..Default::default()
                    }),
                    subscription: vec![
                        wire::Subscription {
                            path: Some(expected_keyed),
                            mode: wire::SubscriptionMode::TargetDefined as i32,
                            ..Default::default()
                        },
                        wire::Subscription {
                            path: Some(wire_path(&["other"])),
                            mode: wire::SubscriptionMode::TargetDefined as i32,
                            ..Default::default()
                        },
                    ],
                    mode: wire::subscription_list::Mode::Stream as i32,
                    encoding: if encoding.unwrap_or(Encoding::Json) == Encoding::Json {
                        wire::Encoding::Json as i32
                    } else {
                        wire::Encoding::JsonIetf as i32
                    },
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        assert_eq!(requests.as_slice(), &[expected]);
    }
}

#[tokio::test]
async fn request_options_preserve_modes_order_and_overlaps_across_acquisitions() {
    let mock = Mock::new(Script::default());
    let requests = Arc::clone(&mock.requests);
    let root = path(&["root"]);

    let spec = SubscriptionSpec::from_requests(
        vec![
            SubscriptionRequest::sample(root.clone(), Duration::from_secs(7))
                .with_heartbeat_interval(Duration::from_secs(23))
                .with_suppress_redundant(true),
            SubscriptionRequest::on_change(path(&["root", "status"]))
                .with_heartbeat_interval(Duration::from_millis(500)),
            SubscriptionRequest::target_defined(root)
                .with_heartbeat_interval(Duration::ZERO)
                .with_suppress_redundant(false),
        ],
        vec![Mapping::new(
            path(&["root", "counter"]),
            descriptor("a"),
            NumericType::Uint,
        )],
    )
    .expect("mixed request options")
    .with_origin("example")
    .with_target("endpoint-a")
    .with_encoding(Encoding::JsonIetf);

    let unit = ConfiguredSubscription::new(endpoint(), mock, spec).expect("subscription");

    for _ in 0..2 {
        let stream = unit.perform().await.expect("opens");
        drop(stream);
    }

    let requests = requests.lock().expect("request lock");

    let expected = wire::SubscribeRequest {
        request: Some(wire::subscribe_request::Request::Subscribe(
            wire::SubscriptionList {
                prefix: Some(wire::Path {
                    origin: "example".into(),
                    target: "endpoint-a".into(),
                    ..Default::default()
                }),
                subscription: vec![
                    wire::Subscription {
                        path: Some(wire_path(&["root"])),
                        mode: wire::SubscriptionMode::Sample as i32,
                        sample_interval: 7_000_000_000,
                        heartbeat_interval: 23_000_000_000,
                        suppress_redundant: true,
                    },
                    wire::Subscription {
                        path: Some(wire_path(&["root", "status"])),
                        mode: wire::SubscriptionMode::OnChange as i32,
                        heartbeat_interval: 500_000_000,
                        ..Default::default()
                    },
                    wire::Subscription {
                        path: Some(wire_path(&["root"])),
                        mode: wire::SubscriptionMode::TargetDefined as i32,
                        ..Default::default()
                    },
                ],
                mode: wire::subscription_list::Mode::Stream as i32,
                encoding: wire::Encoding::JsonIetf as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    };

    assert_eq!(requests.as_slice(), &[expected.clone(), expected]);
}

#[test]
fn request_intervals_preserve_zero_nanoseconds_and_uint64_bounds() {
    let leaf = path(&["leaf"]);
    let maximum = Duration::from_nanos(u64::MAX);

    let cases = [
        (
            SubscriptionRequest::sample(leaf.clone(), Duration::ZERO),
            0,
            0,
        ),
        (
            SubscriptionRequest::sample(leaf.clone(), Duration::from_nanos(1)),
            1,
            0,
        ),
        (
            SubscriptionRequest::sample(leaf.clone(), maximum),
            u64::MAX,
            0,
        ),
        (
            SubscriptionRequest::sample(leaf.clone(), Duration::ZERO)
                .with_heartbeat_interval(maximum),
            0,
            u64::MAX,
        ),
        (
            SubscriptionRequest::on_change(leaf.clone()).with_heartbeat_interval(maximum),
            0,
            u64::MAX,
        ),
        (
            SubscriptionRequest::on_change(leaf.clone()).with_heartbeat_interval(Duration::ZERO),
            0,
            0,
        ),
    ];

    for (request, sample, heartbeat) in cases {
        let spec = SubscriptionSpec::from_requests(
            vec![request],
            vec![Mapping::new(
                leaf.clone(),
                descriptor("a"),
                NumericType::Uint,
            )],
        )
        .expect("representable intervals");

        let wire::subscribe_request::Request::Subscribe(list) =
            spec.request().request.expect("request")
        else {
            panic!("Subscribe list")
        };

        let subscription = list.subscription.first().expect("one request");

        assert_eq!(subscription.sample_interval, sample);
        assert_eq!(subscription.heartbeat_interval, heartbeat);
    }
}

#[test]
fn request_options_reject_inapplicable_settings_and_interval_overflow() {
    let leaf = path(&["leaf"]);
    let overflow = Duration::from_nanos(u64::MAX) + Duration::from_nanos(1);

    let cases = [
        (
            SubscriptionRequest::sample(leaf.clone(), overflow),
            "sample_interval",
        ),
        (
            SubscriptionRequest::sample(leaf.clone(), Duration::ZERO)
                .with_heartbeat_interval(overflow),
            "heartbeat_interval",
        ),
        (
            SubscriptionRequest::on_change(leaf.clone()).with_heartbeat_interval(overflow),
            "heartbeat_interval",
        ),
        (
            SubscriptionRequest::target_defined(leaf.clone())
                .with_heartbeat_interval(Duration::from_nanos(1)),
            "heartbeat_interval",
        ),
        (
            SubscriptionRequest::target_defined(leaf.clone()).with_suppress_redundant(true),
            "suppress_redundant",
        ),
        (
            SubscriptionRequest::on_change(leaf.clone()).with_suppress_redundant(true),
            "suppress_redundant",
        ),
    ];

    for (request, field) in cases {
        let error = SubscriptionSpec::from_requests(
            vec![request],
            vec![Mapping::new(
                leaf.clone(),
                descriptor("a"),
                NumericType::Uint,
            )],
        )
        .expect_err("invalid request configuration");

        assert!(error.to_string().contains(field), "{field}: {error}");
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

            let request = SubscriptionRequest::sample(path(&["leaf"]), Duration::from_secs(1))
                .with_heartbeat_interval(Duration::from_secs(2))
                .with_suppress_redundant(true);

            let spec = SubscriptionSpec::from_requests(
                vec![request],
                vec![Mapping::new(
                    path(&["leaf"]),
                    descriptor("a"),
                    NumericType::Uint,
                )],
            )
            .expect("sampled subscription");

            let unit = ConfiguredSubscription::new(endpoint(), mock, spec).expect("subscription");

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
async fn stream_unit_reports_mixed_notifications_transitions_and_distinct_collector_timestamp() {
    let updates = [
        ("leaf", Value::UintVal(3)),
        ("enabled", Value::BoolVal(false)),
        ("invalid", Value::StringVal("unknown".into())),
    ]
    .into_iter()
    .map(|(name, value)| wire::Update {
        path: Some(wire_path(&[name])),
        ..update(value)
    })
    .collect();

    let transition = wire::Update {
        path: Some(wire_path(&["enabled"])),
        ..update(Value::BoolVal(true))
    };

    let mock = Mock::new(Script {
        responses: vec![
            notification(updates),
            notification(vec![transition]),
            notification(vec![]),
        ],
        ..Default::default()
    });

    let subject = descriptor("a").key().subject().clone();

    let spec = SubscriptionSpec::new(
        vec![path(&["leaf"]), path(&["enabled"]), path(&["invalid"])],
        vec![
            Mapping::new(path(&["leaf"]), descriptor("a"), NumericType::Uint),
            Mapping::boolean_state(
                path(&["enabled"]),
                StateIdentity::new(subject.clone(), "enabled"),
            ),
            Mapping::boolean_state(path(&["invalid"]), StateIdentity::new(subject, "status")),
        ],
    )
    .expect("mixed specification");

    let unit =
        Arc::new(ConfiguredSubscription::new(endpoint(), mock, spec).expect("mixed subscription"));

    let plan = nv_telemetry_orchestration::plan(
        Needs::default().with_streams([StreamNeed::new(
            endpoint(),
            ConfiguredSubscription::<Mock>::REQUEST_CLASS,
        )]),
        &[ConfiguredSubscription::<Mock>::declaration()],
    )
    .expect("provider declaration");

    let stream = StreamUnit::new(plan.streams()[0].clone(), unit, ReconnectPolicy::default());
    let (_collector, mut reports) = collect(stream);

    for (index, (expected, count)) in [(Some(false), 2), (Some(true), 1), (None, 0)]
        .into_iter()
        .enumerate()
    {
        let report = next_report(&mut reports).await;

        assert_eq!(report.status().provider(), "gnmi.configured");
        assert_eq!(report.status().request_class(), "configured-subscribe");
        assert_eq!(report.status().outcome(), Outcome::Succeeded);
        assert_eq!(report.batches().len(), count);

        assert_eq!(
            report.issues().map_or(0, |issues| issues.issues().len()),
            usize::from(index == 0)
        );

        if let Some(issues) = report.issues() {
            assert_eq!(issues.issues()[0].path(), "update[2].val");
        }

        let mut state_count = 0;

        for batch in report.batches() {
            assert_eq!(batch.endpoint(), &endpoint());
            assert_eq!(batch.window().start().seconds(), 99);

            let observed_at = match batch.payload() {
                Payload::Readings(readings) => {
                    assert_eq!(readings.samples().len(), 1);
                    assert_eq!(*readings.samples()[0].value(), NumericValue::Uint(3));
                    readings.samples()[0].observed_at()
                }
                Payload::States(states) => {
                    state_count += 1;

                    assert_eq!(states.observations().len(), 1);

                    assert_eq!(
                        states.observations()[0].value(),
                        &ModelValue::bool(expected.expect("state report"))
                    );

                    states.observations()[0].observed_at()
                }
                _ => panic!("reading or state payload"),
            };

            assert_eq!(
                observed_at,
                Some(&Timestamp::new(7, 123).expect("device timestamp"))
            );
        }

        assert_eq!(state_count, usize::from(expected.is_some()));
    }
}

#[test]
fn collection_subjects_use_full_path_keys_and_preserve_descriptor_metadata() {
    let subject = SubjectTemplate::new("interface", IdentityValue::path_key(1, "name"))
        .with_scope(vec![IdentityValue::literal("switch-a")]);

    let spec = SubscriptionSpec::new(
        vec![path(&["interfaces"])],
        vec![Mapping::collection_reading(
            path(&["interfaces", "interface", "state", "in-octets"]),
            subject,
            ReadingTemplate::new(NumericType::Uint)
                .with_facet("in-octets")
                .with_unit("By")
                .with_accumulation(Accumulation::Cumulative),
        )],
    )
    .expect("collection spec");

    let updates = ["eth0", "eth1"].map(|name| {
        let mut path = wire_path(&["interface", "state", "in-octets"]);
        path.elem[0].key.insert("name".into(), name.into());
        path.elem[0].key.insert("extra".into(), "ignored".into());

        wire::Update {
            path: Some(path),
            val: Some(wire::TypedValue {
                value: Some(Value::UintVal(u64::MAX)),
            }),
            ..Default::default()
        }
    });

    let parts = projection::project(
        &spec.projection,
        wire::Notification {
            prefix: Some(wire_path(&["interfaces"])),
            timestamp: 7_000_000_123,
            update: updates.into(),
            ..Default::default()
        },
    )
    .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
    .expect("projection");

    assert!(parts.issues().is_empty());

    let Payload::Readings(readings) = &parts.payloads()[0].1 else {
        panic!("readings")
    };

    assert_eq!(readings.samples().len(), 2);

    let ids: BTreeSet<_> = readings
        .descriptors()
        .iter()
        .map(|descriptor| {
            assert_eq!(descriptor.unit(), Some("By"));
            assert_eq!(descriptor.accumulation(), Some(Accumulation::Cumulative));
            assert_eq!(descriptor.key().facet(), Some("in-octets"));
            assert_eq!(descriptor.key().subject().scope(), &["switch-a"]);

            descriptor.key().subject().id()
        })
        .collect();

    assert_eq!(ids, BTreeSet::from(["eth0", "eth1"]));

    assert!(readings
        .samples()
        .iter()
        .all(|sample| sample.value() == &NumericValue::Uint(u64::MAX)
            && sample.observed_at() == Some(&Timestamp::new(7, 123).unwrap())));
}

#[test]
fn collection_configuration_rejects_bad_templates_and_overlapping_rules() {
    let leaf = path(&["interfaces", "interface", "enabled"]);
    let subject = SubjectTemplate::new("interface", IdentityValue::path_key(1, "name"));

    let mapping = Mapping::collection_boolean_state(
        leaf.clone(),
        subject.clone(),
        StateTemplate::new("enabled"),
    );

    let validate = |mappings| SubscriptionSpec::new(vec![path(&["interfaces"])], mappings);

    for subject in [
        SubjectTemplate::new("interface", IdentityValue::literal("fixed")),
        SubjectTemplate::new("interface", IdentityValue::path_key(3, "name")),
        SubjectTemplate::new("interface", IdentityValue::path_key(1, "")),
        SubjectTemplate::new("interface", IdentityValue::path_key(1, "*")),
        SubjectTemplate::new("", IdentityValue::path_key(1, "name")),
        subject.clone().with_scope(vec![IdentityValue::literal("")]),
        subject
            .clone()
            .with_scope(vec![IdentityValue::literal("a".repeat(257))]),
        subject
            .clone()
            .with_scope(vec![IdentityValue::literal("a"); 17]),
    ] {
        assert!(validate(vec![Mapping::collection_boolean_state(
            leaf.clone(),
            subject,
            StateTemplate::new("enabled")
        )])
        .is_err());
    }

    for state in [
        StateTemplate::new(""),
        StateTemplate::new("enabled").with_facet(""),
    ] {
        assert!(validate(vec![Mapping::collection_boolean_state(
            leaf.clone(),
            subject.clone(),
            state
        )])
        .is_err());
    }

    for reading in [
        ReadingTemplate::new(NumericType::Int).with_unit(""),
        ReadingTemplate::new(NumericType::Int).with_kind(""),
        ReadingTemplate::new(NumericType::Int).with_facet(""),
    ] {
        assert!(validate(vec![Mapping::collection_reading(
            leaf.clone(),
            subject.clone(),
            reading
        )])
        .is_err());
    }

    let counter = |leaf: &str, reading: ReadingTemplate| {
        Mapping::collection_reading(
            path(&["interfaces", "interface", "state", "counters", leaf]),
            subject.clone(),
            reading,
        )
    };

    let uint = || ReadingTemplate::new(NumericType::Uint);

    // Without facets, both counters of every interface share one signal key.
    assert!(validate(vec![
        counter("in-octets", uint()),
        counter("out-octets", uint())
    ])
    .is_err());

    assert!(validate(vec![
        counter("in-octets", uint().with_facet("in-octets")),
        counter("out-octets", uint().with_facet("out-octets")),
    ])
    .is_ok());

    let status = Mapping::collection_string_state(
        path(&["interfaces", "interface", "status"]),
        subject.clone(),
        StateTemplate::new("enabled"),
    );

    assert!(validate(vec![mapping.clone(), status]).is_err());

    let filtered = |name: &str| {
        SubscriptionPath::new(vec![
            PathElement::new("interfaces"),
            PathElement::new("interface").with_key("name", name),
            PathElement::new("enabled"),
        ])
    };

    let exact = Mapping::boolean_state(
        filtered("eth0"),
        StateIdentity::new(descriptor("a").key().subject().clone(), "enabled"),
    );

    assert!(validate(vec![mapping.clone(), exact.clone()]).is_err());
    assert!(validate(vec![exact, mapping.clone()]).is_err());

    let filtered_mapping = |name| {
        Mapping::collection_boolean_state(
            filtered(name),
            subject.clone(),
            StateTemplate::new("enabled"),
        )
    };

    assert!(validate(vec![mapping.clone(), filtered_mapping("eth0")]).is_err());
    assert!(validate(vec![filtered_mapping("eth0"), filtered_mapping("eth1")]).is_ok());
    assert!(SubscriptionSpec::new(vec![filtered("eth0")], vec![mapping]).is_err());
}

fn nested_collection() -> (SubscriptionSpec, wire::Path) {
    let leaf = SubscriptionPath::new(vec![
        PathElement::new("list"),
        PathElement::new("entry").with_key("group", "a"),
        PathElement::new("entry"),
        PathElement::new("enabled"),
    ]);

    let subject =
        SubjectTemplate::new("port", IdentityValue::path_key(2, "name")).with_scope(vec![
            IdentityValue::path_key(1, "name"),
            IdentityValue::literal("switch-a"),
        ]);

    let spec = SubscriptionSpec::new(
        vec![path(&["list"])],
        vec![Mapping::collection_boolean_state(
            leaf,
            subject,
            StateTemplate::new("enabled").with_facet("link"),
        )],
    )
    .unwrap();

    let mut valid = wire_path(&["list", "entry", "entry", "enabled"]);
    valid.elem[1]
        .key
        .extend([("group".into(), "a".into()), ("name".into(), "rack".into())]);

    valid.elem[2].key.insert("name".into(), "eth0".into());

    (spec, valid)
}

#[test]
fn collection_prefix_splits_resolve_nested_key_selectors() {
    let (spec, valid) = nested_collection();

    // All prefix splits must use the same absolute selector positions.
    for split in 0..=valid.elem.len() {
        let parts = projection::project(
            &spec.projection,
            wire::Notification {
                prefix: Some(wire::Path {
                    elem: valid.elem[..split].to_vec(),
                    origin: "openconfig".into(),
                    target: "switch-a".into(),
                    ..Default::default()
                }),
                update: vec![wire::Update {
                    path: Some(wire::Path {
                        elem: valid.elem[split..].to_vec(),
                        ..Default::default()
                    }),
                    ..update(Value::BoolVal(false))
                }],
                ..Default::default()
            },
        )
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .unwrap();

        assert!(parts.issues().is_empty(), "split {split}");

        let Payload::States(states) = &parts.payloads()[0].1 else {
            panic!("states")
        };

        let state = &states.observations()[0];

        assert_eq!(state.subject().id(), "eth0");
        assert_eq!(state.subject().scope(), &["rack", "switch-a"]);
        assert_eq!(state.facet(), Some("link"));
        assert_eq!(state.value(), &ModelValue::bool(false));
    }
}

#[test]
fn collection_filters_keys_and_reports_invalid_keys_and_ancestors() {
    let (spec, valid) = nested_collection();

    let mut other_group = valid.clone();
    other_group.elem[1].key.insert("group".into(), "b".into());

    let mut missing = valid.clone();
    missing.elem[2].key.clear();

    let mut empty = valid.clone();
    empty.elem[2].key.insert("name".into(), String::new());

    let mut oversized = valid.clone();
    oversized.elem[2].key.insert("name".into(), "a".repeat(257));

    let mut wrong_ancestor = other_group;
    wrong_ancestor.elem.truncate(2);

    let mut ancestor = valid.clone();
    ancestor.elem.truncate(2);

    let paths = [
        missing,
        empty,
        oversized,
        ancestor.clone(),
        wrong_ancestor.clone(),
    ];

    let parts = projection::project(
        &spec.projection,
        wire::Notification {
            update: paths
                .into_iter()
                .map(|path| wire::Update {
                    path: Some(path),
                    ..update(Value::BoolVal(true))
                })
                .collect(),
            delete: vec![ancestor, wrong_ancestor],
            ..Default::default()
        },
    )
    .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
    .unwrap();

    assert!(parts.payloads().is_empty());

    let issues: BTreeSet<_> = parts
        .issues()
        .iter()
        .map(nv_telemetry_source::ProjectionIssue::path)
        .collect();

    assert_eq!(
        issues,
        BTreeSet::from([
            "update[0].path",
            "update[1].path",
            "update[2].path",
            "update[3].path",
            "delete[0]"
        ])
    );
}

#[test]
fn collection_duplicates_keep_last_values_per_concrete_entry() {
    let spec = SubscriptionSpec::new(
        vec![path(&["entry"])],
        vec![Mapping::collection_string_state(
            path(&["entry", "status"]),
            SubjectTemplate::new("port", IdentityValue::path_key(0, "name")),
            StateTemplate::new("status"),
        )],
    )
    .unwrap();

    let make = |name: &str, value| {
        let mut path = wire_path(&["entry", "status"]);
        path.elem[0].key.insert("name".into(), name.into());

        wire::Update {
            path: Some(path),
            ..update(value)
        }
    };

    let parts = projection::project(
        &spec.projection,
        wire::Notification {
            update: vec![
                make("a", Value::StringVal("UP".into())),
                make("b", Value::StringVal("UP".into())),
                make("a", Value::UintVal(1)),
                make("b", Value::StringVal("NEW".into())),
                make("c", Value::StringVal("UP".into())),
            ],
            ..Default::default()
        },
    )
    .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
    .unwrap();

    let Payload::States(states) = &parts.payloads()[0].1 else {
        panic!("states")
    };

    let observed: std::collections::BTreeMap<_, _> = states
        .observations()
        .iter()
        .map(|state| (state.subject().id(), state.value().clone()))
        .collect();

    assert_eq!(
        observed,
        std::collections::BTreeMap::from([
            ("b", ModelValue::string("NEW").unwrap()),
            ("c", ModelValue::string("UP").unwrap())
        ])
    );

    let issues: BTreeSet<_> = parts
        .issues()
        .iter()
        .map(nv_telemetry_source::ProjectionIssue::path)
        .collect();

    assert_eq!(issues, BTreeSet::from(["update[2].val"]));
}

#[test]
fn collection_identity_collisions_suppress_all_valid_candidates_and_keep_siblings() {
    let subject = SubjectTemplate::new("sensor", IdentityValue::path_key(0, "name"));

    let spec = SubscriptionSpec::new(
        vec![path(&["entry"]), path(&["exact"])],
        vec![
            Mapping::collection_reading(
                path(&["entry", "counter"]),
                subject.clone(),
                ReadingTemplate::new(NumericType::Uint),
            ),
            Mapping::new(path(&["exact"]), descriptor("fixed"), NumericType::Uint),
            Mapping::collection_string_state(
                path(&["entry", "status"]),
                subject,
                StateTemplate::new("status"),
            ),
        ],
    )
    .unwrap();

    let make = |name: &str, extra: &str, leaf: &str, value| {
        let mut path = wire_path(&["entry", leaf]);
        path.elem[0]
            .key
            .extend([("name".into(), name.into()), ("extra".into(), extra.into())]);

        wire::Update {
            path: Some(path),
            ..update(value)
        }
    };

    let parts = projection::project(
        &spec.projection,
        wire::Notification {
            update: vec![
                make("fixed", "a", "counter", Value::UintVal(1)),
                wire::Update {
                    path: Some(wire_path(&["exact"])),
                    ..update(Value::UintVal(2))
                },
                make("bad", "a", "counter", Value::StringVal("bad".into())),
                make("bad", "b", "counter", Value::UintVal(3)),
                make("state", "a", "status", Value::StringVal("NEW".into())),
                make("state", "c", "status", Value::StringVal("UP".into())),
                make("good", "a", "status", Value::StringVal("UP".into())),
            ],
            ..Default::default()
        },
    )
    .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
    .unwrap();

    assert_eq!(parts.payloads().len(), 2);

    for (_, payload) in parts.payloads() {
        match payload {
            Payload::Readings(readings) => {
                assert_eq!(readings.samples().len(), 1);
                assert_eq!(readings.samples()[0].key().subject().id(), "bad");
                assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(3));
            }
            Payload::States(states) => {
                assert_eq!(states.observations().len(), 1);
                assert_eq!(states.observations()[0].subject().id(), "good");
            }
            _ => panic!("readings or states"),
        }
    }

    let issues: BTreeSet<_> = parts
        .issues()
        .iter()
        .map(nv_telemetry_source::ProjectionIssue::path)
        .collect();

    assert_eq!(
        issues,
        BTreeSet::from([
            "update[0].path",
            "update[1].path",
            "update[2].val",
            "update[4].path",
            "update[5].path"
        ])
    );
}

#[test]
fn collection_colliding_observations_are_omitted_before_payloads_are_built() {
    let limit = nv_telemetry_model::limits::READINGS_SAMPLES_MAX_ITEMS as usize;

    for numeric in [true, false] {
        let subject = SubjectTemplate::new("port", IdentityValue::path_key(0, "name"));

        let mapping = if numeric {
            Mapping::collection_reading(
                path(&["entry", "leaf"]),
                subject,
                ReadingTemplate::new(NumericType::Uint),
            )
        } else {
            Mapping::collection_boolean_state(
                path(&["entry", "leaf"]),
                subject,
                StateTemplate::new("enabled"),
            )
        };

        let spec = SubscriptionSpec::new(vec![path(&["entry"])], vec![mapping]).unwrap();

        let mut updates: Vec<_> = (0..limit + 2)
            .map(|index| {
                let mut path = wire_path(&["entry", "leaf"]);
                path.elem[0].key.insert("name".into(), index.to_string());

                wire::Update {
                    path: Some(path),
                    ..update(if numeric {
                        Value::UintVal(u64::MAX)
                    } else {
                        Value::BoolVal(false)
                    })
                }
            })
            .collect();

        let mut collision = updates[0].clone();
        collision.path.as_mut().unwrap().elem[0]
            .key
            .insert("extra".into(), "different-path".into());

        updates.push(collision);

        let parts = projection::project(
            &spec.projection,
            wire::Notification {
                update: updates,
                ..Default::default()
            },
        )
        .map(|(payloads, issues)| AcquisitionParts::new(payloads, issues))
        .unwrap();

        assert_eq!(parts.payloads().len(), 2);
        assert_eq!(parts.issues().len(), 2);

        let mut total = 0;

        for (coverage, payload) in parts.payloads() {
            assert_eq!(coverage.completeness(), Completeness::Partial);

            match payload {
                Payload::Readings(readings) => {
                    assert!(readings.samples().len() <= limit);
                    assert_eq!(readings.samples().len(), readings.descriptors().len());

                    let keys: BTreeSet<_> = readings
                        .descriptors()
                        .iter()
                        .map(SignalDescriptor::key)
                        .collect();

                    for sample in readings.samples() {
                        assert_ne!(sample.key().subject().id(), "0");
                        assert!(keys.contains(sample.key()));
                        assert_eq!(sample.value(), &NumericValue::Uint(u64::MAX));
                    }

                    total += readings.samples().len();
                }
                Payload::States(states) => {
                    assert!(states.observations().len() <= limit);

                    for state in states.observations() {
                        assert_ne!(state.subject().id(), "0");
                        assert_eq!(state.value(), &ModelValue::bool(false));
                    }

                    total += states.observations().len();
                }
                _ => panic!("reading or state"),
            }
        }

        assert_eq!(total, limit + 1);
    }
}

#[tokio::test]
async fn collection_stream_reports_interfaces_components_and_issues_in_one_acquisition() {
    let mut interface = wire_path(&["interfaces", "interface", "counter"]);
    interface.elem[1].key.insert("name".into(), "eth0".into());

    let mut component = wire_path(&["components", "component", "status"]);
    component.elem[1].key.insert("name".into(), "fan0".into());

    let mut second_component = component.clone();
    second_component.elem[1]
        .key
        .insert("name".into(), "fan1".into());

    let mut invalid = interface.clone();
    invalid.elem[1].key.insert("name".into(), "eth1".into());

    let mock = Mock::new(Script {
        responses: vec![notification(vec![
            wire::Update {
                path: Some(interface),
                ..update(Value::UintVal(42))
            },
            wire::Update {
                path: Some(component),
                ..update(Value::JsonIetfVal(br#""module:NEW""#.to_vec()))
            },
            wire::Update {
                path: Some(second_component),
                ..update(Value::StringVal("module:ACTIVE".into()))
            },
            wire::Update {
                path: Some(invalid),
                ..update(Value::BoolVal(true))
            },
        ])],
        ..Default::default()
    });

    let spec = SubscriptionSpec::new(
        vec![
            path(&["interfaces", "interface"]),
            path(&["components", "component"]),
        ],
        vec![
            Mapping::collection_reading(
                path(&["interfaces", "interface", "counter"]),
                SubjectTemplate::new("interface", IdentityValue::path_key(1, "name")),
                ReadingTemplate::new(NumericType::Uint),
            ),
            Mapping::collection_string_state(
                path(&["components", "component", "status"]),
                SubjectTemplate::new("component", IdentityValue::path_key(1, "name")),
                StateTemplate::new("status"),
            ),
        ],
    )
    .unwrap()
    .with_encoding(Encoding::JsonIetf);

    let unit = Arc::new(ConfiguredSubscription::new(endpoint(), mock, spec).unwrap());

    let plan = nv_telemetry_orchestration::plan(
        Needs::default().with_streams([StreamNeed::new(
            endpoint(),
            ConfiguredSubscription::<Mock>::REQUEST_CLASS,
        )]),
        &[ConfiguredSubscription::<Mock>::declaration()],
    )
    .unwrap();

    let stream = StreamUnit::new(plan.streams()[0].clone(), unit, ReconnectPolicy::default());
    let (_collector, mut reports) = collect(stream);

    let report = next_report(&mut reports).await;

    assert_eq!(report.status().outcome(), Outcome::Succeeded);
    assert_eq!(report.batches().len(), 2);
    assert_eq!(report.issues().unwrap().issues().len(), 1);

    for batch in report.batches() {
        assert_eq!(batch.window().start().seconds(), 99);
        assert_eq!(batch.endpoint(), &endpoint());

        match batch.payload() {
            Payload::Readings(readings) => {
                assert_eq!(readings.samples().len(), 1);
                assert_eq!(readings.samples()[0].key().subject().id(), "eth0");
                assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(42));
            }
            Payload::States(states) => {
                let values: std::collections::BTreeMap<_, _> = states
                    .observations()
                    .iter()
                    .map(|state| (state.subject().id(), state.value().clone()))
                    .collect();

                assert_eq!(
                    values,
                    std::collections::BTreeMap::from([
                        ("fan0", ModelValue::string("module:NEW").unwrap()),
                        ("fan1", ModelValue::string("module:ACTIVE").unwrap()),
                    ])
                );
            }
            _ => panic!("reading or state"),
        }
    }
}

#[test]
fn json_objects_match_leaf_updates_and_keep_received_keys() {
    let subject = SubjectTemplate::new("sensor", IdentityValue::path_key(1, "name"));

    let spec = SubscriptionSpec::new(
        vec![path(&["telemetry"])],
        vec![
            Mapping::collection_reading(
                path(&["telemetry", "entry", "state", "count"]),
                subject.clone(),
                ReadingTemplate::new(NumericType::Uint).with_facet("count"),
            ),
            Mapping::collection_boolean_state(
                path(&["telemetry", "entry", "state", "enabled"]),
                subject.clone(),
                StateTemplate::new("enabled"),
            ),
            Mapping::collection_string_state(
                path(&["telemetry", "entry", "state", "vendor:status"]),
                subject,
                StateTemplate::new("status"),
            ),
        ],
    )
    .unwrap();

    let mut prefix = wire_path(&["telemetry", "entry"]);
    prefix.elem[1].key.insert("name".into(), "sensor-a".into());

    let leaves = [
        ("count", Value::UintVal(u64::MAX)),
        ("enabled", Value::BoolVal(false)),
        ("vendor:status", Value::StringVal("vendor:ACTIVE".into())),
    ]
    .into_iter()
    .map(|(name, value)| wire::Update {
        path: Some(wire_path(&["state", name])),
        ..update(value)
    })
    .collect();

    let expected = projection::project(
        &spec.projection,
        wire::Notification {
            prefix: Some(prefix.clone()),
            timestamp: 7_000_000_123,
            update: leaves,
            ..Default::default()
        },
    )
    .unwrap();

    for value in [
        Value::JsonVal(
            br#"{"state":{"count":18446744073709551615,"enabled":false,"vendor:status":"vendor:ACTIVE"}}"#
                .to_vec(),
        ),
        Value::JsonIetfVal(
            br#"{"state":{"count":"18446744073709551615","enabled":false,"vendor:status":"vendor:ACTIVE"}}"#
                .to_vec(),
        ),
    ] {
        let actual = projection::project(
            &spec.projection,
            wire::Notification {
                prefix: Some(prefix.clone()),
                timestamp: 7_000_000_123,
                update: vec![wire::Update {
                    path: Some(wire_path(&[])),
                    ..update(value)
                }],
                ..Default::default()
            },
        )
        .unwrap();

        assert!(actual.1.is_empty(), "{:?}", actual.1);
        assert_eq!(actual.0, expected.0);
    }
}

#[test]
fn json_ietf_qualified_name_mismatches_keep_literal_siblings() {
    let spec = SubscriptionSpec::new(
        vec![
            path(&["leaf"]),
            path(&["other"]),
            path(&["vendor:parent"]),
            path(&["container"]),
        ],
        vec![
            Mapping::new(path(&["leaf"]), descriptor("a"), NumericType::Uint),
            Mapping::new(path(&["other"]), descriptor("b"), NumericType::Uint),
            Mapping::new(
                path(&["vendor:parent", "leaf"]),
                descriptor("c"),
                NumericType::Uint,
            ),
            Mapping::new(
                path(&["container", "leaf"]),
                descriptor("d"),
                NumericType::Uint,
            ),
        ],
    )
    .unwrap();

    for (json, pointer) in [
        (r#"{"m:leaf":7,"other":9}"#, Some("update[0].val/m:leaf")),
        (
            r#"{"vendor:parent":{"m:leaf":7},"other":9}"#,
            Some("update[0].val/vendor:parent/m:leaf"),
        ),
        (r#"{"m:leaf":[7],"other":9}"#, Some("update[0].val/m:leaf")),
        (r#"{"m:leaf":{},"other":9}"#, Some("update[0].val/m:leaf")),
        (
            r#"{"m:container":{"leaf":7},"other":9}"#,
            Some("update[0].val/m:container"),
        ),
        (
            r#"{"m:container":{},"other":9}"#,
            Some("update[0].val/m:container"),
        ),
        (r#"{"unmapped:field":7,"other":9}"#, None),
        (r#"{"different:parent":{"leaf":7},"other":9}"#, None),
    ] {
        for ietf in [false, true] {
            let value = if ietf {
                Value::JsonIetfVal(json.as_bytes().to_vec())
            } else {
                Value::JsonVal(json.as_bytes().to_vec())
            };

            let parts = projection::project(
                &spec.projection,
                snapshot(
                    0,
                    vec![wire::Update {
                        path: Some(wire_path(&[])),
                        ..update(value)
                    }],
                ),
            )
            .unwrap();

            let expected = pointer.filter(|_| ietf);

            assert_eq!(parts.1.len(), usize::from(expected.is_some()), "{json}");

            assert_eq!(
                parts
                    .1
                    .first()
                    .map(nv_telemetry_source::ProjectionIssue::path),
                expected
            );

            let [(_, Payload::Readings(readings))] = parts.0.as_slice() else {
                panic!("expected the literal sibling reading")
            };

            assert_eq!(readings.samples().len(), 1);
            assert_eq!(readings.samples()[0].key().subject().id(), "b");
            assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(9));
        }
    }
}

#[test]
fn json_objects_preserve_numeric_tokens_and_scalar_validation() {
    for (kind, raw, expected) in [
        (
            NumericType::Int,
            "-9223372036854775808",
            Some(NumericValue::Int(i64::MIN)),
        ),
        (
            NumericType::Int,
            "\"9223372036854775807\"",
            Some(NumericValue::Int(i64::MAX)),
        ),
        (
            NumericType::Uint,
            "18446744073709551615",
            Some(NumericValue::Uint(u64::MAX)),
        ),
        (
            NumericType::Double,
            "1.25",
            Some(NumericValue::double(1.25).unwrap()),
        ),
        (NumericType::Int, "1.0", None),
        (NumericType::Uint, "-1", None),
        (NumericType::Uint, "18446744073709551616", None),
        (NumericType::Double, "1e999", None),
        (NumericType::Uint, "null", None),
    ] {
        let spec = spec(kind);
        let object = format!("{{\"leaf\":{raw}}}");

        let parts = projection::project(
            &spec.projection,
            snapshot(
                0,
                vec![wire::Update {
                    path: Some(wire_path(&[])),
                    ..update(Value::JsonVal(object.into_bytes()))
                }],
            ),
        )
        .unwrap();

        let actual = parts.0.iter().find_map(|(_, payload)| match payload {
            Payload::Readings(readings) => Some(*readings.samples()[0].value()),
            _ => None,
        });

        assert_eq!(actual, expected, "{kind:?}: {raw}");
        assert_eq!(parts.1.len(), usize::from(expected.is_none()));
    }
}

#[test]
fn json_container_and_leaf_overlaps_keep_final_contributions() {
    let spec = spec(NumericType::Uint);

    let container = |bytes: &[u8]| wire::Update {
        path: Some(wire_path(&[])),
        ..update(Value::JsonVal(bytes.to_vec()))
    };

    for (updates, expected, issue) in [
        (
            vec![container(br#"{"leaf":1}"#), update(Value::UintVal(2))],
            Some(2),
            None,
        ),
        (
            vec![update(Value::UintVal(2)), container(br#"{"leaf":1}"#)],
            Some(1),
            None,
        ),
        (
            vec![container(br#"{"leaf":1}"#), container(b"{}")],
            None,
            None,
        ),
        (
            vec![container(br#"{"leaf":1}"#), container(b"{")],
            None,
            Some("update[1].val"),
        ),
        (
            vec![container(br#"{"leaf":1}"#), container(br#"{"leaf":false}"#)],
            None,
            Some("update[1].val/leaf"),
        ),
        (
            vec![update(Value::UintVal(1)), container(br#"{"leaf":false}"#)],
            None,
            Some("update[1].val/leaf"),
        ),
        (
            vec![container(br#"{"leaf":false}"#), update(Value::UintVal(1))],
            Some(1),
            None,
        ),
        (
            vec![update(Value::UintVal(1)), container(b"{}")],
            Some(1),
            None,
        ),
    ] {
        let parts = projection::project(&spec.projection, snapshot(0, updates)).unwrap();

        let actual = parts.0.iter().find_map(|(_, payload)| match payload {
            Payload::Readings(readings) => Some(*readings.samples()[0].value()),
            _ => None,
        });

        assert_eq!(actual, expected.map(NumericValue::Uint));

        assert_eq!(
            parts
                .1
                .first()
                .map(nv_telemetry_source::ProjectionIssue::path),
            issue
        );

        assert_eq!(parts.1.len(), usize::from(issue.is_some()));
    }
}

#[test]
fn json_object_validation_checks_syntax_and_traversed_member_names() {
    let long_name = "m".repeat(1024);
    let long_path = path(&[&long_name]);

    let spec = SubscriptionSpec::new(
        vec![path(&["leaf"]), path(&["other"]), long_path.clone()],
        vec![
            Mapping::new(path(&["leaf"]), descriptor("a"), NumericType::Uint),
            Mapping::new(path(&["other"]), descriptor("b"), NumericType::Uint),
            Mapping::new(long_path, descriptor("c"), NumericType::Uint),
        ],
    )
    .expect("valid specification");

    for (bytes, keep_leaf) in [
        (br#"{"leaf":7,"unused":{"bad":}}"#.as_slice(), false),
        (br#"{"leaf":7} trailing"#, false),
        (br#"{"leaf":7,"leaf":8}"#, false),
        (br#"{"leaf":7,"\u006ceaf":8}"#, false),
        (br#"{"leaf":7,"unused":{"a":1,"a":2}}"#, true),
        (br#"{"leaf":7,"unused":[{"a":1,"a":2}]}"#, true),
        (
            format!("{{\"leaf\":7,\"{}\":0}}", "p".repeat(1024)).as_bytes(),
            true,
        ),
        (
            format!("{{\"leaf\":7,\"{long_name}\":0}}").as_bytes(),
            false,
        ),
    ] {
        let parts = projection::project(
            &spec.projection,
            snapshot(
                0,
                vec![
                    wire::Update {
                        path: Some(wire_path(&[])),
                        ..update(Value::JsonVal(bytes.to_vec()))
                    },
                    // A distinct path keeps deduplication from hiding a leaked leaf.
                    wire::Update {
                        path: Some(wire_path(&["other"])),
                        ..update(Value::UintVal(9))
                    },
                ],
            ),
        )
        .unwrap();

        assert_eq!(parts.0.len(), 1);
        assert_eq!(parts.1.len(), usize::from(!keep_leaf));

        let Payload::Readings(readings) = &parts.0[0].1 else {
            panic!("readings")
        };

        assert_eq!(readings.samples().len(), 1 + usize::from(keep_leaf));

        assert_eq!(
            readings
                .samples()
                .iter()
                .find(|s| s.key().subject().id() == "a")
                .map(nv_telemetry_model::Reading::value),
            keep_leaf.then_some(&NumericValue::Uint(7))
        );

        assert_eq!(
            readings
                .samples()
                .iter()
                .find(|s| s.key().subject().id() == "b")
                .map(nv_telemetry_model::Reading::value),
            Some(&NumericValue::Uint(9))
        );

        for issue in parts.1 {
            assert!(issue.path().starts_with("update[0].val"));
            assert!(issue
                .at_index("notification", usize::MAX)
                .into_model()
                .is_ok());
        }
    }
}

#[test]
fn json_objects_accept_deep_objects_and_many_members() {
    let depth = 65;

    let mut names = vec!["a"; depth - 1];
    names.push("leaf");

    let nested = SubscriptionSpec::new(
        vec![path(&["a"])],
        vec![Mapping::new(
            path(&names),
            descriptor("a"),
            NumericType::Uint,
        )],
    )
    .unwrap();

    let text = format!(
        "{}{{\"leaf\":7}}{}",
        "{\"a\":".repeat(depth - 1),
        "}".repeat(depth - 1)
    );

    let flat = spec(NumericType::Uint);

    let mut fields = std::collections::BTreeMap::from([("leaf".to_owned(), 7)]);
    fields.extend((1..65_537).map(|n| (format!("unused{n}"), 0)));

    for (spec, bytes) in [
        (&nested, text.into_bytes()),
        (&flat, serde_json::to_vec(&fields).unwrap()),
    ] {
        let parts = projection::project(
            &spec.projection,
            snapshot(
                0,
                vec![wire::Update {
                    path: Some(wire_path(&[])),
                    ..update(Value::JsonVal(bytes))
                }],
            ),
        )
        .unwrap();

        assert_eq!(parts.0.len(), 1);
        assert!(parts.1.is_empty());

        let Payload::Readings(readings) = &parts.0[0].1 else {
            panic!("readings")
        };

        assert_eq!(readings.samples().len(), 1);
        assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(7));
    }
}

#[test]
fn deep_json_objects_project_on_a_caller_thread_stack() {
    std::thread::Builder::new()
        .name("gnmi-json-projection".into())
        .stack_size(1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("caller runtime");

            runtime.block_on(async {
                // Empty names keep issue paths short, isolating call-stack growth.
                let text = format!(
                    "{{\"leaf\":7,\"unused\":{}0{}}}",
                    "{\"\":".repeat(900),
                    "}".repeat(900)
                );

                for value in [
                    Value::JsonVal(text.as_bytes().to_vec()),
                    Value::JsonIetfVal(text.as_bytes().to_vec()),
                ] {
                    let notification = snapshot(
                        0,
                        vec![wire::Update {
                            path: Some(wire_path(&[])),
                            ..update(value)
                        }],
                    );

                    let mock = Mock::new(Script {
                        get_responses: vec![wire::GetResponse {
                            notification: vec![notification.clone()],
                            ..Default::default()
                        }],
                        responses: vec![wire::SubscribeResponse {
                            response: Some(wire::subscribe_response::Response::Update(
                                notification,
                            )),
                            ..Default::default()
                        }],
                        ..Default::default()
                    });

                    let get = mock.get(get_spec()).perform().await.unwrap();
                    let mut stream = mock.subscription().perform().await.unwrap();
                    let item = stream.next().await.unwrap().unwrap();

                    assert_eq!(get.payloads(), item.payloads());
                    assert!(get.issues().is_empty());
                    assert!(item.issues().is_empty());

                    let [(_, Payload::Readings(readings))] = get.payloads() else {
                        panic!("expected one readings payload")
                    };

                    assert_eq!(readings.samples().len(), 1);
                    assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(7));
                }
            });
        })
        .expect("caller thread")
        .join()
        .expect("caller projection");
}

#[test]
fn json_member_issues_use_unique_escaped_locations() {
    let spec = SubscriptionSpec::new(
        vec![path(&["root"])],
        [
            vec!["root", "a/b"],
            vec!["root", "a~1b"],
            vec!["root", "a.b"],
            vec!["root", "a", "b"],
        ]
        .into_iter()
        .enumerate()
        .map(|(index, names)| {
            Mapping::new(
                path(&names),
                descriptor(&index.to_string()),
                NumericType::Uint,
            )
        })
        .collect(),
    )
    .unwrap();

    let parts = projection::project(
        &spec.projection,
        snapshot(
            0,
            vec![wire::Update {
                path: Some(wire_path(&["root"])),
                ..update(Value::JsonIetfVal(
                    br#"{"a/b":false,"a~1b":false,"a.b":false,"a":{"b":false}}"#.to_vec(),
                ))
            }],
        ),
    )
    .unwrap();

    let paths: BTreeSet<_> = parts
        .1
        .iter()
        .map(nv_telemetry_source::ProjectionIssue::path)
        .collect();

    assert!(parts.0.is_empty());

    assert_eq!(
        paths,
        BTreeSet::from([
            "update[0].val/a~1b",
            "update[0].val/a~01b",
            "update[0].val/a.b",
            "update[0].val/a/b",
        ])
    );
}

#[test]
fn json_objects_report_arrays_and_missing_keys_without_inferring_subjects() {
    let spec = SubscriptionSpec::new(
        vec![path(&["root"])],
        vec![
            Mapping::new(
                path(&["root", "leaf"]),
                descriptor("fixed"),
                NumericType::Uint,
            ),
            Mapping::collection_reading(
                path(&["root", "entry", "count"]),
                SubjectTemplate::new("sensor", IdentityValue::path_key(1, "name")),
                ReadingTemplate::new(NumericType::Uint),
            ),
        ],
    )
    .unwrap();

    for text in [
        r#"{"leaf":7,"entry":[{"name":"invented","count":1}],"unmapped":[1,2]}"#,
        r#"{"leaf":7,"entry":{"name":"invented","count":1}}"#,
    ] {
        let parts = projection::project(
            &spec.projection,
            snapshot(
                0,
                vec![wire::Update {
                    path: Some(wire_path(&["root"])),
                    ..update(Value::JsonVal(text.as_bytes().to_vec()))
                }],
            ),
        )
        .unwrap();

        assert_eq!(parts.0.len(), 1);
        assert_eq!(parts.1.len(), 1);

        let Payload::Readings(readings) = &parts.0[0].1 else {
            panic!("readings")
        };

        assert_eq!(readings.samples().len(), 1);
        assert_eq!(readings.samples()[0].key().subject().id(), "fixed");
    }
}

#[test]
fn json_objects_keep_parent_type_errors_and_distinct_collision_locations() {
    let entry = PathElement::new("entry").with_key("name", "fixed");

    let fixed = |names: &[&str]| {
        let mut elements = vec![entry.clone()];
        elements.extend(names.iter().map(|name| PathElement::new(*name)));
        SubscriptionPath::new(elements)
    };

    let spec = SubscriptionSpec::new(
        vec![path(&["entry"])],
        vec![
            Mapping::new(fixed(&[]), descriptor("parent"), NumericType::Uint),
            Mapping::new(fixed(&["exact"]), descriptor("fixed"), NumericType::Uint),
            Mapping::new(
                fixed(&["sibling"]),
                descriptor("sibling"),
                NumericType::Uint,
            ),
            Mapping::collection_reading(
                path(&["entry", "count"]),
                SubjectTemplate::new("sensor", IdentityValue::path_key(0, "name")),
                ReadingTemplate::new(NumericType::Uint),
            ),
        ],
    )
    .unwrap();

    let mut prefix = wire_path(&["entry"]);
    prefix.elem[0].key.insert("name".into(), "fixed".into());

    let parts = projection::project(
        &spec.projection,
        wire::Notification {
            prefix: Some(prefix),
            update: vec![wire::Update {
                path: Some(wire_path(&[])),
                ..update(Value::JsonVal(
                    br#"{"count":3,"exact":7,"sibling":9}"#.to_vec(),
                ))
            }],
            ..Default::default()
        },
    )
    .unwrap();

    let Payload::Readings(readings) = &parts.0[0].1 else {
        panic!("readings")
    };

    assert_eq!(readings.samples().len(), 1);
    assert_eq!(readings.samples()[0].key().subject().id(), "sibling");

    assert_eq!(
        parts
            .1
            .iter()
            .map(nv_telemetry_source::ProjectionIssue::path)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "update[0].val",
            "update[0].val/count",
            "update[0].val/exact"
        ])
    );
}

#[tokio::test]
async fn json_objects_share_get_and_subscribe_projection_boundaries() {
    let notifications: Vec<_> = [7_000_000_123, 8_000_000_456]
        .into_iter()
        .enumerate()
        .map(|(index, timestamp)| {
            let json = if index == 0 {
                br#"{"leaf":42}"#.as_slice()
            } else {
                br#"{"leaf":42,"m:leaf":43}"#
            };

            snapshot(
                timestamp,
                vec![wire::Update {
                    path: Some(wire_path(&[])),
                    ..update(Value::JsonIetfVal(json.to_vec()))
                }],
            )
        })
        .collect();

    let mock = Mock::new(Script {
        get_responses: vec![wire::GetResponse {
            notification: notifications.clone(),
            ..Default::default()
        }],
        responses: notifications
            .into_iter()
            .map(|notification| wire::SubscribeResponse {
                response: Some(wire::subscribe_response::Response::Update(notification)),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    });

    let get = mock.get(get_spec()).perform().await.unwrap();
    let mut subscription = mock.subscription().perform().await.unwrap();

    for (index, expected) in get.payloads().chunks_exact(1).enumerate() {
        let item = subscription.next().await.unwrap().unwrap();

        assert_eq!(item.payloads(), expected);
        assert_eq!(item.issues().len(), index);

        if index == 1 {
            assert_eq!(item.issues()[0].path(), "update[0].val/m:leaf");
        }
    }

    assert_eq!(get.payloads().len(), 2);
    assert_eq!(get.issues().len(), 1);

    assert_eq!(
        get.issues()[0].path(),
        "notification[1].update[0].val/m:leaf"
    );
}

#[tokio::test]
async fn json_objects_ignore_nested_arrays_on_unmapped_branches() {
    let text =
        serde_json::to_string(&format!("{}\\\"{}", "[".repeat(200), "]".repeat(200))).unwrap();

    for json in [
        format!(
            r#"{{"leaf":7,"unused":{}0{}}}"#,
            "[".repeat(200),
            "]".repeat(200)
        ),
        format!(r#"{{"leaf":7,"unused":{text}}}"#),
    ] {
        for value in [
            Value::JsonVal(json.as_bytes().to_vec()),
            Value::JsonIetfVal(json.as_bytes().to_vec()),
        ] {
            let notification = snapshot(
                0,
                vec![wire::Update {
                    path: Some(wire_path(&[])),
                    ..update(value)
                }],
            );

            let mock = Mock::new(Script {
                get_responses: vec![wire::GetResponse {
                    notification: vec![notification.clone()],
                    ..Default::default()
                }],
                responses: vec![wire::SubscribeResponse {
                    response: Some(wire::subscribe_response::Response::Update(notification)),
                    ..Default::default()
                }],
                ..Default::default()
            });

            let get = mock.get(get_spec()).perform().await.unwrap();
            let mut stream = mock.subscription().perform().await.unwrap();
            let item = stream.next().await.unwrap().unwrap();

            assert_eq!(get.payloads(), item.payloads());
            assert_eq!(get.payloads().len(), 1);
            assert!(get.issues().is_empty());
            assert!(item.issues().is_empty());

            let Payload::Readings(readings) = &get.payloads()[0].1 else {
                panic!("readings")
            };

            assert_eq!(readings.samples().len(), 1);
            assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(7));
        }
    }
}

fn get_spec() -> GetSpec {
    GetSpec::new(
        vec![path(&["leaf"])],
        vec![Mapping::new(
            path(&["leaf"]),
            descriptor("a"),
            NumericType::Uint,
        )],
    )
    .expect("Get specification")
}

fn snapshot(timestamp: i64, updates: Vec<wire::Update>) -> wire::Notification {
    wire::Notification {
        timestamp,
        update: updates,
        ..Default::default()
    }
}

#[tokio::test]
async fn get_requests_preserve_defaults_paths_context_encoding_and_data_type() {
    let mock = Mock::new(Script {
        get_responses: vec![wire::GetResponse {
            notification: vec![snapshot(0, vec![])],
            ..Default::default()
        }],
        ..Default::default()
    });

    mock.get(get_spec()).perform().await.expect("default Get");

    let request = mock.get_requests.lock().unwrap()[0].clone();

    assert_eq!(request.r#type, wire::get_request::DataType::All as i32);
    assert_eq!(request.encoding, wire::Encoding::Json as i32);
    assert_eq!(request.prefix, Some(wire::Path::default()));

    for (data_type, expected) in [
        (GetDataType::All, wire::get_request::DataType::All),
        (GetDataType::Config, wire::get_request::DataType::Config),
        (GetDataType::State, wire::get_request::DataType::State),
        (
            GetDataType::Operational,
            wire::get_request::DataType::Operational,
        ),
    ] {
        for (encoding, expected_encoding) in [
            (Encoding::Json, wire::Encoding::Json),
            (Encoding::JsonIetf, wire::Encoding::JsonIetf),
        ] {
            let keyed =
                SubscriptionPath::new(vec![PathElement::new("root").with_key("name", "eth0")]);

            let spec = GetSpec::new(
                vec![keyed.clone(), path(&["leaf"]), keyed],
                vec![Mapping::new(
                    path(&["leaf"]),
                    descriptor("a"),
                    NumericType::Uint,
                )],
            )
            .unwrap()
            .with_origin("openconfig")
            .with_target("switch-a")
            .with_data_type(data_type)
            .with_encoding(encoding);

            mock.get(spec).perform().await.expect("explicit Get");

            let request = mock.get_requests.lock().unwrap().last().unwrap().clone();
            let prefix = request.prefix.unwrap();

            assert_eq!(prefix.origin, "openconfig");
            assert_eq!(prefix.target, "switch-a");
            assert!(prefix.elem.is_empty());
            assert_eq!(request.r#type, expected as i32);
            assert_eq!(request.encoding, expected_encoding as i32);
            assert_eq!(request.path.len(), 3);
            assert_eq!(request.path[0], request.path[2]);
            assert_eq!(request.path[0].elem[0].key["name"], "eth0");
            assert_eq!(request.path[1], wire_path(&["leaf"]));
            assert!(request.use_models.is_empty());
            assert!(request.extension.is_empty());
        }
    }

    assert_eq!(mock.get_requests.lock().unwrap().len(), 9);
}

#[tokio::test]
async fn get_projects_notifications_independently_and_indexes_all_issue_locations() {
    let subject = descriptor("a").key().subject().clone();

    let spec = GetSpec::new(
        vec![path(&["root"])],
        vec![
            Mapping::new(path(&["root", "leaf"]), descriptor("a"), NumericType::Uint),
            Mapping::string_state(
                path(&["root", "status"]),
                StateIdentity::new(subject.clone(), "status"),
            ),
            Mapping::boolean_state(
                path(&["root", "enabled"]),
                StateIdentity::new(subject, "enabled"),
            ),
        ],
    )
    .unwrap()
    .with_origin("openconfig")
    .with_target("switch-a");

    let notifications = [7_000_000_123, 8_000_000_456].map(|timestamp| {
        let updates = [
            ("leaf", Value::UintVal(1)),
            ("leaf", Value::JsonIetfVal(b"42".to_vec())),
            ("status", Value::JsonVal(br#""UP""#.to_vec())),
            ("enabled", Value::BoolVal(false)),
            ("unmapped", Value::UintVal(9)),
            ("status", Value::JsonVal(b"{}".to_vec())),
        ]
        .into_iter()
        .map(|(name, value)| wire::Update {
            path: Some(wire_path(&[name])),
            ..update(value)
        })
        .collect();

        let mut prefix = wire_path(&["root"]);

        prefix.origin = "openconfig".into();
        prefix.target = "switch-a".into();

        wire::Notification {
            prefix: Some(prefix),
            delete: vec![wire_path(&["leaf"])],
            ..snapshot(timestamp, updates)
        }
    });

    let mock = Mock::new(Script {
        get_responses: vec![
            wire::GetResponse {
                notification: notifications.into(),
                ..Default::default()
            },
            wire::GetResponse {
                notification: vec![snapshot(9_000_000_000, vec![])],
                ..Default::default()
            },
        ],
        ..Default::default()
    });

    let unit = mock.get(spec);

    let parts = unit.perform().await.unwrap();

    assert_eq!(parts.payloads().len(), 4);
    assert_eq!(parts.issues().len(), 4);

    let locations: BTreeSet<_> = parts
        .issues()
        .iter()
        .map(nv_telemetry_source::ProjectionIssue::path)
        .collect();

    assert_eq!(
        locations,
        BTreeSet::from([
            "notification[0].update[5].val",
            "notification[0].delete[0]",
            "notification[1].update[5].val",
            "notification[1].delete[0]",
        ])
    );

    for (index, pair) in parts.payloads().chunks_exact(2).enumerate() {
        let observed = Timestamp::new(
            7 + i64::try_from(index).unwrap(),
            if index == 0 { 123 } else { 456 },
        )
        .unwrap();

        for (coverage, payload) in pair {
            assert_eq!(coverage.completeness(), Completeness::Partial);

            match payload {
                Payload::Readings(readings) => {
                    assert_eq!(readings.samples().len(), 1);
                    assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(42));
                    assert_eq!(readings.samples()[0].observed_at(), Some(&observed));
                }
                Payload::States(states) => {
                    assert_eq!(states.observations().len(), 1);
                    assert_eq!(states.observations()[0].value(), &ModelValue::bool(false));
                    assert_eq!(states.observations()[0].observed_at(), Some(&observed));
                }
                _ => panic!("scalar payload"),
            }
        }
    }

    let empty = unit.perform().await.unwrap();

    assert!(empty.payloads().is_empty());
    assert!(empty.issues().is_empty());
    assert_eq!(mock.get_requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn get_collection_and_fixed_mappings_keep_valid_siblings_of_bad_keys_and_containers() {
    let subject = SubjectTemplate::new("interface", IdentityValue::path_key(0, "name"));

    let spec = GetSpec::new(
        vec![path(&["entry"]), path(&["leaf"])],
        vec![
            Mapping::new(path(&["leaf"]), descriptor("fixed"), NumericType::Uint),
            Mapping::collection_string_state(
                path(&["entry", "status"]),
                subject.clone(),
                StateTemplate::new("status"),
            ),
            Mapping::collection_reading(
                path(&["entry", "count"]),
                subject,
                ReadingTemplate::new(NumericType::Uint).with_unit("1"),
            ),
        ],
    )
    .unwrap();

    let make = |name: &str, leaf: &str, value| {
        let mut path = wire_path(&["entry", leaf]);
        path.elem[0].key.insert("name".into(), name.into());
        wire::Update {
            path: Some(path),
            ..update(value)
        }
    };

    let mock = Mock::new(Script {
        get_responses: vec![wire::GetResponse {
            notification: vec![snapshot(
                1,
                vec![
                    update(Value::UintVal(7)),
                    make("eth0", "status", Value::StringVal("UP".into())),
                    make("eth0", "count", Value::UintVal(5)),
                    make("", "count", Value::UintVal(8)),
                    wire::Update {
                        path: Some(wire_path(&["entry"])),
                        ..update(Value::JsonVal(b"[]".to_vec()))
                    },
                ],
            )],
            ..Default::default()
        }],
        ..Default::default()
    });

    let parts = mock.get(spec).perform().await.unwrap();

    assert_eq!(parts.issues().len(), 2);
    assert_eq!(parts.payloads().len(), 2);
    let mut readings_seen = 0;
    let mut states_seen = 0;

    for (_, payload) in parts.payloads() {
        match payload {
            Payload::Readings(readings) => {
                readings_seen += readings.samples().len();
                assert!(readings
                    .descriptors()
                    .iter()
                    .any(|d| d.key().subject().id() == "eth0"));
            }
            Payload::States(states) => {
                states_seen += states.observations().len();
                assert_eq!(states.observations()[0].subject().id(), "eth0");

                assert_eq!(
                    states.observations()[0].value(),
                    &ModelValue::string("UP").unwrap()
                );
            }
            _ => panic!("scalar payload"),
        }
    }

    assert_eq!(readings_seen, 2);
    assert_eq!(states_seen, 1);
}

#[tokio::test]
async fn get_rejects_rpc_and_legacy_errors_without_leaking_target_text_or_observations() {
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
        (Code::Cancelled, Class::Device, true),
        (Code::Unknown, Class::Device, true),
        (Code::Aborted, Class::Device, true),
        (Code::ResourceExhausted, Class::Device, true),
        (Code::Internal, Class::Protocol, false),
        (Code::DataLoss, Class::Protocol, false),
        (Code::OutOfRange, Class::Protocol, false),
    ] {
        for opening_error in [true, false] {
            let mock = Mock::new(Script {
                get_responses: vec![wire::GetResponse {
                    notification: vec![snapshot(1, vec![update(Value::UintVal(5))])],
                    ..Default::default()
                }],
                status: Some(code),
                opening_error,
                ..Default::default()
            });

            let failure = mock
                .get(get_spec())
                .perform()
                .await
                .expect_err("RPC failure");

            assert_eq!(failure.class(), class, "{code:?}, opening={opening_error}");
            assert_eq!(failure.retryable(), Some(retryable));
            assert!(failure.detail().unwrap().contains("Get"));
            assert!(!failure.detail().unwrap().contains("target-secret"));
        }
    }

    for (code, class, retryable) in [
        (Code::PermissionDenied as u32, Class::Authentication, false),
        (Code::Unavailable as u32, Class::Connectivity, true),
        (0, Class::Protocol, false),
        (17, Class::Protocol, false),
        (u32::MAX, Class::Protocol, false),
    ] {
        #[allow(deprecated)]
        let response = wire::GetResponse {
            notification: vec![snapshot(1, vec![update(Value::UintVal(5))])],
            error: Some(wire::Error {
                code,
                message: "target-secret".into(),
                data: None,
            }),
            ..Default::default()
        };

        let mock = Mock::new(Script {
            get_responses: vec![response],
            ..Default::default()
        });

        let failure = mock
            .get(get_spec())
            .perform()
            .await
            .expect_err("legacy error");

        assert_eq!(failure.class(), class);
        assert_eq!(failure.retryable(), Some(retryable));
        assert!(!failure.detail().unwrap().contains("target-secret"));
    }
}

#[tokio::test]
async fn get_distinguishes_empty_responses_from_empty_or_unmapped_notifications_and_bounds_decode()
{
    for (notifications, valid) in [
        (vec![], false),
        (vec![snapshot(0, vec![])], true),
        (
            vec![snapshot(
                0,
                vec![wire::Update {
                    path: Some(wire_path(&["other"])),
                    ..update(Value::UintVal(1))
                }],
            )],
            true,
        ),
        (
            vec![snapshot(
                0,
                vec![update(Value::JsonVal(vec![b'0'; 4 * 1024 * 1024 + 1]))],
            )],
            false,
        ),
    ] {
        let mock = Mock::new(Script {
            get_responses: vec![wire::GetResponse {
                notification: notifications,
                ..Default::default()
            }],
            ..Default::default()
        });

        match mock.get(get_spec()).perform().await {
            Ok(parts) => {
                assert!(valid);
                assert!(parts.payloads().is_empty());
                assert!(parts.issues().is_empty());
            }
            Err(failure) => {
                assert!(!valid);
                assert_eq!(failure.class(), Class::Protocol);
                assert_eq!(failure.retryable(), Some(false));
            }
        }
    }
}

#[tokio::test]
async fn dropping_pending_get_connect_or_response_body_cancels_the_rpc() {
    for hold_connect in [true, false] {
        let mock = Mock::new(Script {
            hold_connect,
            hold_open: !hold_connect,
            get_responses: vec![wire::GetResponse {
                notification: vec![snapshot(1, vec![update(Value::UintVal(3))])],
                ..Default::default()
            }],
            ..Default::default()
        });

        let unit = mock.get(get_spec());

        let mut future = Box::pin(unit.perform());

        assert!(futures_util::poll!(&mut future).is_pending());
        assert_eq!(mock.get_requests.lock().unwrap().len(), 1);
        assert_eq!(mock.dropped.load(Ordering::SeqCst), 0);
        drop(future);

        assert_eq!(mock.dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn get_bounds_issues_once_across_notifications() {
    let limit = nv_telemetry_model::limits::PROJECTIONISSUES_ISSUES_MAX_ITEMS as usize;

    // Each invalid path produces a separate issue.
    let invalid = wire::Update {
        path: Some(wire_path(&[""])),
        ..update(Value::UintVal(1))
    };

    let mock = Mock::new(Script {
        get_responses: vec![wire::GetResponse {
            notification: vec![
                snapshot(1, vec![invalid; limit + 2]),
                snapshot(2, vec![update(Value::BoolVal(false))]),
            ],
            ..Default::default()
        }],
        ..Default::default()
    });

    let parts = mock.get(get_spec()).perform().await.unwrap();

    assert!(parts.payloads().is_empty());
    assert_eq!(parts.issues().len(), limit);

    let omitted = parts.issues().last().unwrap();

    assert_eq!(omitted.path(), nv_telemetry_source::OMITTED_ISSUES_LOCATOR);

    assert_eq!(
        omitted.kind(),
        &nv_telemetry_source::ProjectionIssueKind::Invalid {
            detail: "4 further issues omitted at the envelope's bound".into(),
        }
    );
}

/// Tokio's paused clock, plus the time the mock says its Gets took.
#[derive(Clone)]
struct GetClock {
    epoch: tokio::time::Instant,
    taken: Arc<Mutex<Duration>>,
}

impl GetClock {
    fn new() -> Self {
        Self {
            epoch: tokio::time::Instant::now(),
            taken: Arc::default(),
        }
    }

    fn advance(&self, by: Duration) {
        *self.taken.lock().unwrap() += by;
    }

    fn elapsed(&self) -> Duration {
        self.epoch.elapsed() + *self.taken.lock().unwrap()
    }
}

impl Clock for GetClock {
    fn timestamp(&self) -> Timestamp {
        let elapsed = self.elapsed();
        Timestamp::new(
            99 + i64::try_from(elapsed.as_secs()).unwrap(),
            elapsed.subsec_nanos(),
        )
        .unwrap()
    }

    fn instant(&self) -> Instant {
        self.epoch.into_std() + self.elapsed()
    }
}

#[tokio::test(start_paused = true)]
async fn get_poll_reports_once_per_call_stamps_time_and_polls_again() {
    let clock = GetClock::new();
    let elapsed = Duration::from_millis(25);

    let mock = Mock::new(Script {
        get_responses: vec![
            wire::GetResponse {
                notification: vec![
                    snapshot(7_000_000_123, vec![update(Value::UintVal(3))]),
                    snapshot(8_000_000_456, vec![update(Value::UintVal(4))]),
                ],
                ..Default::default()
            },
            wire::GetResponse::default(),
            wire::GetResponse {
                notification: vec![snapshot(9, vec![])],
                ..Default::default()
            },
        ],
        advance_clock: Some((clock.clone(), elapsed)),
        ..Default::default()
    });

    let plan = nv_telemetry_orchestration::plan(
        Needs::default().with_polls([PollNeed::new(
            endpoint(),
            ConfiguredGet::<Mock>::REQUEST_CLASS,
            "snapshot",
            Duration::from_secs(1),
        )]),
        &[ConfiguredGet::<Mock>::declaration()],
    )
    .unwrap();

    assert_eq!(plan.polls()[0].cost(), 1);

    let (collector, mut reports) = Collector::new(clock.clone());
    collector
        .add(
            EndpointUnits::new(EndpointPolicy::default()).with_polls([PollUnit::new(
                plan.polls()[0].clone(),
                Arc::new(mock.get(get_spec())),
            )]),
        )
        .expect("a poll forms an endpoint");

    for (tick, (outcome, count, failure)) in [
        (Outcome::Succeeded, 2, None),
        (Outcome::Failed, 0, Some(FailureClass::Protocol)),
        (Outcome::Succeeded, 0, None),
    ]
    .into_iter()
    .enumerate()
    {
        let report = next_report(&mut reports).await;
        let started_at = report.status().started_at();

        assert_eq!(report.status().failure_class(), failure);
        assert_eq!(report.status().provider(), "gnmi.configured");
        assert_eq!(report.status().request_class(), "configured-get");
        // One Get a second, each taking its 25 ms.
        assert_eq!(started_at.seconds(), 99 + i64::try_from(tick).unwrap());

        assert_eq!(
            report.status().duration_nanos(),
            Some(u64::try_from(elapsed.as_nanos()).unwrap())
        );

        assert_eq!(report.status().outcome(), outcome);
        assert_eq!(report.batches().len(), count);

        for (index, batch) in report.batches().iter().enumerate() {
            assert_eq!(batch.endpoint(), &endpoint());
            assert_eq!(batch.window().start(), started_at);

            let Payload::Readings(readings) = batch.payload() else {
                panic!("readings")
            };

            let sample = &readings.samples()[0];

            assert_eq!(sample.value(), &NumericValue::Uint(3 + index as u64));

            let device_times = [
                Timestamp::new(7, 123).unwrap(),
                Timestamp::new(8, 456).unwrap(),
            ];

            assert_eq!(sample.observed_at(), device_times.get(index));
        }

        assert_eq!(mock.get_requests.lock().unwrap().len(), tick + 1);
    }
}

fn once_spec() -> OnceSpec {
    OnceSpec::new(
        vec![path(&["leaf"])],
        vec![Mapping::new(
            path(&["leaf"]),
            descriptor("a"),
            NumericType::Uint,
        )],
    )
    .expect("ONCE specification")
}

// A complete ONCE response ends with synchronization and the mock's status trailers.
fn once_script(notifications: Vec<wire::Notification>) -> Script {
    let mut responses: Vec<_> = notifications
        .into_iter()
        .map(|notification| wire::SubscribeResponse {
            response: Some(wire::subscribe_response::Response::Update(notification)),
            ..Default::default()
        })
        .collect();

    responses.push(wire::SubscribeResponse {
        response: Some(wire::subscribe_response::Response::SyncResponse(true)),
        ..Default::default()
    });

    Script {
        responses,
        ..Default::default()
    }
}

#[tokio::test]
async fn once_validates_configuration_and_encodes_only_snapshot_options() {
    for (requests, mappings) in [
        (vec![], vec![]),
        (
            vec![path(&["*"])],
            vec![Mapping::new(
                path(&["leaf"]),
                descriptor("a"),
                NumericType::Uint,
            )],
        ),
        (
            vec![path(&["other"])],
            vec![Mapping::new(
                path(&["leaf"]),
                descriptor("a"),
                NumericType::Uint,
            )],
        ),
        (
            vec![path(&["leaf"])],
            vec![Mapping::new(path(&["leaf"]), descriptor("a"), NumericType::Uint); 2],
        ),
    ] {
        assert!(OnceSpec::new(requests, mappings).is_err());
    }

    let mock = Mock::new(once_script(vec![]));

    mock.once(once_spec()).perform().await.unwrap();

    let wire::subscribe_request::Request::Subscribe(defaults) =
        mock.requests.lock().unwrap()[0].request.clone().unwrap()
    else {
        panic!("subscription list")
    };

    assert_eq!(defaults.encoding, wire::Encoding::Json as i32);

    assert_eq!(defaults.prefix, Some(wire::Path::default()));

    for (encoding, expected) in [
        (Encoding::Json, wire::Encoding::Json),
        (Encoding::JsonIetf, wire::Encoding::JsonIetf),
    ] {
        let keyed =
            SubscriptionPath::new(vec![PathElement::new("root").with_key("name", "sensor-a")]);

        let spec = OnceSpec::new(
            vec![keyed.clone(), path(&["leaf"]), keyed],
            vec![Mapping::new(
                path(&["leaf"]),
                descriptor("a"),
                NumericType::Uint,
            )],
        )
        .unwrap()
        .with_origin("openconfig")
        .with_target("target-a")
        .with_encoding(encoding);

        mock.once(spec).perform().await.unwrap();

        let wire::subscribe_request::Request::Subscribe(request) = mock
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .request
            .clone()
            .unwrap()
        else {
            panic!("subscription list")
        };

        let prefix = request.prefix.unwrap();

        assert_eq!(prefix.origin, "openconfig");

        assert_eq!(prefix.target, "target-a");

        assert_eq!(request.mode, wire::subscription_list::Mode::Once as i32);

        assert_eq!(request.encoding, expected as i32);

        assert!(!request.updates_only);

        assert!(!request.allow_aggregation);

        assert!(request.use_models.is_empty());

        assert_eq!(request.subscription.len(), 3);

        assert_eq!(request.subscription[0], request.subscription[2]);

        assert_eq!(
            request.subscription[0].path.as_ref().unwrap().elem[0]
                .key
                .get("name")
                .unwrap(),
            "sensor-a"
        );

        assert_eq!(request.subscription[1].path, Some(wire_path(&["leaf"])));

        for leaf in request.subscription {
            assert_eq!(leaf.mode, wire::SubscriptionMode::TargetDefined as i32);

            assert_eq!(leaf.sample_interval, 0);

            assert_eq!(leaf.heartbeat_interval, 0);

            assert!(!leaf.suppress_redundant);
        }
    }
}

#[tokio::test]
async fn once_preserves_get_projection_and_out_of_order_notification_boundaries() {
    let state_subject = descriptor("a").key().subject().clone();

    let mappings = vec![
        Mapping::new(path(&["leaf"]), descriptor("a"), NumericType::Uint),
        Mapping::new(path(&["bad"]), descriptor("b"), NumericType::Uint),
        Mapping::boolean_state(
            path(&["flag"]),
            StateIdentity::new(state_subject.clone(), "flag"),
        ),
        Mapping::string_state(path(&["label"]), StateIdentity::new(state_subject, "label")),
    ];

    let paths = vec![
        path(&["leaf"]),
        path(&["bad"]),
        path(&["flag"]),
        path(&["label"]),
    ];

    let notifications: Vec<_> = [8_000_000_456, 7_000_000_123]
        .into_iter()
        .map(|timestamp| {
            snapshot(timestamp, vec![wire::Update {
                path: Some(wire_path(&[])),
                ..update(Value::JsonIetfVal(br#"{"leaf":"18446744073709551615","bad":null,"flag":false,"label":"vendor:ACTIVE"}"#.to_vec()))
            }])
        })
        .collect();

    let mut script = once_script(notifications.clone());

    script.get_responses = vec![wire::GetResponse {
        notification: notifications,
        ..Default::default()
    }];

    let mock = Mock::new(script);

    let actual = mock
        .once(OnceSpec::new(paths.clone(), mappings.clone()).unwrap())
        .perform()
        .await
        .unwrap();

    let expected = mock
        .get(GetSpec::new(paths, mappings).unwrap())
        .perform()
        .await
        .unwrap();

    assert_eq!(actual, expected);

    assert_eq!(actual.payloads().len(), 4);

    assert_eq!(actual.issues().len(), 2);

    assert_eq!(
        actual.issues()[0].path(),
        "notification[0].update[0].val/bad"
    );

    assert_eq!(
        actual.issues()[1].path(),
        "notification[1].update[0].val/bad"
    );

    for (payloads, seconds) in actual.payloads().chunks_exact(2).zip([8, 7]) {
        let Payload::Readings(readings) = &payloads[0].1 else {
            panic!("readings")
        };

        assert_eq!(
            readings.samples()[0].observed_at().unwrap().seconds(),
            seconds
        );

        assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(u64::MAX));

        assert_eq!(payloads[0].0.completeness(), Completeness::Partial);
    }
}

#[tokio::test]
async fn once_requires_true_synchronization_and_rejects_late_updates() {
    let sync = |done| wire::SubscribeResponse {
        response: Some(wire::subscribe_response::Response::SyncResponse(done)),
        ..Default::default()
    };

    let valid = notification(vec![update(Value::UintVal(3))]);

    let invalid = notification(vec![update(Value::BoolVal(false))]);

    for (responses, succeeds, payloads, issues) in [
        (vec![], false, 0, 0),
        (vec![sync(false)], false, 0, 0),
        (vec![valid.clone()], false, 0, 0),
        (vec![sync(true)], true, 0, 0),
        (
            vec![
                sync(false),
                valid.clone(),
                sync(true),
                sync(true),
                sync(false),
            ],
            true,
            1,
            0,
        ),
        (vec![valid.clone(), sync(true), valid.clone()], false, 0, 0),
        (
            vec![valid, sync(true), wire::SubscribeResponse::default()],
            false,
            0,
            0,
        ),
        (vec![invalid, sync(true)], true, 0, 1),
    ] {
        let mock = Mock::new(Script {
            responses,
            ..Default::default()
        });

        match mock.once(once_spec()).perform().await {
            Ok(parts) => {
                assert!(succeeds);

                assert_eq!(parts.payloads().len(), payloads);

                assert_eq!(parts.issues().len(), issues);
            }
            Err(error) => {
                assert!(!succeeds);

                assert_eq!(error.class(), Class::Protocol);

                assert_eq!(error.retryable(), Some(false));
            }
        }
    }
}

#[tokio::test]
async fn once_checks_opening_errors_and_final_status_after_synchronization() {
    for (code, class, retryable) in [
        (Code::Unavailable, Class::Connectivity, true),
        (Code::DeadlineExceeded, Class::Timeout, true),
        (Code::PermissionDenied, Class::Authentication, false),
        (Code::Unimplemented, Class::Unsupported, false),
    ] {
        for opening_error in [true, false] {
            let mut script = once_script(vec![snapshot(1, vec![update(Value::UintVal(3))])]);
            script.status = Some(code);
            script.opening_error = opening_error;

            let mock = Mock::new(script);

            let error = mock.once(once_spec()).perform().await.unwrap_err();

            assert_eq!(error.class(), class);

            assert_eq!(error.retryable(), Some(retryable));

            assert!(!format!("{error:?}").contains("target-secret"));
        }
    }

    #[allow(deprecated)]
    let response = wire::SubscribeResponse {
        response: Some(wire::subscribe_response::Response::Error(wire::Error {
            code: Code::Unavailable as u32,
            ..Default::default()
        })),
        ..Default::default()
    };

    let mut script = once_script(vec![snapshot(1, vec![update(Value::UintVal(3))])]);
    script.responses.push(response);

    let mock = Mock::new(script);

    assert_eq!(
        mock.once(once_spec()).perform().await.unwrap_err().class(),
        Class::Connectivity
    );
}

#[tokio::test(start_paused = true)]
async fn once_pending_phases_cancel_and_caller_timeout_releases_the_rpc() {
    for (hold_connect, synced) in [(true, false), (false, false), (false, true)] {
        let mut script = once_script(vec![snapshot(1, vec![update(Value::UintVal(3))])]);
        script.hold_connect = hold_connect;
        script.hold_open = !hold_connect;

        if !synced {
            script.responses.pop();
        }

        let mock = Mock::new(script);

        let unit = mock.once(once_spec());

        let mut future = Box::pin(unit.perform());

        assert!(futures_util::poll!(&mut future).is_pending());

        assert_eq!(mock.requests.lock().unwrap().len(), 1);

        assert_eq!(mock.dropped.load(Ordering::SeqCst), 0);

        drop(future);

        assert_eq!(mock.dropped.load(Ordering::SeqCst), 1);

        assert!(tokio::time::timeout(Duration::from_secs(1), unit.perform())
            .await
            .is_err());

        assert_eq!(mock.dropped.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn once_bounds_issues_once_across_notifications() {
    let limit = nv_telemetry_model::limits::PROJECTIONISSUES_ISSUES_MAX_ITEMS as usize;

    let invalid = wire::Update {
        path: Some(wire_path(&[""])),
        ..update(Value::UintVal(1))
    };

    let mock = Mock::new(once_script(vec![
        snapshot(0, vec![invalid.clone(); limit]),
        snapshot(0, vec![invalid; 4]),
    ]));

    let parts = mock.once(once_spec()).perform().await.unwrap();

    assert_eq!(parts.issues().len(), limit);

    assert!(parts.payloads().is_empty());

    assert!(parts.issues()[0]
        .path()
        .starts_with("notification[0].update["));

    let omitted = parts.issues().last().unwrap();

    assert_eq!(omitted.path(), nv_telemetry_source::OMITTED_ISSUES_LOCATOR);

    assert_eq!(
        omitted.kind(),
        &nv_telemetry_source::ProjectionIssueKind::Invalid {
            detail: "5 further issues omitted at the envelope's bound".into()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn once_polls_report_one_complete_result_per_rpc_and_poll_again() {
    for status in [None, Some(Code::Unavailable)] {
        let clock = GetClock::new();

        let mut script = once_script(vec![
            snapshot(8_000_000_456, vec![update(Value::UintVal(8))]),
            snapshot(7_000_000_123, vec![update(Value::UintVal(7))]),
        ]);

        script.status = status;
        script.advance_clock = Some((clock.clone(), Duration::from_millis(150)));

        let mock = Mock::new(script);

        let unit = Arc::new(mock.once(once_spec()));

        let planned = nv_telemetry_orchestration::plan(
            Needs::default().with_polls([PollNeed::new(
                endpoint(),
                ConfiguredOnce::<Mock>::REQUEST_CLASS,
                "snapshot",
                Duration::from_secs(1),
            )]),
            &[ConfiguredOnce::<Mock>::declaration()],
        )
        .unwrap();

        let (collector, mut reports) = Collector::new(clock);
        collector
            .add(
                EndpointUnits::new(EndpointPolicy::default())
                    .with_polls([PollUnit::new(planned.polls()[0].clone(), unit)]),
            )
            .unwrap();

        for _ in 0..2 {
            let report = next_report(&mut reports).await;

            assert_eq!(report.status().request_class(), "configured-once");

            assert_eq!(
                report.status().outcome(),
                if status.is_none() {
                    Outcome::Succeeded
                } else {
                    Outcome::Failed
                }
            );

            assert_eq!(report.status().duration_nanos(), Some(150_000_000));

            assert_eq!(report.batches().len(), if status.is_none() { 2 } else { 0 });

            for batch in report.batches() {
                assert_eq!(batch.endpoint(), &endpoint());

                assert_eq!(batch.origin().provider(), "gnmi.configured");
            }
        }

        assert_eq!(mock.requests.lock().unwrap().len(), 2);

        assert!(mock.get_requests.lock().unwrap().is_empty());

        collector.shutdown();
    }
}

#[tokio::test]
async fn source_notifications_bind_readings_and_preserve_deletes_without_issue_text() {
    let mock = Mock::new(Script {
        responses: vec![wire::SubscribeResponse {
            response: Some(wire::subscribe_response::Response::Update(
                wire::Notification {
                    timestamp: 7_000_000_123,
                    update: vec![update(Value::UintVal(9))],
                    delete: vec![wire_path(&["leaf"])],
                    ..Default::default()
                },
            )),
            ..Default::default()
        }],
        ..Default::default()
    });

    let source = mock.subscription();

    let mut notifications = source.subscribe_with_sources().await.unwrap();

    let notification = notifications.next().await.unwrap().unwrap();

    assert_eq!(notification.observed_at(), Timestamp::new(7, 123).unwrap());

    assert_eq!(notification.bindings().len(), 1);

    let binding = &notification.bindings()[0];

    assert_eq!(
        binding.identity(),
        &ObservationIdentity::Reading(descriptor("a").key().clone())
    );

    assert_eq!(binding.source().origin(), "openconfig");

    assert_eq!(binding.source().target(), "");

    assert_eq!(binding.source().elements()[0].name(), "leaf");

    assert!(binding.source().elements()[0].keys().is_empty());

    assert_eq!(notification.deletions().len(), 1);

    assert!(notification.deletions()[0].covers(binding.source()));

    assert!(notification.parts().issues().is_empty());

    let (parts, bindings, deletions) = notification.into_parts();

    let acquired =
        nv_telemetry_source::stamp_item(&source, Timestamp::new(99, 0).unwrap(), Ok(parts))
            .unwrap();

    assert_eq!(acquired.batches().len(), 1);

    assert_eq!(bindings.len(), 1);

    assert_eq!(deletions.len(), 1);

    let legacy = source
        .perform()
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();

    assert_eq!(legacy.issues().len(), 1);

    assert_eq!(mock.requests.lock().unwrap().len(), 2);
}

fn source_collection_spec() -> SubscriptionSpec {
    let subject = SubjectTemplate::new("interface", IdentityValue::path_key(1, "name"));
    SubscriptionSpec::new(
        vec![path(&["interfaces", "interface"])],
        vec![
            Mapping::collection_reading(
                path(&["interfaces", "interface", "count"]),
                subject.clone(),
                ReadingTemplate::new(NumericType::Uint).with_facet("count"),
            ),
            Mapping::collection_string_state(
                path(&["interfaces", "interface", "status"]),
                subject.clone(),
                StateTemplate::new("status").with_facet("status"),
            ),
            Mapping::collection_boolean_state(
                path(&["interfaces", "interface", "enabled"]),
                subject,
                StateTemplate::new("enabled"),
            ),
        ],
    )
    .unwrap()
}

#[test]
fn source_bindings_follow_final_json_values_and_preserve_state_identity() {
    let spec = source_collection_spec()
        .with_origin("vendor")
        .with_target("device");

    let mut prefix = wire_path(&["interfaces", "interface"]);
    prefix.elem[1].key.insert("name".into(), "eth0".into());
    prefix.elem[1].key.insert("slot".into(), "2".into());

    let updates = vec![
        wire::Update {
            path: Some(wire_path(&[])),
            ..update(Value::JsonIetfVal(
                br#"{"count":1,"status":"vendor:ACTIVE","enabled":false}"#.to_vec(),
            ))
        },
        wire::Update {
            path: Some(wire_path(&["count"])),
            ..update(Value::UintVal(7))
        },
        wire::Update {
            path: Some(wire_path(&["count"])),
            ..update(Value::BoolVal(true))
        },
    ];

    let notification = wire::Notification {
        prefix: Some(prefix),
        ..snapshot(7_000_000_123, updates)
    };

    let actual = projection::project_with_sources(&spec.projection, notification.clone()).unwrap();
    let (payloads, issues) = projection::project(&spec.projection, notification).unwrap();

    assert_eq!(actual.parts(), &AcquisitionParts::new(payloads, issues));
    assert_eq!(actual.bindings().len(), 2);
    assert_eq!(actual.parts().issues().len(), 1);

    for binding in actual.bindings() {
        let ObservationIdentity::State {
            subject,
            facet,
            name,
        } = binding.identity()
        else {
            panic!("state binding")
        };

        assert_eq!(subject.id(), "eth0");
        assert_eq!(facet.as_deref(), (name == "status").then_some("status"));
        assert!(name == "status" || name == "enabled");
        assert_eq!(binding.source().elements().len(), 3);
        assert_eq!(binding.source().elements()[2].name(), name);

        assert_eq!(
            binding.source().elements()[1]
                .keys()
                .get("slot")
                .map(String::as_str),
            Some("2")
        );

        assert_eq!(binding.source().origin(), "vendor");
        assert_eq!(binding.source().target(), "device");
    }
}

#[test]
fn source_bindings_omit_collisions_and_do_not_bind_invalid_candidates() {
    let spec = source_collection_spec();
    let mut first = wire_path(&["interfaces", "interface", "count"]);
    first.elem[1].key.insert("name".into(), "eth0".into());
    first.elem[1].key.insert("slot".into(), "1".into());
    let mut second = first.clone();
    second.elem[1].key.insert("slot".into(), "2".into());

    let updates = vec![
        wire::Update {
            path: Some(first.clone()),
            ..update(Value::UintVal(1))
        },
        wire::Update {
            path: Some(second),
            ..update(Value::UintVal(2))
        },
    ];

    let collided =
        projection::project_with_sources(&spec.projection, snapshot(0, updates.clone())).unwrap();

    assert!(collided.parts().payloads().is_empty());
    assert!(collided.bindings().is_empty());
    assert_eq!(collided.parts().issues().len(), 2);

    let mut replacement = updates;

    replacement.push(wire::Update {
        path: Some(first),
        ..update(Value::BoolVal(false))
    });

    let accepted =
        projection::project_with_sources(&spec.projection, snapshot(0, replacement)).unwrap();

    assert_eq!(accepted.bindings().len(), 1);

    assert_eq!(
        accepted.bindings()[0].source().elements()[1]
            .keys()
            .get("slot")
            .map(String::as_str),
        Some("2")
    );

    assert_eq!(accepted.parts().issues().len(), 1);
}

#[test]
fn source_deletes_resolve_context_and_cover_paths_directionally() {
    let spec = source_collection_spec();
    let mut source = wire_path(&["interfaces", "interface", "count"]);
    source.elem[1].key.insert("name".into(), "eth0".into());
    source.elem[1].key.insert("slot".into(), "1".into());

    let update = wire::Update {
        path: Some(source.clone()),
        ..update(Value::UintVal(5))
    };

    let mut keyed = wire_path(&["interfaces", "interface"]);
    keyed.elem[1].key.insert("name".into(), "eth0".into());
    let mut other = keyed.clone();
    other.elem[1].key.insert("name".into(), "eth1".into());
    let mut explicit_origin = keyed.clone();
    explicit_origin.origin = "openconfig".into();

    let deletes = vec![
        source.clone(),
        keyed.clone(),
        wire_path(&["interfaces", "interface"]),
        wire_path(&[]),
        other,
        explicit_origin,
        keyed,
        wire_path(&["unrelated"]),
        wire_path(&[""]),
    ];

    let projected = projection::project_with_sources(
        &spec.projection,
        wire::Notification {
            delete: deletes,
            ..snapshot(0, vec![update.clone()])
        },
    )
    .unwrap();

    let retained = projected.bindings()[0].source();

    let covers: Vec<_> = projected
        .deletions()
        .iter()
        .map(|path| path.covers(retained))
        .collect();

    assert_eq!(covers, [true, true, true, true, false, true, true]);
    assert!(projected.deletions()[3].elements().is_empty());
    assert_eq!(projected.deletions()[1], projected.deletions()[6]);
    assert!(!retained.covers(&projected.deletions()[1]));
    assert_eq!(projected.parts().issues().len(), 1);
}

#[test]
fn source_deletes_keep_prefix_origin_and_target_scopes() {
    let spec = source_collection_spec();
    let mut source = wire_path(&["interfaces", "interface", "count"]);
    source.elem[1].key.insert("name".into(), "eth0".into());
    source.elem[1].key.insert("slot".into(), "1".into());

    let update = wire::Update {
        path: Some(source.clone()),
        ..update(Value::UintVal(5))
    };

    let projected = projection::project_with_sources(
        &spec.projection,
        wire::Notification {
            delete: vec![wire_path(&[])],
            ..snapshot(0, vec![update.clone()])
        },
    )
    .unwrap();

    let retained = projected.bindings()[0].source();

    let mut relative = update.clone();
    relative.path.as_mut().unwrap().elem.remove(0);

    let mut relative_delete = wire_path(&["interface"]);
    relative_delete.elem[0]
        .key
        .insert("name".into(), "eth0".into());

    let prefixed = projection::project_with_sources(
        &spec.projection,
        wire::Notification {
            prefix: Some(wire_path(&["interfaces"])),
            delete: vec![relative_delete],
            ..snapshot(0, vec![relative])
        },
    )
    .unwrap();

    assert!(prefixed.deletions()[0].covers(retained));

    assert_eq!(prefixed.bindings()[0].source(), retained);

    for (origin, target) in [("other", ""), ("", "other")] {
        let prefix = wire::Path {
            origin: origin.into(),
            target: target.into(),
            ..Default::default()
        };

        let different = projection::project_with_sources(
            &spec.projection,
            wire::Notification {
                prefix: Some(prefix),
                delete: vec![wire_path(&[])],
                ..snapshot(0, vec![update.clone()])
            },
        )
        .unwrap();

        assert!(!projected.deletions()[0].covers(different.bindings()[0].source()));
        assert!(!different.deletions()[0].covers(retained));
    }

    let scoped = spec.clone().with_origin("vendor").with_target("device");
    let mut conflicting = source;
    conflicting.target = "other".into();

    let invalid = projection::project_with_sources(
        &scoped.projection,
        wire::Notification {
            delete: vec![conflicting],
            ..snapshot(0, vec![update])
        },
    )
    .unwrap();

    assert_eq!(invalid.bindings().len(), 1);
    assert!(invalid.deletions().is_empty());
    assert_eq!(invalid.parts().issues()[0].path(), "delete[0].path");
}

#[test]
fn source_deletes_respect_literal_mapping_key_filters() {
    let filtered = SubscriptionSpec::new(
        vec![path(&["interfaces", "interface"])],
        vec![Mapping::collection_reading(
            SubscriptionPath::new(vec![
                PathElement::new("interfaces"),
                PathElement::new("interface").with_key("role", "front"),
                PathElement::new("count"),
            ]),
            SubjectTemplate::new("interface", IdentityValue::path_key(1, "name")),
            ReadingTemplate::new(NumericType::Uint),
        )],
    )
    .unwrap();

    let mut excluded = wire_path(&["interfaces", "interface"]);
    excluded.elem[1].key.insert("role".into(), "back".into());

    let mut included = excluded.clone();
    included.elem[1].key.insert("role".into(), "front".into());

    let relevance = projection::project_with_sources(
        &filtered.projection,
        wire::Notification {
            delete: vec![excluded, included],
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(relevance.deletions().len(), 1);

    assert_eq!(
        relevance.deletions()[0].elements()[1]
            .keys()
            .get("role")
            .map(String::as_str),
        Some("front")
    );
}

#[tokio::test]
async fn source_notifications_share_controls_terminal_failures_and_projection_errors() {
    for opening in [false, true] {
        let mock = Mock::new(Script {
            responses: vec![
                wire::SubscribeResponse {
                    response: Some(wire::subscribe_response::Response::SyncResponse(true)),
                    ..Default::default()
                },
                notification(vec![update(Value::UintVal(3))]),
            ],
            status: Some(Code::Unavailable),
            opening_error: opening,
            ..Default::default()
        });

        let source = mock.subscription();

        if opening {
            assert_eq!(
                source.subscribe_with_sources().await.err().unwrap().class(),
                Class::Connectivity
            );
        } else {
            let mut stream = source.subscribe_with_sources().await.unwrap();

            assert_eq!(stream.next().await.unwrap().unwrap().bindings().len(), 1);

            assert_eq!(
                stream.next().await.unwrap().unwrap_err().class(),
                Class::Connectivity
            );

            assert!(stream.next().await.is_none());
        }

        assert_eq!(mock.requests.lock().unwrap().len(), 1);
    }

    let mock = Mock::new(Script {
        responses: vec![
            notification(vec![]),
            notification(vec![update(Value::UintVal(1))]),
        ],
        ..Default::default()
    });

    let source = mock.subscription();

    // The shared loop must end if projection fails, before any later notification.
    let mut failed = source
        .subscribe::<(), _>(
            |_, _| Err(AcquisitionFailure::new(Class::Internal)),
            || None,
        )
        .await
        .unwrap();

    assert_eq!(
        failed.next().await.unwrap().unwrap_err().class(),
        Class::Internal
    );

    assert!(failed.next().await.is_none());
}

#[tokio::test]
async fn source_notifications_cancel_pending_open_or_idle_stream_and_open_fresh_instances() {
    let mock = Mock::new(Script {
        hold_connect: true,
        ..Default::default()
    });

    let source = mock.subscription();
    let mut opening = Box::pin(source.subscribe_with_sources());

    assert!(futures_util::poll!(&mut opening).is_pending());
    drop(opening);

    assert_eq!(mock.dropped.load(Ordering::SeqCst), 1);

    let mock = Mock::new(Script {
        responses: vec![notification(vec![])],
        hold_open: true,
        ..Default::default()
    });

    let source = mock.subscription();

    for expected in 1..=2 {
        let mut stream = source.subscribe_with_sources().await.unwrap();

        let empty = stream.next().await.unwrap().unwrap();

        assert!(empty.bindings().is_empty());
        assert!(empty.deletions().is_empty());
        assert!(futures_util::poll!(stream.next()).is_pending());
        drop(stream);
        assert_eq!(mock.dropped.load(Ordering::SeqCst), expected);
    }

    assert_eq!(mock.requests.lock().unwrap().len(), 2);
}

#[test]
fn source_bindings_deduplicate_within_the_effective_origin() {
    let spec = spec(NumericType::Uint);

    for expanded in [false, true] {
        for origins in [["openconfig", "vendor"], ["", "openconfig"]] {
            let mut first = if expanded {
                wire::Update {
                    path: Some(wire_path(&[])),
                    ..update(Value::JsonVal(br#"{"leaf":1}"#.to_vec()))
                }
            } else {
                update(Value::UintVal(1))
            };

            first.path.as_mut().unwrap().origin = origins[0].into();

            let mut last = update(Value::UintVal(2));
            last.path.as_mut().unwrap().origin = origins[1].into();

            let notification = snapshot(0, vec![first, last]);

            let actual =
                projection::project_with_sources(&spec.projection, notification.clone()).unwrap();

            let (payloads, issues) = projection::project(&spec.projection, notification).unwrap();

            // The standard acquisition keeps its existing element/key precedence.
            assert_eq!(payloads.len(), 1);
            assert!(issues.is_empty());

            if origins[1] == "vendor" {
                assert!(actual.parts().payloads().is_empty());
                assert!(actual.bindings().is_empty());
                assert_eq!(actual.parts().issues().len(), 2);
            } else {
                assert_eq!(actual.parts(), &AcquisitionParts::new(payloads, issues));
                assert_eq!(actual.bindings().len(), 1);
                assert_eq!(actual.bindings()[0].source().origin(), "openconfig");

                let Payload::Readings(readings) = &actual.parts().payloads()[0].1 else {
                    panic!("reading payload")
                };

                assert_eq!(readings.samples()[0].value(), &NumericValue::Uint(2));
            }
        }
    }
}

#[tokio::test]
async fn subscription_events_preserve_order_and_existing_notification_outputs() {
    use wire::subscribe_response::Response;

    let sync = |complete| wire::SubscribeResponse {
        response: Some(Response::SyncResponse(complete)),
        ..Default::default()
    };

    let replaced = wire::SubscribeResponse {
        response: Some(Response::Update(wire::Notification {
            timestamp: 7_000_000_123,
            update: vec![update(Value::UintVal(9))],
            delete: vec![wire_path(&["leaf"])],
            ..Default::default()
        })),
        ..Default::default()
    };

    let mock = Mock::new(Script {
        responses: vec![
            sync(false),
            sync(true),
            notification(vec![]),
            notification(vec![update(Value::BoolVal(true))]),
            replaced,
            sync(true),
            sync(true),
            sync(false),
        ],
        ..Default::default()
    });

    let source = mock.subscription();

    let mut events = source.subscribe_events().await.expect("event stream");

    let mut sourced = source
        .subscribe_with_sources()
        .await
        .expect("source stream");

    let mut standard = source.perform().await.expect("acquisition stream");

    let mut order = Vec::new();
    let mut counts = Vec::new();

    while let Some(event) = events.next().await {
        let event = match event {
            Ok(event) => event,
            Err(failure) => {
                assert_eq!(failure.class(), Class::Protocol);
                assert_eq!(failure.retryable(), Some(true));
                assert_eq!(sourced.next().await.unwrap().unwrap_err(), failure);
                assert_eq!(standard.next().await.unwrap().unwrap_err(), failure);
                order.push("error");
                break;
            }
        };

        match event {
            SubscriptionEvent::Synchronized => order.push("sync"),
            SubscriptionEvent::Notification(notification) => {
                assert_eq!(sourced.next().await.unwrap().unwrap(), notification);
                let parts = standard.next().await.unwrap().unwrap();

                assert_eq!(parts.payloads(), notification.parts().payloads());

                if notification.deletions().is_empty() {
                    assert_eq!(parts.issues(), notification.parts().issues());
                } else {
                    assert_eq!(parts.issues().len(), 1);
                    assert_eq!(parts.issues()[0].path(), "delete[0]");
                }

                counts.push((
                    notification.parts().payloads().len(),
                    notification.parts().issues().len(),
                ));

                order.push("notification");

                if !notification.bindings().is_empty() {
                    assert_eq!(notification.observed_at(), Timestamp::new(7, 123).unwrap());
                    assert_eq!(notification.bindings().len(), 1);
                    assert_eq!(notification.deletions().len(), 1);
                    assert!(notification.deletions()[0].covers(notification.bindings()[0].source()));
                }
            }
        }
    }

    assert_eq!(
        order,
        [
            "sync",
            "notification",
            "notification",
            "notification",
            "sync",
            "sync",
            "error"
        ]
    );

    assert_eq!(counts, [(0, 0), (0, 1), (1, 0)]);

    assert!(events.next().await.is_none());
    assert!(sourced.next().await.is_none());
    assert!(standard.next().await.is_none());
    assert_eq!(mock.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn subscription_events_classify_opening_and_terminal_errors_before_or_after_sync() {
    use wire::subscribe_response::Response;

    for (code, class, retryable, opening_error, synced) in [
        (Code::Unavailable, Class::Connectivity, true, true, false),
        (Code::Unavailable, Class::Connectivity, true, false, false),
        (Code::Unavailable, Class::Connectivity, true, false, true),
        (
            Code::Unauthenticated,
            Class::Authentication,
            false,
            true,
            false,
        ),
        (Code::DataLoss, Class::Protocol, false, false, true),
    ] {
        let responses = synced
            .then(|| wire::SubscribeResponse {
                response: Some(Response::SyncResponse(true)),
                ..Default::default()
            })
            .into_iter()
            .collect();

        let mock = Mock::new(Script {
            responses,
            status: Some(code),
            opening_error,
            ..Default::default()
        });

        let source = mock.subscription();

        let failure = match source.subscribe_events().await {
            Err(failure) => {
                assert!(opening_error);
                failure
            }
            Ok(mut stream) => {
                assert!(!opening_error);

                if synced {
                    assert_eq!(
                        stream.next().await.unwrap().unwrap(),
                        SubscriptionEvent::Synchronized
                    );
                }

                let failure = stream.next().await.unwrap().unwrap_err();

                assert!(stream.next().await.is_none());
                failure
            }
        };

        assert_eq!(failure.class(), class);
        assert_eq!(failure.retryable(), Some(retryable));
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
    }

    #[allow(deprecated)]
    let responses = [
        wire::SubscribeResponse::default(),
        wire::SubscribeResponse {
            response: Some(Response::Error(wire::Error {
                code: Code::DataLoss as u32,
                message: "target-secret".into(),
                ..Default::default()
            })),
            ..Default::default()
        },
    ];

    for response in responses {
        let mock = Mock::new(Script {
            responses: vec![response],
            ..Default::default()
        });

        let source = mock.subscription();

        let mut events = source.subscribe_events().await.unwrap();
        let failure = events.next().await.unwrap().unwrap_err();

        assert_eq!(failure.class(), Class::Protocol);
        assert!(!format!("{failure:?}").contains("target-secret"));
        assert!(events.next().await.is_none());
    }
}

#[tokio::test]
async fn subscription_events_cancel_pending_open_and_independent_idle_instances() {
    use wire::subscribe_response::Response;

    let mock = Mock::new(Script {
        hold_connect: true,
        ..Default::default()
    });

    let source = mock.subscription();

    let mut opening = Box::pin(source.subscribe_events());

    assert!(futures_util::poll!(&mut opening).is_pending());
    drop(opening);

    assert_eq!(mock.dropped.load(Ordering::SeqCst), 1);

    for synced in [false, true] {
        let responses = synced
            .then(|| wire::SubscribeResponse {
                response: Some(Response::SyncResponse(true)),
                ..Default::default()
            })
            .into_iter()
            .collect();

        let mock = Mock::new(Script {
            responses,
            hold_open: true,
            ..Default::default()
        });

        let source = mock.subscription();

        let mut first = source.subscribe_events().await.unwrap();
        let mut second = source.subscribe_events().await.unwrap();

        drop(source);

        if synced {
            assert_eq!(
                first.next().await.unwrap().unwrap(),
                SubscriptionEvent::Synchronized
            );

            assert_eq!(
                second.next().await.unwrap().unwrap(),
                SubscriptionEvent::Synchronized
            );
        }

        assert!(futures_util::poll!(first.next()).is_pending());
        drop(first);

        assert_eq!(mock.dropped.load(Ordering::SeqCst), 1);
        assert!(futures_util::poll!(second.next()).is_pending());
        drop(second);

        assert_eq!(mock.dropped.load(Ordering::SeqCst), 2);
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
    }
}
