// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Properties of the generated wire types, as they reach a consumer rather
//! than as they read in the `.proto`. The presence discipline in particular
//! is only worth anything if it survives into the generated API — a scalar
//! that arrived as a bare `f64` instead of an `Option<f64>` would mean an
//! absent reading and a reading of zero had become the same value again.

use prost::Message as _;
use prost_reflect::DescriptorPool;
use prost_reflect::DynamicMessage;
use prost_reflect::Value as V;

use crate::generated::wire;

#[test]
fn an_absent_reading_and_a_zero_reading_are_different_bytes() {
    let absent = wire::NumericValue { kind: None };
    let zero = wire::NumericValue {
        kind: Some(wire::numeric_value::Kind::DoubleValue(0.0)),
    };

    assert_ne!(absent.encode_to_vec(), zero.encode_to_vec());

    // And they survive the round trip as different values, which is the whole
    // point: a device that did not answer must not decode as a device that
    // answered zero.
    let decoded = wire::NumericValue::decode(zero.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded, zero);
    assert_ne!(decoded, absent);
}

#[test]
fn explicit_presence_survives_into_the_generated_api() {
    // Every scalar the schema declared `optional` must be an Option here. If
    // prost ever lowered one to a bare value, absence would stop being
    // representable and the presence rule would be enforced only on paper.
    let empty = wire::Subject::default();
    assert!(empty.kind.is_none(), "Subject.kind lost explicit presence");
    assert!(empty.id.is_none(), "Subject.id lost explicit presence");

    let empty = wire::Timestamp::default();
    assert!(empty.seconds.is_none());
    assert!(
        empty.nanos.is_none(),
        "Timestamp.nanos lost explicit presence"
    );

    let empty = wire::ObservedResource::default();
    assert!(
        empty.properties_complete.is_none(),
        "properties_complete lost explicit presence; absent would read as false"
    );

    // Enum-typed fields are the highest-risk case: a proto3 enum that loses
    // `optional` becomes a bare i32 where 0 is UNSPECIFIED, which collapses
    // "the device did not say" into "the device said unspecified".
    assert!(wire::Coverage::default().completeness.is_none());
    assert!(wire::LogRecord::default().severity.is_none());
    let status = wire::AcquisitionStatus::default();
    assert!(status.outcome.is_none());
    assert!(status.failure_class.is_none());
    assert!(wire::ProjectionIssue::default().kind.is_none());
}

#[test]
fn an_unknown_enum_value_survives_rather_than_collapsing() {
    // proto3 enums are open. A newer producer's completeness value must reach
    // a consumer intact so it can refuse to interpret it, rather than being
    // silently rewritten to UNSPECIFIED — which reads as "did not say".
    let ahead = wire::Coverage {
        completeness: Some(99),
        scope: None,
    };
    let decoded = wire::Coverage::decode(ahead.encode_to_vec().as_slice()).unwrap();

    assert_eq!(decoded.completeness, Some(99));
    assert!(wire::Completeness::try_from(99).is_err());
}

#[test]
fn unknown_fields_are_dropped_on_re_encode() {
    // prost keeps no unknown-field storage. A component built on these types
    // cannot forward a newer producer's fields — it silently strips them. The
    // behavior is pinned here as a property of the current substrate, not a
    // promise of compatibility across schema revisions.
    let known = wire::Subject {
        kind: Some("sensor".into()),
        scope: vec![],
        id: Some("x".into()),
    };
    let mut bytes = known.encode_to_vec();
    // Field 99, varint, value 1 — as a future revision might add.
    bytes.extend_from_slice(&[0xF8, 0x06, 0x01]);

    let decoded = wire::Subject::decode(bytes.as_slice()).expect("forward-compatible decode");
    assert_eq!(decoded, known);
    assert_eq!(
        decoded.encode_to_vec(),
        known.encode_to_vec(),
        "the unknown field was retained; this test's premise changed"
    );
    assert!(decoded.encode_to_vec().len() < bytes.len());
}

#[test]
fn a_batch_round_trips_byte_for_byte() {
    let batch = wire::ObservationBatch {
        endpoint: Some(wire::EndpointContext {
            endpoint_id: Some("bmc-lab-07".into()),
            attributes: None,
        }),
        origin: Some(wire::Origin {
            provider: Some("redfish.sensor.odata".into()),
            request_class: Some("sensor-read".into()),
        }),
        window: Some(wire::ObservationWindow {
            start: Some(wire::Timestamp {
                seconds: Some(1_785_621_243),
                nanos: Some(0),
            }),
            end: None,
        }),
        coverage: Some(wire::Coverage {
            completeness: Some(wire::Completeness::Partial as i32),
            scope: None,
        }),
        payload: Some(wire::observation_batch::Payload::Readings(wire::Readings {
            descriptors: vec![wire::SignalDescriptor {
                key: Some(signal_key()),
                kind: Some("temperature".into()),
                unit: Some("Cel".into()),
                range: None,
            }],
            samples: vec![wire::Reading {
                key: Some(signal_key()),
                value: Some(wire::NumericValue {
                    kind: Some(wire::numeric_value::Kind::DoubleValue(47.5)),
                }),
                observed_at: None,
            }],
        })),
    };

    let encoded = batch.encode_to_vec();
    let decoded = wire::ObservationBatch::decode(encoded.as_slice()).expect("batch decodes");

    assert_eq!(decoded, batch);
    assert_eq!(
        decoded.encode_to_vec(),
        encoded,
        "re-encoding is not stable"
    );
}

#[test]
fn a_counter_above_two_to_the_fifty_third_survives() {
    // The reason NumericValue is a union rather than a bare double: this
    // value is exact as a uint64 and is not representable as an f64.
    let big = 91_827_364_554_433_777_u64;
    // The lossy round trip is the point: it asserts the constant genuinely
    // does not survive a double, so this test cannot quietly stop proving
    // anything if someone later picks a smaller one.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let through_double = big as f64 as u64;
    assert_ne!(through_double, big, "the test value must not fit a double");

    let value = wire::NumericValue {
        kind: Some(wire::numeric_value::Kind::UintValue(big)),
    };
    let decoded = wire::NumericValue::decode(value.encode_to_vec().as_slice()).unwrap();

    assert_eq!(
        decoded.kind,
        Some(wire::numeric_value::Kind::UintValue(big)),
        "a 64-bit counter lost precision on the wire"
    );
}

fn signal_key() -> wire::SignalKey {
    wire::SignalKey {
        subject: Some(wire::Subject {
            kind: Some("sensor".into()),
            scope: vec!["1U".into()],
            id: Some("CPU1Temp".into()),
        }),
        facet: None,
    }
}

#[test]
fn generated_types_and_descriptors_encode_identically() {
    let pool = pool();

    // Built with the generated types.
    let native = wire::Subject {
        kind: Some("sensor".into()),
        scope: vec!["1U".into(), "PSU1".into()],
        id: Some("CPU1Temp".into()),
    };

    // The same value, built against the descriptors the rules ran over.
    let descriptor = pool
        .get_message_by_name("nv.telemetry.v1.Subject")
        .expect("Subject is in the shipped schema");
    let mut dynamic = DynamicMessage::new(descriptor.clone());
    dynamic.set_field_by_name("kind", V::String("sensor".into()));
    dynamic.set_field_by_name(
        "scope",
        V::List(vec![V::String("1U".into()), V::String("PSU1".into())]),
    );
    dynamic.set_field_by_name("id", V::String("CPU1Temp".into()));

    assert_eq!(
        native.encode_to_vec(),
        dynamic.encode_to_vec(),
        "the generated type and the descriptors disagree about the wire format"
    );

    // And across the boundary in both directions.
    let from_dynamic = wire::Subject::decode(dynamic.encode_to_vec().as_slice())
        .expect("descriptor-encoded bytes decode into the generated type");
    assert_eq!(from_dynamic, native);

    DynamicMessage::decode(descriptor, native.encode_to_vec().as_slice())
        .expect("generated-encoded bytes decode against the descriptors");
}

#[test]
fn oneof_arms_carry_the_field_numbers_the_schema_declares() {
    let pool = pool();
    let descriptor = pool
        .get_message_by_name("nv.telemetry.v1.NumericValue")
        .expect("NumericValue is in the shipped schema");

    // Arm selection is a wire-level fact: picking the wrong tag would silently
    // reinterpret an integer counter as a float.
    for (name, native, value) in [
        (
            "uint_value",
            wire::NumericValue {
                kind: Some(wire::numeric_value::Kind::UintValue(91_827_364_554_433_777)),
            },
            V::U64(91_827_364_554_433_777),
        ),
        (
            "int_value",
            wire::NumericValue {
                kind: Some(wire::numeric_value::Kind::IntValue(-42)),
            },
            V::I64(-42),
        ),
    ] {
        let mut dynamic = DynamicMessage::new(descriptor.clone());
        dynamic.set_field_by_name(name, value);
        assert_eq!(
            native.encode_to_vec(),
            dynamic.encode_to_vec(),
            "generated `{name}` does not match the descriptor's encoding"
        );
    }
}

/// The descriptors the invariant rules ran over, decoded from the schema crate.
///
/// Built here rather than borrowed from the compiler so that this crate's
/// tests do not depend on it — the dependency runs the other way.
fn pool() -> DescriptorPool {
    DescriptorPool::decode(nv_telemetry_schema::DESCRIPTOR_SET).expect("shipped schema decodes")
}
