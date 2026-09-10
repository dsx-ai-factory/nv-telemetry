// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The direct encoder against prost, arm by arm.

use prost::Message as _;

use crate::generated::wire;
use crate::ObservationBatch;

// The maximal-batch test covers every field of every message, but its values
// exercise only two arms of the value vocabulary. The hand-written `Emit`
// impls are transcriptions of `value.proto`'s wire types — `sint64` zigzag on
// the integer arms, an empty nested message for null, `int64` for a
// timestamp's seconds — and a slip there (say `int64` where the schema says
// `sint64`) would survive every other test in this file. The next two tests
// hold every arm of `Value` and `NumericValue` to byte identity with prost,
// in a batch whose optional context fields are all absent — which is itself
// the pin that absence contributes no bytes on either path.

/// A minimal batch — required context only — around `payload`, asserted to
/// encode byte-identically through the direct encoder and through prost.
fn assert_direct_encoder_matches_prost(payload: wire::observation_batch::Payload) {
    let wire_batch = wire::ObservationBatch {
        endpoint: Some(wire::EndpointContext {
            endpoint_id: Some("bmc-lab-07".into()),
            attributes: None,
        }),
        origin: Some(wire::Origin {
            provider: Some("redfish".into()),
            request_class: Some("read".into()),
        }),
        window: Some(wire::ObservationWindow {
            start: Some(wire::Timestamp {
                seconds: Some(1),
                nanos: Some(0),
            }),
            end: None,
        }),
        coverage: Some(wire::Coverage {
            completeness: Some(wire::Completeness::Complete as i32),
            scope: None,
        }),
        payload: Some(payload),
    };
    let validated = ObservationBatch::try_from(wire_batch).expect("a valid batch");
    assert_eq!(
        validated.encode_to_vec(),
        wire::ObservationBatch::from(validated).encode_to_vec(),
        "the direct encoder diverged from prost on a value arm"
    );
}

#[test]
fn every_value_arm_encodes_byte_identically_to_prost() {
    use wire::value::Kind;
    let wire_value = |kind: Kind| wire::Value { kind: Some(kind) };
    let entry = |key: &str, kind: Kind| wire::value::map::Entry {
        key: Some(key.into()),
        value: Some(wire_value(kind)),
    };
    let every_arm = Kind::MapValue(wire::value::Map {
        entries: vec![
            entry("null", Kind::NullValue(wire::Null {})),
            // `false` also pins that a present zero scalar still encodes.
            entry("bool", Kind::BoolValue(false)),
            // Negative, so a zigzag slip would double the bytes.
            entry("int", Kind::IntValue(-3)),
            // Above 2^53, so the arm cannot be quietly a double.
            entry("uint", Kind::UintValue(91_827_364_554_433_777)),
            entry("double", Kind::DoubleValue(2.5)),
            entry("string", Kind::StringValue("s".into())),
            entry("bytes", Kind::BytesValue(vec![0, 255])),
            // Negative seconds: `int64`, not zigzag — a ten-byte varint.
            entry(
                "ts",
                Kind::TimestampValue(wire::Timestamp {
                    seconds: Some(-5),
                    nanos: Some(7),
                }),
            ),
            entry(
                "list",
                Kind::ListValue(wire::value::List {
                    values: vec![
                        wire_value(Kind::IntValue(5)),
                        // An empty container: a zero-length nested message.
                        wire_value(Kind::ListValue(wire::value::List { values: vec![] })),
                    ],
                }),
            ),
            // A map inside a `Value` — arm 10 — distinct from the bare map
            // fields the maximal test covers.
            entry(
                "map",
                Kind::MapValue(wire::value::Map {
                    entries: vec![entry("k", Kind::NullValue(wire::Null {}))],
                }),
            ),
        ],
    });

    assert_direct_encoder_matches_prost(wire::observation_batch::Payload::States(wire::States {
        observations: vec![wire::StateObservation {
            subject: Some(wire::Subject {
                kind: Some("sensor".into()),
                scope: vec![],
                id: Some("Fan1".into()),
            }),
            name: Some("health".into()),
            value: Some(wire_value(every_arm)),
            observed_at: None,
            facet: Some("power".into()),
        }],
    }));
}

#[test]
fn every_numeric_arm_encodes_byte_identically_to_prost() {
    use wire::numeric_value::Kind;
    let key = |id: &str| wire::SignalKey {
        subject: Some(wire::Subject {
            kind: Some("sensor".into()),
            scope: vec![],
            id: Some(id.into()),
        }),
        facet: None,
    };
    let sample = |id: &str, kind: Kind| wire::Reading {
        key: Some(key(id)),
        value: Some(wire::NumericValue { kind: Some(kind) }),
        observed_at: None,
    };
    let descriptor = |id: &str| wire::SignalDescriptor {
        key: Some(key(id)),
        kind: Some("counter".into()),
        unit: None,
        range: None,
    };

    assert_direct_encoder_matches_prost(wire::observation_batch::Payload::Readings(
        wire::Readings {
            descriptors: vec![descriptor("A"), descriptor("B"), descriptor("C")],
            samples: vec![
                // Negative, so a zigzag slip would double the bytes.
                sample("A", Kind::IntValue(-47)),
                // Above 2^53, so the arm cannot be quietly a double.
                sample("B", Kind::UintValue(91_827_364_554_433_777)),
                sample("C", Kind::DoubleValue(47.5)),
            ],
        },
    ));
}
