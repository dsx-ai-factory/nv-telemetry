// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Instruction-count benchmarks (gungraun / Valgrind Callgrind) for the
//! validated model, through the public API only — the numbers are what a
//! consumer pays.
//!
//! Three questions, one group each:
//!
//! - `boundary`: what bytes cost at the process edge. Decode is every
//!   consumer's ingress price. Encode is the known open question: it clones
//!   the whole batch to rebuild the wire tree, and the deferred fix — a
//!   direct-from-validated encoder — is justified exactly when `clone_graph`
//!   is a large share of `encode`. That comparison is this file's reason to
//!   exist.
//! - `construct`: what a source pays per poll to build observations through
//!   the builders, validation included.
//! - `values`: the recursive value vocabulary on its own — map assembly with
//!   its sorted, duplicate-free representation, and nesting to the schema's
//!   depth bound.
//!
//! Sizes are deliberately modest: instruction counts are deterministic, so a
//! thousand-element batch answers the scaling question without a
//! million-element run, and Callgrind makes big inputs slow.

// The `library_benchmark` macro expands to code the workspace's
// `unused_qualifications` lint misreads as our own spans — and, when the
// attribute carries a `config`, it additionally wraps the function and emits
// an `Option`-returning `__get_config`, tripping `unreachable_pub` and
// `clippy::unnecessary_wraps` on spans this file cannot edit. A benchmark
// takes its setup output by value because that is how gungraun hands it
// over — borrowing instead would measure a call shape no consumer uses.
#![allow(
    unused_qualifications,
    unreachable_pub,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps
)]

#[cfg(unix)]
mod unix {
    use std::hint::black_box;

    use gungraun::library_benchmark;
    use gungraun::Callgrind;
    use gungraun::EventKind;
    use gungraun::LibraryBenchmarkConfig;
    use nv_telemetry_model::Accumulation;
    use nv_telemetry_model::AcquisitionStatus;
    use nv_telemetry_model::Completeness;
    use nv_telemetry_model::Coverage;
    use nv_telemetry_model::EndpointContext;
    use nv_telemetry_model::NumericValue;
    use nv_telemetry_model::ObservationBatch;
    use nv_telemetry_model::ObservationWindow;
    use nv_telemetry_model::ObservedResource;
    use nv_telemetry_model::Origin;
    use nv_telemetry_model::Outcome;
    use nv_telemetry_model::Payload;
    use nv_telemetry_model::Reading;
    use nv_telemetry_model::Readings;
    use nv_telemetry_model::ResourceGraph;
    use nv_telemetry_model::ResourceRelation;
    use nv_telemetry_model::SignalDescriptor;
    use nv_telemetry_model::SignalKey;
    use nv_telemetry_model::Subject;
    use nv_telemetry_model::Timestamp;
    use nv_telemetry_model::Value;

    /// Bulk size for readings: a large-but-plausible per-endpoint batch.
    const BULK: usize = 1024;

    /// The regression gate for benchmarks whose counts sit under a few tens
    /// of thousands of instructions. At that size a single inlining decision
    /// moved by unrelated code is several percent by itself — a +100 swing on
    /// a 611-instruction build once failed CI with the source of that path
    /// line-identical — while a real regression arrives in tens of percent.
    /// Everything bulk-sized holds the strict default set at `main!`.
    const MICRO_IR_LIMIT: f64 = 15.0;

    /// Graph size: resources in a chassis subtree walk.
    const RESOURCES: usize = 128;

    /// Entries per observed resource's property map.
    const PROPERTIES: usize = 8;

    fn subject(kind: &str, id: &str) -> Subject {
        Subject::builder()
            .kind(kind)
            .id(id)
            .build()
            .expect("a valid subject")
    }

    fn key(index: usize) -> SignalKey {
        SignalKey::builder()
            .subject(subject("sensor", &format!("Sensor{index}")))
            .build()
            .expect("a valid key")
    }

    fn batch(payload: Payload) -> ObservationBatch {
        ObservationBatch::builder()
            .endpoint(
                EndpointContext::builder()
                    .endpoint_id("bmc-lab-07")
                    .build()
                    .expect("a valid endpoint"),
            )
            .origin(
                Origin::builder()
                    .provider("redfish.sensor.odata")
                    .request_class("sensor-read")
                    .build()
                    .expect("a valid origin"),
            )
            .window(
                ObservationWindow::builder()
                    .start(Timestamp::new(1_785_621_243, 0).expect("a valid instant"))
                    .build()
                    .expect("a valid window"),
            )
            .coverage(
                Coverage::builder()
                    .completeness(Completeness::Partial)
                    .build()
                    .expect("valid coverage"),
            )
            .payload(payload)
            .build()
            .expect("a valid batch")
    }

    /// What the readings measure.
    #[derive(Clone, Copy)]
    enum Signals {
        /// Temperatures: values now.
        Instant,
        /// Energy totals, each sample dating the device's start.
        Cumulative,
    }

    /// Descriptor and sample vectors for `count` distinct signals, the parts
    /// a source has in hand before batch assembly.
    fn reading_parts(count: usize, signals: Signals) -> (Vec<SignalDescriptor>, Vec<Reading>) {
        let start = Timestamp::new(1_785_000_000, 0).expect("a valid instant");
        let descriptors = (0..count)
            .map(|index| {
                let descriptor = SignalDescriptor::builder().key(key(index));
                match signals {
                    Signals::Instant => descriptor.kind("temperature").unit("Cel"),
                    Signals::Cumulative => descriptor
                        .kind("energy")
                        .unit("kW.h")
                        .accumulation(Accumulation::Cumulative),
                }
                .build()
                .expect("a valid descriptor")
            })
            .collect();
        #[allow(clippy::cast_precision_loss)]
        let samples = (0..count)
            .map(|index| {
                let reading = Reading::builder()
                    .key(key(index))
                    .value(NumericValue::double(20.0 + index as f64 / 8.0).expect("finite"));
                match signals {
                    Signals::Instant => reading,
                    Signals::Cumulative => reading.accumulation_start(start),
                }
                .build()
                .expect("a valid reading")
            })
            .collect();
        (descriptors, samples)
    }

    fn reading_batch(count: usize, signals: Signals) -> ObservationBatch {
        let (descriptors, samples) = reading_parts(count, signals);
        batch(Payload::Readings(
            Readings::builder()
                .descriptors(descriptors)
                .samples(samples)
                .build()
                .expect("a valid readings payload"),
        ))
    }

    /// The graph from [`graph_batch`], bare, for hashing.
    fn graph_payload() -> ResourceGraph {
        match graph_batch().payload() {
            Payload::Resources(graph) => graph.clone(),
            _ => unreachable!("graph_batch builds a resources payload"),
        }
    }

    /// A chassis subtree: one root, `RESOURCES - 1` children hanging off it,
    /// each resource carrying a small property map.
    fn graph_batch() -> ObservationBatch {
        let properties = |seed: usize| {
            (0..PROPERTIES)
                .map(|index| {
                    (
                        format!("property_{index}"),
                        Value::string(format!("value-{seed}-{index}")).expect("a valid value"),
                    )
                })
                .collect()
        };
        let resource = |name: &str, seed: usize| {
            ObservedResource::builder()
                .subject(subject("chassis", name))
                .source_key(format!("/redfish/v1/Chassis/{name}"))
                .properties(properties(seed))
                .properties_complete(true)
                .build()
                .expect("a valid resource")
        };

        let mut resources = vec![resource("Root", 0)];
        let mut relations = Vec::new();
        for index in 1..RESOURCES {
            let name = format!("Child{index}");
            resources.push(resource(&name, index));
            relations.push(
                ResourceRelation::builder()
                    .source(subject("chassis", "Root"))
                    .target(subject("chassis", &name))
                    .kind("contains")
                    .build()
                    .expect("a valid relation"),
            );
        }

        batch(Payload::Resources(
            ResourceGraph::builder()
                .resources(resources)
                .relations(relations)
                .build()
                .expect("a valid graph"),
        ))
    }

    fn status_bytes() -> Vec<u8> {
        AcquisitionStatus::builder()
            .endpoint_id("bmc-lab-07")
            .provider("redfish.sensor.odata")
            .request_class("sensor-read")
            .outcome(Outcome::Succeeded)
            .started_at(Timestamp::new(1_785_621_243, 0).expect("a valid instant"))
            .build()
            .expect("a valid status")
            .encode_to_vec()
    }

    /// `(key, value)` pairs for a map value, unsorted on arrival as a
    /// device's JSON would be.
    fn map_entries(count: usize) -> Vec<(String, Value)> {
        (0..count)
            .rev()
            .map(|index| {
                (
                    format!("key_{index}"),
                    Value::string(format!("value-{index}")).expect("a valid value"),
                )
            })
            .collect()
    }

    // --- boundary: the process edge ---

    #[library_benchmark]
    #[bench::readings_bulk(reading_batch(BULK, Signals::Instant).encode_to_vec())]
    #[bench::cumulative_bulk(reading_batch(BULK, Signals::Cumulative).encode_to_vec())]
    #[bench::graph(graph_batch().encode_to_vec())]
    pub fn decode(bytes: Vec<u8>) -> ObservationBatch {
        black_box(ObservationBatch::decode(&bytes).expect("a valid batch"))
    }

    // Its own benchmark rather than a case of `decode`, because the gate is
    // per benchmark and a one-reading batch needs the micro one.
    #[library_benchmark(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::default().soft_limits([(EventKind::Ir, MICRO_IR_LIMIT)])))]
    #[bench::readings_small(reading_batch(1, Signals::Instant).encode_to_vec())]
    pub fn decode_small(bytes: Vec<u8>) -> ObservationBatch {
        black_box(ObservationBatch::decode(&bytes).expect("a valid batch"))
    }

    #[library_benchmark(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::default().soft_limits([(EventKind::Ir, MICRO_IR_LIMIT)])))]
    #[bench::status(status_bytes())]
    pub fn decode_status(bytes: Vec<u8>) -> AcquisitionStatus {
        black_box(AcquisitionStatus::decode(&bytes).expect("a valid status"))
    }

    #[library_benchmark]
    #[bench::readings_bulk(reading_batch(BULK, Signals::Instant))]
    #[bench::cumulative_bulk(reading_batch(BULK, Signals::Cumulative))]
    #[bench::graph(graph_batch())]
    pub fn encode(batch: ObservationBatch) -> Vec<u8> {
        black_box(batch.encode_to_vec())
    }

    // What `encode` paid before the direct encoder existed; kept as the
    // measured cost of duplicating the representation, which is what any
    // future by-value boundary would pay.
    #[library_benchmark]
    #[bench::readings_bulk(reading_batch(BULK, Signals::Instant))]
    #[bench::graph(graph_batch())]
    pub fn clone_batch(batch: ObservationBatch) -> ObservationBatch {
        black_box(batch.clone())
    }

    // --- construct: what a source pays per poll ---

    #[library_benchmark(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::default().soft_limits([(EventKind::Ir, MICRO_IR_LIMIT)])))]
    #[bench::one()]
    pub fn build_reading() -> Reading {
        black_box(
            Reading::builder()
                .key(
                    SignalKey::builder()
                        .subject(
                            Subject::builder()
                                .kind("sensor")
                                .id("CPU1Temp")
                                .build()
                                .expect("a valid subject"),
                        )
                        .build()
                        .expect("a valid key"),
                )
                .value(NumericValue::double(47.5).expect("finite"))
                .build()
                .expect("a valid reading"),
        )
    }

    // Batch assembly from parts already in hand: the per-batch validation
    // cost — presence, bounds, the descriptor uniqueness set, and the
    // sample-key resolution rule.
    #[library_benchmark]
    #[bench::bulk(reading_parts(BULK, Signals::Instant))]
    #[bench::cumulative_bulk(reading_parts(BULK, Signals::Cumulative))]
    pub fn build_readings(parts: (Vec<SignalDescriptor>, Vec<Reading>)) -> Readings {
        let (descriptors, samples) = parts;
        black_box(
            Readings::builder()
                .descriptors(descriptors)
                .samples(samples)
                .build()
                .expect("a valid readings payload"),
        )
    }

    // --- values: the recursive vocabulary ---

    #[library_benchmark]
    #[bench::entries_64(map_entries(64))]
    pub fn build_value_map(entries: Vec<(String, Value)>) -> Value {
        black_box(Value::map(entries).expect("a valid map"))
    }

    // --- hashing: the content digest over the canonical representation ---

    // Discards bytes: the stream's traversal is what is measured, not any
    // hash function.
    struct Discard;

    impl std::hash::Hasher for Discard {
        fn write(&mut self, bytes: &[u8]) {
            black_box(bytes);
        }

        fn finish(&self) -> u64 {
            0
        }
    }

    #[library_benchmark]
    #[bench::graph(graph_payload())]
    pub fn content_hash(graph: ResourceGraph) -> u64 {
        let mut sink = Discard;
        graph.content_hash(&mut sink);
        std::hash::Hasher::finish(&black_box(sink))
    }

    #[library_benchmark(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::default().soft_limits([(EventKind::Ir, MICRO_IR_LIMIT)])))]
    #[bench::depth_16()]
    pub fn build_value_deep() -> Value {
        // Fifteen wraps over a scalar reach the schema's depth bound of 16;
        // the constructors recompute and check depth at every level, so this
        // is the worst-case nesting price.
        let mut value = Value::int(0);
        for _ in 0..15 {
            value = Value::list(vec![value]).expect("within the depth bound");
        }
        black_box(value)
    }
}

#[cfg(unix)]
use unix::build_reading;
#[cfg(unix)]
use unix::build_readings;
#[cfg(unix)]
use unix::build_value_deep;
#[cfg(unix)]
use unix::build_value_map;
#[cfg(unix)]
use unix::clone_batch;
#[cfg(unix)]
use unix::content_hash;
#[cfg(unix)]
use unix::decode;
#[cfg(unix)]
use unix::decode_small;
#[cfg(unix)]
use unix::decode_status;
#[cfg(unix)]
use unix::encode;

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = boundary;
    benchmarks = decode, decode_small, decode_status, encode, clone_batch
);

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = construct;
    benchmarks = build_reading, build_readings
);

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = values;
    benchmarks = build_value_map, build_value_deep
);

#[cfg(unix)]
gungraun::library_benchmark_group!(
    name = hashing;
    benchmarks = content_hash
);

// The regression gate lives here rather than on the CI command line, so the
// thresholds sit next to the benchmarks they judge and a benchmark can carry
// its own — a command-line limit would override every one of them.
#[cfg(unix)]
gungraun::main!(
    config = gungraun::LibraryBenchmarkConfig::default()
        .tool(gungraun::Callgrind::default().soft_limits([(gungraun::EventKind::Ir, 2.0)]));
    library_benchmark_groups = boundary,
    construct,
    values,
    hashing
);

#[cfg(not(unix))]
fn main() {}
