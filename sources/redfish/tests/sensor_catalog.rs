// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Discovery and caller-owned reads through the public transport boundary.

mod support;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt as _;
use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Error;
use nv_redfish_bmc_mock::Expect;
use nv_redfish_bmc_mock::ExpectedRequest;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_redfish::EventFrames;
use nv_telemetry_redfish::RedfishService;
use nv_telemetry_redfish::RedfishTransport;
use nv_telemetry_redfish::SensorCatalogRead;
use nv_telemetry_redfish::SensorRead;
use nv_telemetry_redfish::TransportFault;
use nv_telemetry_redfish::WalkBudget;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::ProjectionIssue;
use serde_json::json;
use serde_json::Value;

const ROOT: &str = r#"{"@odata.id":"/redfish/v1","Chassis":{"@odata.id":"/redfish/v1/Chassis"}}"#;
const CHASSIS_LIST: &str =
    r#"{"@odata.id":"/redfish/v1/Chassis","Members":[{"@odata.id":"/redfish/v1/Chassis/1U"}]}"#;

const CHASSIS: &str = r#"{"@odata.id":"/redfish/v1/Chassis/1U","Sensors":{"@odata.id":"/redfish/v1/Chassis/1U/Sensors"}}"#;
const SENSORS: &str = r#"{"@odata.id":"/redfish/v1/Chassis/1U/Sensors","Members":[{"@odata.id":"/redfish/v1/Chassis/1U/Sensors/CPU1Temp"}]}"#;
const SENSOR: &str = "/redfish/v1/Chassis/1U/Sensors/CPU1Temp";

/// Builds public discovery work over the existing scripted transport.
fn discovery(
    answers: impl IntoIterator<Item = Expect<Error>>,
    budget: WalkBudget,
) -> SensorCatalogRead<support::MockTransport> {
    let bmc = Arc::new(Bmc::default());

    for answer in answers {
        bmc.expect(answer);
    }

    SensorCatalogRead::new(
        EndpointContext::builder()
            .endpoint_id("bmc")
            .build()
            .expect("valid endpoint"),
        support::service(&bmc),
        budget,
    )
    .expect("valid origin")
}

/// Scripts the failure classification at the transport boundary.
fn status(uri: &str, status: u16) -> Expect<Error> {
    Expect {
        request: ExpectedRequest::Get {
            id: uri.to_owned().into(),
        },
        response: Err(Error::HttpStatus(status)),
    }
}

#[tokio::test]
async fn relative_expanded_links_are_fetched_without_decoding_unused_values() {
    let bmc = Arc::new(Bmc::<Error>::default());

    let answers = [
        Expect::get(
            "/redfish/v1",
            json!({"@odata.id":"/redfish/v1", "Chassis":{"@odata.id":"v1/Chassis", "Members":false}}),
        ),
        Expect::get(
            "/redfish/v1/Chassis",
            json!({"@odata.id":"/redfish/v1/Chassis", "Members":[{"@odata.id":"Chassis/1U", "Sensors":false}]}),
        ),
        Expect::get(
            "/redfish/v1/Chassis/1U",
            json!({"@odata.id":"ignored", "Sensors":{"@odata.id":"1U/Sensors", "Members":"invalid unused expansion"}}),
        ),
        Expect::get(
            "/redfish/v1/Chassis/1U/Sensors",
            json!({"@odata.id":"ignored", "Members":[{"@odata.id":"Sensors/CPU1Temp", "Reading":{"bad":"number"}}]}),
        ),
    ];

    for answer in answers {
        bmc.expect(answer);
    }

    let endpoint = EndpointContext::builder()
        .endpoint_id("bmc")
        .build()
        .expect("valid endpoint");

    let service = support::service(&bmc);

    let read = SensorCatalogRead::new(endpoint.clone(), Arc::clone(&service), WalkBudget::DEFAULT)
        .expect("valid origin");

    let catalog = read
        .perform()
        .await
        .expect("discovered without a sensor GET");

    assert_eq!(catalog.locations(), [SENSOR]);
    assert_eq!(catalog.completeness(), Completeness::Complete);
    assert!(catalog.issues().is_empty());
    assert_eq!(read.origin().provider(), "redfish.sensor.catalog");
    assert_eq!(read.origin().request_class(), "sensor-catalog");
    assert_eq!(read.endpoint(), &endpoint);

    bmc.expect(Expect::get(
        SENSOR,
        include_str!("fixtures/sensor/nominal.json"),
    ));

    let sensor = SensorRead::new(endpoint, &catalog.locations()[0], service);

    let parts = sensor
        .perform()
        .await
        .expect("caller reads discovered sensor");

    assert_eq!(parts.payloads().len(), 2);
    assert!(parts.issues().is_empty());
}

#[tokio::test]
async fn a_cold_service_classifies_then_fetches_the_minimal_root_each_acquisition() {
    let bmc = Arc::new(Bmc::<Error>::default());
    bmc.expect(Expect::get("/redfish/v1", ROOT));
    let empty = json!({"@odata.id":"/redfish/v1/Chassis", "Members":[]});

    for _ in 0..2 {
        bmc.expect(Expect::get("/redfish/v1", ROOT));
        bmc.expect(Expect::get("/redfish/v1/Chassis", &empty));
    }

    let read = SensorCatalogRead::new(
        EndpointContext::builder()
            .endpoint_id("bmc")
            .build()
            .expect("valid endpoint"),
        Arc::new(RedfishService::new(support::MockTransport::scripted(&bmc))),
        WalkBudget::DEFAULT,
    )
    .expect("valid origin");

    let first = read.perform().await.expect("cold traversal");

    let second = read
        .perform()
        .await
        .expect("fresh traversal with cached classification");

    assert_eq!(first, second);
    assert!(second.locations().is_empty());
    assert_eq!(second.completeness(), Completeness::Complete);
}

#[tokio::test]
async fn both_collection_levels_page_and_deduplicate_by_resolved_location() {
    let read = discovery(
        [
            Expect::get("/redfish/v1", ROOT),
            Expect::get(
                "/redfish/v1/Chassis",
                json!({"@odata.id":"ignored", "Members":[{"@odata.id":"/redfish/v1/Chassis/1U"}], "Members@odata.count":3, "Members@odata.nextLink":"?page=2"}),
            ),
            Expect::get(
                "/redfish/v1/Chassis?page=2",
                json!({"@odata.id":"ignored", "Members":[{"@odata.id":"https://device/redfish/v1/Chassis/1U"},{"@odata.id":"Chassis/2U"}]}),
            ),
            Expect::get("/redfish/v1/Chassis/1U", CHASSIS),
            Expect::get(
                "/redfish/v1/Chassis/1U/Sensors",
                json!({"@odata.id":"ignored", "Members":[{"@odata.id":SENSOR}], "Members@odata.count":3, "Members@odata.nextLink":"./Sensors?page=2"}),
            ),
            Expect::get(
                "/redfish/v1/Chassis/1U/Sensors?page=2",
                json!({"@odata.id":"ignored", "Members":[{"@odata.id":SENSOR},{"@odata.id":"Sensors/Fan?view=1"}]}),
            ),
            Expect::get(
                "/redfish/v1/Chassis/2U",
                json!({"@odata.id":"ignored", "Sensors":{"@odata.id":"1U/Sensors"}}),
            ),
        ],
        WalkBudget::DEFAULT,
    );

    let catalog = read.perform().await.expect("paged discovery");

    assert_eq!(
        catalog.locations(),
        [SENSOR, "/redfish/v1/Chassis/1U/Sensors/Fan?view=1"]
    );

    assert_eq!(catalog.completeness(), Completeness::Complete);
    assert!(catalog.issues().is_empty());
}

#[tokio::test]
async fn missing_root_navigation_is_unsupported_but_empty_and_absent_optional_sensors_are_complete()
{
    let missing = discovery(
        [Expect::get("/redfish/v1", r#"{"@odata.id":"/redfish/v1"}"#)],
        WalkBudget::DEFAULT,
    );

    let failure = missing.perform().await.expect_err("no root navigation");

    assert_eq!(failure.class(), AcquisitionFailureClass::Unsupported);

    for (root, class) in [
        (status("/redfish/v1", 500), AcquisitionFailureClass::Device),
        (
            Expect::get(
                "/redfish/v1",
                json!({"@odata.id":"/redfish/v1", "Chassis":false}),
            ),
            AcquisitionFailureClass::Protocol,
        ),
    ] {
        let read = discovery([root], WalkBudget::DEFAULT);

        let failure = read.perform().await.expect_err("root prerequisite failed");

        assert_eq!(failure.class(), class);
    }

    for chassis in [
        json!({"@odata.id":"ignored"}),
        json!({"@odata.id":"ignored", "Sensors":null}),
        json!({"@odata.id":"ignored", "Sensors":{"@odata.id":"/redfish/v1/Chassis/1U/Sensors"}}),
    ] {
        let mut answers = vec![
            Expect::get("/redfish/v1", ROOT),
            Expect::get("/redfish/v1/Chassis", CHASSIS_LIST),
            Expect::get("/redfish/v1/Chassis/1U", &chassis),
        ];

        if !chassis["Sensors"].is_null() {
            answers.push(Expect::get(
                "/redfish/v1/Chassis/1U/Sensors",
                json!({"@odata.id":"ignored", "Members":[], "Members@odata.count":0}),
            ));
        }

        let read = discovery(answers, WalkBudget::DEFAULT);

        let catalog = read.perform().await.expect("empty modern discovery");

        assert!(catalog.locations().is_empty());
        assert!(catalog.issues().is_empty());
        assert_eq!(catalog.completeness(), Completeness::Complete);
    }
}

#[tokio::test]
async fn a_failed_or_malformed_chassis_preserves_healthy_siblings() {
    for failed in [
        status("/redfish/v1/Chassis/bad", 404),
        Expect::get(
            "/redfish/v1/Chassis/bad",
            json!({"@odata.id":"bad", "Sensors":false}),
        ),
    ] {
        let read = discovery(
            [
                Expect::get("/redfish/v1", ROOT),
                Expect::get(
                    "/redfish/v1/Chassis",
                    json!({"@odata.id":"ignored", "Members":[{"@odata.id":"/redfish/v1/Chassis/bad"},{"@odata.id":"/redfish/v1/Chassis/1U"}]}),
                ),
                failed,
                Expect::get("/redfish/v1/Chassis/1U", CHASSIS),
                Expect::get("/redfish/v1/Chassis/1U/Sensors", SENSORS),
            ],
            WalkBudget::DEFAULT,
        );

        let catalog = read.perform().await.expect("healthy sibling retained");

        assert_eq!(catalog.locations(), [SENSOR]);
        assert_eq!(catalog.completeness(), Completeness::Partial);
        assert_eq!(catalog.issues().len(), 1);
        assert_eq!(catalog.issues()[0].path(), "Chassis.Pages[0].Members[0]");
    }
}

#[tokio::test]
async fn chassis_failure_paths_retain_original_members_after_deduplication_and_paging() {
    for (paged, rejected, nested, expected_path) in [
        (false, false, false, "Chassis.Pages[0].Members[2]"),
        (false, true, false, "Chassis.Pages[0].Members[2]"),
        (
            true,
            false,
            true,
            "Chassis.Pages[1].Members[1].Sensors.Pages[0]",
        ),
    ] {
        let healthy = "/redfish/v1/Chassis/1U";
        let bad = "/redfish/v1/Chassis/bad";
        let predecessor = if rejected { "#unusable" } else { healthy };
        let mut members = vec![json!({"@odata.id":healthy})];

        if !paged {
            members.extend([json!({"@odata.id":predecessor}), json!({"@odata.id":bad})]);
        }

        let mut page = json!({"@odata.id":"ignored", "Members":members});

        if paged {
            page["Members@odata.nextLink"] = json!("?page=2");
        }

        let mut answers = vec![
            Expect::get("/redfish/v1", ROOT),
            Expect::get("/redfish/v1/Chassis", page),
        ];

        if paged {
            answers.push(Expect::get("/redfish/v1/Chassis?page=2", json!({"@odata.id":"ignored", "Members":[{"@odata.id":predecessor},{"@odata.id":bad}]})));
        }

        answers.push(Expect::get(healthy, CHASSIS));
        answers.push(Expect::get("/redfish/v1/Chassis/1U/Sensors", SENSORS));

        if nested {
            answers.push(Expect::get(bad, json!({"@odata.id":"ignored", "Sensors":{"@odata.id":"/redfish/v1/Chassis/bad/Sensors"}})));
            answers.push(status("/redfish/v1/Chassis/bad/Sensors", 404));
        } else {
            answers.push(status(bad, 404));
        }

        let read = discovery(answers, WalkBudget::DEFAULT);

        let expected_paths = if rejected {
            vec!["Chassis.Pages[0].Members[1]", expected_path]
        } else {
            vec![expected_path]
        };

        let catalog = read.perform().await.expect("healthy branch retained");

        assert_eq!(catalog.locations(), [SENSOR]);
        assert_eq!(catalog.completeness(), Completeness::Partial);

        assert_eq!(
            catalog
                .issues()
                .iter()
                .map(ProjectionIssue::path)
                .collect::<Vec<_>>(),
            expected_paths
        );
    }
}

#[tokio::test]
async fn device_faults_on_initial_or_later_sensor_pages_are_partial() {
    for later in [false, true] {
        let mut answers = vec![
            Expect::get("/redfish/v1", ROOT),
            Expect::get("/redfish/v1/Chassis", CHASSIS_LIST),
            Expect::get("/redfish/v1/Chassis/1U", CHASSIS),
        ];

        if later {
            answers.push(Expect::get("/redfish/v1/Chassis/1U/Sensors", json!({"@odata.id":"ignored", "Members":[{"@odata.id":SENSOR}], "Members@odata.nextLink":"?page=2"})));
            answers.push(status("/redfish/v1/Chassis/1U/Sensors?page=2", 500));
        } else {
            answers.push(status("/redfish/v1/Chassis/1U/Sensors", 404));
        }

        let read = discovery(answers, WalkBudget::DEFAULT);

        let catalog = read.perform().await.expect("device answer stays local");

        assert_eq!(
            catalog.locations(),
            if later {
                vec![SENSOR.to_owned()]
            } else {
                Vec::new()
            }
        );

        assert_eq!(catalog.completeness(), Completeness::Partial);
        assert_eq!(catalog.issues().len(), 1);
        assert!(catalog.issues()[0]
            .path()
            .starts_with("Chassis.Pages[0].Members[0].Sensors.Pages["));
    }
}

#[tokio::test]
async fn endpoint_and_internal_faults_discard_previously_found_locations() {
    for (status_code, class) in [
        (401, AcquisitionFailureClass::Authentication),
        (408, AcquisitionFailureClass::Timeout),
        (304, AcquisitionFailureClass::Internal),
    ] {
        let read = discovery(
            [
                Expect::get("/redfish/v1", ROOT),
                Expect::get("/redfish/v1/Chassis", CHASSIS_LIST),
                Expect::get("/redfish/v1/Chassis/1U", CHASSIS),
                Expect::get(
                    "/redfish/v1/Chassis/1U/Sensors",
                    json!({"@odata.id":"ignored", "Members":[{"@odata.id":SENSOR}], "Members@odata.nextLink":"?page=2"}),
                ),
                status("/redfish/v1/Chassis/1U/Sensors?page=2", status_code),
            ],
            WalkBudget::DEFAULT,
        );

        let failure = read
            .perform()
            .await
            .expect_err("no partial success for endpoint faults");

        assert_eq!(failure.class(), class);
    }
}

#[tokio::test]
async fn unusable_member_links_preserve_siblings_and_redact_diagnostics() {
    let read = discovery(
        [
            Expect::get("/redfish/v1", ROOT),
            Expect::get("/redfish/v1/Chassis", CHASSIS_LIST),
            Expect::get("/redfish/v1/Chassis/1U", CHASSIS),
            Expect::get(
                "/redfish/v1/Chassis/1U/Sensors",
                json!({"@odata.id":"ignored", "Members":[{"@odata.id":"https://user:password@device/sensor?token=secret#fragment"},{"@odata.id":""},{"@odata.id":"mailto:secret@example.com"},{"@odata.id":"https://advertised.example//escape.example/sensor?token=secret"},{"@odata.id":SENSOR}]}),
            ),
        ],
        WalkBudget::DEFAULT,
    );

    let catalog = read.perform().await.expect("usable sibling");

    assert_eq!(catalog.locations(), [SENSOR]);
    assert_eq!(catalog.completeness(), Completeness::Partial);
    assert_eq!(catalog.issues().len(), 4);

    assert_eq!(
        catalog.issues()[0],
        ProjectionIssue::invalid(
            "Chassis.Pages[0].Members[0].Sensors.Pages[0].Members[0]",
            "unusable resource reference"
        )
    );

    let debug = format!("{read:?} {catalog:?} {:?}", catalog.issues());

    assert!(!debug.contains("secret"));
    assert!(!debug.contains("password"));
    assert!(!debug.contains(SENSOR));
}

#[tokio::test]
async fn invalid_navigation_and_continuation_links_make_discovery_partial() {
    for (root, chassis, sensors) in [
        (
            json!({"@odata.id":"ignored", "Chassis":{"@odata.id":"#bad"}}),
            None,
            None,
        ),
        (
            serde_json::from_str(ROOT).expect("JSON"),
            Some(json!({"@odata.id":"ignored", "Sensors":{"@odata.id":"#bad"}})),
            None,
        ),
        (
            serde_json::from_str(ROOT).expect("JSON"),
            Some(serde_json::from_str(CHASSIS).expect("JSON")),
            Some(
                json!({"@odata.id":"ignored", "Members":[{"@odata.id":SENSOR}], "Members@odata.nextLink":"#bad"}),
            ),
        ),
    ] {
        let mut answers = vec![Expect::get("/redfish/v1", root)];

        if let Some(chassis) = chassis {
            answers.push(Expect::get("/redfish/v1/Chassis", CHASSIS_LIST));
            answers.push(Expect::get("/redfish/v1/Chassis/1U", chassis));
        }

        if let Some(sensors) = sensors {
            answers.push(Expect::get("/redfish/v1/Chassis/1U/Sensors", sensors));
        }

        let read = discovery(answers, WalkBudget::DEFAULT);

        let catalog = read.perform().await.expect("unusable child navigation");

        assert_eq!(catalog.completeness(), Completeness::Partial);
        assert_eq!(catalog.issues().len(), 1);
    }
}

#[tokio::test]
async fn repeated_empty_and_short_pages_preserve_locations_and_report_partial_traversal() {
    for (next, continuation, expected_path) in [
        (
            Some("?page=2"),
            Some(json!({"@odata.id":"ignored", "Members":[]})),
            "Chassis.Pages[0].Members[0].Sensors.Pages[1]",
        ),
        (
            Some("Sensors"),
            None,
            "Chassis.Pages[0].Members[0].Sensors.Pages[1]",
        ),
        (
            None,
            None,
            "Chassis.Pages[0].Members[0].Sensors.Members@odata.count",
        ),
    ] {
        let mut page = json!({"@odata.id":"ignored", "Members":[{"@odata.id":SENSOR}]});

        if let Some(next) = next {
            page["Members@odata.nextLink"] = json!(next);
        } else {
            page["Members@odata.count"] = json!(2);
        }

        let mut answers = vec![
            Expect::get("/redfish/v1", ROOT),
            Expect::get("/redfish/v1/Chassis", CHASSIS_LIST),
            Expect::get("/redfish/v1/Chassis/1U", CHASSIS),
            Expect::get("/redfish/v1/Chassis/1U/Sensors", page),
        ];

        if let Some(continuation) = continuation {
            answers.push(Expect::get(
                "/redfish/v1/Chassis/1U/Sensors?page=2",
                continuation,
            ));
        }

        let read = discovery(answers, WalkBudget::DEFAULT);

        let catalog = read.perform().await.expect("bounded malformed paging");

        assert_eq!(catalog.locations(), [SENSOR]);
        assert_eq!(catalog.completeness(), Completeness::Partial);
        assert_eq!(catalog.issues().len(), 1);
        assert_eq!(catalog.issues()[0].path(), expected_path);
    }
}

#[tokio::test]
async fn duplicate_listed_entries_spend_the_graph_budget_before_deduplication() {
    let read = discovery(
        [
            Expect::get("/redfish/v1", ROOT),
            Expect::get(
                "/redfish/v1/Chassis",
                json!({"@odata.id":"ignored", "Members":[{"@odata.id":"/redfish/v1/Chassis/1U"},{"@odata.id":"/redfish/v1/Chassis/1U"}]}),
            ),
            Expect::get("/redfish/v1/Chassis/1U", CHASSIS),
            Expect::get(
                "/redfish/v1/Chassis/1U/Sensors",
                json!({"@odata.id":"ignored", "Members":[{"@odata.id":SENSOR},{"@odata.id":SENSOR},{"@odata.id":"/unvisited"}], "Members@odata.nextLink":"?unvisited"}),
            ),
        ],
        WalkBudget::new(4, Duration::from_secs(30)),
    );

    let catalog = read.perform().await.expect("bounded discovery");

    assert_eq!(catalog.locations(), [SENSOR]);
    assert_eq!(catalog.completeness(), Completeness::Partial);

    assert_eq!(
        catalog.issues(),
        [ProjectionIssue::invalid(
            "@truncated",
            "member budget spent"
        )]
    );
}

#[tokio::test]
async fn zero_budgets_do_no_protocol_work() {
    for budget in [
        WalkBudget::new(0, Duration::from_secs(30)),
        WalkBudget::new(10, Duration::ZERO),
    ] {
        let read = discovery([], budget);

        let catalog = read.perform().await.expect("budget stop");

        assert!(catalog.locations().is_empty());
        assert_eq!(catalog.completeness(), Completeness::Partial);
        assert_eq!(catalog.issues().len(), 1);
        assert_eq!(catalog.issues()[0].path(), "@truncated");
    }
}

/// Blocks exactly 1 reachable GET and records when its future is dropped.
struct BlockingTransport {
    block_at: usize,
    calls: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    delay: Duration,
}

struct InFlight(Arc<AtomicUsize>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl RedfishTransport for BlockingTransport {
    async fn get(&self, _uri: &str) -> Result<Value, TransportFault> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);

        if call == self.block_at {
            let _in_flight = InFlight(Arc::clone(&self.dropped));

            if self.delay.is_zero() {
                return std::future::pending().await;
            }

            futures_timer::Delay::new(self.delay).await;
        }

        let fixture = match call {
            0 | 1 => ROOT,
            2 => CHASSIS_LIST,
            3 => CHASSIS,
            _ => SENSORS,
        };

        Ok(serde_json::from_str(fixture).expect("fixture JSON"))
    }

    async fn events(&self, _: &str, _: Option<&str>) -> Result<EventFrames, TransportFault> {
        Err(TransportFault::Client)
    }
}

#[tokio::test]
async fn dropping_discovery_cancels_each_reachable_get() {
    for block_at in 0..5 {
        let dropped = Arc::new(AtomicUsize::new(0));

        let read = SensorCatalogRead::new(
            EndpointContext::builder()
                .endpoint_id("bmc")
                .build()
                .expect("valid endpoint"),
            Arc::new(RedfishService::new(BlockingTransport {
                block_at,
                calls: Arc::new(AtomicUsize::new(0)),
                dropped: Arc::clone(&dropped),
                delay: Duration::ZERO,
            })),
            WalkBudget::DEFAULT,
        )
        .expect("valid origin");

        assert!(read.perform().now_or_never().is_none());

        assert_eq!(dropped.load(Ordering::SeqCst), 1, "GET {block_at} canceled");
    }
}

#[tokio::test]
async fn elapsed_time_after_classification_prevents_the_next_get() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));

    let read = SensorCatalogRead::new(
        EndpointContext::builder()
            .endpoint_id("bmc")
            .build()
            .expect("valid endpoint"),
        Arc::new(RedfishService::new(BlockingTransport {
            block_at: 0,
            calls: Arc::clone(&calls),
            dropped,
            delay: Duration::from_millis(200),
        })),
        WalkBudget::new(10, Duration::from_millis(100)),
    )
    .expect("valid origin");

    let catalog = read.perform().await.expect("elapsed budget stop");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(catalog.completeness(), Completeness::Partial);

    assert_eq!(
        catalog.issues(),
        [ProjectionIssue::invalid("@truncated", "time budget spent")]
    );
}

#[tokio::test]
async fn the_outer_deadline_cancels_an_unfinished_get_and_returns_failure() {
    let dropped = Arc::new(AtomicUsize::new(0));

    let read = SensorCatalogRead::new(
        EndpointContext::builder()
            .endpoint_id("bmc")
            .build()
            .expect("valid endpoint"),
        Arc::new(RedfishService::new(BlockingTransport {
            block_at: 4,
            calls: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::clone(&dropped),
            delay: Duration::ZERO,
        })),
        WalkBudget::new(10, Duration::from_secs(1)),
    )
    .expect("valid origin");

    let failure = read
        .perform()
        .await
        .expect_err("deadline cancels acquisition");

    assert_eq!(failure.class(), AcquisitionFailureClass::Timeout);
    assert_eq!(failure.detail(), Some("walk deadline exceeded"));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
