// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Streamed acquisition: a subscription's connect attempt is an [`Acquire`]
//! whose output is a stream, not a single response.
//!
//! Each successful item represents one device notification. Protocol control
//! messages, including synchronization markers, produce no item and therefore
//! no observation or status. An `Err` item reports the terminal connection or
//! protocol failure and ends that stream instance. A reconnect calls
//! [`Acquire::perform`] again and creates a new stream instance.
//!
//! A connection failure is the outer `Err` from [`Acquire::perform`], before
//! any stream exists. Orchestration reports that completed attempt with one
//! failed status. A successful connection is control flow and earns no status
//! until a notification or terminal error arrives.
//!
//! Dropping the connect future cancels connection establishment. Dropping the
//! returned stream cancels the live subscription. Cancellation is not a
//! completed item and produces no status. The source must stop yielding after
//! an `Err`; [`stamp_item`] handles one item at a time and cannot enforce that
//! stream-level rule.

use nv_telemetry_model::Timestamp;

use crate::acquire::stamp;
use crate::Acquire;
use crate::Acquired;
use crate::AcquisitionFailure;
use crate::AcquisitionParts;

/// One reportable item from a streamed subscription.
///
/// `Ok` carries one device notification after protocol-specific projection.
/// `Err` carries the terminal failure and must be the final item in that stream
/// instance. Protocol control messages do not become `SubscriptionItem`s.
pub type SubscriptionItem = Result<AcquisitionParts, AcquisitionFailure>;

/// Stamps one stream item with the admitted unit's identity and the
/// caller-supplied instant it arrived - the same rule [`crate::acquire()`]
/// applies once to a whole polled request, applied per item here. Reads
/// `endpoint`/`origin` fresh on every call rather than capturing them once,
/// relying on [`Acquire`]'s own contract that one admitted unit names one
/// stable identity for its whole lifetime. Does no polling and keeps no
/// state: the caller drains the stream and supplies `at` fresh for each
/// item, exactly as the instant `acquire` receives is supplied fresh for
/// each polled admission. Sources receive no timestamp and cannot
/// substitute one; this holds per item, not just per request.
///
/// # Errors
///
/// Returns the item's own failure unchanged, or `Internal` if already
/// validated acquisition parts cannot form their model envelopes.
pub fn stamp_item<A>(
    acquisition: &A,
    at: Timestamp,
    item: SubscriptionItem,
) -> Result<Acquired, AcquisitionFailure>
where
    A: Acquire + ?Sized,
{
    let parts = item?;
    stamp(acquisition.endpoint(), acquisition.origin(), at, parts)
}

#[cfg(test)]
pub(crate) mod tests {
    use nv_telemetry_model::Coverage;
    use nv_telemetry_model::EndpointContext;
    use nv_telemetry_model::ObservedResource;
    use nv_telemetry_model::Origin;
    use nv_telemetry_model::Payload;
    use nv_telemetry_model::ResourceGraph;
    use nv_telemetry_model::States;
    use nv_telemetry_model::Subject;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::AcquisitionFailureClass;

    struct FixtureSubscription {
        endpoint: EndpointContext,
        origin: Origin,
        items: Vec<SubscriptionItem>,
        connect_failure: Option<AcquisitionFailure>,
    }

    impl Acquire for FixtureSubscription {
        type Output = tokio_stream::Iter<std::vec::IntoIter<SubscriptionItem>>;

        fn endpoint(&self) -> &EndpointContext {
            &self.endpoint
        }

        fn origin(&self) -> &Origin {
            &self.origin
        }

        async fn perform(&self) -> Result<Self::Output, AcquisitionFailure> {
            if let Some(failure) = &self.connect_failure {
                return Err(failure.clone());
            }

            Ok(tokio_stream::iter(self.items.clone()))
        }
    }

    fn fixture(items: Vec<SubscriptionItem>) -> FixtureSubscription {
        FixtureSubscription {
            endpoint: EndpointContext::builder()
                .endpoint_id("endpoint-a")
                .build()
                .expect("a valid endpoint"),
            origin: Origin::builder()
                .provider("provider-a")
                .request_class("subscribe-a")
                .build()
                .expect("a valid origin"),
            items,
            connect_failure: None,
        }
    }

    fn item() -> AcquisitionParts {
        let coverage = Coverage::builder()
            .completeness(nv_telemetry_model::Completeness::Partial)
            .build()
            .expect("valid coverage");

        let payload = Payload::States(States::builder().build().expect("an empty payload"));

        AcquisitionParts::new(vec![(coverage, payload)], Vec::new())
    }

    // A complete, scoped resource graph whose root cannot reach a listed
    // resource is the one case `Acquired::from_parts` refuses. Shared with
    // `result::tests::an_envelope_that_cannot_form_a_batch_is_an_internal_failure`,
    // which exercises the same boundary through `acquire` rather than
    // `stamp_item`.
    pub(crate) fn unreachable_graph_item() -> AcquisitionParts {
        let chassis = Subject::builder()
            .kind("chassis")
            .id("1U")
            .build()
            .expect("a valid subject");

        let sensor = Subject::builder()
            .kind("sensor")
            .id("Inlet")
            .build()
            .expect("a valid subject");

        let resource = |subject: &Subject| {
            ObservedResource::builder()
                .subject(subject.clone())
                .source_key(format!("/redfish/v1/{}", subject.id()))
                .properties_complete(true)
                .build()
                .expect("a valid resource")
        };

        let graph = ResourceGraph::builder()
            .resources(vec![resource(&chassis), resource(&sensor)])
            .build()
            .expect("structurally valid graph");

        let coverage = Coverage::builder()
            .completeness(nv_telemetry_model::Completeness::Complete)
            .scope(chassis)
            .build()
            .expect("valid coverage");

        AcquisitionParts::new(vec![(coverage, Payload::Resources(graph))], Vec::new())
    }

    #[tokio::test]
    async fn each_stream_item_is_stamped_with_its_own_caller_supplied_instant() {
        let subscription = fixture(vec![Ok(item()), Ok(item())]);

        let mut items = subscription
            .perform()
            .await
            .expect("connects and yields a stream");

        let mut stamped = Vec::new();
        let mut clock = 100;

        while let Some(next) = items.next().await {
            let at = Timestamp::new(clock, 0).expect("a valid instant");
            clock += 1;
            stamped.push(stamp_item(&subscription, at, next).expect("a successful item"));
        }

        assert_eq!(stamped.len(), 2);
        assert_eq!(stamped[0].batches()[0].window().start().seconds(), 100);
        assert_eq!(stamped[1].batches()[0].window().start().seconds(), 101);

        for acquired in &stamped {
            assert_eq!(acquired.batches()[0].endpoint(), subscription.endpoint());
            assert_eq!(acquired.batches()[0].origin(), subscription.origin());
        }
    }

    #[tokio::test]
    async fn a_terminal_item_preserves_its_failure() {
        let failure = AcquisitionFailure::new(AcquisitionFailureClass::Connectivity);
        let subscription = fixture(vec![Ok(item()), Err(failure)]);

        let mut items = subscription
            .perform()
            .await
            .expect("connects and yields a stream");

        let at = Timestamp::new(0, 0).expect("a valid instant");

        let first = items.next().await.expect("one item before the failure");
        stamp_item(&subscription, at, first).expect("a successful item");

        let second = items.next().await.expect("the terminal failure item");
        let error = stamp_item(&subscription, at, second).expect_err("the terminal item fails");

        assert_eq!(error.class(), AcquisitionFailureClass::Connectivity);
    }

    #[tokio::test]
    async fn a_connect_failure_never_yields_a_stream() {
        let mut subscription = fixture(vec![Ok(item())]);

        subscription.connect_failure = Some(AcquisitionFailure::new(
            AcquisitionFailureClass::Authentication,
        ));

        let Err(error) = subscription.perform().await else {
            panic!("connect fails before any stream exists");
        };

        assert_eq!(error.class(), AcquisitionFailureClass::Authentication);
    }

    #[tokio::test]
    async fn an_item_that_cannot_form_a_batch_is_an_internal_failure() {
        let subscription = fixture(vec![Ok(unreachable_graph_item())]);

        let mut items = subscription
            .perform()
            .await
            .expect("connects and yields a stream");

        let at = Timestamp::new(0, 0).expect("a valid instant");

        let only_item = items.next().await.expect("one item");

        let error =
            stamp_item(&subscription, at, only_item).expect_err("an unreachable scoped graph");

        assert_eq!(error.class(), AcquisitionFailureClass::Internal);
    }
}
