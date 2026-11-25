//! Regression tests for the `Arc`-based broadcast in `Engine`.
//!
//! Before the `Arc` refactor, every event was deep-cloned once per strategy and every action
//! once per executor (including executors whose `ExecutorMap` immediately discarded it).

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use burberry::{
    async_trait, map_arc_executor, map_executor, ActionSubmitter, Collector, CollectorFilterMap,
    CollectorStream, Engine, Executor, Strategy,
};
use futures::StreamExt;

static EVENT_CLONES: AtomicUsize = AtomicUsize::new(0);
static ACTION_PAYLOAD_CLONES: AtomicUsize = AtomicUsize::new(0);
static ARC_ACTION_CLONES: AtomicUsize = AtomicUsize::new(0);
static ARC_ACTION_EXECUTED: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct EventPayload;

impl Clone for EventPayload {
    fn clone(&self) -> Self {
        EVENT_CLONES.fetch_add(1, Ordering::SeqCst);
        EventPayload
    }
}

#[derive(Debug)]
struct ActionPayload;

impl Clone for ActionPayload {
    fn clone(&self) -> Self {
        ACTION_PAYLOAD_CLONES.fetch_add(1, Ordering::SeqCst);
        ActionPayload
    }
}

#[derive(Debug)]
struct ArcActionPayload;

impl Clone for ArcActionPayload {
    fn clone(&self) -> Self {
        ARC_ACTION_CLONES.fetch_add(1, Ordering::SeqCst);
        ArcActionPayload
    }
}

#[derive(Debug, Clone)]
struct Event(#[allow(dead_code)] EventPayload);

#[derive(Debug, Clone)]
enum Action {
    Handled(ActionPayload),
    #[allow(dead_code)]
    Ignored(ActionPayload),
}

#[derive(Debug)]
enum ArcAction {
    Handled(Arc<ArcActionPayload>),
    #[allow(dead_code)]
    Ignored(Arc<ArcActionPayload>),
}

struct OnceCollector;

#[async_trait]
impl Collector<Event> for OnceCollector {
    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Event>> {
        Ok(Box::pin(futures::stream::once(async {
            Event(EventPayload)
        })))
    }
}

struct SubmittingStrategy;

#[async_trait]
impl Strategy<Event, Action> for SubmittingStrategy {
    async fn process_event(
        &mut self,
        _event: Arc<Event>,
        submitter: Arc<dyn ActionSubmitter<Action>>,
    ) {
        submitter.submit(Action::Handled(ActionPayload));
    }
}

struct CountingExecutor;

#[async_trait]
impl Executor<ActionPayload> for CountingExecutor {
    async fn execute(&self, _action: Arc<ActionPayload>) -> eyre::Result<()> {
        Ok(())
    }
}

struct ArcSubmittingStrategy;

#[async_trait]
impl Strategy<Event, ArcAction> for ArcSubmittingStrategy {
    async fn process_event(
        &mut self,
        _event: Arc<Event>,
        submitter: Arc<dyn ActionSubmitter<ArcAction>>,
    ) {
        submitter.submit(ArcAction::Handled(Arc::new(ArcActionPayload)));
    }
}

struct ArcCountingExecutor;

#[async_trait]
impl Executor<ArcActionPayload> for ArcCountingExecutor {
    async fn execute(&self, _action: Arc<ArcActionPayload>) -> eyre::Result<()> {
        ARC_ACTION_EXECUTED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn broadcast_does_not_deep_clone_payloads() {
    let mut engine: Engine<Event, Action> = Engine::new();

    engine.add_collector(Box::new(OnceCollector));

    // Two strategies: both must observe the single event without it being cloned.
    engine.add_strategy(Box::new(SubmittingStrategy));
    engine.add_strategy(Box::new(SubmittingStrategy));

    // Two executors: only one of them handles `Action::Handled`.
    engine.add_executor(map_executor!(CountingExecutor, Action::Handled));
    engine.add_executor(map_executor!(CountingExecutor, Action::Ignored));

    let set = engine.run().await.expect("engine should start");

    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(set);

    assert_eq!(
        EVENT_CLONES.load(Ordering::SeqCst),
        0,
        "the event must reach both strategies without a single clone"
    );

    // Each of the 2 strategies submits one action; the matching executor extracts the inner
    // payload exactly once, and the non-matching executor extracts nothing.
    assert_eq!(
        ACTION_PAYLOAD_CLONES.load(Ordering::SeqCst),
        2,
        "each action should be unwrapped once, only by the executor that handles its variant"
    );
}

#[tokio::test]
async fn map_arc_executor_forwards_arc_payload_with_zero_clones() {
    let mut engine: Engine<Event, ArcAction> = Engine::new();

    engine.add_collector(Box::new(OnceCollector));
    engine.add_strategy(Box::new(ArcSubmittingStrategy));
    engine.add_executor(map_arc_executor!(ArcCountingExecutor, ArcAction::Handled));
    engine.add_executor(map_arc_executor!(ArcCountingExecutor, ArcAction::Ignored));

    let set = engine.run().await.expect("engine should start");

    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(set);

    assert_eq!(ARC_ACTION_EXECUTED.load(Ordering::SeqCst), 1);
    assert_eq!(
        ARC_ACTION_CLONES.load(Ordering::SeqCst),
        0,
        "map_arc_executor! must never clone the inner payload"
    );
}

#[tokio::test]
async fn collector_filter_map_supports_non_copy_closures() {
    struct VecCollector(Vec<u64>);

    #[async_trait]
    impl Collector<u64> for VecCollector {
        async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, u64>> {
            Ok(Box::pin(futures::stream::iter(self.0.clone())))
        }
    }

    // Captures a `HashSet` (not `Copy`), which previously failed to compile.
    let allowed: HashSet<u64> = [2, 4].into_iter().collect();
    let collector =
        CollectorFilterMap::new(Box::new(VecCollector(vec![1, 2, 3, 4, 5])), move |n| {
            allowed.contains(&n).then_some(n * 10)
        });

    let stream = collector.get_event_stream().await.unwrap();
    let items: Vec<u64> = stream.collect().await;
    assert_eq!(items, vec![20, 40]);
}
