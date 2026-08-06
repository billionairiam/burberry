//! Tests for executor concurrency and per-action timeouts.
//!
//! By default an executor processes actions strictly serially and waits forever, so one slow or
//! hanging action stalls every action behind it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use burberry::{
    async_trait, ActionSubmitter, Collector, CollectorStream, Engine, Executor, Strategy,
};

#[derive(Debug)]
struct Event;

#[derive(Debug)]
struct Action(Duration);

struct OnceCollector;

#[async_trait]
impl Collector<Event> for OnceCollector {
    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Event>> {
        Ok(Box::pin(futures::stream::once(async { Event })))
    }
}

/// Submits one action per configured delay as soon as it sees the event.
struct FanOutStrategy(Vec<Duration>);

#[async_trait]
impl Strategy<Event, Action> for FanOutStrategy {
    async fn process_event(
        &mut self,
        _event: Arc<Event>,
        submitter: Arc<dyn ActionSubmitter<Action>>,
    ) {
        for delay in &self.0 {
            submitter.submit(Action(*delay));
        }
    }
}

struct SleepExecutor(&'static AtomicUsize);

#[async_trait]
impl Executor<Action> for SleepExecutor {
    async fn execute(&self, action: Arc<Action>) -> eyre::Result<()> {
        tokio::time::sleep(action.0).await;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

static CONCURRENT_DONE: AtomicUsize = AtomicUsize::new(0);
static SERIAL_DONE: AtomicUsize = AtomicUsize::new(0);
static TIMEOUT_DONE: AtomicUsize = AtomicUsize::new(0);

#[tokio::test]
async fn executor_concurrency_runs_actions_in_parallel() {
    let delays = vec![Duration::from_millis(150); 4];

    let mut engine: Engine<Event, Action> = Engine::new().with_executor_concurrency(4);
    engine.add_collector(Box::new(OnceCollector));
    engine.add_strategy(Box::new(FanOutStrategy(delays)));
    engine.add_executor(Box::new(SleepExecutor(&CONCURRENT_DONE)));

    let _set = engine.run().await.expect("engine should start");

    // Serially these 4 actions would need 600ms.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        CONCURRENT_DONE.load(Ordering::SeqCst),
        4,
        "all 4 actions should have run concurrently"
    );
}

#[tokio::test]
async fn executors_are_serial_by_default() {
    let delays = vec![Duration::from_millis(150); 4];

    let mut engine: Engine<Event, Action> = Engine::new();
    engine.add_collector(Box::new(OnceCollector));
    engine.add_strategy(Box::new(FanOutStrategy(delays)));
    engine.add_executor(Box::new(SleepExecutor(&SERIAL_DONE)));

    let _set = engine.run().await.expect("engine should start");

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Ordering must stay the default so nonce-assigning executors keep working.
    assert_eq!(
        SERIAL_DONE.load(Ordering::SeqCst),
        2,
        "without opt-in concurrency only 2 of the 150ms actions fit in 400ms"
    );
}

#[tokio::test]
async fn executor_timeout_unblocks_later_actions() {
    // The first action hangs far longer than the test; the second is instant.
    let delays = vec![Duration::from_secs(30), Duration::from_millis(0)];

    let mut engine: Engine<Event, Action> =
        Engine::new().with_executor_timeout(Duration::from_millis(100));
    engine.add_collector(Box::new(OnceCollector));
    engine.add_strategy(Box::new(FanOutStrategy(delays)));
    engine.add_executor(Box::new(SleepExecutor(&TIMEOUT_DONE)));

    let _set = engine.run().await.expect("engine should start");

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Without the timeout the second action would be stuck behind the first for 30s.
    assert_eq!(
        TIMEOUT_DONE.load(Ordering::SeqCst),
        1,
        "the hanging action should be dropped, letting the next one through"
    );
}
