//! Tests for executor concurrency, timeouts, per-executor configs, shutdown, and reconnects.
//!
//! By default an executor processes actions strictly serially and waits forever, so one slow or
//! hanging action stalls every action behind it.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use burberry::{
    async_trait, map_executor, ActionSubmitter, Collector, CollectorStream, Engine, Executor,
    ExecutorConfig, Strategy,
};
use tokio::sync::watch;

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

/// Emits one event and then keeps the stream open until cancelled (simulates a live websocket).
struct LiveOnceCollector;

#[async_trait]
impl Collector<Event> for LiveOnceCollector {
    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Event>> {
        let stream = async_stream::stream! {
            yield Event;
            std::future::pending::<()>().await;
        };
        Ok(Box::pin(stream))
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
    let stats = engine.stats();

    let _set = engine.run().await.expect("engine should start");

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Without the timeout the second action would be stuck behind the first for 30s.
    assert_eq!(
        TIMEOUT_DONE.load(Ordering::SeqCst),
        1,
        "the hanging action should be dropped, letting the next one through"
    );
    assert!(
        stats.actions_timed_out.load(Ordering::Relaxed) >= 1,
        "timed-out actions should be reflected in EngineStats"
    );
}

#[tokio::test]
async fn per_executor_config_mixes_serial_and_concurrent_executors() {
    let serial_done = Arc::new(AtomicUsize::new(0));
    let concurrent_done = Arc::new(AtomicUsize::new(0));

    struct CountingSleepExecutor(Arc<AtomicUsize>);

    #[async_trait]
    impl Executor<Action> for CountingSleepExecutor {
        async fn execute(&self, action: Arc<Action>) -> eyre::Result<()> {
            tokio::time::sleep(action.0).await;
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let delays = vec![Duration::from_millis(150); 4];
    let mut engine: Engine<Event, Action> = Engine::new();
    engine.add_collector(Box::new(LiveOnceCollector));
    engine.add_strategy(Box::new(FanOutStrategy(delays)));
    engine.add_executor_with_config(
        Box::new(CountingSleepExecutor(Arc::clone(&serial_done))),
        ExecutorConfig::new().with_concurrency(1),
    );
    engine.add_executor_with_config(
        Box::new(CountingSleepExecutor(Arc::clone(&concurrent_done))),
        ExecutorConfig::new().with_concurrency(4),
    );

    let _set = engine.run().await.expect("engine should start");
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        serial_done.load(Ordering::SeqCst),
        2,
        "serial executor must stay strictly ordered"
    );
    assert_eq!(
        concurrent_done.load(Ordering::SeqCst),
        4,
        "concurrent executor must run all 4 actions in parallel"
    );
}

#[tokio::test]
async fn non_matching_actions_do_not_consume_concurrent_slots() {
    #[derive(Debug, Clone)]
    enum MixedAction {
        Mine(Duration),
        Other,
    }

    struct MixedStrategy;

    #[async_trait]
    impl Strategy<Event, MixedAction> for MixedStrategy {
        async fn process_event(
            &mut self,
            _event: Arc<Event>,
            submitter: Arc<dyn ActionSubmitter<MixedAction>>,
        ) {
            // Fill the single concurrent slot with a slow action, then flood unrelated variants,
            // then send a fast matching action.
            submitter.submit(MixedAction::Mine(Duration::from_millis(120)));
            for _ in 0..64 {
                submitter.submit(MixedAction::Other);
            }
            submitter.submit(MixedAction::Mine(Duration::from_millis(10)));
        }
    }

    struct InnerExecutor {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Executor<Duration> for InnerExecutor {
        async fn execute(&self, action: Arc<Duration>) -> eyre::Result<()> {
            tokio::time::sleep(*action).await;
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let mut engine: Engine<Event, MixedAction> = Engine::new().with_action_channel_capacity(128);
    engine.add_collector(Box::new(LiveOnceCollector));
    engine.add_strategy(Box::new(MixedStrategy));
    engine.add_executor_with_config(
        map_executor!(
            InnerExecutor {
                calls: Arc::clone(&calls),
            },
            MixedAction::Mine
        ),
        ExecutorConfig::new().with_concurrency(2),
    );

    let stats = engine.stats();
    let _set = engine.run().await.expect("engine should start");
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        stats.actions_executed.load(Ordering::Relaxed),
        2,
        "unrelated variants must be filtered via Executor::accepts before execution"
    );
}

#[tokio::test]
async fn dropping_join_set_aborts_in_flight_concurrent_actions() {
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct HangingExecutor {
        started: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Executor<Action> for HangingExecutor {
        async fn execute(&self, _action: Arc<Action>) -> eyre::Result<()> {
            let _flag = DropFlag(Arc::clone(&self.dropped));
            self.started.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));

    let mut engine: Engine<Event, Action> = Engine::new().with_executor_concurrency(4);
    engine.add_collector(Box::new(LiveOnceCollector));
    engine.add_strategy(Box::new(FanOutStrategy(vec![Duration::from_secs(60)])));
    engine.add_executor(Box::new(HangingExecutor {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    }));

    let set = engine.run().await.expect("engine should start");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(started.load(Ordering::SeqCst), "action should have started");

    // Dropping the JoinSet must cancel in-flight concurrent actions rather than leaking them.
    drop(set);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        dropped.load(Ordering::SeqCst),
        "in-flight concurrent action must be aborted when the engine JoinSet is dropped"
    );
}

#[tokio::test]
async fn graceful_shutdown_drains_in_flight_actions_and_exits_cleanly() {
    let completed = Arc::new(AtomicUsize::new(0));

    struct DrainingExecutor(Arc<AtomicUsize>);

    #[async_trait]
    impl Executor<Action> for DrainingExecutor {
        async fn execute(&self, action: Arc<Action>) -> eyre::Result<()> {
            tokio::time::sleep(action.0).await;
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut engine: Engine<Event, Action> = Engine::new()
        .with_executor_concurrency(4)
        .with_shutdown(shutdown_rx);
    engine.add_collector(Box::new(LiveOnceCollector));
    engine.add_strategy(Box::new(FanOutStrategy(vec![
        Duration::from_millis(120),
        Duration::from_millis(120),
    ])));
    engine.add_executor(Box::new(DrainingExecutor(Arc::clone(&completed))));

    let stats = engine.stats();
    let handle = tokio::spawn(async move { engine.run_and_join().await });

    // Give the event time to reach the strategy and launch both in-flight actions.
    tokio::time::sleep(Duration::from_millis(40)).await;
    let _ = shutdown_tx.send(true);

    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("engine should shut down promptly")
        .expect("engine task should not panic")
        .expect("engine should exit without error");

    assert_eq!(
        completed.load(Ordering::SeqCst),
        2,
        "graceful shutdown must wait for in-flight actions to finish"
    );
    assert_eq!(stats.events_published.load(Ordering::Relaxed), 1);
    assert_eq!(stats.actions_submitted.load(Ordering::Relaxed), 2);
    assert_eq!(stats.actions_executed.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn collector_stops_when_all_strategies_terminate() {
    struct PanickingStrategy;

    #[async_trait]
    impl Strategy<Event, Action> for PanickingStrategy {
        async fn process_event(
            &mut self,
            _event: Arc<Event>,
            _submitter: Arc<dyn ActionSubmitter<Action>>,
        ) {
            panic!("strategy crashed");
        }
    }

    struct RepeatingCollector;

    #[async_trait]
    impl Collector<Event> for RepeatingCollector {
        async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Event>> {
            let stream = async_stream::stream! {
                loop {
                    yield Event;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            Ok(Box::pin(stream))
        }
    }

    let mut engine: Engine<Event, Action> =
        Engine::new().with_collector_retry_interval(Duration::from_millis(10));
    engine.add_collector(Box::new(RepeatingCollector));
    engine.add_strategy(Box::new(PanickingStrategy));
    engine.add_executor(Box::new(SleepExecutor(&IGNORED_STOP)));

    // Once the sole strategy panics and drops its receiver, the collector and executor must exit
    // cleanly rather than spinning forever.
    tokio::time::timeout(Duration::from_secs(2), engine.run_and_join())
        .await
        .expect("engine should terminate once all strategies are gone")
        .expect("run_and_join returns Ok after logging task panics");
}

static IGNORED_STOP: AtomicUsize = AtomicUsize::new(0);

mod reconnect {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static DELIVERED: AtomicUsize = AtomicUsize::new(0);

    /// Fails to subscribe on the first attempt, then yields one event per attempt and ends the
    /// stream -- the shape of a websocket that drops and has to be re-subscribed.
    struct FlakyCollector;

    #[async_trait]
    impl Collector<Event> for FlakyCollector {
        async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Event>> {
            if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
                eyre::bail!("connection refused");
            }

            Ok(Box::pin(futures::stream::once(async { Event })))
        }
    }

    struct CountingStrategy;

    #[async_trait]
    impl Strategy<Event, Action> for CountingStrategy {
        async fn process_event(
            &mut self,
            _event: Arc<Event>,
            _submitter: Arc<dyn ActionSubmitter<Action>>,
        ) {
            DELIVERED.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn collectors_reconnect_instead_of_panicking() {
        let mut engine: Engine<Event, Action> =
            Engine::new().with_collector_retry_interval(Duration::from_millis(20));
        engine.add_collector(Box::new(FlakyCollector));
        engine.add_strategy(Box::new(CountingStrategy));
        engine.add_executor(Box::new(SleepExecutor(&IGNORED)));

        let _set = engine.run().await.expect("engine should start");

        tokio::time::sleep(Duration::from_millis(300)).await;

        // The old code unwrapped the first error and killed the task for good.
        assert!(
            ATTEMPTS.load(Ordering::SeqCst) > 2,
            "collector should keep re-subscribing, got {} attempts",
            ATTEMPTS.load(Ordering::SeqCst)
        );
        assert!(
            DELIVERED.load(Ordering::SeqCst) > 1,
            "events should keep flowing after a reconnect, got {}",
            DELIVERED.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn consecutive_failures_back_off_exponentially() {
        struct AlwaysFailCollector(Arc<AtomicUsize>);

        #[async_trait]
        impl Collector<Event> for AlwaysFailCollector {
            async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Event>> {
                self.0.fetch_add(1, Ordering::SeqCst);
                eyre::bail!("rpc down")
            }
        }

        let attempts = Arc::new(AtomicUsize::new(0));
        let mut engine: Engine<Event, Action> = Engine::new()
            .with_collector_retry_interval(Duration::from_millis(40))
            .with_collector_max_retry_interval(Duration::from_millis(400));
        engine.add_collector(Box::new(AlwaysFailCollector(Arc::clone(&attempts))));
        engine.add_strategy(Box::new(CountingStrategy));
        engine.add_executor(Box::new(SleepExecutor(&IGNORED)));

        let start = Instant::now();
        let set = engine.run().await.expect("engine should start");
        tokio::time::sleep(Duration::from_millis(320)).await;
        drop(set);

        let count = attempts.load(Ordering::SeqCst);
        // Fixed 40ms retries would do ~8 attempts in 320ms; exponential backoff (40 + 80 + 160 =
        // 280ms + jitter) keeps attempts to at most 5.
        assert!(
            (2..=5).contains(&count),
            "expected exponential backoff (2..=5 attempts in {:?}), got {count}",
            start.elapsed()
        );
    }

    static IGNORED: AtomicUsize = AtomicUsize::new(0);
}
