use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    action_submitter::ActionChannelSubmitter,
    types::{Collector, Executor, Strategy},
};
use eyre::Context;
use futures::StreamExt;
use tokio::{
    sync::{
        broadcast::{self, error::RecvError, Sender},
        watch,
    },
    task::JoinSet,
};
use tracing::{debug, error, info, warn};

const DEFAULT_COLLECTOR_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_COLLECTOR_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(30);

/// Per-executor runtime configuration.
///
/// Allows mixing strictly serial executors (such as nonce-assigning transaction senders) with
/// concurrent or differently timed-out executors (such as bundle relayers or alert dispatchers)
/// inside the same [`Engine`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorConfig {
    pub concurrency: usize,
    pub timeout: Option<Duration>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            concurrency: 1,
            timeout: None,
        }
    }
}

impl ExecutorConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum number of actions this executor may run concurrently.
    ///
    /// # Panics
    ///
    /// Panics if `concurrency` is 0.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "executor concurrency must be at least 1");
        self.concurrency = concurrency;
        self
    }

    /// Abort an action if this executor does not finish it within `timeout`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Lock-free runtime counters exposed by [`Engine::stats`].
#[derive(Debug, Default)]
pub struct EngineStats {
    pub events_published: AtomicU64,
    pub events_lagged: AtomicU64,
    pub actions_submitted: AtomicU64,
    pub actions_lagged: AtomicU64,
    pub actions_executed: AtomicU64,
    pub actions_failed: AtomicU64,
    pub actions_timed_out: AtomicU64,
    pub collector_reconnects: AtomicU64,
}

struct RegisteredExecutor<A> {
    executor: Box<dyn Executor<A>>,
    config: Option<ExecutorConfig>,
}

pub struct Engine<E, A> {
    collectors: Vec<Box<dyn Collector<E>>>,
    strategies: Vec<Box<dyn Strategy<E, A>>>,
    executors: Vec<RegisteredExecutor<A>>,

    event_channel_capacity: usize,
    action_channel_capacity: usize,

    executor_concurrency: usize,
    executor_timeout: Option<Duration>,
    collector_retry_interval: Duration,
    collector_max_retry_interval: Duration,

    shutdown: Option<watch::Receiver<bool>>,
    stats: Arc<EngineStats>,
}

impl<E, A> Engine<E, A> {
    pub fn new() -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            executors: vec![],
            event_channel_capacity: 512,
            action_channel_capacity: 512,
            executor_concurrency: 1,
            executor_timeout: None,
            collector_retry_interval: DEFAULT_COLLECTOR_RETRY_INTERVAL,
            collector_max_retry_interval: DEFAULT_COLLECTOR_MAX_RETRY_INTERVAL,
            shutdown: None,
            stats: Arc::new(EngineStats::default()),
        }
    }

    /// Capacity of the event broadcast channel.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "event channel capacity must be at least 1");
        self.event_channel_capacity = capacity;
        self
    }

    /// Capacity of the action broadcast channel.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn with_action_channel_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "action channel capacity must be at least 1");
        self.action_channel_capacity = capacity;
        self
    }

    /// Default maximum number of actions a single executor may process at the same time.
    ///
    /// Defaults to `1`, meaning each executor handles actions strictly one after another. With the
    /// default, a slow action (a hanging HTTP request, say) blocks every later action for that
    /// executor, which in turn makes the action channel lag and silently drop actions.
    ///
    /// Raising this spawns matching actions onto a bounded per-executor task set. Only do this for
    /// executors that are safe to run out of order — [`crate::executor::transaction::TransactionSender`]
    /// is *not*, since it assigns nonces sequentially. Use [`Self::add_executor_with_config`] to
    /// configure concurrency per executor when mixing serial and concurrent executors.
    ///
    /// # Panics
    ///
    /// Panics if `concurrency` is 0.
    pub fn with_executor_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "executor concurrency must be at least 1");
        self.executor_concurrency = concurrency;
        self
    }

    /// Default timeout applied to each executor action unless overridden via
    /// [`Self::add_executor_with_config`].
    ///
    /// Defaults to no timeout, in which case a request that never completes wedges the executor
    /// forever. The timed-out action is logged and dropped.
    pub fn with_executor_timeout(mut self, timeout: Duration) -> Self {
        self.executor_timeout = Some(timeout);
        self
    }

    /// Initial delay before reconnecting a collector whose stream failed or ended.
    ///
    /// Consecutive failures back off exponentially with jitter up to
    /// [`Self::with_collector_max_retry_interval`], resetting once a stream delivers an event.
    /// Defaults to one second.
    pub fn with_collector_retry_interval(mut self, interval: Duration) -> Self {
        self.collector_retry_interval = interval;
        if self.collector_max_retry_interval < interval {
            self.collector_max_retry_interval = interval;
        }
        self
    }

    /// Upper bound for exponential backoff when a collector repeatedly fails to reconnect.
    ///
    /// Defaults to 30 seconds.
    pub fn with_collector_max_retry_interval(mut self, max_interval: Duration) -> Self {
        self.collector_max_retry_interval = max_interval.max(self.collector_retry_interval);
        self
    }

    /// Attach a cooperative shutdown signal.
    ///
    /// When the watch channel flips to `true` (or its sender is dropped), collectors stop
    /// producing events, strategies drain remaining buffered events and exit, and executors drain
    /// buffered and in-flight actions before [`Self::run_and_join`] returns cleanly.
    pub fn with_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Returns a handle to the engine's atomic runtime counters.
    pub fn stats(&self) -> Arc<EngineStats> {
        Arc::clone(&self.stats)
    }

    pub fn collector_count(&self) -> usize {
        self.collectors.len()
    }

    pub fn strategy_count(&self) -> usize {
        self.strategies.len()
    }

    pub fn executor_count(&self) -> usize {
        self.executors.len()
    }
}

impl<E, A> Default for Engine<E, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, A> Engine<E, A>
where
    E: Send + Sync + 'static,
    A: Send + Sync + Debug + 'static,
{
    pub fn add_collector(&mut self, collector: Box<dyn Collector<E>>) {
        self.collectors.push(collector);
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy<E, A>>) {
        self.strategies.push(strategy);
    }

    pub fn add_executor(&mut self, executor: Box<dyn Executor<A>>) {
        self.executors.push(RegisteredExecutor {
            executor,
            config: None,
        });
    }

    /// Register an executor with its own concurrency and timeout settings, overriding the engine
    /// defaults.
    pub fn add_executor_with_config(
        &mut self,
        executor: Box<dyn Executor<A>>,
        config: ExecutorConfig,
    ) {
        self.executors.push(RegisteredExecutor {
            executor,
            config: Some(config),
        });
    }

    pub async fn run_and_join(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut js = self.run().await?;

        while let Some(event) = js.join_next().await {
            if let Err(err) = event {
                error!("task terminated unexpectedly: {err:#}");
            }
        }

        Ok(())
    }

    pub async fn run(self) -> Result<JoinSet<()>, Box<dyn std::error::Error + Send + Sync>> {
        // Events and actions are broadcast as `Arc`, so each additional subscriber only pays for
        // a reference count bump rather than a deep clone of the payload.
        let (event_sender, _): (Sender<Arc<E>>, _) =
            broadcast::channel(self.event_channel_capacity);
        let (action_sender, _): (Sender<Arc<A>>, _) =
            broadcast::channel(self.action_channel_capacity);

        let mut set = JoinSet::new();

        if self.executors.is_empty() {
            return Err("no executors".into());
        }

        if self.collectors.is_empty() {
            return Err("no collectors".into());
        }

        if self.strategies.is_empty() {
            return Err("no strategies".into());
        }

        let default_executor_config = ExecutorConfig {
            concurrency: self.executor_concurrency,
            timeout: self.executor_timeout,
        };

        // Spawn executors in separate tasks.
        for registered in self.executors {
            let mut receiver = action_sender.subscribe();
            let executor: Arc<dyn Executor<A>> = Arc::from(registered.executor);
            let config = registered.config.unwrap_or(default_executor_config);
            let stats = Arc::clone(&self.stats);
            let shutdown = self.shutdown.clone();

            set.spawn(async move {
                debug!(
                    name = executor.name(),
                    concurrency = config.concurrency,
                    timeout = ?config.timeout,
                    "starting executor..."
                );

                if config.concurrency == 1 {
                    loop {
                        match receiver.recv().await {
                            Ok(action) => {
                                if !executor.accepts(&action) {
                                    continue;
                                }
                                execute_action(executor.as_ref(), action, config.timeout, &stats)
                                    .await;
                            }
                            Err(RecvError::Closed) => {
                                if is_shutdown(&shutdown) {
                                    info!(name = executor.name(), "executor shutting down");
                                } else {
                                    error!(name = executor.name(), "action channel closed!");
                                }
                                break;
                            }
                            Err(RecvError::Lagged(num)) => {
                                stats.actions_lagged.fetch_add(num, Ordering::Relaxed);
                                error!(name = executor.name(), "action channel lagged by {num}");
                            }
                        }
                    }
                    return;
                }

                // Concurrent mode: manage all in-flight actions inside a local `JoinSet` so that
                // dropping/aborting the engine's `JoinSet` aborts every in-flight action too, and
                // graceful channel closure drains every in-flight action before exiting.
                let mut in_flight: JoinSet<()> = JoinSet::new();

                loop {
                    let recv_result = loop {
                        tokio::select! {
                            Some(res) = in_flight.join_next(), if !in_flight.is_empty() => {
                                if let Err(err) = res {
                                    error!(
                                        name = executor.name(),
                                        "executor action task panicked: {err:#}"
                                    );
                                }
                            }
                            recv = receiver.recv() => break recv,
                        }
                    };

                    match recv_result {
                        Ok(action) => {
                            // Skip non-matching actions before enforcing the concurrency cap so an
                            // unrelated variant never waits for a slot or spawns a no-op task.
                            if !executor.accepts(&action) {
                                continue;
                            }

                            while in_flight.len() >= config.concurrency {
                                if let Some(Err(err)) = in_flight.join_next().await {
                                    error!(
                                        name = executor.name(),
                                        "executor action task panicked: {err:#}"
                                    );
                                }
                            }

                            let executor = Arc::clone(&executor);
                            let stats = Arc::clone(&stats);
                            let timeout = config.timeout;

                            in_flight.spawn(async move {
                                execute_action(executor.as_ref(), action, timeout, &stats).await;
                            });
                        }
                        Err(RecvError::Closed) => {
                            if is_shutdown(&shutdown) {
                                info!(
                                    name = executor.name(),
                                    in_flight = in_flight.len(),
                                    "executor draining in-flight actions before shutdown"
                                );
                            } else {
                                error!(name = executor.name(), "action channel closed!");
                            }
                            break;
                        }
                        Err(RecvError::Lagged(num)) => {
                            stats.actions_lagged.fetch_add(num, Ordering::Relaxed);
                            error!(name = executor.name(), "action channel lagged by {num}");
                        }
                    }
                }

                while let Some(res) = in_flight.join_next().await {
                    if let Err(err) = res {
                        error!(
                            name = executor.name(),
                            "executor action task panicked: {err:#}"
                        );
                    }
                }
            });
        }

        // Spawn strategies in separate tasks.
        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let action_sender = action_sender.clone();
            let stats = Arc::clone(&self.stats);
            let shutdown = self.shutdown.clone();

            let action_submitter = Arc::new(ActionChannelSubmitter::with_stats(
                action_sender,
                Arc::clone(&stats),
            ));

            strategy
                .sync_state(action_submitter.clone())
                .await
                .wrap_err("fail to sync state")?;

            set.spawn(async move {
                debug!(name = strategy.name(), "starting strategy...");

                loop {
                    match event_receiver.recv().await {
                        Ok(event) => {
                            strategy
                                .process_event(event, action_submitter.clone())
                                .await
                        }
                        Err(RecvError::Closed) => {
                            if is_shutdown(&shutdown) {
                                info!(name = strategy.name(), "strategy shutting down");
                            } else {
                                error!(name = strategy.name(), "event channel closed!");
                            }
                            break;
                        }
                        Err(RecvError::Lagged(num)) => {
                            stats.events_lagged.fetch_add(num, Ordering::Relaxed);
                            warn!(name = strategy.name(), "event channel lagged by {num}");
                        }
                    }
                }
            });
        }

        // Spawn collectors in separate tasks.
        for (idx, collector) in self.collectors.into_iter().enumerate() {
            let event_sender = event_sender.clone();
            let retry_interval = self.collector_retry_interval;
            let max_retry_interval = self.collector_max_retry_interval;
            let stats = Arc::clone(&self.stats);
            let mut shutdown = self.shutdown.clone();
            let salt = (idx as u64).wrapping_add(1);

            set.spawn(async move {
                debug!(name = collector.name(), "starting collector...");

                let mut consecutive_failures: u32 = 0;

                // Websocket subscriptions drop routinely, and an RPC endpoint is often not
                // reachable at the exact moment the bot starts, so neither a failure to
                // subscribe nor an ended stream may take the collector down for good.
                loop {
                    if is_shutdown(&shutdown) {
                        info!(name = collector.name(), "collector shutting down");
                        break;
                    }

                    if event_sender.receiver_count() == 0 {
                        warn!(
                            name = collector.name(),
                            "all event receivers dropped, stopping collector"
                        );
                        break;
                    }

                    let stream_res = tokio::select! {
                        biased;
                        _ = wait_for_shutdown(&mut shutdown) => {
                            info!(name = collector.name(), "collector shutting down");
                            break;
                        }
                        res = collector.get_event_stream() => res,
                    };

                    match stream_res {
                        Ok(mut event_stream) => {
                            let mut yielded_any = false;
                            let mut stop = false;

                            loop {
                                let next_event = tokio::select! {
                                    biased;
                                    _ = wait_for_shutdown(&mut shutdown) => {
                                        info!(name = collector.name(), "collector shutting down");
                                        stop = true;
                                        break;
                                    }
                                    event = event_stream.next() => event,
                                };

                                let Some(event) = next_event else {
                                    break;
                                };

                                yielded_any = true;
                                consecutive_failures = 0;

                                if event_sender.send(Arc::new(event)).is_err() {
                                    warn!(
                                        name = collector.name(),
                                        "all event receivers dropped, stopping collector"
                                    );
                                    stop = true;
                                    break;
                                }

                                stats.events_published.fetch_add(1, Ordering::Relaxed);
                            }

                            if stop {
                                break;
                            }

                            if yielded_any {
                                consecutive_failures = 1;
                            } else {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                            }

                            let delay = backoff_with_jitter(
                                retry_interval,
                                max_retry_interval,
                                consecutive_failures,
                                salt,
                            );

                            error!(
                                name = collector.name(),
                                "event stream ended, reconnecting in {delay:?}"
                            );

                            stats.collector_reconnects.fetch_add(1, Ordering::Relaxed);

                            tokio::select! {
                                biased;
                                _ = wait_for_shutdown(&mut shutdown) => {
                                    info!(name = collector.name(), "collector shutting down");
                                    break;
                                }
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }
                        Err(e) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            let delay = backoff_with_jitter(
                                retry_interval,
                                max_retry_interval,
                                consecutive_failures,
                                salt,
                            );

                            error!(
                                name = collector.name(),
                                "fail to get event stream, retrying in {delay:?}: {e:#}"
                            );

                            stats.collector_reconnects.fetch_add(1, Ordering::Relaxed);

                            tokio::select! {
                                biased;
                                _ = wait_for_shutdown(&mut shutdown) => {
                                    info!(name = collector.name(), "collector shutting down");
                                    break;
                                }
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }
                    }
                }
            });
        }

        Ok(set)
    }
}

fn is_shutdown(shutdown: &Option<watch::Receiver<bool>>) -> bool {
    match shutdown {
        Some(rx) => *rx.borrow() || rx.has_changed().is_err(),
        None => false,
    }
}

async fn wait_for_shutdown(shutdown: &mut Option<watch::Receiver<bool>>) {
    match shutdown {
        Some(rx) => {
            if *rx.borrow() {
                return;
            }
            loop {
                if rx.changed().await.is_err() || *rx.borrow_and_update() {
                    return;
                }
            }
        }
        None => std::future::pending::<()>().await,
    }
}

fn backoff_with_jitter(
    base: Duration,
    max: Duration,
    consecutive_failures: u32,
    salt: u64,
) -> Duration {
    let effective_max = max.max(base);
    let shift = consecutive_failures.saturating_sub(1).min(16);
    let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
    let capped = base.saturating_mul(factor).min(effective_max);

    let nanos = capped.as_nanos();
    if nanos == 0 {
        return Duration::ZERO;
    }

    // Add up to 25% jitter (bounded by `effective_max`) so reconnects do not synchronize into a
    // thundering herd against an RPC node.
    let jitter_range = (nanos / 4).max(1);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut x =
        now_nanos ^ salt.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ u64::from(consecutive_failures);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;

    let jitter = u128::from(x) % jitter_range;
    let total = (nanos + jitter).min(effective_max.as_nanos());
    Duration::from_nanos(total.min(u128::from(u64::MAX)) as u64)
}

async fn execute_action<A>(
    executor: &dyn Executor<A>,
    action: Arc<A>,
    timeout: Option<Duration>,
    stats: &EngineStats,
) where
    A: Send + Sync + 'static,
{
    let result = match timeout {
        Some(timeout) => match tokio::time::timeout(timeout, executor.execute(action)).await {
            Ok(result) => result,
            Err(_) => {
                stats.actions_timed_out.fetch_add(1, Ordering::Relaxed);
                error!(
                    name = executor.name(),
                    "action timed out after {timeout:?}, dropping it"
                );
                return;
            }
        },
        None => executor.execute(action).await,
    };

    match result {
        Ok(()) => {
            stats.actions_executed.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            stats.actions_failed.fetch_add(1, Ordering::Relaxed);
            error!(name = executor.name(), "error executing action: {e:#}");
        }
    }
}
