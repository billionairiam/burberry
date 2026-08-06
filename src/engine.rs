use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::{
    action_submitter::ActionChannelSubmitter,
    types::{Collector, Executor, Strategy},
};
use eyre::Context;
use futures::StreamExt;
use tokio::{
    sync::broadcast::{self, error::RecvError, Sender},
    sync::Semaphore,
    task::JoinSet,
};
use tracing::{debug, error, warn};

pub struct Engine<E, A> {
    collectors: Vec<Box<dyn Collector<E>>>,
    strategies: Vec<Box<dyn Strategy<E, A>>>,
    executors: Vec<Box<dyn Executor<A>>>,

    event_channel_capacity: usize,
    action_channel_capacity: usize,

    executor_concurrency: usize,
    executor_timeout: Option<Duration>,
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
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    pub fn with_action_channel_capacity(mut self, capacity: usize) -> Self {
        self.action_channel_capacity = capacity;
        self
    }

    /// Maximum number of actions a single executor may process at the same time.
    ///
    /// Defaults to `1`, meaning each executor handles actions strictly one after another. With the
    /// default, a slow action (a hanging HTTP request, say) blocks every later action for that
    /// executor, which in turn makes the action channel lag and silently drop actions.
    ///
    /// Raising this spawns each action on its own task, bounded by a semaphore. Only do this for
    /// executors that are safe to run out of order — [`crate::executor::transaction::TransactionSender`]
    /// is *not*, since it assigns nonces sequentially.
    ///
    /// # Panics
    ///
    /// Panics if `concurrency` is 0.
    pub fn with_executor_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "executor concurrency must be at least 1");
        self.executor_concurrency = concurrency;
        self
    }

    /// Abort an action if its executor has not finished within `timeout`.
    ///
    /// Defaults to no timeout, in which case a request that never completes wedges the executor
    /// forever. The timed-out action is logged and dropped.
    pub fn with_executor_timeout(mut self, timeout: Duration) -> Self {
        self.executor_timeout = Some(timeout);
        self
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
        self.executors.push(executor);
    }

    pub async fn run_and_join(self) -> Result<(), Box<dyn std::error::Error>> {
        let mut js = self.run().await?;

        while let Some(event) = js.join_next().await {
            if let Err(err) = event {
                error!("task terminated unexpectedly: {err:#}");
            }
        }

        Ok(())
    }

    pub async fn run(self) -> Result<JoinSet<()>, Box<dyn std::error::Error>> {
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

        // Spawn executors in separate threads.
        for executor in self.executors {
            let mut receiver = action_sender.subscribe();
            let executor: Arc<dyn Executor<A>> = Arc::from(executor);
            let timeout = self.executor_timeout;
            let semaphore = (self.executor_concurrency > 1)
                .then(|| Arc::new(Semaphore::new(self.executor_concurrency)));

            set.spawn(async move {
                debug!(name = executor.name(), "starting executor... ");

                loop {
                    match receiver.recv().await {
                        Ok(action) => match &semaphore {
                            // Serial: a slow action delays the next one, but ordering is
                            // preserved for executors that depend on it (nonce assignment).
                            None => execute_action(executor.as_ref(), action, timeout).await,
                            Some(semaphore) => {
                                let permit = match Arc::clone(semaphore).acquire_owned().await {
                                    Ok(permit) => permit,
                                    Err(_) => break,
                                };
                                let executor = Arc::clone(&executor);

                                tokio::spawn(async move {
                                    let _permit = permit;
                                    execute_action(executor.as_ref(), action, timeout).await;
                                });
                            }
                        },
                        Err(RecvError::Closed) => {
                            error!(name = executor.name(), "action channel closed!");
                            break;
                        }
                        Err(RecvError::Lagged(num)) => {
                            warn!(name = executor.name(), "action channel lagged by {num}")
                        }
                    }
                }
            });
        }

        // Spawn strategies in separate threads.
        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let action_sender = action_sender.clone();

            let action_submitter = Arc::new(ActionChannelSubmitter::new(action_sender));

            strategy
                .sync_state(action_submitter.clone())
                .await
                .wrap_err("fail to sync state")?;

            set.spawn(async move {
                debug!(name = strategy.name(), "starting strategy... ");

                loop {
                    match event_receiver.recv().await {
                        Ok(event) => {
                            strategy
                                .process_event(event, action_submitter.clone())
                                .await
                        }
                        Err(RecvError::Closed) => {
                            error!(name = strategy.name(), "event channel closed!");
                            break;
                        }
                        Err(RecvError::Lagged(num)) => {
                            warn!(name = strategy.name(), "event channel lagged by {num}")
                        }
                    }
                }
            });
        }

        // Spawn collectors in separate threads.
        for collector in self.collectors {
            let event_sender = event_sender.clone();

            set.spawn(async move {
                debug!(name = collector.name(), "starting collector... ");
                let mut event_stream = collector.get_event_stream().await.unwrap();

                while let Some(event) = event_stream.next().await {
                    if let Err(e) = event_sender.send(Arc::new(event)) {
                        error!(name = collector.name(), "error sending event: {e:#}");
                    }
                }

                error!(name = collector.name(), "event stream ended!");
            });
        }

        Ok(set)
    }
}

async fn execute_action<A>(executor: &dyn Executor<A>, action: Arc<A>, timeout: Option<Duration>)
where
    A: Send + Sync + 'static,
{
    let result = match timeout {
        Some(timeout) => match tokio::time::timeout(timeout, executor.execute(action)).await {
            Ok(result) => result,
            Err(_) => {
                error!(
                    name = executor.name(),
                    "action timed out after {timeout:?}, dropping it"
                );
                return;
            }
        },
        None => executor.execute(action).await,
    };

    if let Err(e) = result {
        error!(name = executor.name(), "error executing action: {e:#}");
    }
}
