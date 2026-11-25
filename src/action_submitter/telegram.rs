use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::sync::mpsc::{self, error::TrySendError, Sender};

use crate::executor::telegram_message::{Message, TelegramMessageDispatcher};
use crate::ActionSubmitter;

/// Bounded so that a Telegram outage cannot grow the queue without limit. Telegram rate limits
/// bots to roughly 30 messages per second, so anything beyond this is never going to be delivered
/// in a useful timeframe anyway.
const QUEUE_CAPACITY: usize = 1024;

pub struct TelegramSubmitter {
    sender: Sender<Message>,
    redirect_to: Option<(String, String, Option<String>)>,
    dropped: Arc<AtomicU64>,
}

impl TelegramSubmitter {
    pub fn new_with_redirect(
        bot_token: String,
        chat_id: String,
        thread_id: Option<String>,
    ) -> Self {
        Self::default().with_redirect(bot_token, chat_id, thread_id)
    }

    /// Send through the given dispatcher instead of a default one.
    pub fn with_dispatcher(dispatcher: TelegramMessageDispatcher) -> Self {
        Self::with_dispatcher_and_capacity(dispatcher, QUEUE_CAPACITY)
    }

    /// Send through the given dispatcher using a custom bounded queue capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn with_dispatcher_and_capacity(
        dispatcher: TelegramMessageDispatcher,
        capacity: usize,
    ) -> Self {
        assert!(capacity > 0, "telegram queue capacity must be at least 1");
        Self {
            sender: spawn_worker(dispatcher, capacity),
            redirect_to: None,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Redirect all submitted messages to a single bot token / chat / optional topic thread.
    pub fn with_redirect(
        mut self,
        bot_token: String,
        chat_id: String,
        thread_id: Option<String>,
    ) -> Self {
        self.redirect_to = Some((bot_token, chat_id, thread_id));
        self
    }

    /// Total number of messages dropped because the queue was full or the worker stopped.
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Default for TelegramSubmitter {
    fn default() -> Self {
        Self::with_dispatcher(TelegramMessageDispatcher::default())
    }
}

impl ActionSubmitter<Message> for TelegramSubmitter {
    fn submit(&self, action: Message) {
        let action = if let Some((bot_token, chat_id, thread_id)) = &self.redirect_to {
            Message {
                bot_token: bot_token.clone(),
                chat_id: chat_id.clone(),
                thread_id: thread_id.clone(),
                ..action
            }
        } else {
            action
        };

        // `submit` is sync and is called from inside the strategy's async task, so it must never
        // wait on the network: handing the message to the worker is the whole job.
        match self.sender.try_send(action) {
            Ok(()) => (),
            Err(TrySendError::Full(message)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::error!("telegram queue is full, dropping message: {message:?}")
            }
            Err(TrySendError::Closed(message)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::error!("telegram worker is gone, dropping message: {message:?}")
            }
        }
    }
}

/// Drain the queue on a dedicated thread with its own runtime.
///
/// A thread of its own keeps this working no matter how the caller is scheduled, including from
/// outside any runtime, and it is created once for the submitter rather than once per message.
fn spawn_worker(dispatcher: TelegramMessageDispatcher, capacity: usize) -> Sender<Message> {
    let (sender, mut receiver) = mpsc::channel::<Message>(capacity);

    let spawned = std::thread::Builder::new()
        .name("telegram-submitter".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(e) => {
                    tracing::error!("fail to start telegram submitter runtime: {e:#}");
                    return;
                }
            };

            // Messages are sent one at a time on purpose: Telegram rate limits aggressively and
            // replies with 429 when a bot bursts.
            runtime.block_on(async move {
                while let Some(message) = receiver.recv().await {
                    dispatcher.send_message(&message).await;
                }
            });
        });

    if let Err(e) = spawned {
        tracing::error!("fail to spawn telegram submitter thread: {e:#}");
    }

    sender
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{Duration, Instant};

    use crate::executor::telegram_message::MessageBuilder;

    fn message() -> Message {
        MessageBuilder::new()
            .bot_token("token")
            .chat_id("chat")
            .text("hi")
            .build()
    }

    /// Binds a port and never accepts: the kernel backlog completes the TCP handshake, so a
    /// request to it connects and then hangs until reqwest's timeout fires. Any implementation
    /// that waits for the response inside `submit` therefore stalls for tens of seconds.
    #[tokio::test]
    async fn submit_returns_without_waiting_for_the_network() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());

        let submitter = TelegramSubmitter::with_dispatcher(
            TelegramMessageDispatcher::default()
                .with_base_url(base_url)
                .with_timeout(Duration::from_millis(200)),
        );

        let start = Instant::now();
        for _ in 0..8 {
            submitter.submit(message());
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(100),
            "submit blocked the caller for {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn submit_drops_messages_instead_of_growing_without_bound() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());

        let capacity = 32;
        let submitter = TelegramSubmitter::with_dispatcher_and_capacity(
            TelegramMessageDispatcher::default()
                .with_base_url(base_url)
                .with_timeout(Duration::from_millis(200)),
            capacity,
        )
        .with_redirect("redirected".to_string(), "chat-2".to_string(), None);

        // The worker is stuck on the first message, so everything past the queue capacity is
        // dropped with an error rather than accumulating forever.
        let start = Instant::now();
        let total = capacity * 3;
        for _ in 0..total {
            submitter.submit(message());
        }

        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(
            submitter.dropped_count() >= capacity as u64,
            "expected saturated queue to drop excess messages, got {} drops",
            submitter.dropped_count()
        );
    }
}
