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
}

impl TelegramSubmitter {
    pub fn new_with_redirect(ot_token: String, chat_id: String, thread_id: Option<String>) -> Self {
        Self {
            sender: spawn_worker(TelegramMessageDispatcher::default()),
            redirect_to: Some((ot_token, chat_id, thread_id)),
        }
    }

    /// Send through the given dispatcher instead of a default one.
    pub fn with_dispatcher(dispatcher: TelegramMessageDispatcher) -> Self {
        Self {
            sender: spawn_worker(dispatcher),
            redirect_to: None,
        }
    }
}

impl Default for TelegramSubmitter {
    fn default() -> Self {
        Self {
            sender: spawn_worker(TelegramMessageDispatcher::default()),
            redirect_to: None,
        }
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
                tracing::error!("telegram queue is full, dropping message: {message:?}")
            }
            Err(TrySendError::Closed(message)) => {
                tracing::error!("telegram worker is gone, dropping message: {message:?}")
            }
        }
    }
}

/// Drain the queue on a dedicated thread with its own runtime.
///
/// A thread of its own keeps this working no matter how the caller is scheduled, including from
/// outside any runtime, and it is created once for the submitter rather than once per message.
fn spawn_worker(dispatcher: TelegramMessageDispatcher) -> Sender<Message> {
    let (sender, mut receiver) = mpsc::channel::<Message>(QUEUE_CAPACITY);

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
            TelegramMessageDispatcher::default().with_base_url(base_url),
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

        let submitter = TelegramSubmitter::with_dispatcher(
            TelegramMessageDispatcher::default().with_base_url(base_url),
        );

        // The worker is stuck on the first message, so everything past the queue capacity is
        // dropped with an error rather than accumulating forever.
        let start = Instant::now();
        for _ in 0..(QUEUE_CAPACITY * 2) {
            submitter.submit(message());
        }

        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
