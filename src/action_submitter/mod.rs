mod map;
mod printer;

#[cfg(feature = "telegram")]
mod telegram;

use std::fmt::Debug;
use std::sync::{atomic::Ordering, Arc};

pub use map::ActionSubmitterMap;
pub use printer::ActionPrinter;

#[cfg(feature = "telegram")]
pub use telegram::TelegramSubmitter;

use tokio::sync::broadcast::Sender;

use crate::{engine::EngineStats, ActionSubmitter};

#[derive(Clone)]
pub struct ActionChannelSubmitter<A> {
    sender: Sender<Arc<A>>,
    stats: Option<Arc<EngineStats>>,
}

impl<A> ActionChannelSubmitter<A> {
    pub fn new(sender: Sender<Arc<A>>) -> Self {
        Self {
            sender,
            stats: None,
        }
    }

    pub(crate) fn with_stats(sender: Sender<Arc<A>>, stats: Arc<EngineStats>) -> Self {
        Self {
            sender,
            stats: Some(stats),
        }
    }
}

impl<A> ActionSubmitter<A> for ActionChannelSubmitter<A>
where
    A: Send + Sync + Debug + 'static,
{
    fn submit(&self, action: A) {
        match self.sender.send(Arc::new(action)) {
            Ok(_) => {
                if let Some(stats) = &self.stats {
                    stats.actions_submitted.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => tracing::error!("error submitting action: {:?}", e),
        }
    }
}
