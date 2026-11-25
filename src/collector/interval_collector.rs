use std::time::{Duration, Instant};

use crate::types::{Collector, CollectorStream};
use async_trait::async_trait;
use tokio::time::MissedTickBehavior;

pub struct IntervalCollector {
    interval: Duration,
}

impl IntervalCollector {
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }
}

#[async_trait]
impl Collector<Instant> for IntervalCollector {
    fn name(&self) -> &str {
        "IntervalCollector"
    }

    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Instant>> {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Consume the immediate first tick so the first event fires after `self.interval`,
        // matching the previous sleep-then-yield timing without cumulative drift.
        ticker.tick().await;

        let stream = async_stream::stream! {
            loop {
                ticker.tick().await;
                yield Instant::now();
            }
        };

        Ok(Box::pin(stream))
    }
}
