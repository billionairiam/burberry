use std::sync::Arc;
use std::time::Duration;

use alloy::{providers::Provider, rpc::types::eth::Block};
use async_trait::async_trait;
use futures::StreamExt;
use tracing::{debug, error, warn};

use crate::types::{Collector, CollectorStream};

const DEFAULT_MAX_RETRIES: usize = 20;

pub struct FullBlockCollector {
    provider: Arc<dyn Provider>,
    retry_interval: Duration,
    max_retries: usize,
}

impl FullBlockCollector {
    pub fn new(provider: Arc<dyn Provider>) -> Self {
        Self::new_with_config(provider, Duration::from_millis(50))
    }

    /// Create a new `FullBlockCollector` with a custom retry interval. A retry will happen when the client returns
    /// "header not found"
    pub fn new_with_config(provider: Arc<dyn Provider>, retry_interval: Duration) -> Self {
        Self {
            provider,
            retry_interval,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }

    /// How many times to retry fetching a block body before giving up on that block.
    ///
    /// Retries block the whole stream: while we wait for one block body, new headers pile up in
    /// the subscription buffer and are eventually dropped. Bounding the retries bounds that
    /// stall to roughly `max_retries * retry_interval` (1s by default).
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Fetch the block body, retrying transient misses and RPC errors.
    ///
    /// Returns `None` once the retries are exhausted, in which case the block is skipped rather
    /// than stalling the stream forever.
    async fn fetch_full_block(&self, block_number: u64) -> Option<Block> {
        for attempt in 0..=self.max_retries {
            let last_attempt = attempt == self.max_retries;

            match self
                .provider
                .get_block_by_number(block_number.into())
                .full()
                .await
            {
                Ok(Some(block)) => return Some(block),
                Ok(None) => {
                    if last_attempt {
                        error!(
                            block = block_number,
                            "block still not found after {} attempts, skipping it",
                            attempt + 1
                        );
                        return None;
                    }

                    // The node routinely announces a header slightly before it can serve the
                    // body, so an early miss is expected rather than noteworthy.
                    debug!(block = block_number, "block not found yet, retrying");
                }
                Err(e) => {
                    if last_attempt {
                        error!(
                            block = block_number,
                            "fail to get full block after {} attempts, skipping it: {e:#}",
                            attempt + 1
                        );
                        return None;
                    }

                    warn!(
                        block = block_number,
                        "fail to get full block, retrying: {e:#}"
                    );
                }
            }

            tokio::time::sleep(self.retry_interval).await;
        }

        None
    }
}

#[async_trait]
impl Collector<Block> for FullBlockCollector {
    fn name(&self) -> &str {
        "FullBlockCollector"
    }

    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Block>> {
        // `eth_subscribe("newHeads")` only carries headers, so a second request per block is
        // unavoidable here.
        let mut stream = self.provider.subscribe_blocks().await?.into_stream();

        let stream = async_stream::stream! {
            while let Some(header) = stream.next().await {
                if let Some(block) = self.fetch_full_block(header.number).await {
                    yield block;
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy::providers::ProviderBuilder;
    use alloy::transports::mock::Asserter;

    fn block_at(number: u64) -> Block {
        let mut block: Block = Block::default();
        block.header.inner.number = number;
        block
    }

    fn collector(asserter: &Asserter, max_retries: usize) -> FullBlockCollector {
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        FullBlockCollector::new_with_config(Arc::new(provider), Duration::from_millis(1))
            .with_max_retries(max_retries)
    }

    #[tokio::test]
    async fn retries_until_the_body_is_available() {
        let asserter = Asserter::new();
        asserter.push_success(&Option::<Block>::None);
        asserter.push_success(&Option::<Block>::None);
        asserter.push_success(&block_at(9));

        let collector = collector(&asserter, 5);

        assert_eq!(
            collector.fetch_full_block(9).await.unwrap().header.number,
            9
        );
        assert!(asserter.read_q().is_empty(), "should stop once it succeeds");
    }

    #[tokio::test]
    async fn retries_rpc_errors_too() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("connection reset");
        asserter.push_success(&block_at(9));

        let collector = collector(&asserter, 5);

        // The previous implementation gave up on the first error and dropped the block.
        assert_eq!(
            collector.fetch_full_block(9).await.unwrap().header.number,
            9
        );
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn gives_up_instead_of_stalling_the_stream_forever() {
        let asserter = Asserter::new();
        for _ in 0..4 {
            asserter.push_success(&Option::<Block>::None);
        }

        let collector = collector(&asserter, 3);

        // `max_retries = 3` means 4 attempts total, then the block is skipped. An unbounded retry
        // loop would keep going past the mock queue, so the timeout makes that a failure.
        let result = tokio::time::timeout(Duration::from_secs(2), collector.fetch_full_block(9))
            .await
            .expect("fetch should give up rather than retry forever");

        assert!(result.is_none());
        assert!(
            asserter.read_q().is_empty(),
            "expected exactly max_retries + 1 attempts"
        );
    }
}
