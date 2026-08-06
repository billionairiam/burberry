use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::Duration;

use alloy::{providers::Provider, rpc::types::eth::Block};
use async_trait::async_trait;
use tracing::{error, warn};

use crate::types::{Collector, CollectorStream};

pub struct PollFullBlockCollector {
    provider: Arc<dyn Provider>,
    interval: Duration,
    current_block: AtomicU64,
}

impl PollFullBlockCollector {
    pub fn new(provider: Arc<dyn Provider>, interval: Duration) -> Self {
        Self {
            provider,
            interval,
            current_block: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Collector<Block> for PollFullBlockCollector {
    fn name(&self) -> &str {
        "PollFullBlockCollector"
    }

    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Block>> {
        let stream = async_stream::stream! {
            loop {
                // Probe with `eth_blockNumber` (a few bytes) rather than pulling a full block
                // (megabytes on mainnet) on every tick. Most ticks see no new block at all.
                let block_number = match self.provider.get_block_number().await {
                    Ok(block_number) => block_number,
                    Err(e) => {
                        error!("fail to get latest block number: {e:#}");
                        tokio::time::sleep(self.interval).await;
                        continue;
                    }
                };

                if block_number > self.current_block.load(Ordering::Relaxed) {
                    // Fetch by number rather than by `latest` so the block we yield is the one we
                    // just observed, even if the chain advances in between.
                    match self.provider.get_block_by_number(block_number.into()).full().await {
                        Ok(Some(block)) => {
                            self.current_block.fetch_max(block_number, Ordering::Relaxed);
                            yield block;
                        }
                        Ok(None) => {
                            // Not indexed yet; leave `current_block` alone and retry next tick.
                            warn!(block = block_number, "block not found yet");
                        }
                        Err(e) => {
                            error!(block = block_number, "fail to get full block: {e:#}");
                        }
                    }
                }

                tokio::time::sleep(self.interval).await;
            }
        };

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy::primitives::U64;
    use alloy::providers::ProviderBuilder;
    use alloy::transports::mock::Asserter;
    use futures::StreamExt;

    fn block_at(number: u64) -> Block {
        let mut block: Block = Block::default();
        block.header.inner.number = number;
        block
    }

    /// The mock transport answers every request from a single FIFO queue regardless of method, so
    /// an extra `eth_getBlockByNumber` call desyncs the queue: the collector then reads a block
    /// number where a block is expected, errors out, and never yields again. The timeout turns
    /// that into a failure instead of a hang.
    async fn next_block(stream: &mut CollectorStream<'_, Block>) -> Block {
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("collector stopped yielding: it issued more RPC calls than expected")
            .expect("stream ended unexpectedly")
    }

    #[tokio::test]
    async fn only_fetches_full_block_when_height_advances() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        // Tick 1: height is 5, so the full block is fetched.
        asserter.push_success(&U64::from(5));
        asserter.push_success(&block_at(5));
        // Tick 2: height unchanged, so only the cheap probe must be issued.
        asserter.push_success(&U64::from(5));
        // Tick 3: height advanced, so the full block is fetched again.
        asserter.push_success(&U64::from(6));
        asserter.push_success(&block_at(6));

        let collector = PollFullBlockCollector::new(Arc::new(provider), Duration::from_millis(1));
        let mut stream = collector.get_event_stream().await.unwrap();

        assert_eq!(next_block(&mut stream).await.header.number, 5);
        assert_eq!(next_block(&mut stream).await.header.number, 6);

        assert!(
            asserter.read_q().is_empty(),
            "expected exactly 3 probes and 2 full block fetches"
        );
    }

    #[tokio::test]
    async fn retries_when_the_block_is_not_indexed_yet() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        asserter.push_success(&U64::from(7));
        asserter.push_success(&Option::<Block>::None);
        // A miss must not advance `current_block`, so the next tick retries the same height.
        asserter.push_success(&U64::from(7));
        asserter.push_success(&block_at(7));

        let collector = PollFullBlockCollector::new(Arc::new(provider), Duration::from_millis(1));
        let mut stream = collector.get_event_stream().await.unwrap();

        assert_eq!(next_block(&mut stream).await.header.number, 7);
        assert!(asserter.read_q().is_empty());
    }
}
