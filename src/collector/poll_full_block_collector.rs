use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::Duration;

use alloy::{providers::Provider, rpc::types::eth::Block};
use async_trait::async_trait;
use tokio::time::MissedTickBehavior;
use tracing::{error, warn};

use crate::types::{Collector, CollectorStream};

/// Maximum number of missed blocks fetched in order when the polled tip jumps ahead.
const DEFAULT_MAX_CATCHUP_BLOCKS: u64 = 32;

pub struct PollFullBlockCollector {
    provider: Arc<dyn Provider>,
    interval: Duration,
    current_block: AtomicU64,
    max_catchup_blocks: u64,
}

impl PollFullBlockCollector {
    pub fn new(provider: Arc<dyn Provider>, interval: Duration) -> Self {
        Self {
            provider,
            interval,
            current_block: AtomicU64::new(0),
            max_catchup_blocks: DEFAULT_MAX_CATCHUP_BLOCKS,
        }
    }

    /// Cap how many intermediate blocks are backfilled if the chain advances by multiple blocks
    /// between ticks (e.g. after a brief RPC outage or on fast L2s).
    ///
    /// Defaults to 32. Set to `1` to always jump straight to the latest height.
    ///
    /// # Panics
    ///
    /// Panics if `max_catchup_blocks` is 0.
    pub fn with_max_catchup_blocks(mut self, max_catchup_blocks: u64) -> Self {
        assert!(
            max_catchup_blocks > 0,
            "max_catchup_blocks must be at least 1"
        );
        self.max_catchup_blocks = max_catchup_blocks;
        self
    }
}

#[async_trait]
impl Collector<Block> for PollFullBlockCollector {
    fn name(&self) -> &str {
        "PollFullBlockCollector"
    }

    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Block>> {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let stream = async_stream::stream! {
            loop {
                ticker.tick().await;

                // Probe with `eth_blockNumber` (a few bytes) rather than pulling a full block
                // (megabytes on mainnet) on every tick. Most ticks see no new block at all.
                let block_number = match self.provider.get_block_number().await {
                    Ok(block_number) => block_number,
                    Err(e) => {
                        error!("fail to get latest block number: {e:#}");
                        continue;
                    }
                };

                let last = self.current_block.load(Ordering::Relaxed);
                if block_number <= last {
                    continue;
                }

                // On the very first poll (`last == 0`), start at the current tip rather than
                // backfilling from genesis. Afterwards, catch up any skipped blocks in order up to
                // `max_catchup_blocks`.
                let start = if last == 0 {
                    block_number
                } else {
                    let next = last.saturating_add(1);
                    let earliest = block_number
                        .saturating_sub(self.max_catchup_blocks.saturating_sub(1));
                    if earliest > next {
                        warn!(
                            skipped_from = next,
                            skipped_to = earliest - 1,
                            latest = block_number,
                            "chain jumped ahead of max_catchup_blocks, skipping older blocks"
                        );
                        earliest
                    } else {
                        next
                    }
                };

                for height in start..=block_number {
                    // Fetch by number rather than by `latest` so the block we yield matches the
                    // height we are advancing through, even if the chain moves on in between.
                    match self.provider.get_block_by_number(height.into()).full().await {
                        Ok(Some(block)) => {
                            self.current_block.fetch_max(height, Ordering::Relaxed);
                            yield block;
                        }
                        Ok(None) => {
                            // Not indexed yet; stop advancing so the next tick retries this height.
                            warn!(block = height, "block not found yet");
                            break;
                        }
                        Err(e) => {
                            error!(block = height, "fail to get full block: {e:#}");
                            break;
                        }
                    }
                }
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

    #[tokio::test]
    async fn catches_up_skipped_intermediate_blocks_in_order() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        // Tick 1: start at block 5.
        asserter.push_success(&U64::from(5));
        asserter.push_success(&block_at(5));
        // Tick 2: tip jumped from 5 to 7 -> must fetch 6 and then 7 without dropping 6.
        asserter.push_success(&U64::from(7));
        asserter.push_success(&block_at(6));
        asserter.push_success(&block_at(7));

        let collector = PollFullBlockCollector::new(Arc::new(provider), Duration::from_millis(1));
        let mut stream = collector.get_event_stream().await.unwrap();

        assert_eq!(next_block(&mut stream).await.header.number, 5);
        assert_eq!(next_block(&mut stream).await.header.number, 6);
        assert_eq!(next_block(&mut stream).await.header.number, 7);
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn caps_catchup_at_max_catchup_blocks() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        // Tick 1: block 5.
        asserter.push_success(&U64::from(5));
        asserter.push_success(&block_at(5));
        // Tick 2: jump to 20 with max_catchup_blocks = 2 -> fetch only 19 and 20.
        asserter.push_success(&U64::from(20));
        asserter.push_success(&block_at(19));
        asserter.push_success(&block_at(20));

        let collector = PollFullBlockCollector::new(Arc::new(provider), Duration::from_millis(1))
            .with_max_catchup_blocks(2);
        let mut stream = collector.get_event_stream().await.unwrap();

        assert_eq!(next_block(&mut stream).await.header.number, 5);
        assert_eq!(next_block(&mut stream).await.header.number, 19);
        assert_eq!(next_block(&mut stream).await.header.number, 20);
        assert!(asserter.read_q().is_empty());
    }
}
