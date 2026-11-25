use std::sync::Arc;
use std::time::Duration;

use alloy::{
    providers::Provider,
    rpc::types::{
        eth::{Filter, Log},
        Header,
    },
};
use async_trait::async_trait;
use futures::StreamExt;
use tracing::{error, warn};

use crate::types::{Collector, CollectorStream};

const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_MAX_RETRIES: usize = 20;

pub struct LogsInBlockCollector {
    provider: Arc<dyn Provider>,
    filter: Filter,
    retry_interval: Duration,
    max_retries: usize,
}

impl LogsInBlockCollector {
    pub fn new(provider: Arc<dyn Provider>, filter: Filter) -> Self {
        Self {
            provider,
            filter,
            retry_interval: DEFAULT_RETRY_INTERVAL,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }

    /// Delay between retries when `eth_getLogs` transiently fails right after a header arrives.
    pub fn with_retry_interval(mut self, retry_interval: Duration) -> Self {
        self.retry_interval = retry_interval;
        self
    }

    /// Maximum number of retries per block before skipping that block's logs.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    async fn fetch_logs(&self, filter: &Filter, block_number: u64) -> Option<Vec<Log>> {
        for attempt in 0..=self.max_retries {
            let last_attempt = attempt == self.max_retries;

            match self.provider.get_logs(filter).await {
                Ok(logs) => return Some(logs),
                Err(e) => {
                    if last_attempt {
                        error!(
                            block = block_number,
                            block_hash = ?filter.get_block_hash(),
                            "fail to get logs after {} attempts, skipping block: {e:#}",
                            attempt + 1
                        );
                        return None;
                    }

                    warn!(
                        block = block_number,
                        block_hash = ?filter.get_block_hash(),
                        "fail to get logs, retrying: {e:#}"
                    );
                }
            }

            tokio::time::sleep(self.retry_interval).await;
        }

        None
    }
}

#[async_trait]
impl Collector<(Header, Vec<Log>)> for LogsInBlockCollector {
    fn name(&self) -> &str {
        "LogsInBlockCollector"
    }

    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, (Header, Vec<Log>)>> {
        let mut stream = self.provider.subscribe_blocks().await?.into_stream();

        // Cloned once for the life of the stream. `at_block_hash` consumes and returns the
        // filter, so retargeting it per block only overwrites the block selector instead of
        // copying the address list and topics again.
        let mut filter = self.filter.clone();

        let stream = async_stream::stream! {
            while let Some(header) = stream.next().await {
                filter = filter.at_block_hash(header.hash);

                if let Some(logs) = self.fetch_logs(&filter, header.number).await {
                    yield (header, logs);
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy::primitives::BlockHash;
    use alloy::providers::ProviderBuilder;
    use alloy::transports::mock::Asserter;

    #[tokio::test]
    async fn retries_transient_get_logs_errors() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("header not found");
        let expected: Vec<Log> = vec![Log::default()];
        asserter.push_success(&expected);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let collector = LogsInBlockCollector::new(Arc::new(provider), Filter::new())
            .with_retry_interval(Duration::from_millis(1))
            .with_max_retries(3);

        let filter = Filter::new().at_block_hash(BlockHash::repeat_byte(0x11));
        let logs = collector.fetch_logs(&filter, 42).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn skips_block_after_max_retries() {
        let asserter = Asserter::new();
        for _ in 0..3 {
            asserter.push_failure_msg("unknown block");
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let collector = LogsInBlockCollector::new(Arc::new(provider), Filter::new())
            .with_retry_interval(Duration::from_millis(1))
            .with_max_retries(2);

        let filter = Filter::new().at_block_hash(BlockHash::repeat_byte(0x22));
        let logs = collector.fetch_logs(&filter, 42).await;
        assert!(logs.is_none());
        assert!(asserter.read_q().is_empty());
    }
}
