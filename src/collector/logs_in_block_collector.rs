use std::sync::Arc;

use alloy::{
    providers::Provider,
    rpc::types::{
        eth::{Filter, Log},
        Header,
    },
};
use async_trait::async_trait;
use futures::StreamExt;

use crate::types::{Collector, CollectorStream};

pub struct LogsInBlockCollector {
    provider: Arc<dyn Provider>,
    filter: Filter,
}

impl LogsInBlockCollector {
    pub fn new(provider: Arc<dyn Provider>, filter: Filter) -> Self {
        Self { provider, filter }
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

                let logs = match self.provider.get_logs(&filter).await {
                    Ok(logs) => logs,
                    Err(e) => {
                        tracing::error!(block_hash = ?header.hash, "fail to get logs: {e:#}");
                        continue;
                    }
                };

                yield (header, logs);
            }
        };

        Ok(Box::pin(stream))
    }
}
