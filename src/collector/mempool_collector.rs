use std::sync::Arc;

use crate::types::{Collector, CollectorStream};
use alloy::transports::{RpcError, TransportErrorKind};
use alloy::{primitives::B256, providers::Provider, rpc::types::eth::Transaction};
use async_trait::async_trait;
use eyre::WrapErr;
use futures::prelude::{stream::FuturesUnordered, Stream};
use futures::{FutureExt, StreamExt};
use std::future::Future;
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::{debug, error, warn};

/// Number of `eth_getTransactionByHash` requests kept in flight while resolving pending hashes.
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 256;

/// Maximum pending hashes buffered waiting for a fetch slot before evicting the oldest entries.
const DEFAULT_MAX_BUFFERED_HASHES: usize = 4096;

/// Cap hashes drained per `poll_next` so an always-ready upstream stream cannot starve polling
/// in-flight transaction requests.
const MAX_HASHES_PER_POLL: usize = 256;

pub struct MempoolCollector {
    provider: Arc<dyn Provider>,
    max_concurrent_requests: usize,
    max_buffered: usize,
}

impl MempoolCollector {
    pub fn new(provider: Arc<dyn Provider>) -> Self {
        Self {
            provider,
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            max_buffered: DEFAULT_MAX_BUFFERED_HASHES,
        }
    }

    /// How many transaction bodies to fetch in parallel.
    ///
    /// The subscription only carries hashes, so each one needs its own request. Raising this
    /// drains a busy mempool faster at the cost of more load on the endpoint; lowering it helps
    /// with providers that rate limit.
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrent_requests` is 0.
    pub fn with_max_concurrent_requests(mut self, max_concurrent_requests: usize) -> Self {
        assert!(
            max_concurrent_requests > 0,
            "max_concurrent_requests must be at least 1"
        );
        self.max_concurrent_requests = max_concurrent_requests;
        self
    }

    /// Maximum number of pending hashes buffered while all fetch slots are busy.
    ///
    /// When the buffer is full, the oldest queued hash is evicted so freshly observed pending
    /// transactions are prioritized and memory stays bounded during mempool bursts.
    pub fn with_max_buffered(mut self, max_buffered: usize) -> Self {
        self.max_buffered = max_buffered;
        self
    }
}

#[async_trait]
impl Collector<Transaction> for MempoolCollector {
    fn name(&self) -> &str {
        "MempoolCollector"
    }

    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Transaction>> {
        let stream = self
            .provider
            .subscribe_pending_transactions()
            .await
            .wrap_err("fail to subscribe to pending transaction stream")?
            .into_stream();

        let stream =
            TransactionStream::new(self.provider.as_ref(), stream, self.max_concurrent_requests)
                .with_max_buffered(self.max_buffered);

        let stream = stream.filter_map(|res| async move {
            match res {
                Ok(tx) => Some(tx),
                Err(e) => {
                    match &e {
                        // A pending hash routinely disappears before its body can be fetched
                        // because it got mined, replaced or evicted.
                        GetTransactionError::NotFound(_) => debug!("{e}"),
                        GetTransactionError::ProviderError(..) => error!("{e}"),
                    }

                    None
                }
            }
        });

        Ok(Box::pin(stream))
    }
}

/// Subscribes directly to full pending transaction objects via
/// `eth_subscribe("newPendingTransactions", true)`.
///
/// Supported by Reth, Geth, and Erigon over WS/IPC. Avoids the extra `eth_getTransactionByHash`
/// round-trip per transaction required by [`MempoolCollector`].
pub struct FullMempoolCollector {
    provider: Arc<dyn Provider>,
}

impl FullMempoolCollector {
    pub fn new(provider: Arc<dyn Provider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Collector<Transaction> for FullMempoolCollector {
    fn name(&self) -> &str {
        "FullMempoolCollector"
    }

    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, Transaction>> {
        let stream = self
            .provider
            .subscribe_full_pending_transactions()
            .await
            .wrap_err("fail to subscribe to full pending transaction stream")?
            .into_stream();

        Ok(Box::pin(stream))
    }
}

/// Errors `TransactionStream` can throw
#[derive(Debug, thiserror::Error)]
pub enum GetTransactionError {
    #[error("Failed to get transaction `{0}`: {1}")]
    ProviderError(B256, RpcError<TransportErrorKind>),

    /// `get_transaction` resulted in a `None`
    #[error("Transaction `{0}` not found")]
    NotFound(B256),
}

pub(crate) type TransactionFut<'a> = Pin<Box<dyn Future<Output = TransactionResult> + Send + 'a>>;

pub(crate) type TransactionResult = Result<Transaction, GetTransactionError>;

/// Drains a stream of transaction hashes and yields entire `Transaction`.
#[must_use = "streams do nothing unless polled"]
pub struct TransactionStream<'a, St> {
    /// Currently running futures pending completion.
    pub(crate) pending: FuturesUnordered<TransactionFut<'a>>,
    /// Temporary buffered transactions that get started as soon as another future finishes.
    pub(crate) buffered: VecDeque<B256>,
    /// The provider that gets the transaction
    pub(crate) provider: &'a dyn Provider,
    /// A stream of transaction hashes.
    pub(crate) stream: St,
    /// Marks if the stream is done
    stream_done: bool,
    /// Max allowed futures to execute at once.
    pub(crate) max_concurrent: usize,
    /// Max buffered hashes kept before dropping oldest hashes.
    pub(crate) max_buffered: usize,
    /// Total hashes dropped due to buffer saturation.
    dropped_buffered: u64,
}

impl<'a, St> TransactionStream<'a, St> {
    /// Create a new `TransactionStream` instance.
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrent` is 0.
    pub fn new(provider: &'a dyn Provider, stream: St, max_concurrent: usize) -> Self {
        assert!(max_concurrent > 0, "max_concurrent must be at least 1");
        Self {
            pending: Default::default(),
            buffered: Default::default(),
            provider,
            stream,
            stream_done: false,
            max_concurrent,
            max_buffered: DEFAULT_MAX_BUFFERED_HASHES,
            dropped_buffered: 0,
        }
    }

    /// Cap how many hashes are buffered while `max_concurrent` requests are in flight.
    pub fn with_max_buffered(mut self, max_buffered: usize) -> Self {
        self.max_buffered = max_buffered;
        self
    }

    /// Number of stale buffered hashes evicted due to saturation.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_buffered
    }

    /// Push a future into the set
    pub(crate) fn push_tx(&mut self, tx: B256) {
        let fut = self
            .provider
            .root()
            .raw_request::<_, Option<Transaction>>("eth_getTransactionByHash".into(), (tx,))
            .then(move |res| match res {
                Ok(Some(tx)) => futures::future::ok(tx),
                Ok(None) => futures::future::err(GetTransactionError::NotFound(tx)),
                Err(err) => futures::future::err(GetTransactionError::ProviderError(tx, err)),
            });
        self.pending.push(Box::pin(fut));
    }

    fn buffer_hash(&mut self, tx: B256) {
        if self.max_buffered == 0 {
            self.record_drop();
            return;
        }

        if self.buffered.len() >= self.max_buffered {
            self.buffered.pop_front();
            self.record_drop();
        }

        self.buffered.push_back(tx);
    }

    fn record_drop(&mut self) {
        self.dropped_buffered += 1;
        if self.dropped_buffered == 1 || self.dropped_buffered % 1024 == 0 {
            warn!(
                dropped = self.dropped_buffered,
                max_buffered = self.max_buffered,
                "mempool hash buffer full, evicting oldest pending tx hashes"
            );
        }
    }
}

impl<'a, St> Stream for TransactionStream<'a, St>
where
    St: Stream<Item = B256> + Unpin + 'a,
{
    type Item = TransactionResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Drain buffered transactions first.
        while this.pending.len() < this.max_concurrent {
            if let Some(tx) = this.buffered.pop_front() {
                this.push_tx(tx);
            } else {
                break;
            }
        }

        let mut hit_poll_cap = false;
        if !this.stream_done {
            let drain_limit = this.max_concurrent.max(MAX_HASHES_PER_POLL);
            for _ in 0..drain_limit {
                match Stream::poll_next(Pin::new(&mut this.stream), cx) {
                    Poll::Ready(Some(tx)) => {
                        if this.pending.len() < this.max_concurrent {
                            this.push_tx(tx);
                        } else {
                            this.buffer_hash(tx);
                        }
                    }
                    Poll::Ready(None) => {
                        this.stream_done = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            if !this.stream_done && this.pending.len() >= this.max_concurrent {
                hit_poll_cap = true;
            }
        }

        // Poll running futures.
        if let tx @ Poll::Ready(Some(_)) = this.pending.poll_next_unpin(cx) {
            return tx;
        }

        if this.stream_done && this.pending.is_empty() && this.buffered.is_empty() {
            // All done.
            return Poll::Ready(None);
        }

        if hit_poll_cap && this.pending.is_empty() {
            cx.waker().wake_by_ref();
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy::providers::ProviderBuilder;
    use alloy::transports::mock::Asserter;

    #[tokio::test]
    async fn bounded_buffer_evicts_oldest_hashes() {
        let asserter = Asserter::new();
        // 1 in flight + 2 buffered = 3 fetched total out of 5 submitted hashes;
        // hashes 2 and 3 are evicted while 1 (in-flight), 4, and 5 survive.
        for _ in 0..3 {
            asserter.push_success(&Option::<Transaction>::None);
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let hashes: Vec<B256> = (1u8..=5).map(B256::repeat_byte).collect();
        let hash_stream = futures::stream::iter(hashes);

        let mut stream = TransactionStream::new(&provider, hash_stream, 1).with_max_buffered(2);

        let mut seen = Vec::new();
        while let Some(res) = stream.next().await {
            match res {
                Err(GetTransactionError::NotFound(hash)) => seen.push(hash),
                other => panic!("unexpected result: {other:?}"),
            }
        }

        assert_eq!(stream.dropped_count(), 2);
        assert_eq!(
            seen,
            vec![
                B256::repeat_byte(1),
                B256::repeat_byte(4),
                B256::repeat_byte(5),
            ]
        );
        assert!(asserter.read_q().is_empty());
    }

    #[test]
    #[should_panic(expected = "max_concurrent_requests must be at least 1")]
    fn zero_max_concurrent_requests_panics() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let _ = MempoolCollector::new(Arc::new(provider)).with_max_concurrent_requests(0);
    }
}
