use std::sync::Arc;
use std::time::Duration;

use alloy::{
    primitives::{keccak256, Bytes},
    providers::{Provider, ProviderBuilder},
    rpc::client::RpcClient,
    transports::http::reqwest,
};
use async_trait::async_trait;
use eyre::{Context, Result};

use crate::types::Executor;

const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub struct RawTransactionSender {
    provider: Arc<dyn Provider>,
}

impl RawTransactionSender {
    pub fn new(provider: Arc<dyn Provider>) -> Self {
        Self { provider }
    }

    /// Fallible HTTP constructor with sensible connection and request timeouts so a stalled RPC
    /// endpoint cannot hang the executor indefinitely.
    pub fn try_new_http(url: &str) -> Result<Self> {
        let parsed = url
            .parse()
            .wrap_err_with(|| format!("invalid HTTP RPC URL: {url}"))?;
        let client = reqwest::Client::builder()
            .timeout(HTTP_REQUEST_TIMEOUT)
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .pool_idle_timeout(HTTP_POOL_IDLE_TIMEOUT)
            .build()
            .wrap_err("fail to build HTTP client")?;
        let rpc_client = RpcClient::new_http_with_client(client, parsed);
        let provider = ProviderBuilder::default().connect_client(rpc_client);
        Ok(Self {
            provider: Arc::new(provider),
        })
    }

    pub fn new_http(url: &str) -> Self {
        Self::try_new_http(url).expect("invalid HTTP RPC URL")
    }

    pub fn new_with_flashbots() -> Self {
        Self::new_http("https://rpc.flashbots.net/fast")
    }

    pub fn new_with_bsc_bloxroute() -> Self {
        Self::new_http("https://bsc.rpc.blxrbdn.com")
    }

    pub fn new_with_48club() -> Self {
        Self::new_http("https://rpc-bsc.48.club")
    }

    pub fn new_with_polygon_bloxroute() -> Self {
        Self::new_http("https://polygon.rpc.blxrbdn.com")
    }

    pub fn new_with_arbitrum_sequencer() -> Self {
        Self::new_http("https://arb1-sequencer.arbitrum.io/rpc")
    }
}

#[async_trait]
impl Executor<Bytes> for RawTransactionSender {
    fn name(&self) -> &str {
        "RawTransactionSender"
    }

    async fn execute(&self, action: Arc<Bytes>) -> Result<()> {
        let send_result = self.provider.send_raw_transaction(action.as_ref()).await;

        match send_result {
            Ok(tx) => {
                tracing::info!(tx = ?tx.tx_hash(), "sent tx");
                Ok(())
            }
            Err(err) => {
                let tx_hash = keccak256(action.as_ref());
                Err(eyre::eyre!("failed to send tx {tx_hash:?}: {err:#}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy::primitives::B256;
    use alloy::transports::mock::Asserter;

    #[test]
    fn try_new_http_rejects_invalid_url() {
        assert!(RawTransactionSender::try_new_http("not a url").is_err());
    }

    #[tokio::test]
    async fn execute_propagates_rpc_errors() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("nonce too low");

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let sender = RawTransactionSender::new(Arc::new(provider));

        let err = sender
            .execute(Arc::new(Bytes::from_static(&[0x01, 0x02])))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("nonce too low"));
    }

    #[tokio::test]
    async fn execute_succeeds_on_valid_submission() {
        let asserter = Asserter::new();
        asserter.push_success(&B256::repeat_byte(0x42));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let sender = RawTransactionSender::new(Arc::new(provider));

        sender
            .execute(Arc::new(Bytes::from_static(&[0x01, 0x02])))
            .await
            .unwrap();
    }
}
