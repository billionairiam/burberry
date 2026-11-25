use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::signers::local::PrivateKeySigner;
use alloy::{
    network::{eip2718::Encodable2718, EthereumWallet, TransactionBuilder},
    primitives::{keccak256, Address, Bytes},
    providers::{Provider, RootProvider},
    rpc::{client::RpcClient, types::eth::TransactionRequest},
    transports::http::reqwest,
};
use eyre::Context;

use crate::types::Executor;

const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub struct TransactionSender {
    provider: Arc<dyn Provider>,
    signers: HashMap<Address, EthereumWallet>,
    tx_submission_provider: Option<Arc<dyn Provider>>,
}

impl TransactionSender {
    pub fn new(provider: Arc<dyn Provider>, signers: Vec<PrivateKeySigner>) -> Self {
        let signers: HashMap<_, _> = signers
            .into_iter()
            .map(|s| (s.address(), EthereumWallet::new(s)))
            .collect();

        for signer in signers.keys() {
            tracing::info!("setting up signer {:#x}", signer);
        }

        Self {
            provider,
            signers,
            tx_submission_provider: None,
        }
    }
}

impl TransactionSender {
    pub fn new_with_dedicated_tx_submission_endpoint(
        provider: Arc<dyn Provider>,
        tx_submission_provider: Arc<dyn Provider>,
        signers: Vec<PrivateKeySigner>,
    ) -> Self {
        let signers: HashMap<_, _> = signers
            .into_iter()
            .map(|s| (s.address(), EthereumWallet::new(s)))
            .collect();

        for signer in signers.keys() {
            tracing::info!("setting up signer {:#x}", signer);
        }

        Self {
            provider,
            signers,
            tx_submission_provider: Some(tx_submission_provider),
        }
    }

    /// Fallible constructor for a dedicated HTTP submission endpoint with bounded HTTP timeouts.
    pub fn try_new_http_dedicated(
        provider: Arc<dyn Provider>,
        tx_submission_endpoint: &str,
        signers: Vec<PrivateKeySigner>,
    ) -> eyre::Result<Self> {
        let parsed = tx_submission_endpoint
            .parse()
            .wrap_err_with(|| format!("invalid tx submission URL: {tx_submission_endpoint}"))?;
        let client = reqwest::Client::builder()
            .timeout(HTTP_REQUEST_TIMEOUT)
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .pool_idle_timeout(HTTP_POOL_IDLE_TIMEOUT)
            .build()
            .wrap_err("fail to build HTTP client")?;
        let rpc_client = RpcClient::new_http_with_client(client, parsed);
        let tx_submission_provider = Arc::new(RootProvider::new(rpc_client));

        Ok(Self::new_with_dedicated_tx_submission_endpoint(
            provider,
            tx_submission_provider,
            signers,
        ))
    }

    pub fn new_http_dedicated(
        provider: Arc<dyn Provider>,
        tx_submission_endpoint: &str,
        signers: Vec<PrivateKeySigner>,
    ) -> Self {
        Self::try_new_http_dedicated(provider, tx_submission_endpoint, signers)
            .expect("invalid tx submission URL")
    }

    pub fn new_with_flashbots(provider: Arc<dyn Provider>, signers: Vec<PrivateKeySigner>) -> Self {
        Self::new_http_dedicated(provider, "https://rpc.flashbots.net/fast", signers)
    }

    pub fn new_with_bsc_bloxroute(
        provider: Arc<dyn Provider>,
        signers: Vec<PrivateKeySigner>,
    ) -> Self {
        Self::new_http_dedicated(provider, "https://bsc.rpc.blxrbdn.com", signers)
    }

    pub fn new_with_48club(provider: Arc<dyn Provider>, signers: Vec<PrivateKeySigner>) -> Self {
        Self::new_http_dedicated(provider, "https://rpc-bsc.48.club", signers)
    }

    pub fn new_with_polygon_bloxroute(
        provider: Arc<dyn Provider>,
        signers: Vec<PrivateKeySigner>,
    ) -> Self {
        Self::new_http_dedicated(provider, "https://polygon.rpc.blxrbdn.com", signers)
    }

    pub fn new_with_arbitrum_sequencer(
        provider: Arc<dyn Provider>,
        signers: Vec<PrivateKeySigner>,
    ) -> Self {
        Self::new_http_dedicated(provider, "https://arb1-sequencer.arbitrum.io/rpc", signers)
    }
}

#[async_trait::async_trait]
impl Executor<TransactionRequest> for TransactionSender {
    fn name(&self) -> &str {
        "TransactionSender"
    }

    async fn execute(&self, action: Arc<TransactionRequest>) -> eyre::Result<()> {
        let mut action = Arc::unwrap_or_clone(action);

        let account = action
            .from
            .ok_or_else(|| eyre::eyre!("missing sender address"))?;

        let signer = self
            .signers
            .get(&account)
            .ok_or_else(|| eyre::eyre!("missing signer for {account:#x}"))?;

        if action.nonce.is_none() {
            // Query `pending` rather than `latest` so back-to-back submissions for the same
            // account do not reuse the same nonce while the earlier tx is still in the mempool.
            let nonce = self
                .provider
                .get_transaction_count(account)
                .pending()
                .await
                .wrap_err_with(|| format!("failed to get pending nonce for {account:#x}"))?;

            action.set_nonce(nonce);
        }

        let raw_tx: Bytes = action
            .build(signer)
            .await
            .wrap_err_with(|| format!("failed to build tx for {account:#x}"))?
            .encoded_2718()
            .into();

        tracing::debug!(?account, tx = ?raw_tx, "signed tx");

        let send_result = match &self.tx_submission_provider {
            Some(dedicated_provider) => dedicated_provider
                .send_raw_transaction(&raw_tx)
                .await
                .map(|v| *v.tx_hash()),
            None => self
                .provider
                .send_raw_transaction(&raw_tx)
                .await
                .map(|v| *v.tx_hash()),
        };

        let tx_hash = send_result.wrap_err_with(|| {
            let hash = keccak256(&raw_tx);
            format!("failed to send tx {hash:?} for {account:#x}")
        })?;

        tracing::info!(?account, "sent tx: {:#x}", tx_hash);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy::primitives::{B256, U64};
    use alloy::providers::ProviderBuilder;
    use alloy::transports::mock::Asserter;

    #[test]
    fn try_new_http_dedicated_rejects_invalid_url() {
        let asserter = Asserter::new();
        let provider = Arc::new(ProviderBuilder::new().connect_mocked_client(asserter));
        assert!(TransactionSender::try_new_http_dedicated(provider, "not a url", vec![]).is_err());
    }

    #[tokio::test]
    async fn execute_returns_error_on_missing_from_or_signer() {
        let asserter = Asserter::new();
        let provider = Arc::new(ProviderBuilder::new().connect_mocked_client(asserter));
        let sender = TransactionSender::new(provider, vec![]);

        let err = sender
            .execute(Arc::new(TransactionRequest::default()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing sender address"));

        let req = TransactionRequest::default().with_from(Address::repeat_byte(0x01));
        let err = sender.execute(Arc::new(req)).await.unwrap_err();
        assert!(err.to_string().contains("missing signer"));
    }

    #[tokio::test]
    async fn execute_fetches_pending_nonce_and_signs_transaction() {
        let asserter = Asserter::new();
        // 1. eth_getTransactionCount(..., "pending") -> 7
        asserter.push_success(&U64::from(7));
        // 2. eth_sendRawTransaction -> tx hash
        asserter.push_success(&B256::repeat_byte(0xab));

        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let provider = Arc::new(ProviderBuilder::new().connect_mocked_client(asserter.clone()));
        let sender = TransactionSender::new(provider, vec![signer]);

        let req = TransactionRequest::default()
            .with_from(from)
            .with_to(Address::repeat_byte(0x02))
            .with_chain_id(1)
            .with_gas_limit(21_000)
            .with_max_fee_per_gas(20_000_000_000)
            .with_max_priority_fee_per_gas(1_000_000_000);

        sender.execute(Arc::new(req)).await.unwrap();
        assert!(asserter.read_q().is_empty());
    }
}
