use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Response, StatusCode};
use serde_json::{json, Map};

use crate::types::Executor;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const ERROR_REPORT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct Message {
    pub bot_token: String,
    pub chat_id: String,
    pub thread_id: Option<String>,
    pub text: String,
    pub disable_notification: Option<bool>,
    pub protect_content: Option<bool>,
    pub disable_link_preview: Option<bool>,
    pub parse_mode: Option<String>,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("bot_token", &"[REDACTED]")
            .field("chat_id", &self.chat_id)
            .field("thread_id", &self.thread_id)
            .field("text", &self.text)
            .field("disable_notification", &self.disable_notification)
            .field("protect_content", &self.protect_content)
            .field("disable_link_preview", &self.disable_link_preview)
            .field("parse_mode", &self.parse_mode)
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct MessageBuilder {
    bot_token: Option<String>,
    chat_id: Option<String>,
    thread_id: Option<String>,
    text: Option<String>,
    disable_notification: Option<bool>,
    protect_content: Option<bool>,
    disable_link_preview: Option<bool>,
    parse_mode: Option<String>,
}

impl MessageBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bot_token<T: Into<String>>(mut self, bot_token: T) -> Self {
        self.bot_token = Some(bot_token.into());
        self
    }

    pub fn chat_id<T: Into<String>>(mut self, chat_id: T) -> Self {
        self.chat_id = Some(chat_id.into());
        self
    }

    pub fn thread_id<T: Into<String>>(mut self, thread_id: T) -> Self {
        self.thread_id = Some(thread_id.into());
        self
    }

    pub fn text<T: Into<String>>(mut self, text: T) -> Self {
        self.text = Some(text.into());
        self
    }

    pub fn disable_notification(mut self, disable_notification: bool) -> Self {
        self.disable_notification = Some(disable_notification);
        self
    }

    pub fn protect_content(mut self, protect_content: bool) -> Self {
        self.protect_content = Some(protect_content);
        self
    }

    pub fn disable_link_preview(mut self, disable_link_preview: bool) -> Self {
        self.disable_link_preview = Some(disable_link_preview);
        self
    }

    pub fn parse_mode<T: Into<String>>(mut self, parse_mode: T) -> Self {
        self.parse_mode = Some(parse_mode.into());
        self
    }

    pub fn build(self) -> Message {
        Message {
            bot_token: self.bot_token.unwrap_or_default(),
            chat_id: self.chat_id.unwrap_or_default(),
            thread_id: self.thread_id,
            text: self.text.unwrap_or_default(),
            disable_notification: self.disable_notification,
            protect_content: self.protect_content,
            disable_link_preview: self.disable_link_preview,
            parse_mode: self.parse_mode,
        }
    }
}

const TELEGRAM_API_BASE_URL: &str = "https://api.telegram.org";

pub struct TelegramMessageDispatcher {
    error_report_bot_token: Option<String>,
    error_report_chat_id: Option<String>,
    error_report_thread_id: Option<String>,

    base_url: String,
    client: reqwest::Client,
}

fn build_http_client(timeout: Duration) -> reqwest::Client {
    reqwest::ClientBuilder::new()
        // Without these, a stalled Telegram request blocks the executor forever.
        .timeout(timeout)
        .connect_timeout(timeout.min(DEFAULT_CONNECT_TIMEOUT))
        .pool_idle_timeout(DEFAULT_POOL_IDLE_TIMEOUT)
        .build()
        .expect("failed to build reqwest client")
}

impl TelegramMessageDispatcher {
    pub fn new(
        error_report_bot_token: Option<String>,
        error_report_chat_id: Option<String>,
        error_report_thread_id: Option<String>,
    ) -> Self {
        Self {
            error_report_bot_token,
            error_report_chat_id,
            error_report_thread_id,
            base_url: TELEGRAM_API_BASE_URL.to_string(),
            client: build_http_client(DEFAULT_REQUEST_TIMEOUT),
        }
    }

    pub fn new_without_error_report() -> Self {
        Self::new(None, None, None)
    }

    /// Point the dispatcher at something other than `https://api.telegram.org`, such as a local
    /// Bot API server or a proxy.
    pub fn with_base_url<T: Into<String>>(mut self, base_url: T) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Override the per-request HTTP timeout (defaults to 30s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.client = build_http_client(timeout);
        self
    }

    fn get_url<T: std::fmt::Display>(&self, bot_token: T) -> String {
        format!("{}/bot{}/sendMessage", self.base_url, bot_token)
    }

    /// Send a Telegram message, logging and reporting API rejections as configured.
    pub async fn send_message(&self, message: &Message) {
        if let Err(err) = self.try_send_message(message).await {
            tracing::error!("fail to send message to telegram: {err:#}");
        }
    }

    /// Fallible variant of [`Self::send_message`] used by the [`Executor`] implementation so the
    /// engine can observe and account for failed deliveries.
    pub async fn try_send_message(&self, message: &Message) -> eyre::Result<()> {
        let url = self.get_url(&message.bot_token);

        let mut data = Map::new();

        data.insert("chat_id".to_string(), json!(&message.chat_id));
        data.insert("text".to_string(), json!(&message.text));
        data.insert(
            "parse_mode".to_string(),
            json!(message.parse_mode.as_deref().unwrap_or("MarkdownV2")),
        );

        if let Some(thread_id) = &message.thread_id {
            data.insert("message_thread_id".to_string(), json!(thread_id));
        }

        if let Some(disable_notification) = &message.disable_notification {
            data.insert(
                "disable_notification".to_string(),
                json!(disable_notification),
            );
        }

        if let Some(protect_content) = &message.protect_content {
            data.insert("protect_content".to_string(), json!(protect_content));
        }

        if let Some(disable_link_preview) = &message.disable_link_preview {
            data.insert(
                "link_preview_options".to_string(),
                json!({
                    "is_disabled": disable_link_preview,
                }),
            );
        }

        tracing::debug!("sending message to telegram: {data:?}");

        let response = self.client.post(&url).json(&data).send().await;
        // Skip reporting errors back to Telegram when the transport itself failed (timeout /
        // connection failure), otherwise a Telegram outage stalls each message twice as long.
        let is_transport_failure = response.is_err();

        if let Err(err) = self.handle_response(response).await {
            if !is_transport_failure {
                self.report_error(message, format!("{err:#}")).await;
            }
            return Err(err);
        }

        Ok(())
    }

    pub async fn report_error(&self, original_message: &Message, error_message: String) {
        let error_report_bot_token = match &self.error_report_bot_token {
            Some(token) => token,
            None => {
                tracing::warn!("telegram message fails to send but error reporting is disabled");
                return;
            }
        };

        let url = self.get_url(error_report_bot_token);

        let mut data = Map::new();

        data.insert("chat_id".to_string(), json!(self.error_report_chat_id));
        data.insert(
            "link_preview_options".to_string(),
            json!({
                "is_disabled": true,
            }),
        );

        if let Some(thread_id) = &self.error_report_thread_id {
            data.insert("message_thread_id".to_string(), json!(thread_id));
        }

        data.insert(
            "text".to_string(),
            json!(format!(
                "❌ Fail to send message\n\nOriginal message: {}\nError: {error_message}",
                json!(original_message.text)
            )),
        );

        let response = self
            .client
            .post(&url)
            .timeout(ERROR_REPORT_TIMEOUT)
            .json(&data)
            .send()
            .await;
        if let Err(err) = self.handle_response(response).await {
            tracing::error!("fail to send error report to telegram: {err:#}");
        }
    }

    async fn handle_response(&self, response: reqwest::Result<Response>) -> eyre::Result<()> {
        let response = match response {
            Ok(response) if response.status() != StatusCode::OK => {
                let status = response.status();
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "fail to read body".to_string());

                eyre::bail!("response status: {status}, body: {body}");
            }
            Ok(response) => response,
            Err(err) => {
                eyre::bail!("failed to send message: {err:#}");
            }
        };

        match response.json::<serde_json::Value>().await {
            Ok(value) => {
                tracing::debug!("response: {value}");
            }
            Err(err) => {
                eyre::bail!("failed to parse response: {err:#}");
            }
        };

        Ok(())
    }
}

impl Default for TelegramMessageDispatcher {
    fn default() -> Self {
        Self::new_without_error_report()
    }
}

#[async_trait]
impl Executor<Message> for TelegramMessageDispatcher {
    fn name(&self) -> &str {
        "TelegramMessageDispatcher"
    }

    async fn execute(&self, action: Arc<Message>) -> eyre::Result<()> {
        tracing::debug!("received message: {action:?}");

        self.try_send_message(&action).await
    }
}

pub fn escape(raw: &str) -> String {
    let escaped_characters = r"\*_[]~`>#-|{}.!+()=";
    let escaped_string: String = raw
        .chars()
        .map(|c| {
            if escaped_characters.contains(c) {
                format!("\\{}", c)
            } else {
                c.to_string()
            }
        })
        .collect();

    escaped_string
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Instant;

    #[test]
    fn debug_redacts_bot_token() {
        let msg = MessageBuilder::new()
            .bot_token("123456:SUPER_SECRET_TOKEN")
            .chat_id("42")
            .text("hello")
            .build();

        let rendered = format!("{msg:?}");
        assert!(
            !rendered.contains("SUPER_SECRET_TOKEN"),
            "bot_token must never appear in Debug output: {rendered}"
        );
        assert!(rendered.contains("[REDACTED]"));
        assert!(rendered.contains("hello"));
    }

    #[tokio::test]
    async fn transport_timeout_skips_redundant_error_report_roundtrip() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());

        let dispatcher = TelegramMessageDispatcher::new(
            Some("error-bot-token".to_string()),
            Some("error-chat".to_string()),
            None,
        )
        .with_base_url(base_url)
        .with_timeout(Duration::from_millis(120));

        let msg = MessageBuilder::new()
            .bot_token("token")
            .chat_id("chat")
            .text("hi")
            .build();

        let start = Instant::now();
        let err = dispatcher.try_send_message(&msg).await.unwrap_err();
        let elapsed = start.elapsed();

        assert!(err.to_string().contains("failed to send message"));
        assert!(
            elapsed < Duration::from_secs(1),
            "transport failure should not wait for a second error-report request, took {elapsed:?}"
        );
    }
}
