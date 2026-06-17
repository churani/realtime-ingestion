use std::sync::Arc;

pub struct Notifier {
    client: reqwest::Client,
    bot_token: String,
    chat_id: String,
}

impl Notifier {
    pub fn new(bot_token: String, chat_id: String) -> Arc<Self> {
        Arc::new(Self {
            client: reqwest::Client::new(),
            bot_token,
            chat_id,
        })
    }

    pub async fn notify(&self, message: &str) {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );
        if let Err(e) = self
            .client
            .post(&url)
            .json(&serde_json::json!({
                "chat_id": &self.chat_id,
                "text": message,
                "parse_mode": "HTML",
            }))
            .send()
            .await
        {
            tracing::warn!(error = ?e, "텔레그램 알림 전송 실패");
        }
    }
}
