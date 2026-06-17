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
            // reqwest 에러에 URL(봇 토큰 포함)이 들어갈 수 있으므로 status만 기록
            let kind = if e.is_timeout() { "timeout" }
                else if e.is_connect() { "connect" }
                else if e.is_status() { "status" }
                else { "unknown" };
            tracing::warn!(kind, "텔레그램 알림 전송 실패");
        }
    }
}
