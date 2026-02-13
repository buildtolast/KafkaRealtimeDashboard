use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub payload: Option<String>,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicsResponse {
    pub topics: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct BrokerRequest {
    pub brokers: String,
}

#[derive(Debug, Deserialize)]
pub struct SeekRequest {
    pub timestamp_ms: i64,
    pub max_messages: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct BrokerResponse {
    pub brokers: String,
}
