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
    #[serde(default = "default_max_messages")]
    pub max_messages: Option<usize>,
}

fn default_max_messages() -> Option<usize> {
    None
}

#[derive(Debug, Serialize)]
pub struct BrokerResponse {
    pub brokers: String,
    pub auth_configured: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_message_roundtrip() {
        let msg = KafkaMessage {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            key: Some("key-1".into()),
            payload: Some("hello".into()),
            timestamp: Some(1700000000000),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: KafkaMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.topic, "orders");
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.key, Some("key-1".into()));
    }

    #[test]
    fn test_kafka_message_null_fields() {
        let msg = KafkaMessage {
            topic: "logs".into(),
            partition: 1,
            offset: 0,
            key: None,
            payload: None,
            timestamp: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("null"));
        let decoded: KafkaMessage = serde_json::from_str(&json).unwrap();
        assert!(decoded.key.is_none());
        assert!(decoded.payload.is_none());
    }

    #[test]
    fn test_topics_response_ser() {
        let resp = TopicsResponse {
            topics: vec!["a".into(), "b".into()],
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"a\""));
        assert!(json.contains("\"b\""));
    }

    #[test]
    fn test_broker_request_deser() {
        let json = r#"{"brokers":"host:9092"}"#;
        let req: BrokerRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.brokers, "host:9092");
    }

    #[test]
    fn test_seek_request_deser() {
        let json = r#"{"timestamp_ms":1700000000000}"#;
        let req: SeekRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.timestamp_ms, 1700000000000);
        assert!(req.max_messages.is_none());
    }

    #[test]
    fn test_seek_request_with_max() {
        let json = r#"{"timestamp_ms":1700000000000,"max_messages":50}"#;
        let req: SeekRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.max_messages, Some(50));
    }

    #[test]
    fn test_broker_response_ser() {
        let resp = BrokerResponse {
            brokers: "host:9092".into(),
            auth_configured: true,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("auth_configured"));
        assert!(json.contains("true"));
    }
}
