//! Integration tests for KafkaDashboard.
//!
//! These tests verify config validation, input validation, model serialization,
//! and error handling without requiring a live Kafka cluster.

use kafka_dashboard::config::{AppConfig, SecurityConfig};
use kafka_dashboard::error::DashboardError;
use kafka_dashboard::models::{BrokerRequest, BrokerResponse, KafkaMessage, SeekRequest, TopicsResponse};

// ═══════════════════════════════════════════════════════════════════
// Config Validation Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_config_validation_valid() {
    let config = AppConfig {
        kafka_brokers: "localhost:9094".into(),
        server_host: "127.0.0.1".into(),
        server_port: 3001,
        static_dir: "./frontend/dist".into(),
        consumer_group: "test-group".into(),
        security: SecurityConfig::default(),
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_validation_empty_brokers() {
    let config = AppConfig {
        kafka_brokers: "".into(),
        server_host: "127.0.0.1".into(),
        server_port: 3001,
        static_dir: "./frontend/dist".into(),
        consumer_group: "test-group".into(),
        security: SecurityConfig::default(),
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("must not be empty"));
}

#[test]
fn test_config_validation_zero_port() {
    let config = AppConfig {
        kafka_brokers: "localhost:9094".into(),
        server_host: "127.0.0.1".into(),
        server_port: 0,
        static_dir: "./frontend/dist".into(),
        consumer_group: "test-group".into(),
        security: SecurityConfig::default(),
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("must not be 0"));
}

#[test]
fn test_config_validation_sasl_without_credentials() {
    let config = AppConfig {
        kafka_brokers: "localhost:9094".into(),
        server_host: "127.0.0.1".into(),
        server_port: 3001,
        static_dir: "./frontend/dist".into(),
        consumer_group: "test-group".into(),
        security: SecurityConfig {
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        },
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("sasl_mechanism is required"));
}

#[test]
fn test_config_validation_invalid_protocol() {
    let config = AppConfig {
        kafka_brokers: "localhost:9094".into(),
        server_host: "127.0.0.1".into(),
        server_port: 3001,
        static_dir: "./frontend/dist".into(),
        consumer_group: "test-group".into(),
        security: SecurityConfig {
            security_protocol: Some("INVALID".into()),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        },
    };
    let err = config.validate().unwrap_err();
    assert!(err.contains("invalid security_protocol"));
}

#[test]
fn test_config_validation_valid_sasl() {
    let config = AppConfig {
        kafka_brokers: "confluent:9092".into(),
        server_host: "0.0.0.0".into(),
        server_port: 3001,
        static_dir: "./frontend/dist".into(),
        consumer_group: "test-group".into(),
        security: SecurityConfig {
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("key".into()),
            sasl_password: Some("secret".into()),
        },
    };
    assert!(config.validate().is_ok());
}

// ═══════════════════════════════════════════════════════════════════
// Input Validation Helper Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_validate_brokers_valid() {
    assert!(kafka_dashboard::config::validate_brokers("localhost:9094").is_ok());
    assert!(kafka_dashboard::config::validate_brokers("host1:9092,host2:9092").is_ok());
}

#[test]
fn test_validate_brokers_empty() {
    assert!(kafka_dashboard::config::validate_brokers("").is_err());
    assert!(kafka_dashboard::config::validate_brokers("  ").is_err());
}

#[test]
fn test_validate_brokers_no_port() {
    let err = kafka_dashboard::config::validate_brokers("localhost").unwrap_err();
    assert!(err.contains("host:port"));
}

#[test]
fn test_validate_topic_name_valid() {
    assert!(kafka_dashboard::config::validate_topic_name("orders").is_ok());
    assert!(kafka_dashboard::config::validate_topic_name("my-topic.v2").is_ok());
    assert!(kafka_dashboard::config::validate_topic_name("test_topic_123").is_ok());
}

#[test]
fn test_validate_topic_name_empty() {
    assert!(kafka_dashboard::config::validate_topic_name("").is_err());
}

#[test]
fn test_validate_topic_name_invalid_chars() {
    assert!(kafka_dashboard::config::validate_topic_name("topic name").is_err());
    assert!(kafka_dashboard::config::validate_topic_name("topic/path").is_err());
    assert!(kafka_dashboard::config::validate_topic_name("topic@special").is_err());
}

// ═══════════════════════════════════════════════════════════════════
// Model Serialization Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_kafka_message_roundtrip() {
    let msg = KafkaMessage {
        topic: "orders".into(),
        partition: 0,
        offset: 42,
        key: Some("key-1".into()),
        payload: Some("hello world".into()),
        timestamp: Some(1700000000000),
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: KafkaMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.topic, "orders");
    assert_eq!(decoded.offset, 42);
    assert_eq!(decoded.key, Some("key-1".into()));
    assert_eq!(decoded.timestamp, Some(1700000000000));
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
    let decoded: KafkaMessage = serde_json::from_str(&json).unwrap();
    assert!(decoded.key.is_none());
    assert!(decoded.payload.is_none());
    assert!(decoded.timestamp.is_none());
}

#[test]
fn test_topics_response_ser() {
    let resp = TopicsResponse {
        topics: vec!["alpha".into(), "beta".into(), "gamma".into()],
    };
    let json = serde_json::to_string(&resp).unwrap();
    let decoded: TopicsResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.topics.len(), 3);
    assert_eq!(decoded.topics[0], "alpha");
}

#[test]
fn test_broker_request_deser() {
    let json = r#"{"brokers":"host1:9092,host2:9092"}"#;
    let req: BrokerRequest = serde_json::from_str(json).unwrap();
    assert_eq!(req.brokers, "host1:9092,host2:9092");
}

#[test]
fn test_broker_response_ser() {
    let resp = BrokerResponse {
        brokers: "kafka:9092".into(),
        auth_configured: true,
    };
    let json = serde_json::to_string(&resp).unwrap();
    assert!(json.contains("auth_configured"));
    assert!(json.contains("true"));
}

#[test]
fn test_seek_request_deser_defaults() {
    let json = r#"{"timestamp_ms":1700000000000}"#;
    let req: SeekRequest = serde_json::from_str(json).unwrap();
    assert_eq!(req.timestamp_ms, 1700000000000);
    assert!(req.max_messages.is_none());
}

#[test]
fn test_seek_request_deser_with_max() {
    let json = r#"{"timestamp_ms":1700000000000,"max_messages":50}"#;
    let req: SeekRequest = serde_json::from_str(json).unwrap();
    assert_eq!(req.max_messages, Some(50));
}

// ═══════════════════════════════════════════════════════════════════
// Error Type Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_dashboard_error_display() {
    let err = DashboardError::Config("bad value".into());
    assert_eq!(err.to_string(), "Configuration error: bad value");
}

#[test]
fn test_dashboard_error_topic_not_found() {
    let err = DashboardError::TopicNotFound("missing-topic".into());
    assert!(err.to_string().contains("missing-topic"));
}

#[test]
fn test_dashboard_error_validation() {
    let err = DashboardError::Validation("invalid input".into());
    assert!(err.to_string().contains("invalid input"));
}

// ═══════════════════════════════════════════════════════════════════
// Security Config Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_security_config_default_not_configured() {
    let sec = SecurityConfig::default();
    assert!(!sec.is_configured());
}

#[test]
fn test_security_config_partial_is_configured() {
    let sec = SecurityConfig {
        security_protocol: Some("SSL".into()),
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password: None,
    };
    assert!(sec.is_configured());
}

#[test]
fn test_security_config_full_roundtrip() {
    let sec = SecurityConfig {
        security_protocol: Some("SASL_SSL".into()),
        sasl_mechanism: Some("SCRAM-SHA-256".into()),
        sasl_username: Some("user".into()),
        sasl_password: Some("pass".into()),
    };
    let json = serde_json::to_string(&sec).unwrap();
    let decoded: SecurityConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.security_protocol, sec.security_protocol);
    assert_eq!(decoded.sasl_mechanism, sec.sasl_mechanism);
    assert_eq!(decoded.sasl_username, sec.sasl_username);
    assert_eq!(decoded.sasl_password, sec.sasl_password);
}
