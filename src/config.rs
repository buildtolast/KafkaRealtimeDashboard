use clap::Parser;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared security config that can be updated at runtime.
pub type SharedSecurityConfig = Arc<RwLock<SecurityConfig>>;

#[derive(Parser, Debug)]
#[command(
    name = "kafka-dashboard",
    about = "Real-time Kafka topic monitoring dashboard"
)]
pub struct Args {
    /// Kafka broker addresses (comma-separated)
    #[arg(long, default_value = "localhost:9094")]
    pub brokers: String,

    /// Server port
    #[arg(long, default_value = "3001")]
    pub port: u16,

    /// Server host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Static files directory (frontend dist)
    #[arg(long, default_value = "./frontend/dist")]
    pub static_dir: String,

    /// Kafka security protocol (e.g. SASL_SSL, SASL_PLAINTEXT, SSL)
    #[arg(long)]
    pub security_protocol: Option<String>,

    /// SASL mechanism (e.g. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    #[arg(long)]
    pub sasl_mechanism: Option<String>,

    /// SASL username / API Key
    #[arg(long)]
    pub sasl_username: Option<String>,

    /// SASL password / API Secret
    #[arg(long)]
    pub sasl_password: Option<String>,
}

/// Kafka security/authentication configuration for SASL, SSL, etc.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

impl SecurityConfig {
    /// Returns true if any security setting is configured.
    pub fn is_configured(&self) -> bool {
        self.security_protocol.is_some()
            || self.sasl_mechanism.is_some()
            || self.sasl_username.is_some()
            || self.sasl_password.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub server_host: String,
    pub server_port: u16,
    pub static_dir: String,
    pub consumer_group: String,
    pub security: SecurityConfig,
}

/// Maximum broker string length
const MAX_BROKER_LEN: usize = 4096;

impl AppConfig {
    pub fn from_args_and_env(args: Args) -> Self {
        let security = SecurityConfig {
            security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                .ok()
                .or(args.security_protocol),
            sasl_mechanism: env::var("KAFKA_SASL_MECHANISM")
                .ok()
                .or(args.sasl_mechanism),
            sasl_username: env::var("KAFKA_SASL_USERNAME")
                .ok()
                .or(args.sasl_username),
            sasl_password: env::var("KAFKA_SASL_PASSWORD")
                .ok()
                .or(args.sasl_password),
        };

        Self {
            kafka_brokers: env::var("KAFKA_BROKERS").unwrap_or(args.brokers),
            server_host: env::var("SERVER_HOST").unwrap_or(args.host),
            server_port: env::var("SERVER_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(args.port),
            static_dir: env::var("STATIC_DIR").unwrap_or(args.static_dir),
            consumer_group: env::var("CONSUMER_GROUP")
                .unwrap_or_else(|_| "kafka-dashboard".into()),
            security,
        }
    }

    /// Validate configuration at startup.
    pub fn validate(&self) -> Result<(), String> {
        if self.kafka_brokers.trim().is_empty() {
            return Err("kafka_brokers must not be empty".into());
        }
        if self.kafka_brokers.len() > MAX_BROKER_LEN {
            return Err(format!(
                "kafka_brokers too long ({} chars, max {})",
                self.kafka_brokers.len(),
                MAX_BROKER_LEN
            ));
        }
        if self.server_port == 0 {
            return Err("server_port must not be 0".into());
        }

        // Validate security config if partially set
        if let Some(ref protocol) = self.security.security_protocol {
            let valid = ["SASL_SSL", "SASL_PLAINTEXT", "SSL"];
            if !valid.contains(&protocol.as_str()) {
                return Err(format!(
                    "invalid security_protocol '{}' — must be one of: {}",
                    protocol,
                    valid.join(", ")
                ));
            }
            if protocol.starts_with("SASL") {
                if self.security.sasl_mechanism.is_none() {
                    return Err(format!(
                        "sasl_mechanism is required when security_protocol is {}",
                        protocol
                    ));
                }
                if self.security.sasl_username.is_none()
                    || self.security.sasl_password.is_none()
                {
                    return Err(format!(
                        "sasl_username and sasl_password are required when security_protocol is {}",
                        protocol
                    ));
                }
            }
        }
        if let Some(ref mechanism) = self.security.sasl_mechanism {
            let valid = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];
            if !valid.contains(&mechanism.as_str()) {
                return Err(format!(
                    "invalid sasl_mechanism '{}' — must be one of: {}",
                    mechanism,
                    valid.join(", ")
                ));
            }
        }

        Ok(())
    }
}

/// Validate a broker address string (used by the set_broker API handler).
pub fn validate_brokers(brokers: &str) -> Result<(), String> {
    if brokers.trim().is_empty() {
        return Err("brokers must not be empty".into());
    }
    if brokers.len() > MAX_BROKER_LEN {
        return Err(format!(
            "brokers too long ({} chars, max {})",
            brokers.len(),
            MAX_BROKER_LEN
        ));
    }
    for part in brokers.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err("empty broker address in comma-separated list".into());
        }
        if !part.contains(':') {
            return Err(format!(
                "invalid broker address '{}' — expected host:port format",
                part
            ));
        }
    }
    Ok(())
}

/// Validate a topic name.
pub fn validate_topic_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 249 {
        return Err(format!(
            "topic name must be 1-249 characters (got {})",
            name.len()
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(format!(
            "topic name '{}' contains invalid characters — only alphanumeric, '.', '_', '-' allowed",
            name
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_values() {
        let args = Args {
            brokers: "localhost:9094".into(),
            port: 3001,
            host: "127.0.0.1".into(),
            static_dir: "./frontend/dist".into(),
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        };
        let config = AppConfig::from_args_and_env(args);
        assert_eq!(config.kafka_brokers, "localhost:9094");
        assert_eq!(config.server_port, 3001);
        assert_eq!(config.server_host, "127.0.0.1");
        assert_eq!(config.consumer_group, "kafka-dashboard");
        assert!(!config.security.is_configured());
    }

    #[test]
    fn test_args_override() {
        let args = Args {
            brokers: "kafka:9092".into(),
            port: 8080,
            host: "0.0.0.0".into(),
            static_dir: "/custom/path".into(),
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        };
        let config = AppConfig::from_args_and_env(args);
        assert_eq!(config.kafka_brokers, "kafka:9092");
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.static_dir, "/custom/path");
    }

    #[test]
    fn test_security_config_not_configured() {
        let sec = SecurityConfig::default();
        assert!(!sec.is_configured());
    }

    #[test]
    fn test_security_config_configured() {
        let sec = SecurityConfig {
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("key".into()),
            sasl_password: Some("secret".into()),
        };
        assert!(sec.is_configured());
    }

    #[test]
    fn test_security_config_from_args() {
        let args = Args {
            brokers: "confluent:9092".into(),
            port: 3001,
            host: "127.0.0.1".into(),
            static_dir: "./frontend/dist".into(),
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("key".into()),
            sasl_password: Some("secret".into()),
        };
        let config = AppConfig::from_args_and_env(args);
        assert!(config.security.is_configured());
        assert_eq!(
            config.security.security_protocol.as_deref(),
            Some("SASL_SSL")
        );
    }

    #[test]
    fn test_security_config_serde_roundtrip() {
        let sec = SecurityConfig {
            security_protocol: Some("SASL_SSL".into()),
            sasl_mechanism: Some("PLAIN".into()),
            sasl_username: Some("user".into()),
            sasl_password: Some("pass".into()),
        };
        let json = serde_json::to_string(&sec).unwrap();
        let decoded: SecurityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.security_protocol, sec.security_protocol);
        assert_eq!(decoded.sasl_mechanism, sec.sasl_mechanism);
    }

    #[test]
    fn test_validate_config_valid() {
        let config = AppConfig {
            kafka_brokers: "localhost:9094".into(),
            server_host: "127.0.0.1".into(),
            server_port: 3001,
            static_dir: "./frontend/dist".into(),
            consumer_group: "test".into(),
            security: SecurityConfig::default(),
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_config_empty_brokers() {
        let config = AppConfig {
            kafka_brokers: "".into(),
            server_host: "127.0.0.1".into(),
            server_port: 3001,
            static_dir: "./frontend/dist".into(),
            consumer_group: "test".into(),
            security: SecurityConfig::default(),
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("must not be empty"));
    }

    #[test]
    fn test_validate_config_zero_port() {
        let config = AppConfig {
            kafka_brokers: "localhost:9094".into(),
            server_host: "127.0.0.1".into(),
            server_port: 0,
            static_dir: "./frontend/dist".into(),
            consumer_group: "test".into(),
            security: SecurityConfig::default(),
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("must not be 0"));
    }

    #[test]
    fn test_validate_config_sasl_without_credentials() {
        let config = AppConfig {
            kafka_brokers: "localhost:9094".into(),
            server_host: "127.0.0.1".into(),
            server_port: 3001,
            static_dir: "./frontend/dist".into(),
            consumer_group: "test".into(),
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
    fn test_validate_config_invalid_protocol() {
        let config = AppConfig {
            kafka_brokers: "localhost:9094".into(),
            server_host: "127.0.0.1".into(),
            server_port: 3001,
            static_dir: "./frontend/dist".into(),
            consumer_group: "test".into(),
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
    fn test_validate_brokers_valid() {
        assert!(validate_brokers("localhost:9094").is_ok());
        assert!(validate_brokers("host1:9092,host2:9092").is_ok());
    }

    #[test]
    fn test_validate_brokers_empty() {
        assert!(validate_brokers("").is_err());
        assert!(validate_brokers("  ").is_err());
    }

    #[test]
    fn test_validate_brokers_no_port() {
        let err = validate_brokers("localhost").unwrap_err();
        assert!(err.contains("host:port"));
    }

    #[test]
    fn test_validate_topic_name_valid() {
        assert!(validate_topic_name("orders").is_ok());
        assert!(validate_topic_name("my-topic.v2").is_ok());
        assert!(validate_topic_name("test_topic_123").is_ok());
    }

    #[test]
    fn test_validate_topic_name_empty() {
        assert!(validate_topic_name("").is_err());
    }

    #[test]
    fn test_validate_topic_name_invalid_chars() {
        assert!(validate_topic_name("topic name").is_err());
        assert!(validate_topic_name("topic/path").is_err());
        assert!(validate_topic_name("topic@special").is_err());
    }
}
