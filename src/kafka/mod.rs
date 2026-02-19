pub mod admin;
pub mod consumer;

use crate::config::SecurityConfig;
use rdkafka::ClientConfig;

/// Apply SASL/SSL security settings to a Kafka ClientConfig.
/// Only sets properties that are configured (non-None).
pub fn apply_security(config: &mut ClientConfig, security: &SecurityConfig) {
    if let Some(ref protocol) = security.security_protocol {
        config.set("security.protocol", protocol);
    }
    if let Some(ref mechanism) = security.sasl_mechanism {
        config.set("sasl.mechanisms", mechanism);
    }
    if let Some(ref username) = security.sasl_username {
        config.set("sasl.username", username);
    }
    if let Some(ref password) = security.sasl_password {
        config.set("sasl.password", password);
    }
}

/// Apply sensible network timeouts for remote/cloud brokers.
pub fn apply_timeouts(config: &mut ClientConfig) {
    config
        .set("socket.timeout.ms", "10000")
        .set("socket.connection.setup.timeout.ms", "5000")
        .set("request.timeout.ms", "10000")
        .set("metadata.max.age.ms", "60000");
}
