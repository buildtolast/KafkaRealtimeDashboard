use crate::config::SecurityConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use std::time::Duration;

/// Create an admin consumer with security and timeout settings.
/// Returns Result instead of panicking.
pub fn create_admin_consumer(
    brokers: &str,
    security: &SecurityConfig,
) -> Result<BaseConsumer, rdkafka::error::KafkaError> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    super::apply_security(&mut config, security);
    super::apply_timeouts(&mut config);
    config.create()
}

pub fn list_topics(consumer: &BaseConsumer) -> Result<Vec<String>, rdkafka::error::KafkaError> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;
    let mut topics: Vec<String> = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .filter(|name| !name.starts_with("__"))
        .collect();
    topics.sort();
    Ok(topics)
}
