use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use std::time::Duration;

pub fn create_admin_consumer(brokers: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Failed to create admin consumer")
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
