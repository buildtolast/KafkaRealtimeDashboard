use crate::config::SecurityConfig;
use crate::models::KafkaMessage;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::Message;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub async fn run_topic_consumer(
    brokers: String,
    topic: String,
    group_id: String,
    tx: broadcast::Sender<KafkaMessage>,
    security: SecurityConfig,
    shutdown: CancellationToken,
) {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("{}-{}", group_id, topic))
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true");
    super::apply_security(&mut config, &security);
    super::apply_timeouts(&mut config);

    let consumer: StreamConsumer = match config.create() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(topic = %topic, error = %e, "Consumer creation failed");
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&[&topic]) {
        tracing::error!(topic = %topic, error = %e, "Subscription failed");
        return;
    }

    tracing::info!(topic = %topic, "Started consumer");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!(topic = %topic, "Consumer shutting down");
                break;
            }
            result = consumer.recv() => {
                match result {
                    Ok(msg) => {
                        let kafka_msg = KafkaMessage {
                            topic: topic.clone(),
                            partition: msg.partition(),
                            offset: msg.offset(),
                            key: msg.key().map(|k| String::from_utf8_lossy(k).to_string()),
                            payload: msg.payload().map(|p| String::from_utf8_lossy(p).to_string()),
                            timestamp: msg.timestamp().to_millis(),
                        };
                        let _ = tx.send(kafka_msg);
                    }
                    Err(e) => {
                        tracing::error!(topic = %topic, error = %e, "Kafka recv error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}
