use crate::models::KafkaMessage;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::Message;
use std::time::Duration;
use tokio::sync::broadcast;

pub async fn run_topic_consumer(
    brokers: String,
    topic: String,
    group_id: String,
    tx: broadcast::Sender<KafkaMessage>,
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("{}-{}", group_id, topic))
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&topic])
        .expect("Subscription failed");

    log::info!("Started consumer for topic: {}", topic);

    loop {
        match consumer.recv().await {
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
                log::error!("Kafka recv error on topic {}: {}", topic, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
