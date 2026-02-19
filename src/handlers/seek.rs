use crate::config::{self, SecurityConfig, SharedSecurityConfig};
use crate::models::{KafkaMessage, SeekRequest};
use crate::TopicManager;
use actix_web::{web, HttpResponse};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::Offset;
use rdkafka::ClientConfig;
use std::time::Duration;

const MAX_SEEK_MESSAGES: usize = 1000;

pub async fn seek_messages(
    path: web::Path<String>,
    body: web::Json<SeekRequest>,
    topic_manager: web::Data<TopicManager>,
    security: web::Data<SharedSecurityConfig>,
) -> actix_web::Result<HttpResponse> {
    let topic = path.into_inner();

    // Validate topic name
    if let Err(e) = config::validate_topic_name(&topic) {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": e
        })));
    }

    let timestamp_ms = body.timestamp_ms;
    let max_messages = body.max_messages.unwrap_or(200).min(MAX_SEEK_MESSAGES);
    let brokers = topic_manager.get_brokers().await;
    let sec = security.read().await.clone();

    let result = web::block(move || {
        fetch_messages_from_timestamp(&brokers, &topic, timestamp_ms, max_messages, &sec)
    })
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "Blocking error in seek");
        actix_web::error::ErrorInternalServerError("Internal error")
    })?
    .map_err(|e| {
        tracing::error!(error = %e, "Seek error");
        actix_web::error::ErrorInternalServerError(format!("Seek failed: {}", e))
    })?;

    Ok(HttpResponse::Ok().json(result))
}

fn fetch_messages_from_timestamp(
    brokers: &str,
    topic: &str,
    timestamp_ms: i64,
    max_messages: usize,
    security: &SecurityConfig,
) -> Result<Vec<KafkaMessage>, String> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", "kafka-dashboard-seek")
        .set("enable.auto.commit", "false");
    crate::kafka::apply_security(&mut config, security);
    crate::kafka::apply_timeouts(&mut config);

    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("Consumer creation failed: {}", e))?;

    // Get metadata to find partitions
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("Metadata fetch failed: {}", e))?;

    let topic_metadata = metadata
        .topics()
        .first()
        .ok_or_else(|| "Topic not found".to_string())?;

    // Build a TPL with the timestamp for each partition
    let mut tpl = rdkafka::TopicPartitionList::new();
    for partition in topic_metadata.partitions() {
        tpl.add_partition_offset(topic, partition.id(), Offset::Offset(timestamp_ms))
            .map_err(|e| format!("Failed to add partition: {}", e))?;
    }

    // Resolve timestamps to offsets
    let offsets = consumer
        .offsets_for_times(tpl, Duration::from_secs(10))
        .map_err(|e| format!("offsets_for_times failed: {}", e))?;

    // Assign the resolved offsets
    let mut assign_tpl = rdkafka::TopicPartitionList::new();
    for elem in offsets.elements() {
        let offset = match elem.offset() {
            Offset::Offset(o) => Offset::Offset(o),
            _ => Offset::End,
        };
        assign_tpl
            .add_partition_offset(elem.topic(), elem.partition(), offset)
            .map_err(|e| format!("Failed to set offset: {}", e))?;
    }

    consumer
        .assign(&assign_tpl)
        .map_err(|e| format!("Assign failed: {}", e))?;

    // Poll messages
    let mut messages = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);

    while messages.len() < max_messages && std::time::Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(msg)) => {
                messages.push(KafkaMessage {
                    topic: topic.to_string(),
                    partition: msg.partition(),
                    offset: msg.offset(),
                    key: msg.key().map(|k| String::from_utf8_lossy(k).to_string()),
                    payload: msg.payload().map(|p| String::from_utf8_lossy(p).to_string()),
                    timestamp: msg.timestamp().to_millis(),
                });
            }
            Some(Err(e)) => {
                tracing::warn!(error = %e, "Poll error during seek");
                break;
            }
            None => {
                if messages.is_empty() {
                    continue;
                }
                break;
            }
        }
    }

    Ok(messages)
}
