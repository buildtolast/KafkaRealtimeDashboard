use crate::models::{KafkaMessage, SeekRequest};
use crate::TopicManager;
use actix_web::{web, HttpResponse};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::Offset;
use rdkafka::ClientConfig;
use std::time::Duration;

pub async fn seek_messages(
    path: web::Path<String>,
    body: web::Json<SeekRequest>,
    topic_manager: web::Data<TopicManager>,
) -> actix_web::Result<HttpResponse> {
    let topic = path.into_inner();
    let timestamp_ms = body.timestamp_ms;
    let max_messages = body.max_messages.unwrap_or(200);
    let brokers = topic_manager.get_brokers().await;

    let result = web::block(move || {
        fetch_messages_from_timestamp(&brokers, &topic, timestamp_ms, max_messages)
    })
    .await
    .map_err(|e| {
        log::error!("Blocking error in seek: {}", e);
        actix_web::error::ErrorInternalServerError("Internal error")
    })?
    .map_err(|e| {
        log::error!("Seek error: {}", e);
        actix_web::error::ErrorInternalServerError(format!("Seek failed: {}", e))
    })?;

    Ok(HttpResponse::Ok().json(result))
}

fn fetch_messages_from_timestamp(
    brokers: &str,
    topic: &str,
    timestamp_ms: i64,
    max_messages: usize,
) -> Result<Vec<KafkaMessage>, String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "kafka-dashboard-seek")
        .set("enable.auto.commit", "false")
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
            _ => Offset::End, // No messages at that timestamp
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
                log::warn!("Poll error during seek: {}", e);
                break;
            }
            None => {
                // No more messages available
                if messages.is_empty() {
                    continue; // Keep trying until deadline
                }
                break;
            }
        }
    }

    Ok(messages)
}
