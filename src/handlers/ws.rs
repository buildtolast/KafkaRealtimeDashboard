use crate::models::KafkaMessage;
use crate::TopicManager;
use actix_web::{web, HttpRequest, HttpResponse};
use actix_ws::AggregatedMessage;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::broadcast;

pub async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
    path: web::Path<String>,
    topic_manager: web::Data<TopicManager>,
) -> actix_web::Result<HttpResponse> {
    let topic = path.into_inner();
    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;

    let rx = topic_manager.subscribe(&topic).await;

    log::info!("WebSocket client connected for topic: {}", topic);

    actix_rt::spawn(ws_session(session, msg_stream, rx, topic));

    Ok(res)
}

async fn ws_session(
    mut session: actix_ws::Session,
    msg_stream: actix_ws::MessageStream,
    mut rx: broadcast::Receiver<KafkaMessage>,
    topic: String,
) {
    let mut msg_stream = msg_stream
        .aggregate_continuations()
        .max_continuation_size(64 * 1024);

    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(kafka_msg) => {
                        if let Ok(json) = serde_json::to_string(&kafka_msg) {
                            if session.text(json).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("WebSocket client lagged {} messages on topic {}", n, topic);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("Broadcast channel closed for topic {}", topic);
                        break;
                    }
                }
            }
            Some(msg) = msg_stream.next() => {
                match msg {
                    Ok(AggregatedMessage::Ping(bytes)) => {
                        if session.pong(&bytes).await.is_err() {
                            break;
                        }
                    }
                    Ok(AggregatedMessage::Pong(_)) => {}
                    Ok(AggregatedMessage::Close(reason)) => {
                        let _ = session.close(reason).await;
                        break;
                    }
                    _ => {}
                }
            }
            _ = heartbeat_interval.tick() => {
                if session.ping(b"").await.is_err() {
                    break;
                }
            }
        }
    }

    log::info!("WebSocket client disconnected from topic: {}", topic);
}
