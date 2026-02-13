use crate::kafka::admin::create_admin_consumer;
use crate::models::{BrokerRequest, BrokerResponse};
use crate::SharedAdminConsumer;
use crate::TopicManager;
use actix_web::{web, HttpResponse};

pub async fn get_broker(
    topic_manager: web::Data<TopicManager>,
) -> actix_web::Result<HttpResponse> {
    let brokers = topic_manager.get_brokers().await;
    Ok(HttpResponse::Ok().json(BrokerResponse { brokers }))
}

pub async fn set_broker(
    body: web::Json<BrokerRequest>,
    topic_manager: web::Data<TopicManager>,
    admin: web::Data<SharedAdminConsumer>,
) -> actix_web::Result<HttpResponse> {
    let new_brokers = body.into_inner().brokers;

    if new_brokers.trim().is_empty() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Broker address cannot be empty"
        })));
    }

    log::info!("Updating broker config to: {}", new_brokers);

    // Update the admin consumer
    let new_admin = create_admin_consumer(&new_brokers);
    {
        let mut guard = admin.lock().await;
        *guard = new_admin;
    }

    // Update the topic manager (clears existing consumers)
    topic_manager.set_brokers(new_brokers.clone()).await;

    Ok(HttpResponse::Ok().json(BrokerResponse {
        brokers: new_brokers,
    }))
}
