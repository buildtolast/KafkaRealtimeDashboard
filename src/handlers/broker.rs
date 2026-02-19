use crate::config::{self, SharedSecurityConfig};
use crate::kafka::admin::create_admin_consumer;
use crate::models::{BrokerRequest, BrokerResponse};
use crate::SharedAdminConsumer;
use crate::TopicManager;
use actix_web::{web, HttpResponse};

pub async fn get_broker(
    topic_manager: web::Data<TopicManager>,
    security: web::Data<SharedSecurityConfig>,
) -> actix_web::Result<HttpResponse> {
    let brokers = topic_manager.get_brokers().await;
    let auth = security.read().await.is_configured();
    Ok(HttpResponse::Ok().json(BrokerResponse {
        brokers,
        auth_configured: auth,
    }))
}

pub async fn set_broker(
    body: web::Json<BrokerRequest>,
    topic_manager: web::Data<TopicManager>,
    admin: web::Data<SharedAdminConsumer>,
    security: web::Data<SharedSecurityConfig>,
) -> actix_web::Result<HttpResponse> {
    let new_brokers = body.into_inner().brokers;

    // Validate broker address
    if let Err(e) = config::validate_brokers(&new_brokers) {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": e
        })));
    }

    tracing::info!(brokers = %new_brokers, "Updating broker config");

    let sec = security.read().await.clone();

    // Update the admin consumer (graceful error instead of panic)
    let new_admin = match create_admin_consumer(&new_brokers, &sec) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "Failed to create admin consumer for new brokers");
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to connect to broker: {}", e)
            })));
        }
    };

    {
        let mut guard = admin.lock().await;
        *guard = new_admin;
    }

    // Update the topic manager (clears existing consumers)
    topic_manager.set_brokers(new_brokers.clone()).await;

    Ok(HttpResponse::Ok().json(BrokerResponse {
        brokers: new_brokers,
        auth_configured: sec.is_configured(),
    }))
}
