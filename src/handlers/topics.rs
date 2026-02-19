use crate::kafka::admin::list_topics;
use crate::models::TopicsResponse;
use crate::SharedAdminConsumer;
use actix_web::{web, HttpResponse};

pub async fn get_topics(
    admin: web::Data<SharedAdminConsumer>,
) -> actix_web::Result<HttpResponse> {
    let consumer = admin.into_inner();
    let topics = web::block(move || {
        let guard = consumer.blocking_lock();
        list_topics(&guard)
    })
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "Blocking error in get_topics");
        actix_web::error::ErrorInternalServerError("Internal error")
    })?
    .map_err(|e| {
        tracing::error!(error = %e, "Kafka metadata error");
        actix_web::error::ErrorInternalServerError("Failed to fetch topics")
    })?;

    Ok(HttpResponse::Ok().json(TopicsResponse { topics }))
}
