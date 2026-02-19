use thiserror::Error;

#[derive(Error, Debug)]
pub enum DashboardError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

impl actix_web::ResponseError for DashboardError {
    fn error_response(&self) -> actix_web::HttpResponse {
        let status = match self {
            DashboardError::TopicNotFound(_) => actix_web::http::StatusCode::NOT_FOUND,
            DashboardError::Config(_) | DashboardError::Validation(_) => {
                actix_web::http::StatusCode::BAD_REQUEST
            }
            _ => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
        };
        actix_web::HttpResponse::build(status).json(serde_json::json!({
            "error": self.to_string()
        }))
    }
}
