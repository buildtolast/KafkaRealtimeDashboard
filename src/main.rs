pub mod config;
pub mod error;
mod handlers;
pub mod kafka;
pub mod models;

use crate::config::{AppConfig, Args, SharedSecurityConfig};
use crate::kafka::admin::create_admin_consumer;
use crate::kafka::consumer::run_topic_consumer;
use crate::models::KafkaMessage;
use actix_cors::Cors;
use actix_files::Files;
use actix_web::{web, App, HttpServer};
use clap::Parser;
use rdkafka::consumer::BaseConsumer;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing_actix_web::TracingLogger;

#[derive(Clone)]
pub struct TopicManager {
    brokers: Arc<RwLock<String>>,
    group_id: String,
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<KafkaMessage>>>>,
    security: SharedSecurityConfig,
    shutdown: CancellationToken,
}

impl TopicManager {
    pub fn new(
        brokers: String,
        group_id: String,
        security: SharedSecurityConfig,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            brokers: Arc::new(RwLock::new(brokers)),
            group_id,
            channels: Arc::new(RwLock::new(HashMap::new())),
            security,
            shutdown,
        }
    }

    pub async fn get_brokers(&self) -> String {
        self.brokers.read().await.clone()
    }

    pub async fn set_brokers(&self, new_brokers: String) {
        let mut brokers = self.brokers.write().await;
        *brokers = new_brokers;
        // Clear all existing channels — consumers for old broker will stop
        // when all receivers are dropped
        let mut channels = self.channels.write().await;
        channels.clear();
        tracing::info!("Broker config updated, cleared all topic consumers");
    }

    pub async fn subscribe(&self, topic: &str) -> broadcast::Receiver<KafkaMessage> {
        let mut channels = self.channels.write().await;
        if let Some(tx) = channels.get(topic) {
            return tx.subscribe();
        }

        let (tx, rx) = broadcast::channel(1024);
        channels.insert(topic.to_string(), tx.clone());

        let brokers = self.brokers.read().await.clone();
        let security = self.security.read().await.clone();
        let topic_owned = topic.to_string();
        let group_id = self.group_id.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            run_topic_consumer(brokers, topic_owned, group_id, tx, security, shutdown).await;
        });

        rx
    }

    /// Clear all consumers (used during graceful shutdown).
    pub async fn clear(&self) {
        let mut channels = self.channels.write().await;
        channels.clear();
        tracing::info!("All topic consumers cleared");
    }
}

pub type SharedAdminConsumer = Arc<Mutex<BaseConsumer>>;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize structured logging (tracing)
    let json_logging = std::env::var("LOG_FORMAT")
        .ok()
        .map(|v| v == "json")
        .unwrap_or(false);

    if json_logging {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_target(true)
            .with_thread_ids(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_target(true)
            .init();
    }

    // Parse CLI args and merge with env vars
    let args = Args::parse();
    let config = AppConfig::from_args_and_env(args);

    // Validate config at startup
    if let Err(e) = config.validate() {
        tracing::error!(error = %e, "Invalid configuration");
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e));
    }

    tracing::info!(
        brokers = %config.kafka_brokers,
        host = %config.server_host,
        port = config.server_port,
        auth = config.security.is_configured(),
        "Starting Kafka Dashboard"
    );

    // Shared security config (runtime-mutable)
    let security: SharedSecurityConfig = Arc::new(RwLock::new(config.security.clone()));

    // Create admin consumer with security + timeouts (graceful error instead of panic)
    let admin_consumer: SharedAdminConsumer = match create_admin_consumer(
        &config.kafka_brokers,
        &config.security,
    ) {
        Ok(c) => Arc::new(Mutex::new(c)),
        Err(e) => {
            tracing::error!(error = %e, "Failed to create admin consumer");
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("Admin consumer creation failed: {}", e),
            ));
        }
    };

    // Graceful shutdown token
    let shutdown = CancellationToken::new();

    let topic_manager = TopicManager::new(
        config.kafka_brokers.clone(),
        config.consumer_group.clone(),
        security.clone(),
        shutdown.clone(),
    );

    let static_dir = config.static_dir.clone();
    let host = config.server_host.clone();
    let port = config.server_port;

    let shutdown_for_signal = shutdown.clone();
    let tm_for_shutdown = topic_manager.clone();

    let server = HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header();

        App::new()
            .wrap(cors)
            .wrap(TracingLogger::default())
            .app_data(web::Data::new(admin_consumer.clone()))
            .app_data(web::Data::new(topic_manager.clone()))
            .app_data(web::Data::new(security.clone()))
            .route("/api/topics", web::get().to(handlers::topics::get_topics))
            .route("/api/broker", web::get().to(handlers::broker::get_broker))
            .route(
                "/api/broker",
                web::post().to(handlers::broker::set_broker),
            )
            .route(
                "/api/seek/{topic}",
                web::post().to(handlers::seek::seek_messages),
            )
            .route("/ws/{topic}", web::get().to(handlers::ws::ws_handler))
            .service(
                Files::new("/", &static_dir)
                    .index_file("index.html")
                    .prefer_utf8(true),
            )
    })
    .bind((host.as_str(), port))?
    .run();

    // Run server with graceful shutdown on SIGINT
    tokio::select! {
        result = server => {
            tracing::info!("Server stopped");
            result
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received SIGINT, starting graceful shutdown...");
            shutdown_for_signal.cancel();
            tm_for_shutdown.clear().await;
            tracing::info!("Shutdown complete");
            Ok(())
        }
    }
}
