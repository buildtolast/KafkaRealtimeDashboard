mod config;
mod handlers;
mod kafka;
mod models;

use crate::config::AppConfig;
use crate::kafka::admin::create_admin_consumer;
use crate::kafka::consumer::run_topic_consumer;
use crate::models::KafkaMessage;
use actix_cors::Cors;
use actix_files::Files;
use actix_web::{middleware, web, App, HttpServer};
use rdkafka::consumer::BaseConsumer;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

#[derive(Clone)]
pub struct TopicManager {
    brokers: Arc<Mutex<String>>,
    group_id: String,
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<KafkaMessage>>>>,
}

impl TopicManager {
    pub fn new(brokers: String, group_id: String) -> Self {
        Self {
            brokers: Arc::new(Mutex::new(brokers)),
            group_id,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_brokers(&self) -> String {
        self.brokers.lock().await.clone()
    }

    pub async fn set_brokers(&self, new_brokers: String) {
        let mut brokers = self.brokers.lock().await;
        *brokers = new_brokers;
        // Clear all existing channels — consumers for old broker will stop
        // when all receivers are dropped
        let mut channels = self.channels.lock().await;
        channels.clear();
        log::info!("Broker config updated, cleared all topic consumers");
    }

    pub async fn subscribe(&self, topic: &str) -> broadcast::Receiver<KafkaMessage> {
        let mut channels = self.channels.lock().await;
        if let Some(tx) = channels.get(topic) {
            return tx.subscribe();
        }

        let (tx, rx) = broadcast::channel(1024);
        channels.insert(topic.to_string(), tx.clone());

        let brokers = self.brokers.lock().await.clone();
        let topic_owned = topic.to_string();
        let group_id = self.group_id.clone();

        tokio::spawn(async move {
            run_topic_consumer(brokers, topic_owned, group_id, tx).await;
        });

        rx
    }
}

pub type SharedAdminConsumer = Arc<Mutex<BaseConsumer>>;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = AppConfig::from_env();

    log::info!(
        "Starting Kafka Dashboard on {}:{}",
        config.server_host,
        config.server_port
    );
    log::info!("Kafka brokers: {}", config.kafka_brokers);

    let admin_consumer: SharedAdminConsumer =
        Arc::new(Mutex::new(create_admin_consumer(&config.kafka_brokers)));
    let topic_manager = TopicManager::new(
        config.kafka_brokers.clone(),
        config.consumer_group.clone(),
    );

    let static_dir = config.static_dir.clone();
    let host = config.server_host.clone();
    let port = config.server_port;

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header();

        App::new()
            .wrap(cors)
            .wrap(middleware::Logger::default())
            .app_data(web::Data::new(admin_consumer.clone()))
            .app_data(web::Data::new(topic_manager.clone()))
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
    .run()
    .await
}
