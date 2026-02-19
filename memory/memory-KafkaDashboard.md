# KafkaDashboard

## Overview
- **Location**: ~/GIT/KafkaDashboard
- **Purpose**: Real-time Kafka topic monitoring dashboard with live message streaming, seek/browse, and multi-topic WebSocket consumers
- **Stack**: Rust (actix-web 4, rdkafka 0.37, tokio 1) + React frontend (react-mosaic + Vite)
- **Edition**: 2021
- **Binary name**: `kafka-dashboard`

## Running
- Dev: `./run.sh` (defaults: localhost:9094, port 3001)
- Full stack Docker: `./run.sh --full` (Kafka + Dashboard + Seed Producer)
- App-only Docker: `./run.sh --docker` (connects to external Kafka)
- Confluent Cloud: `./run.sh --confluent` (prompts for credentials)
- Stop: `./run.sh --stop` (auto-detects and stops all modes)
- Direct: `cargo run -- --brokers localhost:9094 --port 3001`
- Dashboard URL: http://localhost:3001, Kafka external: localhost:9094

## CLI Arguments (clap)
- `--brokers` (default: localhost:9094) — Kafka broker addresses
- `--port` (default: 3001) — Server port
- `--host` (default: 127.0.0.1) — Server host
- `--static-dir` (default: ./frontend/dist) — Frontend dist directory
- `--security-protocol` (optional) — Kafka security protocol (SASL_SSL, SASL_PLAINTEXT, SSL)
- `--sasl-mechanism` (optional) — SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- `--sasl-username` (optional) — SASL username / API Key
- `--sasl-password` (optional) — SASL password / API Secret

## Environment Variable Overrides
KAFKA_BROKERS, SERVER_HOST, SERVER_PORT, CONSUMER_GROUP, STATIC_DIR, KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, RUST_LOG, LOG_FORMAT

## Architecture

### Backend Modules
```
src/
├── main.rs              — Server bootstrap, TopicManager (Arc<RwLock>), SharedSecurityConfig, CancellationToken, graceful shutdown via tokio::select!, routes
├── config.rs            — Args (clap) + AppConfig (env var fallback), SecurityConfig (SASL/SSL auth), SharedSecurityConfig, validate(), 17 unit tests
├── error.rs             — DashboardError (thiserror) with actix ResponseError impl (5 variants)
├── models.rs            — KafkaMessage, TopicsResponse, BrokerRequest, BrokerResponse (auth_configured), SeekRequest, 7 unit tests
├── lib.rs               — Re-exports: pub mod config, error, models (for integration tests)
├── kafka/
│   ├── mod.rs           — apply_security() + apply_timeouts() helpers
│   ├── admin.rs         — create_admin_consumer() with SecurityConfig, returns Result (no panics)
│   └── consumer.rs      — run_topic_consumer() with SecurityConfig + CancellationToken, tokio::select! shutdown
├── handlers/
│   ├── mod.rs           — Handler module declarations
│   ├── topics.rs        — GET /api/topics (list topics via admin consumer)
│   ├── broker.rs        — GET/POST /api/broker (with validate_brokers, security config, auth_configured)
│   ├── seek.rs          — POST /api/seek/{topic} (validate_topic_name, security config, cap max_messages at 1000)
│   └── ws.rs            — WebSocket /ws/{topic} (validate_topic_name, structured tracing)
tests/
└── integration_tests.rs — 25 integration tests (config validation, input validation, model serialization, error types, security config)
```

### Key Patterns
- **TopicManager**: `Arc<RwLock<HashMap<String, (Sender, JoinHandle)>>>` — lazy consumer spawn per topic, holds SharedSecurityConfig + CancellationToken + brokers
- **SharedSecurityConfig**: `Arc<RwLock<SecurityConfig>>` — runtime-mutable via web UI POST /api/broker
- **Graceful shutdown**: `CancellationToken` threaded through TopicManager → spawned consumer tasks, `tokio::select!` in main between server and ctrl_c
- **Input validation**: `validate_brokers()` (host:port format), `validate_topic_name()` (alphanumeric + dots/hyphens/underscores), `AppConfig::validate()` at startup
- **Error handling**: `DashboardError` enum with thiserror derive + actix ResponseError impl, no `.expect()` panics
- **Structured logging**: `tracing` + `tracing-subscriber` with EnvFilter, JSON output via `LOG_FORMAT=json`, `TracingLogger` middleware
- **Feature flags**: `static-kafka` (cmake-build + ssl, default) / `dynamic-kafka` (dynamic-linking + ssl)
- **Connection timeouts**: `apply_timeouts()` — socket 10s, connection setup 5s, request 10s, metadata max age 60s

### Data Flow
```
Kafka Topic -> StreamConsumer (per topic, spawned by TopicManager)
  -> broadcast::Sender<KafkaMessage>
  -> WebSocket handler subscribes to broadcast::Receiver
  -> JSON serialized -> WebSocket client

Seek: POST /api/seek/{topic} -> short-lived BaseConsumer -> seek by timestamp -> return messages
Topics: GET /api/topics -> AdminConsumer (BaseConsumer) -> fetch_metadata -> topic list
Broker: POST /api/broker -> update brokers + security config, clear all consumers
```

## API Routes
| Route | Method | Purpose |
|-------|--------|---------|
| `/` | GET | Serve static frontend files |
| `/api/topics` | GET | List all Kafka topics |
| `/api/broker` | GET | Current broker address + auth status |
| `/api/broker` | POST | Update broker + security config |
| `/api/seek/{topic}` | POST | Seek messages by timestamp (SeekRequest) |
| `/ws/{topic}` | GET | WebSocket stream for live messages |

## Frontend
- **Framework**: React with react-mosaic for resizable panel layout
- **Build**: Vite, output to `frontend/dist/`
- **Served via**: actix-files static file serving

## Tests (49 total)
- `config::tests` (17): default_config_values, args_override, security_config_not_configured, security_config_configured, security_config_from_args, security_config_serde_roundtrip, validate_config_valid, validate_config_empty_brokers, validate_config_zero_port, validate_config_sasl_without_credentials, validate_config_invalid_protocol, validate_brokers_valid, validate_brokers_empty, validate_brokers_no_port, validate_topic_name_valid, validate_topic_name_empty, validate_topic_name_invalid_chars
- `models::tests` (7): kafka_message_roundtrip, kafka_message_null_fields, topics_response_ser, broker_request_deser, broker_response_ser, seek_request_deser, seek_request_with_max
- Integration tests (25): config validation (6), input validation helpers (6), model serialization (7), error types (3), security config (3)

## Docker
- **Dockerfile**: Multi-stage (rust builder -> runtime with frontend dist)
- **docker-compose.yml**: App only (KAFKA_BROKERS env var)
- **docker-compose.full.yml**: Kafka (KRaft) + Dashboard + Seed Producer
- **scripts/seed-topics.sh**: Creates 4 topics (orders/users/notifications/logs), produces sample JSON messages in loop

## Dependencies
actix-web 4, actix-ws 0.3, actix-files 0.6, actix-cors 0.7, actix-rt 2, rdkafka 0.37, tokio 1, tokio-stream 0.1, tokio-util 0.7, futures-util 0.3, serde 1, serde_json 1, chrono 0.4, log 0.4, clap 4, thiserror 2, tracing 0.1, tracing-subscriber 0.3, tracing-actix-web 0.7

## Recent Changes (v0.2.0)
- Added graceful shutdown (CancellationToken + tokio::select!)
- Added input validation (brokers, topic names, config)
- Added structured logging (tracing + JSON output)
- Added SASL/SSL authentication support
- Added error types (DashboardError with thiserror)
- Added CLI argument parsing (clap)
- Added run.sh script (cargo, Docker, Confluent Cloud modes)
- Replaced Arc<Mutex> with Arc<RwLock> for read-heavy state
- Replaced .expect() panics with Result returns
- Added 49 tests (24 unit + 25 integration)
