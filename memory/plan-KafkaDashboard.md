# KafkaDashboard — Plan

## Completed (v0.2.0)

### Graceful Shutdown
- [x] Added `tokio_util::sync::CancellationToken` threaded through TopicManager → consumer tasks
- [x] `tokio::select!` in main between HTTP server and `ctrl_c()` signal
- [x] Consumer tasks use `tokio::select!` between `recv()` and `shutdown.cancelled()`
- [x] TopicManager `clear()` aborts all consumer tasks and empties channel map

### Input Validation
- [x] `AppConfig::validate()` — checks brokers non-empty, port non-zero, SASL config consistency, valid security protocols
- [x] `validate_brokers(input)` — verifies host:port format for all comma-separated entries
- [x] `validate_topic_name(name)` — alphanumeric + dots/hyphens/underscores only
- [x] Applied in all API handlers: broker POST, seek POST, WebSocket upgrade

### Structured Logging
- [x] Replaced `env_logger` with `tracing` + `tracing-subscriber`
- [x] `EnvFilter` for per-module log levels via `RUST_LOG`
- [x] JSON output via `LOG_FORMAT=json` environment variable
- [x] `TracingLogger` middleware replacing actix `Logger`
- [x] `tracing::info!`, `tracing::warn!`, `tracing::error!` with structured fields

### SASL/SSL Authentication
- [x] `SecurityConfig` struct: security_protocol, sasl_mechanism, sasl_username, sasl_password
- [x] `SharedSecurityConfig = Arc<RwLock<SecurityConfig>>` — runtime-mutable via POST /api/broker
- [x] `apply_security()` helper applies SASL/SSL config to any `ClientConfig`
- [x] Applied to admin consumer, topic consumers, and seek consumers
- [x] CLI args + env var support for all auth fields
- [x] `BrokerResponse` includes `auth_configured: bool`

### Error Handling
- [x] `DashboardError` enum with `thiserror`: Kafka, Serialization, Config, TopicNotFound, Validation
- [x] `actix_web::ResponseError` impl with proper HTTP status codes
- [x] Replaced all `.expect()` panics with `Result` returns
- [x] `create_admin_consumer()` returns `Result` instead of panicking

### CLI Arguments
- [x] `clap::Parser` with `Args` struct
- [x] All args have env var fallbacks: `KAFKA_BROKERS`, `SERVER_HOST`, `SERVER_PORT`, etc.
- [x] `AppConfig::from_args_and_env()` constructs config from parsed args

### Shared State
- [x] Replaced `Arc<Mutex>` with `Arc<RwLock>` for read-heavy TopicManager state
- [x] Consistent `RwLock` pattern for channels map and brokers string

### Tests
- [x] 24 unit tests: config (17) + models (7)
- [x] 25 integration tests: config validation (6), input validation (6), model serialization (7), error types (3), security config (3)
- [x] `src/lib.rs` re-exports for integration test access

### run.sh
- [x] Modes: default (cargo run), --full (full Docker), --docker (app-only Docker), --confluent (interactive SASL_SSL), --stop (auto-detect), --help
- [x] Env var configuration with sensible defaults
- [x] Auto-passes CLI args from env vars to `cargo run`

## Potential Future Improvements

### From KafkaRealTimeMessageSizeTracker patterns not yet applied:
- [ ] Docker multi-stage build optimization (rust:1.88-bookworm)
- [ ] MongoDB persistence (optional, for message history)
- [ ] Background tasks pattern (stats broadcaster, minute aggregator, auto-discovery, lag collector)
- [ ] Topic selection picker with Start From position
- [ ] Cluster insights overlay (lag/throughput/recommendations)
- [ ] Health report overlay
- [ ] Consumer progress tracking
- [ ] Process metrics widget (CPU/memory via sysinfo)
- [ ] Alert engine with configurable thresholds
- [ ] Historical message browser with presets
- [ ] Theme support (Terminal/Tokyo Night/Dracula/Light)
- [ ] Embedded single-file frontend (include_str!) vs current React build
- [ ] rdkafka statistics callback for broker performance metrics
- [ ] Confluent Cloud optimizations (scoped lag, cached watermarks, throttled polling)
