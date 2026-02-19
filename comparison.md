# KafkaDashboard — Architecture & Design Comparison

A comparison of KafkaDashboard against best practices implemented in [KafkaRealTimeMessageSizeTracker](https://github.com/buildtolast/KafkaRealTimeMessageSizeTracker).

---

## What It Is

A **real-time Kafka topic monitoring dashboard** — Rust backend (actix-web + rdkafka) with a React/TypeScript frontend (react-mosaic + Vite). Users connect to any Kafka cluster, select topics, and watch live messages in resizable mosaic/tab/grid layouts with search, filtering, seek-by-timestamp, and charting.

---

## Architecture Overview

```
src/
├── main.rs          — TopicManager + server bootstrap (127 lines)
├── config.rs        — AppConfig from env vars (25 lines)
├── models.rs        — KafkaMessage, SeekRequest, BrokerRequest/Response (33 lines)
├── handlers/
│   ├── topics.rs    — GET /api/topics
│   ├── broker.rs    — GET/POST /api/broker
│   ├── seek.rs      — POST /api/seek/{topic}
│   └── ws.rs        — WebSocket /ws/{topic}
└── kafka/
    ├── admin.rs     — AdminConsumer (BaseConsumer) for metadata
    └── consumer.rs  — StreamConsumer per topic
```

**Frontend**: React 18 + TypeScript + Vite, served as static files via `actix-files`.

---

## Core Design Pattern: Broadcast Fan-Out

The `TopicManager` is the central pattern — nearly identical to the one evolved in KafkaRealTimeMessageSizeTracker:

| Aspect | KafkaDashboard | KafkaRealTimeMessageSizeTracker |
|--------|---------------|--------------------------------|
| **Consumer map** | `Arc<Mutex<HashMap<String, Sender>>>` | `Arc<RwLock<HashMap<String, TopicConsumer>>>` |
| **Consumer per topic** | 1 StreamConsumer -> 1 broadcast::Sender | 1 StreamConsumer -> 1 broadcast::Sender + processor task |
| **Fan-out** | N WebSocket clients subscribe to same Sender | N WS clients + event processor task |
| **Lazy spawn** | Yes — consumer created on first subscribe | Yes — same pattern |
| **Cleanup** | `channels.clear()` on broker change | abort task handles + clear on broker change |
| **Start position** | Always `latest` | Configurable (latest/beginning/timestamp/offset) |
| **Buffer size** | 1024 messages | 1024 messages |

---

## What's Present vs. What's Missing

### Present

- **Clean module separation** — handlers, kafka, models, config in own files
- **Broadcast fan-out** — one consumer per topic, N subscribers
- **WebSocket heartbeat** — 10s ping interval with dead-connection detection
- **`tokio::select!`** in WS session — broadcast rx + client msgs + heartbeat
- **Runtime broker switching** — POST /api/broker clears channels, creates new AdminConsumer
- **Docker multi-stage build** — Node -> Rust -> Debian slim (~85MB)
- **Feature flags** — `static-kafka` / `dynamic-kafka`
- **Lagged client handling** — logs warning, client catches up naturally

### Missing (implemented in KafkaRealTimeMessageSizeTracker)

| Gap | Impact | KafkaRealTimeMessageSizeTracker Has |
|-----|--------|-----|
| **No graceful shutdown** | Consumers run forever, no cleanup on SIGINT | `CancellationToken` + `tokio::select!` on all 5 tasks |
| **No input validation** | Only broker empty check; no topic name, timestamp, or format validation | `validate_brokers()`, `validate_topic_name()`, `validate_thresholds()`, `AppConfig::validate()` |
| **No structured logging** | `env_logger` + string interpolation only | `tracing` + `tracing-subscriber` with JSON output, `EnvFilter`, `TracingLogger` |
| **Zero tests** | No unit or integration tests anywhere | 97 unit + 30 integration = 127 total |
| **No error types** | String-based errors via `ErrorInternalServerError("msg")` | `TrackerError` (thiserror) with actix `ResponseError` impl |
| **No CLI args** | Environment variables only, no `clap` | Full clap `Args` struct with `--brokers`, `--port`, etc. |
| **`Arc<Mutex>` everywhere** | Works but suboptimal for read-heavy workloads | `Arc<RwLock>` for read-heavy state, `Mutex` only where needed |
| **`.expect()` on consumer creation** | Panics on failure instead of graceful error | `match` + error logging + graceful return |
| **No background tasks** | Only spawned consumer tasks, no periodic workers | 5 background tasks (stats broadcast, aggregation, auto-discovery, lag collection, admin polling) |
| **No connection timeouts** | Kafka ClientConfig uses defaults | `apply_timeouts()` — socket 10s, setup 5s, request 10s on all 8 ClientConfig creation points |
| **No security/auth** | No SASL/SSL support | Full SASL_SSL/SASL_PLAINTEXT/SSL + PLAIN/SCRAM-SHA-256/512 via CLI/env/web UI |
| **No lib.rs** | Binary-only crate, can't write integration tests against modules | `lib.rs` re-exports shared modules for test access |

---

## Patterns That Could Be Adopted Directly

### 1. Graceful Shutdown

Add `tokio_util::CancellationToken`, pass clones to spawned consumer tasks, wrap consumer loop in `tokio::select!` with cancellation:

```rust
tokio::select! {
    _ = shutdown.cancelled() => { break; }
    result = consumer.recv() => { /* process */ }
}
```

### 2. Input Validation

Add `validate_topic_name()` before `subscribe()`, validate broker format in `set_broker()`, validate timestamps in seek handler.

### 3. Structured Logging

Swap `env_logger` -> `tracing` + `tracing-subscriber`, `middleware::Logger` -> `TracingLogger::default()`.

### 4. Error Types

Create `DashboardError` with `thiserror`, implement `actix_web::ResponseError`, replace all string errors.

### 5. Config Validation

Add `AppConfig::validate()` called at startup, check port != 0, brokers non-empty, etc.

### 6. Tests

Create `src/lib.rs` to export shared modules, write integration tests against TopicManager, models, seek logic.

---

## API Surface Comparison

| KafkaDashboard | KafkaRealTimeMessageSizeTracker |
|---|---|
| 4 routes | 20+ routes |
| `/api/topics` (list) | `/api/topics`, `/api/topics/available`, `/api/topics/toggle` |
| `/api/broker` (get/set) | `/api/broker` (get/set) + `/api/broker/discover` |
| `/api/seek/{topic}` | `/api/browse/{topic}` + presets + download |
| `/ws/{topic}` | `/ws/live` (all topics multiplexed) |
| — | `/api/stats/{topic}`, `/api/alerts/*`, `/api/history/*` |
| — | `/api/cluster/insights`, `/api/consumer/progress/*` |
| — | `/api/persisted/*`, `/api/metrics/process`, `/api/snapshot` |

KafkaDashboard is a focused message viewer; KafkaRealTimeMessageSizeTracker is a full observability platform. The **core plumbing** (TopicManager, broadcast, WebSocket) is the same DNA, evolved.

---

## Code Quality Assessment

| Category | Grade | Notes |
|----------|-------|-------|
| **Architecture** | B+ | Clean separation, correct async patterns, good fan-out design |
| **Error Handling** | C | String-based, `.expect()` panics, no custom error types |
| **Validation** | D | Only broker empty check |
| **Testing** | F | Zero tests |
| **Logging** | C | `env_logger` only, no structured output |
| **Shutdown** | D | No graceful shutdown, consumers leak on exit |
| **Security** | D | No auth, no SASL/SSL, open CORS |
| **Frontend** | A- | React + TypeScript, mosaic layout, virtualized lists, charts |
| **Docker** | A | Clean 3-stage build, small image, multiple compose variants |
| **Documentation** | B+ | Good README with architecture diagrams |

**Overall: B-** — solid architecture foundation, missing production-hardening that KafkaRealTimeMessageSizeTracker has since implemented (graceful shutdown, validation, tracing, tests, error types).
