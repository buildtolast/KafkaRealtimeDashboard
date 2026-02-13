# Kafka Dashboard

Real-time Kafka topic monitoring dashboard with a Rust backend and React frontend. Each topic gets its own draggable, resizable window panel showing live messages as they arrive.

## Features

- **Topic selector** — choose which topics to monitor before opening panels
- **Tiling window layout** — drag, resize, and rearrange topic panels using react-mosaic
- **Live message streaming** — WebSocket per topic with auto-reconnect
- **Pause / Play** — pause live message ingestion per topic, resume anytime
- **Seek by timestamp** — jump to historical messages from a specific date/time
- **Key + Value display** — each message shows both key and value as labeled rows
- **Color-coded messages** — 20-color palette on a black background for readability
- **Export to CSV** — download messages per topic as a CSV file
- **Global search** — filter messages across all topic windows by key or payload text, with highlighted matches
- **Runtime broker config** — change Kafka broker address from the UI without restarting
- **Virtualized scrolling** — handles large message volumes via react-virtuoso

## Architecture

```
Kafka Broker
    │
    ▼
Rust Backend (actix-web + rdkafka)
    │              │              │
    │ REST         │ WebSocket    │ REST
    │ /api/topics  │ /ws/{topic}  │ /api/broker, /api/seek/{topic}
    ▼              ▼              ▼
React Frontend (Vite + react-mosaic)
    │
    ▼
Browser: tiling panel per topic, live-updating
```

- **One Kafka consumer per topic**, shared across all connected WebSocket clients via `tokio::sync::broadcast`
- **REST endpoints**:
  - `GET /api/topics` — list all non-internal Kafka topics
  - `GET /api/broker` — get current broker config
  - `POST /api/broker` — update broker config at runtime
  - `POST /api/seek/{topic}` — fetch historical messages from a timestamp
- **WebSocket endpoint** `ws://.../ws/{topic}` streams messages in real-time

## Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | Rust, actix-web 4, actix-ws, rdkafka 0.37 |
| Async runtime | Tokio |
| Frontend | React 18, TypeScript, Vite |
| Window layout | react-mosaic-component |
| Message list | react-virtuoso |
| Theme | Blueprint.js (dark) |
| Containerization | Docker, Docker Compose |

## Quick Start (Docker)

```bash
docker compose up -d
```

This starts three services:

| Service | Description |
|---------|-------------|
| **kafka** | Apache Kafka 3.7 broker (KRaft mode, no Zookeeper) |
| **dashboard** | Rust backend serving the React SPA on port 3001 |
| **seed** | Creates 4 test topics and produces sample messages every 2 seconds |

Open **http://localhost:3001** in your browser.

To stop:

```bash
docker compose down
```

## Local Development

### Prerequisites

- Rust 1.88+ (for `time` crate compatibility)
- Node.js 18+
- A running Kafka broker (use `docker compose up kafka -d` to start just Kafka)

### Backend

```bash
KAFKA_BROKERS=localhost:9094 cargo run
```

The backend starts on `http://localhost:3001`.

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Vite dev server starts on `http://localhost:5173` with proxy to the backend.

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9094` | Kafka bootstrap servers (can also be changed at runtime via UI) |
| `SERVER_HOST` | `127.0.0.1` | HTTP server bind address |
| `SERVER_PORT` | `3001` | HTTP server port |
| `STATIC_DIR` | `./frontend/dist` | Path to built React assets |
| `CONSUMER_GROUP` | `kafka-dashboard` | Kafka consumer group prefix |
| `RUST_LOG` | `info` | Log level (debug, info, warn, error) |

## Project Structure

```
├── Cargo.toml                 # Rust dependencies and feature flags
├── Dockerfile                 # Multi-stage build (Node + Rust + slim runtime)
├── docker-compose.yml         # Kafka + Dashboard + Seed producer
├── scripts/
│   └── seed-topics.sh         # Creates test topics and produces sample data
├── src/
│   ├── main.rs                # Actix server, TopicManager, route wiring
│   ├── config.rs              # Environment-based configuration
│   ├── models.rs              # KafkaMessage, TopicsResponse, BrokerRequest/Response, SeekRequest
│   ├── kafka/
│   │   ├── admin.rs           # List topics via fetch_metadata
│   │   └── consumer.rs        # Per-topic StreamConsumer with broadcast channel
│   └── handlers/
│       ├── broker.rs          # GET/POST /api/broker — runtime broker config
│       ├── seek.rs            # POST /api/seek/{topic} — historical message fetch
│       ├── topics.rs          # GET /api/topics
│       └── ws.rs              # WebSocket upgrade and session loop
└── frontend/
    ├── package.json
    ├── vite.config.ts          # Dev proxy to Rust backend
    └── src/
        ├── App.tsx             # Top-level: broker config, search bar, topic flow
        ├── types.ts            # KafkaMessage, TopicsResponse, BrokerResponse
        ├── hooks/
        │   ├── useTopics.ts    # Fetch topic list
        │   └── useKafkaStream.ts  # WebSocket per topic, pause/play, seek
        └── components/
            ├── BrokerConfig.tsx   # Broker address input + connect
            ├── MosaicLayout.tsx   # Tiling window manager
            ├── TopicSelector.tsx  # Topic checkbox selection screen
            ├── TopicWindow.tsx    # Single topic panel: pause, seek, CSV export
            ├── MessageList.tsx    # Virtualized message list with search highlighting
            └── ConnectionStatus.tsx
```

## Cargo Feature Flags

| Feature | Description |
|---------|-------------|
| `static-kafka` (default) | Compiles librdkafka from source via cmake. Works everywhere, slower build. |
| `dynamic-kafka` | Links against system librdkafka. Faster build, requires librdkafka >= 2.12.1 installed. |

The Docker build uses `static-kafka`. For local development on systems with librdkafka installed:

```bash
cargo run --no-default-features --features dynamic-kafka
```
