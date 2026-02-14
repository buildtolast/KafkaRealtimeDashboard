# Kafka Dashboard — Audio Presentation Script

> This script accompanies the audio file `presentation.m4a`.
> Duration: ~8 minutes | Voice: Samantha (macOS)

---

## Table of Contents

1. [Introduction](#1-introduction) (0:00)
2. [What It Does](#2-what-it-does) (0:30)
3. [Architecture Overview](#3-architecture-overview) (1:30)
4. [Backend Deep Dive — Rust](#4-backend-deep-dive) (2:45)
5. [Frontend Deep Dive — React](#5-frontend-deep-dive) (4:15)
6. [Docker & Deployment](#6-docker--deployment) (5:30)
7. [Using the Dashboard](#7-using-the-dashboard) (6:15)
8. [Developing Further](#8-developing-further) (7:15)
9. [Closing](#9-closing) (8:00)

---

## Full Transcript

### 1. Introduction

Welcome to the Kafka Dashboard project walkthrough. This is a real-time Kafka topic monitoring dashboard built with a Rust backend and a React frontend. It lets you visualize live Kafka messages as they flow through your topics, all from a single browser window. In this presentation, we'll cover the architecture, how to use it, and how to extend it for your own needs.

### 2. What It Does

Kafka Dashboard gives you a live window into your Kafka cluster. You select which topics to monitor, and each one opens as its own panel showing messages as they arrive in real time. You can choose from three layout modes: Mosaic, which gives you draggable and resizable tiling windows; Tabs, for focused single-topic viewing; or Grid, which auto-fills a responsive column layout.

Beyond live streaming, the dashboard supports pausing and resuming message flow, seeking to historical timestamps, exporting messages to CSV, a timeseries chart showing message rates per topic, global and per-topic date filtering, and a full-text search that collates results across all your monitored topics with highlighted matches.

The entire stack — Kafka broker, the dashboard application, and a sample data producer — runs with a single Docker Compose command.

### 3. Architecture Overview

The system is composed of three Docker containers orchestrated by Docker Compose.

First is the Kafka broker, running Apache Kafka 3.7 in KRaft mode. That means no Zookeeper — the broker handles its own metadata via the Raft consensus protocol. It listens on port 9092 internally for other containers, and on port 9094 externally for local development.

Second is the Dashboard container. This is a single Rust binary compiled with actix-web that serves both the REST API and the React single-page application as static files. Everything runs on port 3001.

Third is the Seed Producer. This is a simple bash script running inside a Kafka container image. It creates four sample topics — orders, users, notifications, and logs — each with two partitions. Then it enters a loop, producing one JSON message per topic every two seconds. This gives you live data to watch the moment you start the stack.

The key architectural insight is that the Rust backend acts as a message multiplexer. It maintains one Kafka consumer per topic, regardless of how many browser tabs are connected. Messages fan out from a single consumer to all connected WebSocket clients through Tokio's broadcast channels.

### 4. Backend Deep Dive — Rust

The heart of the backend is the Topic Manager struct. It holds a map of topic names to broadcast channel senders, protected by an async mutex. When a WebSocket client connects for a topic, the Topic Manager checks if a consumer already exists. If not, it spawns a new Tokio task that creates a Stream Consumer, subscribes to the topic, and begins forwarding every message into the broadcast channel. If a consumer already exists, the client simply subscribes to the existing broadcast sender and gets its own independent receiver.

This design means zero duplicate Kafka reads. Whether one client or fifty are watching the same topic, there's exactly one Kafka consumer doing the work.

The WebSocket session loop uses Tokio select with three branches. Branch one receives Kafka messages from the broadcast channel and serializes them to JSON. Branch two handles incoming WebSocket control frames like ping, pong, and close. Branch three runs a heartbeat timer that pings the client every ten seconds to detect dead connections.

For historical message retrieval, the seek endpoint creates a temporary Base Consumer — separate from the live Stream Consumer — that uses Kafka's offsets-for-times API to jump to a specific timestamp. It polls up to 200 messages within a five-second deadline, returns them as JSON, and then the consumer is dropped. This ensures seeking never interferes with live streaming.

Broker switching at runtime is handled by clearing the entire channel map. Old consumers exit naturally when their broadcast senders are dropped, and new consumers are lazily spawned on the next WebSocket connection.

The REST API exposes five endpoints: GET topics to list non-internal Kafka topics, GET and POST broker for runtime configuration, POST seek for historical replay, and a WebSocket upgrade endpoint per topic.

### 5. Frontend Deep Dive — React

The frontend is built with React 18, TypeScript, and Vite. It's organized around a few key concepts.

The App component manages the top-level state: broker configuration, topic selection, global search, and date filtering. Once topics are selected, it renders the Layout Manager.

The Layout Manager provides a view mode toggle between Monitor and Chart, and within Monitor mode, three layout options. Mosaic layout uses react-mosaic-component — the same tiling library used by Palantir's data tools — giving you draggable, resizable panels. Tabs layout shows one topic at a time with a scrollable tab bar. Grid layout uses CSS Grid with auto-fill and a 480-pixel minimum column width for responsive display. If you select more than six topics, the app automatically switches to Tabs mode to keep things manageable.

Each Topic Window connects to its own WebSocket via the use-Kafka-Stream hook. This custom hook manages the WebSocket lifecycle with automatic reconnection on a three-second delay, pause and resume functionality using a ref rather than state to avoid re-render costs, seek-to-timestamp via the REST API, and a 500-message cap with oldest-first eviction.

The Message List component uses react-virtuoso for virtualized rendering, so even thousands of messages scroll smoothly. Messages are color-coded with a rotating 20-color palette on a dark background. Messages longer than 200 characters are truncated with an expand and collapse toggle.

The Message Tracking Context is a shared React context that records message rates and buffers messages across all topics. This powers both the Chart View — which uses Recharts to render a live line chart of messages per five-second bucket over a rolling five-minute window — and the Search Results Panel, which collates matches from all topics with highlighted search terms.

### 6. Docker & Deployment

The Docker build uses a three-stage pipeline to keep the final image small — around 85 megabytes.

Stage one uses node 18 alpine to install dependencies and run Vite's production build, outputting the static frontend assets.

Stage two uses the Rust 1.88 Bookworm image. It employs a dependency caching trick: first it copies just Cargo.toml and Cargo.lock, creates a dummy main.rs, and runs cargo build release. This compiles all dependencies and caches them in Docker's layer cache. Then it copies the real source code and builds again — but this time only the application code recompiles, taking about 20 seconds instead of five minutes.

A key detail: librdkafka, the C library that rdkafka wraps, is compiled from source using the cmake-build Cargo feature. Debian Bookworm's package manager only has version 1.9, but the Rust crate requires version 2.12 or higher. Static compilation adds about three minutes but produces a fully self-contained binary.

Stage three copies just the binary and static files into a minimal Debian slim image with only the SSL certificates and libraries needed at runtime.

To deploy, simply run docker compose up -d, and the full stack starts: Kafka broker, dashboard, and seed producer. The dashboard is available at localhost port 3001.

### 7. Using the Dashboard

Here's a step-by-step guide to using the dashboard.

Step one: Launch the stack with docker compose up dash d, then open your browser to localhost 3001.

Step two: You'll see the topic selector. Check the topics you want to monitor, or click Select All. Then click the Open Topics button.

Step three: The monitoring view opens in Mosaic layout by default. You'll see live messages streaming in with color-coded entries showing both keys and values.

Step four: Explore the layout modes. Click Tabs for single-topic focus, Grid for a responsive column view, or stay in Mosaic for full drag-and-resize control.

Step five: Toggle to Chart view to see a live timeseries of message rates per topic, updated every two seconds.

Step six: Use the search bar in the header. Type a query and press Enter to open the search results panel, which shows matches from all topics with highlighted text grouped by topic name.

Step seven: Use date filtering. The global date filter in the header applies to all windows. Each individual window also has a local filter that overrides the global one, giving you fine-grained control.

Step eight: You can pause any topic's live stream, seek to a historical timestamp, clear messages, or export the current view to CSV at any time.

If you need to point the dashboard at a different Kafka cluster, edit the broker address in the header and click Connect. The topics will refresh automatically without restarting anything.

### 8. Developing Further

To develop locally, start just the Kafka broker with docker compose up kafka dash d. Then run the Rust backend with KAFKA_BROKERS equals localhost 9094 cargo run. For the frontend, cd into the frontend directory, run npm install, then npm run dev. Vite's dev server starts on port 5173 with hot reload and proxies API requests to the Rust backend on port 3001.

Here are some ideas for extending the project. You could add topic creation and deletion through the UI using Kafka's admin API. You could implement message production, letting users send test messages directly from the dashboard. You could add consumer group monitoring to show lag per partition. You could build in alerting — for example, triggering notifications when message rates exceed a threshold. Schema registry integration would let you deserialize Avro or Protobuf messages. Authentication and multi-tenancy would make it suitable for shared environments. And persistent message storage with a database would enable historical queries beyond what Kafka retention provides.

The codebase is modular. Backend handlers are in separate files under the handlers directory. Frontend components follow a clean separation between hooks, contexts, and presentational components. Adding a new feature typically means creating a new handler on the backend, a new hook or context on the frontend, and a new component to render it.

### 9. Closing

That wraps up the Kafka Dashboard walkthrough. To recap: it's a real-time Kafka monitoring tool with a Rust backend that multiplexes Kafka consumers via broadcast channels, a React frontend with three layout modes and rich interactivity, and a Docker Compose deployment that gets you running in one command. The architecture is clean, the codebase is modular, and there's plenty of room to extend it further. Thanks for listening, and happy streaming!
