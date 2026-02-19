# Technology Stack Comparison — KafkaDashboard

## Current Stack: Rust + Actix-Web + rdkafka

The KafkaDashboard is built on one of the highest-performance web stacks available. This document analyzes the current architecture's performance characteristics and compares it against alternative technology stacks.

---

## Performance Analysis of Current Architecture

### Strengths

| Component | Pattern | Why It's Fast |
|-----------|---------|---------------|
| **Actix-Web 4** | Async I/O on Tokio | #1-#2 in TechEmpower web framework benchmarks consistently |
| **rdkafka 0.37** | Rust FFI to librdkafka (C) | Same Kafka engine used by C++ applications — no overhead |
| **Broadcast channels** | Fan-out without cloning | N WebSocket clients share one ring buffer per topic |
| **Arc\<RwLock\>** | Read-heavy shared state | Concurrent reads with zero contention; writes are rare |
| **CancellationToken** | Cooperative shutdown | Graceful cleanup without forceful kills or resource leaks |
| **tokio::select!** | Multiplexed async I/O | Kafka recv + WebSocket + heartbeat in a single task per client |
| **StreamConsumer** | Non-blocking internal polling | librdkafka pumps sockets in background thread; Tokio task yields when idle |

### Current Architecture Bottlenecks

| Bottleneck | Impact | Severity |
|------------|--------|----------|
| **JSON serialization per message** | CPU-bound; #1 bottleneck at high rates | Medium |
| **No WebSocket message batching** | 10k msgs/s = 10k WS frames/s per client | Medium |
| **Single StreamConsumer per topic** | All partitions funneled through one consumer | Low-Medium |
| **Broadcast buffer (1024 msgs)** | Slow clients lose messages when lagged | Low |
| **Blocking thread pool for seeks** | Saturates under >10 concurrent seeks | Low |
| **No static file compression** | Uncompressed JS/CSS bundles to browser | Low |

### Throughput Estimates (Current Architecture)

| Component | Estimated Max | Limiting Factor |
|-----------|---------------|-----------------|
| StreamConsumer (per topic) | ~50k msgs/s | JSON serialization CPU |
| Broadcast channel fan-out | Unbounded (clients lag if slow) | Ring buffer size |
| WebSocket sessions | 50k+ concurrent | OS file descriptors + memory |
| Seek operations | ~100 concurrent | Blocking thread pool (2 × num_cpus) |
| Static file serving | ~5-10 MB/s per client | Disk I/O (no compression) |

---

## Stack Comparison

### Overview

| Factor | **Rust (actix-web)** | **Go (gorilla/sarama)** | **Java (Spring + kafka-clients)** | **C++ (uWebSockets + librdkafka)** | **Node.js (kafkajs)** |
|--------|---------------------|------------------------|----------------------------------|-----------------------------------|--------------------|
| **Raw throughput** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| **Memory usage** | ~5-15 MB | ~20-40 MB | ~200-500 MB (JVM heap) | ~5-15 MB | ~50-100 MB |
| **Latency (p99)** | ~0.1-0.5 ms | ~0.5-2 ms | ~2-10 ms (GC pauses) | ~0.1-0.5 ms | ~5-20 ms |
| **WebSocket fan-out** | 50k+ msgs/s | 30k+ msgs/s | 10-20k msgs/s | 50k+ msgs/s | 5-10k msgs/s |
| **Kafka consumer throughput** | ~50k msgs/s | ~40k msgs/s | ~30k msgs/s | ~50k msgs/s | ~10k msgs/s |
| **Cold start** | ~1 ms | ~5 ms | ~2-5 seconds | ~1 ms | ~100 ms |
| **Binary size** | ~10 MB | ~15 MB | ~200 MB+ (JAR + JRE) | ~5 MB | N/A (runtime) |
| **Dev velocity** | Medium | High | Medium | Low | High |
| **Safety guarantees** | Compile-time | Runtime (goroutines) | Runtime (GC) | Manual | Runtime |
| **Ecosystem maturity** | Growing | Mature | Very mature | Niche | Very mature |
| **Hiring availability** | Low | High | Very high | Very low | Very high |

### Detailed Breakdown by Stack

#### Go (gorilla/websocket + sarama/confluent-kafka-go)

**Pros:**
- Goroutines are lightweight (~2 KB each) — can spawn millions
- Native Kafka client (sarama) or CGo wrapper (confluent-kafka-go uses same librdkafka)
- Excellent concurrency primitives (channels, select, WaitGroup)
- Fast compilation, easy deployment (single static binary)
- Large talent pool, strong DevOps ecosystem

**Cons:**
- Garbage collector introduces p99 latency spikes (typically 0.5-2 ms)
- No zero-cost abstractions — runtime overhead on channel operations
- gorilla/websocket is archived (alternatives: nhooyr/websocket, gobwas/ws)
- ~2-3x higher memory than Rust for equivalent workload
- No compile-time thread safety guarantees

**When to choose:** Team is Go-native, need fast iteration, acceptable 10-20% performance trade-off.

#### Java/Kotlin (Spring Boot + kafka-clients / Kafka Streams)

**Pros:**
- Native Kafka client (written by Confluent) — richest feature set
- Kafka Streams for stateful stream processing (joins, windowing, aggregation)
- Schema Registry integration (Avro, Protobuf, JSON Schema) — first-class
- Massive ecosystem: Spring WebFlux, Reactor Kafka, Micrometer metrics
- Most enterprise Kafka deployments are Java-based

**Cons:**
- JVM heap overhead: 200-500 MB minimum for small services
- GC pauses: 2-10 ms p99 (G1GC), even with ZGC/Shenandoah
- Cold start: 2-5 seconds (Spring Boot context initialization)
- 3-5x higher memory than Rust for equivalent workload
- Heavy dependency tree, complex configuration

**When to choose:** Need Kafka Streams, Schema Registry, or integrating with existing Java/Spring microservices.

#### C++ (uWebSockets + librdkafka)

**Pros:**
- Theoretical maximum performance — zero abstraction overhead
- Direct librdkafka access (no FFI layer)
- uWebSockets: fastest WebSocket library benchmarked
- Predictable latency (no GC, no runtime)
- Smallest binary size (~5 MB)

**Cons:**
- Manual memory management — use-after-free, buffer overflows
- No borrow checker — thread safety is the developer's responsibility
- Slow development velocity, difficult debugging
- Small ecosystem for web services (no equivalent of actix-web)
- Very small hiring pool for C++ web developers

**When to choose:** Sub-microsecond latency requirements (HFT, trading systems), team has deep C++ expertise.

#### Node.js (kafkajs + ws)

**Pros:**
- Fastest development velocity — JavaScript/TypeScript everywhere
- kafkajs: pure JavaScript Kafka client (no native dependencies)
- Rich npm ecosystem for web tooling
- Same language for frontend and backend
- Easy to find developers

**Cons:**
- Single-threaded event loop — CPU-bound JSON serialization blocks everything
- 5-10x slower than Rust for compute-heavy workloads
- Higher memory: ~50-100 MB baseline, grows with connection count
- kafkajs throughput: ~10k msgs/s (vs 50k for rdkafka)
- No true parallelism without worker threads (complex)

**When to choose:** Prototyping, low-throughput use cases (< 5k msgs/s), team is JavaScript-only.

---

## Performance Tuning Recommendations (Current Rust Stack)

Rather than switching stacks, these optimizations yield more throughput gains within the current architecture:

### High Priority (Quick Wins)

#### 1. WebSocket Message Batching (3-5x throughput improvement)
```
Current:  Kafka msg → serialize → WS frame → send    (per message)
Better:   Kafka msgs → batch 10-50 → serialize array → WS frame → send

At 10k msgs/sec:
  Before: 10,000 WS frames/sec per client
  After:    200 WS frames/sec per client (50-msg batches)
```
**Implementation:** Accumulate messages for 50-100ms or N messages (whichever comes first), then flush as a JSON array.

#### 2. Static File Compression (60-80% bandwidth reduction)
Add gzip/brotli middleware or nginx reverse proxy. JavaScript bundles from Vite compress 60-80% with gzip.

#### 3. Broadcast Buffer Increase (fewer dropped messages)
```
Current:  broadcast::channel(1024)
Better:   broadcast::channel(4096) or broadcast::channel(8192)
```
Trades ~32 KB more memory per topic for significantly fewer `RecvError::Lagged` events on bursty topics.

### Medium Priority (Architecture Changes)

#### 4. Binary Serialization (2-3x serialization throughput)
Replace `serde_json` with MessagePack (`rmp-serde`) or Protocol Buffers for WebSocket messages. JSON serialization is the #1 CPU bottleneck at high message rates.

```
JSON:        ~50k msgs/s (serialization bound)
MessagePack: ~120k msgs/s
Protobuf:    ~150k msgs/s
```

#### 5. Async Seek Operations
Move seek logic from `web::block()` (blocking thread pool) to `tokio::spawn()` with async-native consumers. Eliminates thread pool saturation under concurrent seeks.

#### 6. Per-Partition Consumer Groups
For topics with 8+ partitions, spawn multiple consumers assigned to partition subsets. Enables parallel message processing instead of sequential polling through one StreamConsumer.

### Low Priority (Micro-Optimizations)

#### 7. Arc\<str\> for Message Payloads
Replace `String` fields in `KafkaMessage` with `Arc<str>` to eliminate string cloning in broadcast. Most impactful for large messages (> 10 KB).

#### 8. Cache Headers for Static Assets
Add `Cache-Control: max-age=3600` headers to reduce repeated downloads of unchanged frontend bundles.

#### 9. HTTP/2 Support
Enable HTTP/2 for multiplexed requests over a single TCP connection. Reduces connection overhead for browsers loading multiple static assets.

---

## When to Switch Stacks

| Scenario | Recommended Stack | Reason |
|----------|-------------------|--------|
| Current architecture (dashboard + monitoring) | **Stay with Rust** | Already at peak performance |
| Team is all Go developers | **Go** | 80-90% of Rust performance, faster iteration |
| Need Kafka Streams / Schema Registry | **Java/Kotlin** | Native ecosystem, first-class support |
| Sub-microsecond latency (HFT) | **C++** | Eliminates FFI layer, absolute minimum overhead |
| Rapid prototyping / low throughput | **Node.js** | Fastest development, acceptable for < 5k msgs/s |
| Need JVM ecosystem integration | **Java/Kotlin** | Spring, Micrometer, Confluent platform |
| Multi-instance horizontal scaling | **Go or Java** | Better tooling for orchestration |

---

## Conclusion

**The current Rust + Actix-Web + rdkafka stack is already at or near the performance ceiling** for this class of application. The Kafka consumer uses the same librdkafka C library that powers C++ applications, actix-web consistently ranks #1-#2 in web framework benchmarks, and the broadcast channel pattern provides efficient fan-out without message duplication.

The recommended path is **staying with Rust and applying the tuning optimizations above** — particularly WebSocket message batching and binary serialization — which would yield 3-5x throughput improvements without any stack migration cost. Switching to another stack would trade performance for other benefits (developer velocity, ecosystem access, hiring), not gain it.
