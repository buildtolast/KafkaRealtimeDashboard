#!/usr/bin/env bash
# ============================================================================
# KafkaDashboard — run.sh
# ============================================================================
# Usage:
#   ./run.sh                      # Run with defaults (localhost:9094, port 3001)
#   ./run.sh --build              # Build everything from source (frontend + release backend), then run
#   ./run.sh --full               # Full stack Docker (Kafka + Dashboard + Seed)
#   ./run.sh --docker             # App-only Docker (connect to external Kafka)
#   ./run.sh --confluent          # Connect to Confluent Cloud (set env vars below)
#   ./run.sh --stop               # Stop whatever was started (auto-detects mode)
#   ./run.sh --help               # Show this help
# ============================================================================

set -euo pipefail

# ── Kafka ────────────────────────────────────────────────────────────────────
export KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9094}"

# ── Server ───────────────────────────────────────────────────────────────────
export SERVER_HOST="${SERVER_HOST:-127.0.0.1}"
export SERVER_PORT="${SERVER_PORT:-3001}"

# ── Consumer ─────────────────────────────────────────────────────────────────
export CONSUMER_GROUP="${CONSUMER_GROUP:-kafka-dashboard}"

# ── Static Files ─────────────────────────────────────────────────────────────
export STATIC_DIR="${STATIC_DIR:-./frontend/dist}"

# ── SASL / SSL Authentication (optional — for Confluent Cloud, etc.) ─────────
# export KAFKA_SECURITY_PROTOCOL="SASL_SSL"        # SASL_SSL | SASL_PLAINTEXT | SSL
# export KAFKA_SASL_MECHANISM="PLAIN"               # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
# export KAFKA_SASL_USERNAME="<your-api-key>"
# export KAFKA_SASL_PASSWORD="<your-api-secret>"

# ── Logging ──────────────────────────────────────────────────────────────────
export RUST_LOG="${RUST_LOG:-info}"

# ============================================================================

print_help() {
    echo "KafkaDashboard"
    echo ""
    echo "Usage:"
    echo "  ./run.sh                Run from source with cargo (default)"
    echo "  ./run.sh --build        Build everything from source (frontend + release backend), then run"
    echo "  ./run.sh --full         Full stack Docker: Kafka + Dashboard + Seed Producer"
    echo "  ./run.sh --docker       App-only Docker (set KAFKA_BROKERS env var)"
    echo "  ./run.sh --confluent    Connect to Confluent Cloud (prompts for credentials)"
    echo "  ./run.sh --stop         Stop whatever was started (auto-detects mode)"
    echo "  ./run.sh --help         Show this help"
    echo ""
    echo "Environment Variables:"
    echo "  KAFKA_BROKERS              Kafka broker addresses       (default: localhost:9094)"
    echo "  SERVER_HOST                Server bind host             (default: 127.0.0.1)"
    echo "  SERVER_PORT                Server bind port             (default: 3001)"
    echo "  CONSUMER_GROUP             Kafka consumer group prefix  (default: kafka-dashboard)"
    echo "  STATIC_DIR                 Frontend dist directory      (default: ./frontend/dist)"
    echo "  KAFKA_SECURITY_PROTOCOL    Security protocol            (SASL_SSL | SASL_PLAINTEXT | SSL)"
    echo "  KAFKA_SASL_MECHANISM       SASL mechanism               (PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512)"
    echo "  KAFKA_SASL_USERNAME        SASL username / API Key"
    echo "  KAFKA_SASL_PASSWORD        SASL password / API Secret"
    echo "  RUST_LOG                   Log level                    (default: info)"
    echo "  LOG_FORMAT                 Set to 'json' for JSON logs  (default: text)"
}

print_config() {
    echo "┌─────────────────────────────────────────────────────────┐"
    echo "│  KafkaDashboard                                         │"
    echo "├─────────────────────────────────────────────────────────┤"
    echo "│  Brokers:       ${KAFKA_BROKERS}"
    echo "│  Server:        ${SERVER_HOST}:${SERVER_PORT}"
    echo "│  Consumer Group: ${CONSUMER_GROUP}"
    echo "│  Static Dir:    ${STATIC_DIR}"
    echo "│  Auth:          ${KAFKA_SECURITY_PROTOCOL:-none}"
    echo "│  Log Level:     ${RUST_LOG}"
    echo "└─────────────────────────────────────────────────────────┘"
    echo ""
    echo "  Dashboard: http://${SERVER_HOST}:${SERVER_PORT}"
    echo ""
}

build_frontend() {
    echo "Building frontend from source..."
    echo ""
    (
        cd frontend
        if [[ -f package-lock.json ]]; then
            npm ci
        else
            npm install
        fi
        npm run build
    )
    echo ""
    echo "Frontend built → frontend/dist"
    echo ""
}

run_cargo() {
    if [[ "${BUILD}" -eq 1 ]]; then
        build_frontend
    fi

    print_config

    ARGS=(
        "--brokers" "${KAFKA_BROKERS}"
        "--port" "${SERVER_PORT}"
        "--host" "${SERVER_HOST}"
        "--static-dir" "${STATIC_DIR}"
    )

    if [[ -n "${KAFKA_SECURITY_PROTOCOL:-}" ]]; then
        ARGS+=("--security-protocol" "${KAFKA_SECURITY_PROTOCOL}")
    fi
    if [[ -n "${KAFKA_SASL_MECHANISM:-}" ]]; then
        ARGS+=("--sasl-mechanism" "${KAFKA_SASL_MECHANISM}")
    fi
    if [[ -n "${KAFKA_SASL_USERNAME:-}" ]]; then
        ARGS+=("--sasl-username" "${KAFKA_SASL_USERNAME}")
    fi
    if [[ -n "${KAFKA_SASL_PASSWORD:-}" ]]; then
        ARGS+=("--sasl-password" "${KAFKA_SASL_PASSWORD}")
    fi

    if [[ "${BUILD}" -eq 1 ]]; then
        echo "Building backend (release) from source..."
        echo ""
        cargo build --release
        echo ""
        echo "Starting with: ./target/release/kafka-dashboard ${ARGS[*]}"
        echo ""
        ./target/release/kafka-dashboard "${ARGS[@]}"
    else
        echo "Starting with: cargo run -- ${ARGS[*]}"
        echo ""
        cargo run -- "${ARGS[@]}"
    fi
}

run_full_docker() {
    echo "Starting full stack (Kafka + Dashboard + Seed Producer)..."
    echo ""
    docker compose -f docker-compose.full.yml up -d --build
    echo ""
    echo "Dashboard: http://localhost:3001"
    echo "Kafka external: localhost:9094"
    echo ""
    echo "Logs:  docker compose -f docker-compose.full.yml logs -f dashboard"
    echo "Stop:  ./run.sh --stop"
}

run_docker() {
    echo "Starting app-only Docker (connecting to ${KAFKA_BROKERS})..."
    echo ""
    KAFKA_BROKERS="${KAFKA_BROKERS}" docker compose up -d --build
    echo ""
    echo "Dashboard: http://localhost:3001"
    echo ""
    echo "Logs:  docker compose logs -f"
    echo "Stop:  ./run.sh --stop"
}

run_confluent() {
    if [[ -z "${KAFKA_BROKERS:-}" || "${KAFKA_BROKERS}" == "localhost:9094" ]]; then
        read -rp "Confluent bootstrap server (e.g. pkc-xxxxx.us-east-1.aws.confluent.cloud:9092): " KAFKA_BROKERS
        export KAFKA_BROKERS
    fi
    if [[ -z "${KAFKA_SASL_USERNAME:-}" ]]; then
        read -rp "API Key: " KAFKA_SASL_USERNAME
        export KAFKA_SASL_USERNAME
    fi
    if [[ -z "${KAFKA_SASL_PASSWORD:-}" ]]; then
        read -rsp "API Secret: " KAFKA_SASL_PASSWORD
        echo ""
        export KAFKA_SASL_PASSWORD
    fi

    export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
    export KAFKA_SASL_MECHANISM="PLAIN"

    run_cargo
}

run_stop() {
    local stopped=0

    # Check for full-stack compose
    local full_ps
    full_ps=$(docker compose -f docker-compose.full.yml ps -q 2>/dev/null || true)
    if [[ -n "$full_ps" ]]; then
        echo "Stopping full stack (Kafka + Dashboard + Seed Producer)..."
        docker compose -f docker-compose.full.yml down
        stopped=1
    fi

    # Check for app-only Docker (docker-compose.yml)
    local app_ps
    app_ps=$(docker compose ps -q 2>/dev/null || true)
    if [[ -n "$app_ps" ]]; then
        echo "Stopping app-only Docker..."
        docker compose down
        stopped=1
    fi

    # Check for cargo/binary process on the configured port
    local port_pid
    port_pid=$(lsof -ti :"${SERVER_PORT}" 2>/dev/null || true)
    if [[ -n "$port_pid" ]]; then
        echo "Stopping process on port ${SERVER_PORT} (PID: ${port_pid})..."
        kill $port_pid 2>/dev/null || true
        sleep 2
        # Force kill if still alive
        local remaining
        remaining=$(lsof -ti :"${SERVER_PORT}" 2>/dev/null || true)
        if [[ -n "$remaining" ]]; then
            echo "Force stopping remaining processes..."
            kill -9 $remaining 2>/dev/null || true
        fi
        stopped=1
    fi

    if [[ $stopped -eq 0 ]]; then
        echo "No running instances found."
    else
        echo ""
        echo "Stopped."
    fi
}

# ── Parse arguments ─────────────────────────────────────────────────────────

MODE=""
BUILD=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --build)      BUILD=1; shift ;;
        --full)       MODE="full"; shift ;;
        --docker)     MODE="docker"; shift ;;
        --confluent)  MODE="confluent"; shift ;;
        --stop)       MODE="stop"; shift ;;
        --help|-h)    MODE="help"; shift ;;
        *)
            echo "Unknown option: $1"
            echo "Run ./run.sh --help for usage"
            exit 1 ;;
    esac
done

# ── Main ─────────────────────────────────────────────────────────────────────

case "${MODE}" in
    full)       run_full_docker ;;
    docker)     run_docker ;;
    confluent)  run_confluent ;;
    stop)       run_stop ;;
    help)       print_help ;;
    *)          run_cargo ;;
esac
