#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE=".env"
ENV_EXAMPLE=".env.example"
CERTS_GENERATED=0

log() {
  printf '[auto-run] %s\n' "$*"
}

warn() {
  printf '[auto-run][warning] %s\n' "$*" >&2
}

die() {
  printf '[auto-run][error] %s\n' "$*" >&2
  exit 1
}

ensure_env_file() {
  if [[ -f "$ENV_FILE" ]]; then
    log "Found $ENV_FILE"
    return
  fi

  if [[ ! -f "$ENV_EXAMPLE" ]]; then
    die "Missing $ENV_FILE and $ENV_EXAMPLE. Create $ENV_FILE with Kafka and Elasticsearch credentials."
  fi

  cp "$ENV_EXAMPLE" "$ENV_FILE"
  warn "Created $ENV_FILE from $ENV_EXAMPLE."
  warn "Populate real secrets in $ENV_FILE before production use. Placeholder credentials may only work for a disposable local stack."
}

load_env_file() {
  set -a
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
  log "Loaded environment from $ENV_FILE"
}

resolve_path() {
  local path_value="$1"
  if [[ "$path_value" = /* ]]; then
    printf '%s\n' "$path_value"
  else
    printf '%s/%s\n' "$ROOT_DIR" "$path_value"
  fi
}

ensure_kafka_certs() {
  local ca_path="${KAFKA_CA_CERT_PATH:-.kafka_secrets/kafka.server.cer}"
  local resolved_ca_path
  resolved_ca_path="$(resolve_path "$ca_path")"
  local jaas_path=".kafka_secrets/kafka_server_jaas.conf"

  if [[ -d ".kafka_secrets" && -f "$resolved_ca_path" && -f "$jaas_path" ]]; then
    log "Kafka TLS material is present at $ca_path"
    return
  fi

  warn "Kafka TLS or JAAS material is missing. Generating local Kafka security files now."
  chmod +x scripts/generate-kafka-certs.sh
  ./scripts/generate-kafka-certs.sh
  CERTS_GENERATED=1

  if [[ ! -f "$resolved_ca_path" ]]; then
    die "Expected Kafka CA certificate was not created at $ca_path"
  fi
  if [[ ! -f "$jaas_path" ]]; then
    die "Expected Kafka JAAS file was not created at $jaas_path"
  fi
}

compose_has_running_containers() {
  local running_count
  running_count="$(docker compose ps --status running --services 2>/dev/null | wc -l | tr -d ' ')"
  [[ "${running_count:-0}" -gt 0 ]]
}

sync_infrastructure() {
  log "Current Docker Compose state:"
  docker compose ps || true

  if compose_has_running_containers; then
    if [[ "$CERTS_GENERATED" -eq 1 ]]; then
      warn "Certificates were just generated while containers were already running."
      warn "Restarting Compose so Kafka mounts the new TLS material."
      docker compose down
      docker compose up -d
    else
      log "Compose services are already running."
    fi
  else
    log "Compose services are stopped. Starting infrastructure."
    docker compose up -d
  fi

  log "Waiting briefly for service startup."
  sleep 10
  docker compose ps
}

run_benchmark() {
  if [[ ! -x ".venv/bin/python" ]]; then
    die "Missing .venv/bin/python. Create the virtualenv and install dependencies first."
  fi

  log "Giving containers an additional readiness head start before benchmark checks."
  sleep 15

  log "Launching benchmark with pass-through arguments: $*"
  .venv/bin/python scripts/run_benchmark.py "$@"
}

main() {
  ensure_env_file
  load_env_file
  ensure_kafka_certs
  sync_infrastructure
  run_benchmark "$@"
}

main "$@"
