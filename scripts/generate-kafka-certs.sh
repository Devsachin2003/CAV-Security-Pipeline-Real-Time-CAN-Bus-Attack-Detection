#!/usr/bin/env bash
set -euo pipefail

SECRETS_DIR="${1:-.kafka_secrets}"

: "${KAFKA_SSL_KEYSTORE_PASSWORD:?Set KAFKA_SSL_KEYSTORE_PASSWORD in your environment or source .env first.}"
: "${KAFKA_SSL_TRUSTSTORE_PASSWORD:?Set KAFKA_SSL_TRUSTSTORE_PASSWORD in your environment or source .env first.}"

KAFKA_SSL_KEY_PASSWORD="${KAFKA_SSL_KEY_PASSWORD:-$KAFKA_SSL_KEYSTORE_PASSWORD}"
if [[ "$KAFKA_SSL_KEY_PASSWORD" != "$KAFKA_SSL_KEYSTORE_PASSWORD" ]]; then
  echo "Kafka key password must match the keystore password for the local PKCS12 keystore." >&2
  echo "Set KAFKA_SSL_KEY_PASSWORD to the same value as KAFKA_SSL_KEYSTORE_PASSWORD." >&2
  exit 1
fi

mkdir -p "$SECRETS_DIR"
chmod 700 "$SECRETS_DIR"

KEYSTORE="$SECRETS_DIR/kafka.server.keystore.jks"
TRUSTSTORE="$SECRETS_DIR/kafka.server.truststore.jks"
CERT="$SECRETS_DIR/kafka.server.cer"
JAAS="$SECRETS_DIR/kafka_server_jaas.conf"

: "${KAFKA_BROKER_USER:?Set KAFKA_BROKER_USER in your environment or source .env first.}"
: "${KAFKA_BROKER_PASSWORD:?Set KAFKA_BROKER_PASSWORD in your environment or source .env first.}"
: "${KAFKA_USER:?Set KAFKA_USER in your environment or source .env first.}"
: "${KAFKA_PASSWORD:?Set KAFKA_PASSWORD in your environment or source .env first.}"

rm -f "$KEYSTORE" "$TRUSTSTORE" "$CERT" "$JAAS"

keytool -genkeypair \
  -alias kafka \
  -keyalg RSA \
  -keysize 2048 \
  -validity 825 \
  -keystore "$KEYSTORE" \
  -storepass "$KAFKA_SSL_KEYSTORE_PASSWORD" \
  -keypass "$KAFKA_SSL_KEY_PASSWORD" \
  -dname "CN=kafka,OU=CAV,O=Local,L=Local,ST=Local,C=US" \
  -ext "SAN=dns:kafka,dns:localhost,ip:127.0.0.1"

keytool -exportcert \
  -alias kafka \
  -keystore "$KEYSTORE" \
  -storepass "$KAFKA_SSL_KEYSTORE_PASSWORD" \
  -rfc \
  -file "$CERT"

keytool -importcert \
  -alias kafka \
  -file "$CERT" \
  -keystore "$TRUSTSTORE" \
  -storepass "$KAFKA_SSL_TRUSTSTORE_PASSWORD" \
  -noprompt

printf '%s' "$KAFKA_SSL_KEYSTORE_PASSWORD" > "$SECRETS_DIR/kafka_keystore_creds"
printf '%s' "$KAFKA_SSL_KEY_PASSWORD" > "$SECRETS_DIR/kafka_key_creds"
printf '%s' "$KAFKA_SSL_TRUSTSTORE_PASSWORD" > "$SECRETS_DIR/kafka_truststore_creds"

cat > "$JAAS" <<EOF
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="$KAFKA_BROKER_USER"
  password="$KAFKA_BROKER_PASSWORD"
  user_$KAFKA_BROKER_USER="$KAFKA_BROKER_PASSWORD"
  user_$KAFKA_USER="$KAFKA_PASSWORD";
};
EOF

chmod 600 "$SECRETS_DIR"/*

echo "Kafka SSL material written to $SECRETS_DIR"
