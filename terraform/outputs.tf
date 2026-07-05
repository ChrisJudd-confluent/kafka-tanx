# ─── Schema IDs ───────────────────────────────────────────────────────────────
# Paste these into client-kafka.ini [schema-ids] after terraform apply.

output "schema_id_session" {
  description = "Schema ID for kafkatanx-sessions-value — goes in client-kafka.ini [schema-ids] session="
  value       = confluent_schema.sessions.schema_identifier
}

output "schema_id_gameplay" {
  description = "Schema ID for kafkatanx-gameplay-value — goes in client-kafka.ini [schema-ids] gameplay="
  value       = confluent_schema.gameplay.schema_identifier
}

output "schema_id_player" {
  description = "Schema ID for kafkatanx-players-value — goes in client-kafka.ini [schema-ids] player="
  value       = confluent_schema.players.schema_identifier
}

output "schema_id_shot" {
  description = "Schema ID for kafkatanx-shots-value — goes in client-kafka.ini [schema-ids] shot="
  value       = confluent_schema.shots.schema_identifier
}

output "schema_id_round" {
  description = "Schema ID for kafkatanx-rounds-value — goes in client-kafka.ini [schema-ids] round="
  value       = confluent_schema.rounds.schema_identifier
}

output "schema_id_game" {
  description = "Schema ID for kafkatanx-games-value — goes in client-kafka.ini [schema-ids] game="
  value       = confluent_schema.games.schema_identifier
}

# ─── Client credentials ────────────────────────────────────────────────────────
# Paste these into client-kafka.ini [kafka] and [schema-registry].
# These are marked sensitive — run: terraform output -json to see them.

output "client_kafka_api_key" {
  description = "Kafka API key for the player service account — goes in client-kafka.ini sasl.username="
  value       = confluent_api_key.player_kafka.id
  sensitive   = false  # Key ID is not secret
}

output "client_kafka_api_secret" {
  description = "Kafka API secret for the player service account — goes in client-kafka.ini sasl.password="
  value       = confluent_api_key.player_kafka.secret
  sensitive   = true
}

output "client_sr_api_key" {
  description = "Schema Registry API key for the player service account"
  value       = confluent_api_key.player_sr.id
  sensitive   = false
}

output "client_sr_api_secret" {
  description = "Schema Registry API secret — combine as KEY:SECRET for client-kafka.ini basic.auth.user.info="
  value       = confluent_api_key.player_sr.secret
  sensitive   = true
}

# ─── Convenience summary ───────────────────────────────────────────────────────
# Shows the exact lines to paste into client-kafka.ini. Run: terraform output client_ini_snippet
output "client_ini_snippet" {
  description = "Ready-to-paste client-kafka.ini block (run: terraform output client_ini_snippet)"
  sensitive   = true
  value       = <<-EOT
    [kafka]
    sasl.username=${confluent_api_key.player_kafka.id}
    sasl.password=${confluent_api_key.player_kafka.secret}

    [schema-registry]
    basic.auth.user.info=${confluent_api_key.player_sr.id}:${confluent_api_key.player_sr.secret}

    [schema-ids]
    session=${confluent_schema.sessions.schema_identifier}
    gameplay=${confluent_schema.gameplay.schema_identifier}
    player=${confluent_schema.players.schema_identifier}
    shot=${confluent_schema.shots.schema_identifier}
    round=${confluent_schema.rounds.schema_identifier}
    game=${confluent_schema.games.schema_identifier}
  EOT
}

# ─── Flink ──────────────────────────────────────────────────────────────────
# Used with the Confluent CLI to run the statements in ../flink/*.sql, e.g.:
#   confluent flink statement create session-funnel \
#     --sql "$(cat ../flink/session_funnel.sql)" \
#     --compute-pool "$(terraform output -raw flink_compute_pool_id)" \
#     --database lkc-9kkv5o7

output "flink_compute_pool_id" {
  description = "Flink compute pool ID — pass to `confluent flink statement create --compute-pool`"
  value       = confluent_flink_compute_pool.analytics.id
}

output "flink_api_key" {
  description = "Flink API key ID for the flink_runner service account"
  value       = confluent_api_key.flink_runner.id
  sensitive   = false
}

output "flink_api_secret" {
  description = "Flink API secret — use with `confluent flink` commands or `confluent api-key use`"
  value       = confluent_api_key.flink_runner.secret
  sensitive   = true
}
