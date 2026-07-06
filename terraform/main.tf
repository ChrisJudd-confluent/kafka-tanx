terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# ─── Existing cluster (pre-created in the UI) ────────────────────────────────

data "confluent_environment" "env" {
  id = var.environment_id
}

data "confluent_kafka_cluster" "cluster" {
  id = "lkc-9kkv5o7"
  environment {
    id = data.confluent_environment.env.id
  }
}

data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = data.confluent_environment.env.id
  }
}

# ─── Topics ──────────────────────────────────────────────────────────────────
# All topics use the admin API key created in iam.tf for the topic-management
# credential. If you run `terraform apply` before the service account exists
# Terraform will create them in dependency order automatically.

resource "confluent_kafka_topic" "sessions" {
  topic_name       = "kafkatanx-sessions"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy"  = "compact"
    "retention.ms"    = "-1"     # compact topics: keep forever
    "min.compaction.lag.ms" = "0"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_topic" "gameplay" {
  topic_name       = "kafkatanx-gameplay"
  partitions_count = 12
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "${24 * 60 * 60 * 1000}"  # 24 hours
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_topic" "players" {
  topic_name       = "kafkatanx-players"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy"  = "compact"
    "retention.ms"    = "-1"
    "min.compaction.lag.ms" = "0"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_topic" "shots" {
  topic_name       = "kafkatanx-shots"
  partitions_count = 12
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "${90 * 24 * 60 * 60 * 1000}"  # 90 days
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_topic" "rounds" {
  topic_name       = "kafkatanx-rounds"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "${90 * 24 * 60 * 60 * 1000}"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_topic" "games" {
  topic_name       = "kafkatanx-games"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "${90 * 24 * 60 * 60 * 1000}"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# ─── Flink analytics output topics ──────────────────────────────────────────
# 5-minute tumbling-window rollups produced by the Flink SQL statements in
# flink/*.sql (see terraform/flink.tf for the compute pool that runs them).

resource "confluent_kafka_topic" "agg_session_funnel" {
  topic_name       = "kafkatanx-agg-session-funnel"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "${30 * 24 * 60 * 60 * 1000}"  # 30 days
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_topic" "agg_weapon_usage" {
  topic_name       = "kafkatanx-agg-weapon-usage"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "${30 * 24 * 60 * 60 * 1000}"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_topic" "agg_weapon_accuracy" {
  topic_name       = "kafkatanx-agg-weapon-accuracy"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "${30 * 24 * 60 * 60 * 1000}"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# Cumulative host-vs-client win rate — a continuously-updating single row
# (id='all_time'), not a windowed rollup, so this topic is compacted and
# keyed (unlike the three windowed agg topics above, which are keyless).
resource "confluent_kafka_topic" "agg_host_advantage" {
  topic_name       = "kafkatanx-agg-host-advantage"
  partitions_count = 1
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy"        = "compact"
    "retention.ms"          = "-1"
    "min.compaction.lag.ms" = "0"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# Cumulative outcomes per settings combination — same reasoning as
# agg_host_advantage above: compacted and keyed, one row per combo.
resource "confluent_kafka_topic" "agg_settings_outcomes" {
  topic_name       = "kafkatanx-agg-settings-outcomes"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy"        = "compact"
    "retention.ms"          = "-1"
    "min.compaction.lag.ms" = "0"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# Average move distance per tank per game — a plain per-game transform, not
# an aggregate, so this is keyless/append like the three windowed topics.
resource "confluent_kafka_topic" "agg_move_distance" {
  topic_name       = "kafkatanx-agg-move-distance"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.cluster.rest_endpoint

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "${30 * 24 * 60 * 60 * 1000}"
  }

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# ─── Schemas ─────────────────────────────────────────────────────────────────
# Schema files are read from ../schemas/ relative to this terraform/ dir.
# The schema_registry_cluster REST endpoint is resolved from the data source.

resource "confluent_schema" "sessions" {
  subject_name = "kafkatanx-sessions-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/SessionEvent.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "gameplay" {
  subject_name = "kafkatanx-gameplay-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/GameplayMessage.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "players" {
  subject_name = "kafkatanx-players-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/PlayerProfile.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "shots" {
  subject_name = "kafkatanx-shots-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/ShotEvent.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "rounds" {
  subject_name = "kafkatanx-rounds-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/RoundEvent.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "games" {
  subject_name = "kafkatanx-games-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/GameEvent.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "agg_session_funnel" {
  subject_name = "kafkatanx-agg-session-funnel-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/SessionFunnelAgg.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "agg_weapon_usage" {
  subject_name = "kafkatanx-agg-weapon-usage-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/WeaponUsageAgg.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "agg_weapon_accuracy" {
  subject_name = "kafkatanx-agg-weapon-accuracy-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/WeaponAccuracyAgg.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "agg_host_advantage_key" {
  subject_name = "kafkatanx-agg-host-advantage-key"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/HostAdvantageKey.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "agg_host_advantage_value" {
  subject_name = "kafkatanx-agg-host-advantage-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/HostAdvantageAgg.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "agg_settings_outcomes_key" {
  subject_name = "kafkatanx-agg-settings-outcomes-key"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/SettingsOutcomeKey.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "agg_settings_outcomes_value" {
  subject_name = "kafkatanx-agg-settings-outcomes-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/SettingsOutcomeAgg.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}

resource "confluent_schema" "agg_move_distance" {
  subject_name = "kafkatanx-agg-move-distance-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/MoveDistanceAgg.avsc")

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
}
