# ─── Flink compute pool + service account for the analytics SQL statements ───
# Runs the queries in ../flink/*.sql: reads kafkatanx-sessions and
# kafkatanx-shots, writes the three kafkatanx-agg-* topics defined in main.tf.

data "confluent_flink_region" "main" {
  cloud  = "AWS"
  region = "eu-west-2"
}

data "confluent_organization" "main" {}

resource "confluent_flink_compute_pool" "analytics" {
  display_name = "kafkatanx-analytics-pool"
  cloud        = "AWS"
  region       = "eu-west-2"
  max_cfu      = 5

  environment {
    id = data.confluent_environment.env.id
  }
}

resource "confluent_service_account" "flink_runner" {
  display_name = "kafkatanx-flink-runner"
  description  = "Runs the kafkatanx analytics Flink SQL statements"
}

resource "confluent_role_binding" "flink_runner_developer" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = data.confluent_environment.env.resource_name
}

# Schema Registry: read source-topic schemas (sessions, shots) and
# resolve the pre-registered output schemas.
resource "confluent_role_binding" "flink_runner_sr" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=kafkatanx-*"
}

# Flink's catalog integration resolves "current-database" via RBAC against the
# cluster itself — the topic-level ACLs below govern actual produce/consume,
# but without this the compute pool can't even see the cluster as a database.
resource "confluent_role_binding" "flink_runner_cluster" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = data.confluent_kafka_cluster.cluster.rbac_crn
}

resource "confluent_api_key" "flink_runner" {
  display_name = "kafkatanx-flink-runner-key"
  description  = "Flink API key used to submit the kafkatanx analytics SQL statements"

  owner {
    id          = confluent_service_account.flink_runner.id
    api_version = confluent_service_account.flink_runner.api_version
    kind        = confluent_service_account.flink_runner.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind
    environment {
      id = data.confluent_environment.env.id
    }
  }

  depends_on = [confluent_role_binding.flink_runner_developer]
}

# ─── Kafka ACLs — read the source topics, write the output topics ────────────

locals {
  flink_read_topics = [
    "kafkatanx-sessions",
    "kafkatanx-shots",
    "kafkatanx-games",
  ]
  flink_write_topics = [
    "kafkatanx-agg-session-funnel",
    "kafkatanx-agg-weapon-usage",
    "kafkatanx-agg-weapon-accuracy",
    "kafkatanx-agg-host-advantage",
  ]
}

resource "confluent_kafka_acl" "flink_read" {
  for_each = toset(local.flink_read_topics)

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  resource_type = "TOPIC"
  resource_name = each.value
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink_runner.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = data.confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_acl" "flink_write" {
  for_each = toset(local.flink_write_topics)

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  resource_type = "TOPIC"
  resource_name = each.value
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink_runner.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = data.confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# Flink SQL statements need DESCRIBE on the cluster itself in addition to the
# per-topic ACLs above.
resource "confluent_kafka_acl" "flink_describe_cluster" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink_runner.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
  rest_endpoint = data.confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# ─── Flink SQL statements — `terraform apply` runs the whole pipeline ────────
# SQL text lives in ../flink/*.sql (single source of truth, also runnable by
# hand via `confluent flink statement create`). catalog = environment,
# database = cluster, matching how Confluent Cloud maps Kafka into Flink.
#
# Every table already has a system-provided default watermark, so wiring up
# our own event-time column takes two statements per topic: ADD the computed
# column, then MODIFY the watermark to point at it (can't do both in one ADD).
#
# Note: these are metadata changes on the topic itself, not on the compute
# pool. If the pool is ever destroyed and recreated against the same
# already-altered topics, re-running add_event_time_* will fail because the
# computed column already exists — harmless (nothing to reapply), but worth
# knowing before you `terraform destroy` and expect a clean re-apply.

locals {
  flink_sql_catalog  = data.confluent_environment.env.display_name
  flink_sql_database = data.confluent_kafka_cluster.cluster.id
}

resource "confluent_flink_statement" "add_event_time_sessions" {
  statement_name = "kafkatanx-add-event-time-sessions"
  statement      = file("${path.module}/../flink/add_event_time_sessions.sql")
  properties = {
    "sql.current-catalog"  = local.flink_sql_catalog
    "sql.current-database" = local.flink_sql_database
  }

  organization { id = data.confluent_organization.main.id }
  environment  { id = data.confluent_environment.env.id }
  compute_pool { id = confluent_flink_compute_pool.analytics.id }
  principal    { id = confluent_service_account.flink_runner.id }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [
    confluent_kafka_acl.flink_read,
    confluent_kafka_acl.flink_write,
    confluent_kafka_acl.flink_describe_cluster,
    confluent_role_binding.flink_runner_sr,
    confluent_role_binding.flink_runner_cluster,
  ]
}

resource "confluent_flink_statement" "set_watermark_sessions" {
  statement_name = "kafkatanx-set-watermark-sessions"
  statement      = file("${path.module}/../flink/set_watermark_sessions.sql")
  properties = {
    "sql.current-catalog"  = local.flink_sql_catalog
    "sql.current-database" = local.flink_sql_database
  }

  organization { id = data.confluent_organization.main.id }
  environment  { id = data.confluent_environment.env.id }
  compute_pool { id = confluent_flink_compute_pool.analytics.id }
  principal    { id = confluent_service_account.flink_runner.id }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [confluent_flink_statement.add_event_time_sessions]
}

resource "confluent_flink_statement" "add_event_time_shots" {
  statement_name = "kafkatanx-add-event-time-shots"
  statement      = file("${path.module}/../flink/add_event_time_shots.sql")
  properties = {
    "sql.current-catalog"  = local.flink_sql_catalog
    "sql.current-database" = local.flink_sql_database
  }

  organization { id = data.confluent_organization.main.id }
  environment  { id = data.confluent_environment.env.id }
  compute_pool { id = confluent_flink_compute_pool.analytics.id }
  principal    { id = confluent_service_account.flink_runner.id }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [
    confluent_kafka_acl.flink_read,
    confluent_kafka_acl.flink_write,
    confluent_kafka_acl.flink_describe_cluster,
    confluent_role_binding.flink_runner_sr,
    confluent_role_binding.flink_runner_cluster,
  ]
}

resource "confluent_flink_statement" "set_watermark_shots" {
  statement_name = "kafkatanx-set-watermark-shots"
  statement      = file("${path.module}/../flink/set_watermark_shots.sql")
  properties = {
    "sql.current-catalog"  = local.flink_sql_catalog
    "sql.current-database" = local.flink_sql_database
  }

  organization { id = data.confluent_organization.main.id }
  environment  { id = data.confluent_environment.env.id }
  compute_pool { id = confluent_flink_compute_pool.analytics.id }
  principal    { id = confluent_service_account.flink_runner.id }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [confluent_flink_statement.add_event_time_shots]
}

resource "confluent_flink_statement" "session_funnel" {
  statement_name = "kafkatanx-session-funnel"
  statement      = file("${path.module}/../flink/session_funnel.sql")
  properties = {
    "sql.current-catalog"  = local.flink_sql_catalog
    "sql.current-database" = local.flink_sql_database
  }

  organization { id = data.confluent_organization.main.id }
  environment  { id = data.confluent_environment.env.id }
  compute_pool { id = confluent_flink_compute_pool.analytics.id }
  principal    { id = confluent_service_account.flink_runner.id }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [confluent_flink_statement.set_watermark_sessions]
}

resource "confluent_flink_statement" "weapon_usage" {
  statement_name = "kafkatanx-weapon-usage"
  statement      = file("${path.module}/../flink/weapon_usage.sql")
  properties = {
    "sql.current-catalog"  = local.flink_sql_catalog
    "sql.current-database" = local.flink_sql_database
  }

  organization { id = data.confluent_organization.main.id }
  environment  { id = data.confluent_environment.env.id }
  compute_pool { id = confluent_flink_compute_pool.analytics.id }
  principal    { id = confluent_service_account.flink_runner.id }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [confluent_flink_statement.set_watermark_shots]
}

resource "confluent_flink_statement" "weapon_accuracy" {
  statement_name = "kafkatanx-weapon-accuracy"
  statement      = file("${path.module}/../flink/weapon_accuracy.sql")
  properties = {
    "sql.current-catalog"  = local.flink_sql_catalog
    "sql.current-database" = local.flink_sql_database
  }

  organization { id = data.confluent_organization.main.id }
  environment  { id = data.confluent_environment.env.id }
  compute_pool { id = confluent_flink_compute_pool.analytics.id }
  principal    { id = confluent_service_account.flink_runner.id }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [confluent_flink_statement.set_watermark_shots]
}

# Cumulative host-vs-client win rate, from kafkatanx-games. No windowing, so
# no watermark dependency — just the base ACLs/role bindings.
resource "confluent_flink_statement" "host_advantage" {
  statement_name = "kafkatanx-host-advantage"
  statement      = file("${path.module}/../flink/host_advantage.sql")
  properties = {
    "sql.current-catalog"  = local.flink_sql_catalog
    "sql.current-database" = local.flink_sql_database
  }

  organization { id = data.confluent_organization.main.id }
  environment  { id = data.confluent_environment.env.id }
  compute_pool { id = confluent_flink_compute_pool.analytics.id }
  principal    { id = confluent_service_account.flink_runner.id }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink_runner.id
    secret = confluent_api_key.flink_runner.secret
  }

  depends_on = [
    confluent_kafka_acl.flink_read,
    confluent_kafka_acl.flink_write,
    confluent_kafka_acl.flink_describe_cluster,
    confluent_role_binding.flink_runner_sr,
    confluent_role_binding.flink_runner_cluster,
  ]
}
