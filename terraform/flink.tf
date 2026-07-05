# ─── Flink compute pool + service account for the analytics SQL statements ───
# Runs the queries in ../flink/*.sql: reads kafkatanx-sessions and
# kafkatanx-shots, writes the three kafkatanx-agg-* topics defined in main.tf.

data "confluent_flink_region" "main" {
  cloud  = "AWS"
  region = "eu-west-2"
}

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

# ─── Kafka ACLs — read the two source topics, write the three output topics ──

locals {
  flink_read_topics = [
    "kafkatanx-sessions",
    "kafkatanx-shots",
  ]
  flink_write_topics = [
    "kafkatanx-agg-session-funnel",
    "kafkatanx-agg-weapon-usage",
    "kafkatanx-agg-weapon-accuracy",
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
