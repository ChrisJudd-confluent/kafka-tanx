# ─── Admin API keys (used only by Terraform to create topics and schemas) ─────
# Owned by a dedicated service account rather than a human user, so key
# ownership doesn't depend on whoever happens to run `terraform apply`.

resource "confluent_service_account" "terraform_admin" {
  display_name = "kafkatanx-terraform-admin"
  description  = "Service account owning Terraform-managed admin API keys (topic/schema creation)"
}

resource "confluent_role_binding" "terraform_admin_env" {
  principal   = "User:${confluent_service_account.terraform_admin.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = data.confluent_environment.env.resource_name
}

resource "confluent_api_key" "admin_kafka" {
  display_name = "kafkatanx-terraform-kafka"
  description  = "Admin Kafka API key used by Terraform to manage topics"

  owner {
    id          = confluent_service_account.terraform_admin.id
    api_version = confluent_service_account.terraform_admin.api_version
    kind        = confluent_service_account.terraform_admin.kind
  }

  managed_resource {
    id          = data.confluent_kafka_cluster.cluster.id
    api_version = data.confluent_kafka_cluster.cluster.api_version
    kind        = data.confluent_kafka_cluster.cluster.kind
    environment {
      id = data.confluent_environment.env.id
    }
  }

  depends_on = [confluent_role_binding.terraform_admin_env]

  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_api_key" "admin_sr" {
  display_name = "kafkatanx-terraform-sr"
  description  = "Admin Schema Registry API key used by Terraform to register schemas"

  owner {
    id          = confluent_service_account.terraform_admin.id
    api_version = confluent_service_account.terraform_admin.api_version
    kind        = confluent_service_account.terraform_admin.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = data.confluent_environment.env.id
    }
  }

  depends_on = [confluent_role_binding.terraform_admin_env]

  lifecycle {
    prevent_destroy = true
  }
}

# ─── Player service account + ACLs ────────────────────────────────────────────

resource "confluent_service_account" "player_client" {
  display_name = "kafkatanx-client"
  description  = "Service account for game clients (restricted: read/write kafkatanx-* only)"
}

resource "confluent_api_key" "player_kafka" {
  display_name = "kafkatanx-client-kafka"
  description  = "Kafka credentials shipped in client-kafka.ini"

  owner {
    id          = confluent_service_account.player_client.id
    api_version = confluent_service_account.player_client.api_version
    kind        = confluent_service_account.player_client.kind
  }

  managed_resource {
    id          = data.confluent_kafka_cluster.cluster.id
    api_version = data.confluent_kafka_cluster.cluster.api_version
    kind        = data.confluent_kafka_cluster.cluster.kind
    environment {
      id = data.confluent_environment.env.id
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_api_key" "player_sr" {
  display_name = "kafkatanx-client-sr"
  description  = "Schema Registry credentials shipped in client-kafka.ini"

  owner {
    id          = confluent_service_account.player_client.id
    api_version = confluent_service_account.player_client.api_version
    kind        = confluent_service_account.player_client.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = data.confluent_environment.env.id
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

# ACLs — write access to all kafkatanx-* topics
locals {
  write_topics = [
    "kafkatanx-sessions",
    "kafkatanx-gameplay",
    "kafkatanx-players",
    "kafkatanx-shots",
    "kafkatanx-rounds",
    "kafkatanx-games",
  ]
  # Topics the client also needs to READ from
  read_topics = [
    "kafkatanx-sessions",
    "kafkatanx-gameplay",
  ]
}

resource "confluent_kafka_acl" "player_write" {
  for_each = toset(local.write_topics)

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  resource_type = "TOPIC"
  resource_name = each.value
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.player_client.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = data.confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

resource "confluent_kafka_acl" "player_read" {
  for_each = toset(local.read_topics)

  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  resource_type = "TOPIC"
  resource_name = each.value
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.player_client.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = data.confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# Consumer group ACL — the client creates groups named kafkatanx-{code}-{uuid}
resource "confluent_kafka_acl" "player_consumer_group" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.cluster.id
  }
  resource_type = "GROUP"
  resource_name = "kafkatanx-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.player_client.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = data.confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}
