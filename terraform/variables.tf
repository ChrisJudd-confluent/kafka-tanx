variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (admin-level, from cloud.confluent.io → API Keys)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret"
  type        = string
  sensitive   = true
}

variable "environment_id" {
  description = "Confluent Cloud environment ID (e.g. env-abc123) — visible in the cloud UI URL"
  type        = string
}
