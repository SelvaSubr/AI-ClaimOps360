# infra/outputs.tf
output "resource_group_name" {
  value       = azurerm_resource_group.claims.name
  description = "Resource group — update rg-claims-automation-dev references if env changes"
}

output "key_vault_uri" {
  value       = azurerm_key_vault.claims.vault_uri
  description = "Key Vault URI — reference in Databricks secret scope config"
}

output "storage_account_name" {
  value       = module.storage.storage_account_name
  description = "ADLS Gen2 account name — set as ADLS_ACCOUNT_NAME in .env"
}

output "databricks_host" {
  value       = module.databricks.workspace_url
  description = "Databricks workspace URL — set as DATABRICKS_HOST in .env (include https://)"
}

output "eventhub_namespace_name" {
  value       = module.kafka.namespace_name
  description = "Event Hubs namespace — set as Kafka bootstrap server in .env for cloud mode"
}

output "purview_account_name" {
  value       = azurerm_purview_account.claims.name
}

output "kafka_bootstrap_servers" {
  value       = module.kafka.kafka_endpoint
  description = "Kafka-compatible Event Hubs bootstrap server"
}

output "adls_account_name" {
  value       = module.storage.storage_account_name
  description = "ADLS Gen2 storage account name"
}

output "adls_account_key" {
  value       = module.storage.primary_access_key
  sensitive   = true
  description = "ADLS Gen2 storage account primary key"
}

output "databricks_cluster_id" {
  value       = module.databricks.cluster_id
  description = "Databricks cluster ID"
}

output "databricks_http_path" {
  value       = module.databricks.sql_http_path
  description = "Databricks SQL warehouse HTTP path — set as DATABRICKS_HTTP_PATH in .env"
}
