# infra/modules/storage/main.tf
variable "resource_group_name" { type = string }
variable "location"            { type = string }
variable "environment"         { type = string }
variable "name_suffix"         { type = string }
variable "tags"                { type = map(string) }

resource "azurerm_storage_account" "claims" {
  name                     = "stclaims${var.name_suffix}"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  # CRITICAL: hierarchical_namespace_enabled = true is required for Delta Lake
  # Without this every PySpark Delta write fails with 403 — no migration path
  is_hns_enabled           = true
  tags                     = var.tags
}

locals {
  containers = ["bronze", "silver", "gold", "checkpoints"]
}

resource "azurerm_storage_container" "delta" {
  for_each              = toset(local.containers)
  name                  = each.key
  storage_account_name  = azurerm_storage_account.claims.name
  container_access_type = "private"
}

output "storage_account_name" { value = azurerm_storage_account.claims.name }
output "storage_account_id"   { value = azurerm_storage_account.claims.id }
output "primary_access_key" {
  value     = azurerm_storage_account.claims.primary_access_key
  sensitive = true
}
