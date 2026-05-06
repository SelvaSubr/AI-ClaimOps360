# infra/main.tf
terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.90" }
    azuread = { source = "hashicorp/azuread",  version = "~> 2.47" }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
  skip_provider_registration = true
  #resource_provider_registrations = "none"
  subscription_id = var.subscription_id
  client_id       = var.client_id
  client_secret   = var.client_secret
  tenant_id       = var.tenant_id
}

locals {
  # First 8 hex chars of subscription ID — makes globally-unique Azure names deterministic
  name_suffix = substr(replace(var.subscription_id, "-", ""), 0, 8)
}

# Resource Group
resource "azurerm_resource_group" "claims" {
  name     = "rg-claims-automation-${var.environment}"
  location = var.location
  tags     = var.tags
}

# Azure Key Vault
resource "azurerm_key_vault" "claims" {
  name                = var.key_vault_name
  resource_group_name = azurerm_resource_group.claims.name
  location            = azurerm_resource_group.claims.location
  tenant_id           = var.tenant_id
  sku_name            = "standard"
  tags                = var.tags
}

resource "azurerm_key_vault_access_policy" "sp" {
  key_vault_id = azurerm_key_vault.claims.id
  tenant_id    = var.tenant_id
  object_id    = var.service_principal_object_id
  secret_permissions = ["Get", "List", "Set", "Delete"]
}

# Log Analytics Workspace (for Azure Monitor)
resource "azurerm_log_analytics_workspace" "claims" {
  name                = "law-claims-${var.environment}"
  resource_group_name = azurerm_resource_group.claims.name
  location            = azurerm_resource_group.claims.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = var.tags
}

# Microsoft Purview
resource "azurerm_purview_account" "claims" {
  name                = "purview-claims-${local.name_suffix}"
  resource_group_name = azurerm_resource_group.claims.name
  location            = azurerm_resource_group.claims.location
  tags                = var.tags
  identity { type = "SystemAssigned" }
}

# Submodules
module "storage" {
  source              = "./modules/storage"
  resource_group_name = azurerm_resource_group.claims.name
  location            = var.location
  environment         = var.environment
  name_suffix         = local.name_suffix
  tags                = var.tags
}

module "kafka" {
  source              = "./modules/kafka"
  resource_group_name = azurerm_resource_group.claims.name
  location            = var.location
  environment         = var.environment
  tags                = var.tags
}

module "databricks" {
  source              = "./modules/databricks"
  resource_group_name = azurerm_resource_group.claims.name
  location            = var.location
  environment         = var.environment
  storage_account_id  = module.storage.storage_account_id
  subscription_id     = var.subscription_id
  client_id           = var.client_id
  client_secret       = var.client_secret
  tenant_id           = var.tenant_id
  databricks_host     = var.databricks_host
  tags                = var.tags
}

module "snowflake" {
  source                 = "./modules/snowflake"
  snowflake_org_name     = var.snowflake_org_name
  snowflake_account_name = var.snowflake_account_name
  snowflake_user         = var.snowflake_user
  snowflake_password     = var.snowflake_password
  snowflake_role         = var.snowflake_role
}
