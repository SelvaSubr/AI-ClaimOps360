# infra/modules/databricks/main.tf
terraform {
  required_providers {
    azurerm    = { source = "hashicorp/azurerm",   version = "~> 3.90" }
    databricks = { source = "databricks/databricks", version = "~> 1.39" }
  }
}

variable "resource_group_name" { type = string }
variable "location"            { type = string }
variable "environment"         { type = string }
variable "storage_account_id"  { type = string }
variable "subscription_id"     { type = string }
variable "client_id"           { type = string }
variable "client_secret"       {
  type      = string
  sensitive = true
}
variable "tenant_id"           { type = string }
variable "databricks_host"     {
  type        = string
  description = "Workspace URL — set after first apply, e.g. https://adb-XXXXXX.N.azuredatabricks.net"
  default     = ""
}
variable "tags"                { type = map(string) }

locals {
  # Fully deterministic — no computed references, safe for provider config
  workspace_resource_id = "/subscriptions/${var.subscription_id}/resourceGroups/${var.resource_group_name}/providers/Microsoft.Databricks/workspaces/dbw-claims-${var.environment}"
}

resource "azurerm_databricks_workspace" "claims" {
  name                = "dbw-claims-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  # Premium required for MLflow Model Registry
  sku                 = "premium"
  tags                = var.tags
}

provider "databricks" {
  host                        = var.databricks_host
  azure_workspace_resource_id = local.workspace_resource_id
  azure_client_id             = var.client_id
  azure_client_secret         = var.client_secret
  azure_tenant_id             = var.tenant_id
}

data "databricks_node_type" "smallest" {
  local_disk = false
  depends_on = [azurerm_databricks_workspace.claims]
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  depends_on        = [azurerm_databricks_workspace.claims]
}

resource "databricks_cluster" "claims_dev" {
  cluster_name            = "claims-dev-cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = "Standard_D2ads_v6"
  num_workers             = 0
  autotermination_minutes = 15
  data_security_mode      = "SINGLE_USER"

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                      = "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }

  depends_on = [azurerm_databricks_workspace.claims]
}

resource "databricks_sql_endpoint" "claims_wh" {
  name             = "CLAIMS_WH"
  cluster_size     = "2X-Small"
  max_num_clusters = 1
  auto_stop_mins   = 10
  warehouse_type            = "PRO"
  enable_serverless_compute = true

  depends_on = [azurerm_databricks_workspace.claims]
}

output "workspace_url"    { value = azurerm_databricks_workspace.claims.workspace_url }
output "workspace_id"     { value = azurerm_databricks_workspace.claims.workspace_id }
output "cluster_id"       { value = databricks_cluster.claims_dev.id }
output "sql_http_path"    { value = databricks_sql_endpoint.claims_wh.odbc_params[0].path }
