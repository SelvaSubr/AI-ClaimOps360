# infra/modules/snowflake/main.tf
terraform {
  required_providers {
    snowflake = { source = "Snowflakedb/snowflake", version = "~> 0.87" }
  }
}

variable "snowflake_org_name" {
  description = "Snowflake organization name (from CURRENT_ORGANIZATION_NAME())"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name (from CURRENT_ACCOUNT_NAME())"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  sensitive   = true
  type        = string
}

variable "snowflake_role" {
  description = "Snowflake role for Terraform (must already exist and be granted to the user)"
  type        = string
  default     = "SYSADMIN"
}

provider "snowflake" {
  organization_name = var.snowflake_org_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role
  authenticator     = "snowflake"
}

resource "snowflake_database" "claims" {
  name = "CLAIMS_ANALYTICS"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.claims.name
  name     = "SILVER"
  lifecycle { ignore_changes = all }
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.claims.name
  name     = "GOLD"
  lifecycle { ignore_changes = all }
}

resource "snowflake_warehouse" "claims" {
  name           = "CLAIMS_WH"
  warehouse_size = "xsmall"
  auto_suspend   = 60
  auto_resume    = true
  initially_suspended = true
}

resource "snowflake_account_role" "analyst"  { name = "CLAIMS_ANALYST_ROLE" }
resource "snowflake_account_role" "loader"   { name = "CLAIMS_LOADER_ROLE" }
resource "snowflake_account_role" "admin"    { name = "CLAIMS_ADMIN_ROLE" }

output "database_name"   {
  value = snowflake_database.claims.name
}
output "warehouse_name"  {
  value = snowflake_warehouse.claims.name
}
