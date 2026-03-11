# infra/variables.tf
variable "subscription_id" {
  type = string
}
variable "client_id"       {
  type = string
}
variable "client_secret"   {
  type = string
  sensitive = true
}
variable "tenant_id"       {
  type = string
}
variable "service_principal_object_id" {
  type = string
}

variable "environment" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev staging or prod."
}
}

variable "key_vault_name" {
  type        = string
  description = "Azure Key Vault name — must be globally unique, 3-24 chars. Set TF_VAR_key_vault_name or supply in terraform.tfvars."
}

variable "location" {
  type    = string
  default = "eastus2"
}

variable "tags" {
  type = map(string)
  default = {
    project     = "AI-ClaimOps360"
    environment = "dev"
    managed_by  = "terraform"
}
}

# Snowflake
variable "snowflake_org_name" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name"
  type        = string
}

variable "snowflake_user" {
  type = string
}

variable "snowflake_password" {
  type      = string
  sensitive = true
}

variable "snowflake_role" {
  description = "Snowflake role for Terraform provisioning"
  type        = string
  default     = "SYSADMIN"
}

variable "databricks_host" {
  description = "Databricks workspace URL — set after first apply"
  type        = string
  default     = ""
}
