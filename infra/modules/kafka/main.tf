# infra/modules/kafka/main.tf
variable "resource_group_name" { type = string }
variable "location"            { type = string }
variable "environment"         { type = string }
variable "tags"                { type = map(string) }

resource "azurerm_eventhub_namespace" "claims" {
  name                = "evhns-claims-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "Standard"
  capacity            = 1
  tags                = var.tags
}

variable "eventhub_topics" {
  type    = list(string)
  default = ["claims.raw", "claims.validated", "claims.rejected", "claims.review_queue", "remittance.835"]
}

locals {
  topic_key_map = {
    for t in var.eventhub_topics :
    replace(t, ".", "_") => t
  }
}

resource "azurerm_eventhub" "topics" {
  for_each            = local.topic_key_map
  name                = each.value
  namespace_name      = azurerm_eventhub_namespace.claims.name
  resource_group_name = var.resource_group_name
  partition_count     = 3
  message_retention   = 7
}

resource "azurerm_eventhub_authorization_rule" "producer" {
  name                = "claims-producer"
  namespace_name      = azurerm_eventhub_namespace.claims.name
  eventhub_name       = azurerm_eventhub.topics["claims_raw"].name
  resource_group_name = var.resource_group_name
  listen              = false
  send                = true
  manage              = false
}

output "namespace_name"           { value = azurerm_eventhub_namespace.claims.name }
output "kafka_endpoint"           { value = "${azurerm_eventhub_namespace.claims.name}.servicebus.windows.net:9093" }
output "producer_connection_string" {
  value     = azurerm_eventhub_authorization_rule.producer.primary_connection_string
  sensitive = true
}
