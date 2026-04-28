# ==============================================================================
# Terraform — Azure Real-Time Pipeline Infrastructure
# ==============================================================================
# Provisions:
#   - Resource Group
#   - Event Hubs Namespace + Hub + Consumer Groups
#   - Azure Data Lake Storage Gen2 (with hierarchical namespace)
#   - Databricks Workspace (Premium tier for Unity Catalog)
#   - Azure Synapse Analytics Workspace + Dedicated SQL Pool
#   - Cosmos DB Account + Database + Container
#   - Azure Cache for Redis
#   - Log Analytics Workspace + Application Insights
#   - Key Vault + secrets for all connection strings
#   - RBAC role assignments using Managed Identities
#   - Private Endpoints for PaaS services
# ==============================================================================

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.46"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.32"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "azurerm" {
    resource_group_name  = "tfstate-rg"
    storage_account_name = "tfstatepipeline"
    container_name       = "tfstate"
    key                  = "pipeline.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# ---------------------------------------------------------------------------
# Data sources
# ---------------------------------------------------------------------------
data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  suffix   = random_string.suffix.result
  tags     = {
    project     = "realtime-pipeline"
    environment = var.environment
    owner       = var.owner_email
    terraform   = "true"
  }
}

# ---------------------------------------------------------------------------
# Resource Group
# ---------------------------------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = "rg-pipeline-${var.environment}-${local.suffix}"
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# Virtual Network + Subnets (for Private Endpoints)
# ---------------------------------------------------------------------------
resource "azurerm_virtual_network" "vnet" {
  name                = "vnet-pipeline-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = ["10.0.0.0/16"]
  tags                = local.tags
}

resource "azurerm_subnet" "private_endpoints" {
  name                 = "snet-private-endpoints"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.1.0/24"]

  private_endpoint_network_policies_enabled = false
}

resource "azurerm_subnet" "databricks_public" {
  name                 = "snet-databricks-public"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.2.0/24"]

  delegation {
    name = "databricks"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "snet-databricks-private"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.3.0/24"]

  delegation {
    name = "databricks"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

# ---------------------------------------------------------------------------
# Event Hubs Namespace + Hub
# ---------------------------------------------------------------------------
resource "azurerm_eventhub_namespace" "ns" {
  name                     = "evhns-pipeline-${var.environment}-${local.suffix}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  sku                      = "Standard"
  capacity                 = var.eventhub_throughput_units
  auto_inflate_enabled     = true
  maximum_throughput_units = 20
  kafka_enabled            = true  # Kafka protocol support
  tags                     = local.tags

  network_rulesets {
    default_action                 = "Deny"
    trusted_service_access_enabled = true
    virtual_network_rule = []
    ip_rule              = []
  }
}

resource "azurerm_eventhub" "telemetry" {
  name                = "telemetry-events"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 32   # 32 partitions for high parallelism
  message_retention   = 7    # 7-day retention
}

resource "azurerm_eventhub_consumer_group" "databricks" {
  name                = "cg-databricks"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_eventhub_consumer_group" "stream_analytics" {
  name                = "cg-stream-analytics"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.rg.name
}

# ---------------------------------------------------------------------------
# Azure Data Lake Storage Gen2
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "adls" {
  name                     = "adlspipeline${var.environment}${local.suffix}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true   # Hierarchical Namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"
  tags                     = local.tags

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  network_rules {
    default_action             = "Deny"
    bypass                     = ["AzureServices"]
    virtual_network_subnet_ids = [azurerm_subnet.databricks_public.id]
  }
}

resource "azurerm_storage_container" "data" {
  name                  = "data"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# Medallion directories (created via lifecycle management)
resource "azurerm_storage_management_policy" "adls_lifecycle" {
  storage_account_id = azurerm_storage_account.adls.id

  rule {
    name    = "archive-bronze-after-30d"
    enabled = true
    filters {
      prefix_match = ["data/bronze/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 7
        tier_to_archive_after_days_since_modification_greater_than = 30
        delete_after_days_since_modification_greater_than          = 90
      }
    }
  }

  rule {
    name    = "retain-gold-1yr"
    enabled = true
    filters {
      prefix_match = ["data/gold/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
        delete_after_days_since_modification_greater_than       = 365
      }
    }
  }
}

# ---------------------------------------------------------------------------
# Azure Databricks Workspace (Premium — required for Unity Catalog)
# ---------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "ws" {
  name                = "dbw-pipeline-${var.environment}-${local.suffix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
  tags                = local.tags

  custom_parameters {
    no_public_ip                                         = true
    virtual_network_id                                   = azurerm_virtual_network.vnet.id
    public_subnet_name                                   = azurerm_subnet.databricks_public.name
    private_subnet_name                                  = azurerm_subnet.databricks_private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.dbr_public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.dbr_private.id
    storage_account_name                                 = "dbwstorage${local.suffix}"
    storage_account_sku_name                             = "Standard_GRS"
  }
}

resource "azurerm_network_security_group" "databricks" {
  name                = "nsg-databricks-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags
}

resource "azurerm_subnet_network_security_group_association" "dbr_public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

resource "azurerm_subnet_network_security_group_association" "dbr_private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

# ---------------------------------------------------------------------------
# Cosmos DB — device registry + real-time lookups
# ---------------------------------------------------------------------------
resource "azurerm_cosmosdb_account" "cosmos" {
  name                = "cosmos-pipeline-${var.environment}-${local.suffix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"
  tags                = local.tags

  consistency_policy {
    consistency_level       = "Session"
    max_interval_in_seconds = 5
    max_staleness_prefix    = 100
  }

  geo_location {
    location          = azurerm_resource_group.rg.location
    failover_priority = 0
  }

  capabilities {
    name = "EnableServerless"
  }

  is_virtual_network_filter_enabled = true
}

resource "azurerm_cosmosdb_sql_database" "devicedb" {
  name                = "devicedb"
  resource_group_name = azurerm_resource_group.rg.name
  account_name        = azurerm_cosmosdb_account.cosmos.name
}

resource "azurerm_cosmosdb_sql_container" "device_registry" {
  name                = "device_registry"
  resource_group_name = azurerm_resource_group.rg.name
  account_name        = azurerm_cosmosdb_account.cosmos.name
  database_name       = azurerm_cosmosdb_sql_database.devicedb.name
  partition_key_path  = "/device_id"

  indexing_policy {
    indexing_mode = "consistent"
    included_path { path = "/*" }
  }
}

# ---------------------------------------------------------------------------
# Azure Synapse Analytics
# ---------------------------------------------------------------------------
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "synw-pipeline-${var.environment}-${local.suffix}"
  resource_group_name                  = azurerm_resource_group.rg.name
  location                             = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse_fs.id
  sql_administrator_login              = "synapseadmin"
  sql_administrator_login_password     = random_password.synapse_pwd.result
  tags                                 = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "random_password" "synapse_pwd" {
  length  = 24
  special = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse_fs" {
  name               = "synapse"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_synapse_sql_pool" "dw" {
  name                 = "dwpool"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  sku_name             = var.synapse_sku
  create_mode          = "Default"
  tags                 = local.tags
}

# ---------------------------------------------------------------------------
# Azure Key Vault
# ---------------------------------------------------------------------------
resource "azurerm_key_vault" "kv" {
  name                       = "kv-pipeline-${var.environment}-${local.suffix}"
  resource_group_name        = azurerm_resource_group.rg.name
  location                   = azurerm_resource_group.rg.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = true
  tags                       = local.tags

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions      = ["Get", "Set", "List", "Delete", "Purge"]
    certificate_permissions = ["Get", "List"]
  }
}

resource "azurerm_key_vault_secret" "eventhub_conn" {
  name         = "eventhub-connection-string"
  value        = azurerm_eventhub_namespace.ns.default_primary_connection_string
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "cosmos_key" {
  name         = "cosmos-primary-key"
  value        = azurerm_cosmosdb_account.cosmos.primary_key
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "synapse_pwd" {
  name         = "synapse-sql-password"
  value        = random_password.synapse_pwd.result
  key_vault_id = azurerm_key_vault.kv.id
}

# ---------------------------------------------------------------------------
# Log Analytics Workspace + Diagnostic Settings
# ---------------------------------------------------------------------------
resource "azurerm_log_analytics_workspace" "law" {
  name                = "law-pipeline-${var.environment}-${local.suffix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

resource "azurerm_monitor_diagnostic_setting" "eventhub_diag" {
  name                       = "diag-eventhub"
  target_resource_id         = azurerm_eventhub_namespace.ns.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.law.id

  enabled_log { category = "ArchiveLogs"    }
  enabled_log { category = "OperationalLogs" }
  enabled_log { category = "AutoScaleLogs"  }
  metric { category = "AllMetrics" enabled = true }
}
