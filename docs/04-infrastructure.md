# 04 — Infrastructure as Code Details

This document explains the technical implementation of the infrastructure foundation for the project.

### Separation of Concerns:
To maintain a modular and scalable codebase, this project strictly separates infrastructure into three distinct domains:
1. **Terraform (`infra/`):** Provisions the foundational Azure infrastructure (e.g., Resource Group, ADLS Gen2, Event Hubs, Key Vault, and the empty Databricks Workspace).
2. **Databricks Asset Bundles (`databricks/`):** Manages Databricks-internal data engineering resources (e.g., Jobs, Pipelines, Notebooks, Workflows) using the Databricks CLI.
3. **Liquibase (Planned):** Handles database schema migrations and data model changes.

---

## 1. Provider Configuration (`providers.tf`)

```terraform
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}
```

### Purpose & Technical Rationale:
- **`required_version = ">= 1.5.0"`:** Restricts execution to modern versions of Terraform that support native HCL capabilities (such as `import` blocks, enhanced checks, and stability improvements).
- **`azurerm` Provider from `hashicorp/azurerm`:** Official HashiCorp plugin for managing resources in Microsoft Azure via Azure Resource Manager (ARM) REST APIs.
- **`version = "~> 4.0"`:** Employs the pessimistic constraint operator. This permits non-breaking minor and patch updates (e.g., `4.81.0`) while preventing inadvertent upgrades to a major breaking release (e.g., `5.0.0`).
- **`features {}`:** Required by the AzureRM provider to configure provider-level behavior flags. Even when left default/empty, the block must be explicitly declared.

---

## 2. Parameterization (`variables.tf`)

```terraform
variable "location" {
  description = "Azure region where resources will be created"
  type        = string
  default     = "Central India"
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix for resource naming"
  type        = string
  default     = "scm-platform"
}
```

### Why Variables are Useful:
- **DRY (Don't Repeat Yourself):** Reusable tokens prevent copy-pasting strings across multiple resources.
- **Environment Agnostic:** The exact same Terraform codebase can deploy a `dev`, `qa`, or `prod` environment simply by altering input variables rather than duplicating resource blocks.
- **Validation & Typing:** Setting strict types (`type = string`) catches input errors during early static analysis.

---

## 3. Resource Definitions (`main.tf`)

```terraform
resource "azurerm_resource_group" "rg" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}
```

### Component Details:
- **Resource Group:** The top-level administrative boundary in Azure that holds related cloud resources for the solution lifecycle.
- **Predictable Naming:** Follows the standard pattern `rg-${var.project_name}-${var.environment}`. For the default dev environment, this resolves to `rg-scm-platform-dev`.
- **Governance Tags:**
  - `project = "supply-chain-control-tower"`: Links all resources to the business domain for cost allocation and enterprise reporting.
  - `environment = var.environment`: Clearly marks the tier (`dev`, `qa`, `prod`).
  - `managed_by = "terraform"`: Notifies operational teams that this resource is managed exclusively through automation and should not be modified manually in the Azure Portal.

---

## 4. Storage Architecture (`storage.tf`)

```terraform
resource "azurerm_storage_account" "sa" {
  name                     = replace("st${var.project_name}${var.environment}", "-", "")
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_storage_container" "containers" {
  for_each              = toset(["landing", "checkpoints", "rejected"])
  name                  = each.key
  storage_account_id    = azurerm_storage_account.sa.id
  container_access_type = "private"
}
```

### What is ADLS Gen2?
Azure Data Lake Storage Gen2 (ADLS Gen2) is Microsoft's dedicated cloud big-data analytics store. It combines the mass scalability, high availability, and cost-effectiveness of Azure Blob Storage with a high-performance **Hierarchical Namespace (HNS)**.

### Why This Project Needs ADLS Gen2:
- **Underlying Lakehouse Storage:** Acts as the physical storage layer for all raw files, Delta Lake tables (Bronze, Silver, Gold), and quarantine directories.
- **Spark & Databricks Native Optimization:** The ABFS (Azure Blob File System) driver in Azure Databricks is purpose-built to read and write directly to ADLS Gen2 with optimized parallel I/O.
- **Unified Batch & Stream Sink:** Supports both continuous append streams from Event Hub consumers and large scheduled batch file drops.

### Why Hierarchical Namespace (HNS) is Enabled (`is_hns_enabled = true`):
- **Atomic Operations:** In standard flat blob storage, renaming a "directory" requires reading, copying, and deleting every individual blob under that prefix ($O(N)$ operations). With HNS enabled, directories are real filesystem primitives, making directory renames and moves atomic $O(1)$ operations.
- **Spark Job Performance:** When Spark/Databricks completes a job or commit, it commits output by renaming temporary staging directories (`_temporary`) to the final Delta table location. Without HNS, this commit phase creates massive performance bottlenecks and data inconsistency risks.
- **POSIX-Like Access Control:** Enables fine-grained Access Control Lists (ACLs) across directory hierarchies.

### Storage Account Naming Standard:
Azure Storage accounts impose rigid naming constraints:
- **Constraints:** 3 to 24 characters, numbers and lowercase letters only (no hyphens, underscores, or special characters).
- **Global Uniqueness:** The name must be globally unique across all of Microsoft Azure.
- **Naming Expression:** `replace("st${var.project_name}${var.environment}", "-", "")`
  - Strips hyphens from `scm-platform` and `dev`
  - Default result: `stscmplatformdev` (16 alphanumeric characters, fully compliant).

### Container Manifest & Architectural Purposes:
All containers are configured with `container_access_type = "private"` to block anonymous public access:
| Container Name | Architectural Purpose | Ingestion / Processing Role |
|---|---|---|
| **`landing`** | Raw Staging Zone | Initial drop location for external partner feeds, carrier API dumps, and WMS batch NDJSON files prior to lakehouse ingestion. |
| **`checkpoints`** | Streaming & State Store | Holds stream metadata, transaction offsets, and schema logs for Databricks Auto Loader and Spark Structured Streaming jobs, guaranteeing idempotent execution and exactly-once processing. |
| **`rejected`** | Raw Quarantine Zone | Holds malformed files, schema-violating payloads, and rejected transfer records that fail basic ingress validation before reaching the Bronze layer. |

> **Deployment Status:** **`DEPLOYED`**. Resources are fully declared in `storage.tf` and have been deployed to Azure via `terraform apply`.

---

## 5. Event Streaming Architecture (`eventhub.tf`)

```terraform
resource "azurerm_eventhub_namespace" "evhns" {
  name                = "evhns-${var.project_name}-${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_eventhub" "gps_events" {
  name              = "gps-events"
  namespace_id      = azurerm_eventhub_namespace.evhns.id
  partition_count   = 8
  message_retention = 7
}
```

### What is Azure Event Hubs?
Azure Event Hubs is a fully managed, real-time distributed data streaming service capable of receiving and processing millions of events per second with sub-second latency. It provides a Kafka-compatible endpoint, enabling seamless integration with cloud-native analytics platforms like Apache Spark and Azure Databricks.

### Core Concepts:
- **Namespace vs. Event Hub:**
  - **Event Hubs Namespace:** The root management and scoping container (e.g., `evhns-scm-platform-dev`). It provides a unique FQDN endpoint, network access controls, disaster recovery configuration, and allocated capacity (Throughput Units).
  - **Event Hub:** The specific topic/channel inside the namespace (e.g., `gps-events`). It is where producers send events and from which consumers read.
- **Producer vs. Consumer:**
  - **Producers:** Upstream applications or devices that publish telemetry (e.g., in-vehicle GPS trackers, yard RFID gateways, and Debezium Oracle CDC connectors).
  - **Consumers:** Downstream streaming engines (e.g., Azure Databricks Spark Structured Streaming jobs) that connect via consumer groups, reading partition offsets sequentially without deleting data.

### Purpose of the `gps-events` Event Hub:
The `gps-events` hub serves as the ingestion buffer for all streaming telemetry across the supply chain network:
- Periodic GPS coordinate pings, vehicle speed, odometer readings, and route progression.
- Container environmental telemetry (cold-chain temperature, humidity, vibration sensors).
- Inbound change event envelopes from the Debezium Oracle CDC pipeline.

### Why 8 Partitions are Configured (`partition_count = 8`):
- **Parallelism & Concurrency:** Partitions are the primary unit of throughput and consumer concurrency. In Spark Structured Streaming, each partition maps to an independent Spark task/executor. Having 8 partitions enables up to 8 Spark cores to read and process telemetry in parallel.
- **Order Preservation:** Events submitted with the same partition key (e.g., `vehicle_id` or `shipment_id`) are routed to the same partition, guaranteeing strict chronological ordering for that entity.
- **Capacity Scaling:** Prepares the solution for production fleet scale, avoiding costly partition resizing and stream restarts.

### Why 7-Day Message Retention is Configured (`message_retention = 7`):
- **Maximum Standard Buffer:** 7 days (168 hours) is the maximum retention period supported by the Event Hubs Standard SKU.
- **Resilience & Pipeline Decoupling:** If the downstream Databricks ingestion cluster experiences a failure, routine maintenance, or schema updates, events continue accumulating safely in the Event Hub.
- **Backfill & Replay Capability:** Enables operational teams to replay historical sensor streams up to 7 days in the past during incident investigations or algorithm model retuning.

### Integration in Platform Architecture:
```
[In-Vehicle GPS / RFID Sensors]  ──┐
                                  ├──> [Event Hub: gps-events] ──> [Databricks Structured Streaming] ──> [Bronze Delta]
[Oracle ERP (via Debezium CDC)] ──┘
```

> **Deployment Status:** **`DEPLOYED`**. Defined in `eventhub.tf` and deployed to Azure via `terraform apply`.

---

## 6. Secret Management Architecture (`keyvault.tf`)

```terraform
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "kv" {
  name                          = "kv-${var.project_name}-${var.environment}"
  location                      = var.location
  resource_group_name           = azurerm_resource_group.rg.name
  tenant_id                     = data.azurerm_client_config.current.tenant_id
  sku_name                      = "standard"
  soft_delete_retention_days    = 7
  purge_protection_enabled      = false
  rbac_authorization_enabled    = true
  public_network_access_enabled = true

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}
```

### What is Azure Key Vault?
Azure Key Vault is a cloud service that provides a secure store for secrets, keys, and certificates. It enables centralized management of sensitive information, strict access control, and comprehensive audit logging.

### Why This Project Needs Key Vault:
- **Zero-Trust Secret Handling:** Eliminates the need to store passwords, API keys, or connection strings in code repositories, environment variables, or CI/CD pipelines.
- **Databricks Secret Scope Backend:** In a later phase, this Key Vault will be connected directly to Azure Databricks. Databricks notebooks and data pipelines will seamlessly fetch credentials at runtime without exposing the values to developers or logging them to output.
- **Service Principal Identity:** Provides a secure location to store the credentials of the Service Principals used by Debezium and external APIs to authenticate with the platform.

### Configuration Rationale:
- **Dynamic Tenant ID:** `tenant_id = data.azurerm_client_config.current.tenant_id` automatically associates the Key Vault with the Azure Active Directory tenant executing the Terraform code, preventing hardcoded tenant GUIDs.
- **RBAC Authorization (`rbac_authorization_enabled = true`):** Migrates away from legacy Key Vault Access Policies towards modern Azure Role-Based Access Control (RBAC). This allows assigning permissions (e.g., *Key Vault Secrets User*) natively through Azure IAM at the management group, subscription, or resource group level.
- **Development Lifecycle Protections:**
  - `soft_delete_retention_days = 7`: Provides a 7-day safety net where accidentally deleted secrets can be recovered.
  - `purge_protection_enabled = false`: Allows developers or automation to permanently delete the vault or its contents during the `dev` environment teardown lifecycle without mandatory waiting periods. (This is strictly changed to `true` in production).

> **Deployment Status:** **`DEPLOYED`**. Defined in `keyvault.tf` and deployed to Azure via `terraform apply`.

---

## 7. Databricks Compute & Access Architecture (`databricks.tf`)

```terraform
resource "azurerm_databricks_workspace" "dbw" {
  name                        = "dbw-${var.project_name}-${var.environment}"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = var.location
  sku                         = "premium"
  managed_resource_group_name = "rg-dbw-${var.project_name}-${var.environment}-managed"

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_databricks_access_connector" "dbac" {
  name                = "dbac-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}
```

### What is Azure Databricks Workspace?
Azure Databricks is an Apache Spark-based data analytics platform optimized for the Microsoft Azure cloud services platform. The workspace provides a unified environment for data scientists, data engineers, and business analysts to collaborate.

### Why This Project Needs the Workspace:
- **Core Processing Engine:** It serves as the primary compute engine for ingesting raw data, running data quality validations, and aggregating metrics across the Medallion architecture (Bronze, Silver, Gold).
- **Premium SKU Necessity:** The `premium` SKU is strictly required to enable Unity Catalog, which provides centralized governance, fine-grained access control, and data lineage across the lakehouse.

### What is the Access Connector and Managed Identity?
The Azure Databricks Access Connector is an Azure resource that provides a first-party managed identity for Unity Catalog to access data in Azure storage accounts.
- **System-Assigned Managed Identity:** By specifying `type = "SystemAssigned"`, Azure automatically creates and manages an identity in Azure Active Directory tied to the lifecycle of this connector.
- **Passwordless Security:** This allows Unity Catalog to securely read and write data in the ADLS Gen2 `landing`, `checkpoints`, and `rejected` containers without configuring, managing, or rotating any static storage account keys or Service Principal secrets.

### How it Fits into the Architecture:
In the future Unity Catalog setup:
1. The Access Connector is granted the `Storage Blob Data Contributor` role on the ADLS Gen2 storage account.
2. Unity Catalog is configured with a Storage Credential that references the Access Connector.
3. Unity Catalog External Locations are created to map Databricks tables directly to the ADLS containers securely.

> **Deployment Status:** **`DEPLOYED`**. Defined in `databricks.tf` and deployed to Azure via `terraform apply`.

---

## 8. Role-Based Access Control (RBAC) Architecture (`rbac.tf`)

```terraform
resource "azurerm_role_assignment" "dbac_storage_contributor" {
  scope                = azurerm_storage_account.sa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.dbac.identity[0].principal_id
}
```

### What is Azure RBAC?
Azure Role-Based Access Control (RBAC) is Microsoft's primary authorization system for managing access to Azure resources. Instead of managing individual access keys or SAS tokens, RBAC assigns roles (a collection of permissions) to security principals (users, groups, service principals, or managed identities) at a specific scope (subscription, resource group, or individual resource).

### Role and Scope Details:
- **Principal:** `azurerm_databricks_access_connector.dbac.identity[0].principal_id`. This dynamically references the System-Assigned Managed Identity of the Databricks Access Connector.
- **Scope:** `azurerm_storage_account.sa.id`. The role assignment is scoped specifically to our project's ADLS Gen2 storage account (`stscmplatformdev`). It does not grant access to any other storage accounts in the subscription.
- **Role Definition:** `Storage Blob Data Contributor`. This is a built-in Azure data-plane role that allows read, write, and delete operations on Azure Storage blob containers.

### Why Managed Identity over Storage Keys?
- **Security Posture:** Storage account keys grant full administrative control over the entire storage account and must be rotated manually if compromised. Managed Identities use short-lived tokens automatically generated and rotated by Azure Active Directory.
- **Unity Catalog Integration:** Unity Catalog explicitly requires this architecture to create a "Storage Credential" and "External Locations", mapping Databricks catalogs securely to ADLS Gen2 containers.

> **Deployment Status:** **`DEPLOYED`**. Defined in `rbac.tf` and deployed to Azure via `terraform apply`.

---

## 9. Environment Folder (`envs/`)

### Purpose:
- The `infra/envs/` directory is reserved to house per-environment variable override files (`.tfvars`), such as:
  - `dev.tfvars`
  - `qa.tfvars`
  - `prod.tfvars`
- These files allow engineers or automated CI/CD pipelines to supply distinct parameters (e.g., different regions, sizing, or redundancy tiers) per stage without changing the underlying Terraform definitions.
- **Current Status:** The `envs/` folder is created with a placeholder (`.gitkeep`). **Environment-specific `.tfvars` files are not created yet**, as they will be introduced when multi-environment deployments are configured.

---

## 10. Local & Generated Terraform Files

Understanding the role of auxiliary files generated during development is essential for hygiene and security:

### `.terraform/` (Directory)
- **What it is:** A local cache created when running `terraform init`. It contains the downloaded compiled provider plugins (e.g., `terraform-provider-azurerm_v4.81.0_x5.exe`) and backend configurations.
- **VCS Rule:** **Never commit to Git.** It is platform-specific, huge in binary size, and can always be recreated cleanly by running `terraform init`.

### `.terraform.lock.hcl` (Lock File)
- **What it is:** The dependency lock file. It records the exact version and cryptographic checksums of the providers downloaded during initialization.
- **VCS Rule:** **Normally committed to Git.** Checking this file into version control guarantees that every teammate and CI/CD agent installs the exact same provider binaries, preventing unexpected behavior from newer patch versions.

### `terraform.tfstate` / `*.tfstate.*` (State Files)
- **What it is:** The JSON file mapping your Terraform code to real-world cloud resources, tracking IDs, metadata, and resource attributes.
- **VCS Rule:** **Never commit to Git.** Local state files can contain unencrypted sensitive values, credentials, or private connection strings. Furthermore, sharing local state via Git causes concurrency conflicts and state corruption. (In future steps, state will be stored in a secured, encrypted remote backend such as Azure Blob Storage).

### `.gitignore`
- **What it is:** Instructs Git to ignore transient, machine-specific, or security-sensitive files.
- **Current Content:**
  ```gitignore
  .terraform/
  *.tfstate
  *.tfstate.*
  crash.log
  ```
- **Why these entries matter:** It guarantees that developer machines do not inadvertently leak binary caches, secrets in state files, or debug crash traces into the shared repository.
