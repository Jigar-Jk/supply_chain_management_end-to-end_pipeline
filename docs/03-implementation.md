# 03 — Chronological Implementation Log

This document serves as the running engineering log of all implementation steps completed in the project. Each step details what was changed, why, and how it was verified.

---

## Step 1 — Terraform Foundation

### Objective
Establish a clean, standardized, version-controlled Infrastructure as Code (IaC) foundation targeting Microsoft Azure without hardcoded secrets, establishing the base resource group configuration for the dev environment.

### Why Terraform is Being Used
- **Declarative & Version Controlled:** Infrastructure state is tracked in code, reviewed through pull requests, and auditable in Git.
- **Repeatable Multi-Environment Deployment:** Enables identical deployments across `dev`, `qa`, and `prod` with zero manual configuration drift.
- **Provider Ecosystem:** Tight integration with the Azure Resource Manager (`azurerm`) provider ensures comprehensive resource management.

### What Was Created
A minimal, decoupled Terraform foundation located in the `infra/` folder.

#### Project Folder Structure
```
scm-logistics-platform/
├── .gitignore
├── README.md
├── docs/
│   ├── 01-project-overview.md
│   ├── 02-architecture.md
│   ├── 03-implementation.md
│   ├── 04-infrastructure.md
│   ├── 05-data-flow.md
│   └── decisions.md
└── infra/
    ├── .terraform/               # Local provider binary cache (git-ignored)
    ├── .terraform.lock.hcl       # Provider dependency lock file
    ├── envs/                     # Folder for environment-specific configs (.tfvars)
    ├── main.tf                   # Declares Azure Resource Group
    ├── providers.tf              # Configures Terraform & azurerm provider
    └── variables.tf              # Declares configurable variables & defaults
```

#### File Details
1. **[`infra/providers.tf`](file:///c:/scm-logistics-platform/infra/providers.tf):**
   - Configured minimum Terraform core version `>= 1.5.0`.
   - Added `hashicorp/azurerm` provider pinned to `~> 4.0`.
   - Initialized `provider "azurerm" { features {} }`.
2. **[`infra/variables.tf`](file:///c:/scm-logistics-platform/infra/variables.tf):**
   - `location`: type `string`, default `"Central India"`.
   - `environment`: type `string`, default `"dev"`.
   - `project_name`: type `string`, default `"scm-platform"`.
3. **[`infra/main.tf`](file:///c:/scm-logistics-platform/infra/main.tf):**
   - Configured only the Azure Resource Group (`azurerm_resource_group.rg`).
   - Resource name dynamically set to `"rg-${var.project_name}-${var.environment}"`.
   - Resource location set to `var.location`.
   - Mandatory governance tags applied:
     ```hcl
     tags = {
       project     = "supply-chain-control-tower"
       environment = var.environment
       managed_by  = "terraform"
     }
     ```
4. **[`infra/envs/`](file:///c:/scm-logistics-platform/infra/envs):**
   - Created empty directory structure reserved for future per-environment variables (e.g., `dev.tfvars`, `prod.tfvars`).
5. **[`.gitignore`](file:///c:/scm-logistics-platform/.gitignore):**
   - Configured rules to prevent committing `.terraform/`, `*.tfstate`, `*.tfstate.*`, and `crash.log`.

### Execution & Verification
1. **Terraform Formatting:**
   ```powershell
   terraform -chdir=infra fmt -check -diff
   ```
   *Result:* Code adheres 100% to canonical formatting rules.
2. **Terraform Initialization:**
   ```powershell
   terraform -chdir=infra init
   ```
   *Result:* Successfully initialized backend and downloaded `hashicorp/azurerm v4.81.0`. Created `.terraform.lock.hcl`.
3. **Terraform Validation:**
   ```powershell
   terraform -chdir=infra validate
   ```
   *Result:* `Success! The configuration is valid.`
4. **Terraform Plan:**
   ```powershell
   terraform -chdir=infra plan
   ```
   *Result:* Plan generated successfully against Azure CLI subscription context:
   ```text
   Plan: 1 to add, 0 to change, 0 to destroy.
   + resource "azurerm_resource_group" "rg" {
       + id       = (known after apply)
       + location = "centralindia"
       + name     = "rg-scm-platform-dev"
       + tags     = {
           + "environment" = "dev"
           + "managed_by"  = "terraform"
           + "project"     = "supply-chain-control-tower"
         }
     }
   ```

### Status
- **`terraform apply` Status:** **NOT APPLIED**. Per project policy and instructions, `terraform apply` was intentionally omitted for this foundational step. Resources have not yet been provisioned in the cloud subscription.

---

## Step 2 — ADLS Gen2 Storage Foundation

### Objective
Define the Azure Data Lake Storage Gen2 (ADLS Gen2) foundation with Hierarchical Namespace (HNS) enabled and core blob containers required for supply chain data ingestion, streaming checkpoints, and quarantine handling.

### Why We Changed It
- **Analytical Storage Foundation:** The lakehouse architecture requires high-throughput cloud storage capable of handling both large batch files and micro-batched real-time streams.
- **Hierarchical Namespace (HNS):** Essential for big data engines (Apache Spark / Azure Databricks). It organizes objects into a true directory hierarchy, enabling atomic directory renames and fast metadata operations rather than slow emulated object renames.
- **Dedicated Container Separation:**
  - `landing`: Secure staging area for external files (carrier API payloads, WMS batch exports) before ingestion.
  - `checkpoints`: Isolated location for Structured Streaming and Auto Loader schema/offset state tracking, ensuring idempotent and exactly-once processing.
  - `rejected`: Dedicated zone for raw files failing initial transport or structural validations prior to Bronze materialization.

### Files Created / Modified
- **[NEW]** [`infra/storage.tf`](file:///c:/scm-logistics-platform/infra/storage.tf): Declares the storage account (`azurerm_storage_account.sa`) and three core blob containers (`azurerm_storage_container.containers`).

### Resources Defined
1. `azurerm_storage_account.sa`:
   - Name: `stscmplatformdev` (derived via `replace("st${var.project_name}${var.environment}", "-", "")`)
   - Resource Group: `rg-scm-platform-dev`
   - Location: `Central India` (`var.location`)
   - Kind: `StorageV2`
   - Tier: `Standard`
   - Replication: `LRS`
   - Hierarchical Namespace: `is_hns_enabled = true`
   - Tags: `project = "supply-chain-control-tower"`, `environment = "dev"`, `managed_by = "terraform"`
2. `azurerm_storage_container.containers["landing"]`: Private blob container for incoming raw source files.
3. `azurerm_storage_container.containers["checkpoints"]`: Private blob container for streaming/Auto Loader checkpoint locations.
4. `azurerm_storage_container.containers["rejected"]`: Private blob container for quarantined/rejected raw files.

### Commands Used
```powershell
terraform -chdir=infra fmt -check -diff
terraform -chdir=infra validate
terraform -chdir=infra plan
```

### Verification
1. **Terraform Format:** Canonical indentation and formatting confirmed.
2. **Terraform Validate:**
   ```text
   Success! The configuration is valid.
   ```
3. **Terraform Plan:**
   ```text
   Plan: 5 to add, 0 to change, 0 to destroy.
   # azurerm_resource_group.rg will be created
   # azurerm_storage_account.sa will be created
   # azurerm_storage_container.containers["checkpoints"] will be created
   # azurerm_storage_container.containers["landing"] will be created
   # azurerm_storage_container.containers["rejected"] will be created
   ```

### Status
- **`terraform apply` Status:** **NOT APPLIED**. `terraform apply` was intentionally skipped. Infrastructure definitions are validated in code and plan, but no storage resources have been created in the Azure subscription yet.

---

## Step 3 — Azure Event Hub Foundation

### Objective
Define the Azure Event Hubs Namespace and the foundational `gps-events` Event Hub required to ingest real-time IoT/GPS telemetry and Oracle CDC events into the lakehouse streaming pipeline.

### Why We Changed It
- **Real-Time Streaming Broker:** Telemetry from in-transit vehicles and IoT asset sensors requires a decoupled, high-throughput message ingestion tier that buffers spikes in incoming traffic.
- **Partitioning Strategy (8 Partitions):**
  - Enables parallel consumption by Spark Structured Streaming workers (each partition can be read concurrently by a separate Spark task/core).
  - Distributes message load across partition keys (e.g., `vehicle_id` or `shipment_id`) to prevent partition hot-spotting.
  - Provides adequate throughput capacity for anticipated fleet scale without repartitioning downtime.
- **Message Retention (7 Days):**
  - Standard SKU maximum buffer window (168 hours).
  - Provides a critical safety and replay buffer: if downstream Databricks clusters undergo maintenance, failure, or schema updates, events accumulate safely in the Event Hub and can be caught up without data loss.
- **CDC & IoT Unification:** Serves as the real-time message bus for both live vehicle telemetry (Pattern 4) and upstream transactional database log events emitted by Debezium (Pattern 3).

### Files Created / Modified
- **[NEW]** [`infra/eventhub.tf`](file:///c:/scm-logistics-platform/infra/eventhub.tf): Declares the Event Hubs Namespace (`azurerm_eventhub_namespace.evhns`) and the `gps-events` hub (`azurerm_eventhub.gps_events`).

### Resources Defined
1. `azurerm_eventhub_namespace.evhns`:
   - Name: `evhns-scm-platform-dev`
   - Resource Group: `rg-scm-platform-dev`
   - Location: `Central India` (`var.location`)
   - SKU: `Standard`
   - Capacity: `1` Throughput Unit
   - Tags: `project = "supply-chain-control-tower"`, `environment = "dev"`, `managed_by = "terraform"`
2. `azurerm_eventhub.gps_events`:
   - Name: `gps-events`
   - Namespace ID: `azurerm_eventhub_namespace.evhns.id`
   - Partition Count: `8`
   - Message Retention: `7` days

### Commands Used
```powershell
terraform -chdir=infra fmt
terraform -chdir=infra validate
terraform -chdir=infra plan
```

### Verification
1. **Terraform Format:** Code formatted and verified with `terraform -chdir=infra fmt -check`.
2. **Terraform Validate:**
   ```text
   Success! The configuration is valid.
   ```
3. **Terraform Plan:**
   ```text
   Plan: 7 to add, 0 to change, 0 to destroy.
   # azurerm_resource_group.rg will be created
   # azurerm_storage_account.sa will be created
   # azurerm_storage_container.containers["checkpoints"] will be created
   # azurerm_storage_container.containers["landing"] will be created
   # azurerm_storage_container.containers["rejected"] will be created
   # azurerm_eventhub_namespace.evhns will be created
   # azurerm_eventhub.gps_events will be created
   ```

### Status
- **`terraform apply` Status:** **NOT APPLIED**. `terraform apply` was intentionally skipped. The Event Hub resources are **`CONFIGURED (PLANNED)`** in Terraform code and verified through dry-run plan, but have not been provisioned in Azure yet.


---

## Step 4 — Azure Key Vault Foundation

### Objective
Define the centralized secret-management foundation for the project using Azure Key Vault, securing the infrastructure layer for future storage of API keys, database credentials, Event Hub connection strings, and service principal secrets — without storing any actual secret values in code.

### Why We Changed It
- **Zero Hardcoded Secrets:** Supply chain integrations require credentials for carrier APIs, Event Hub endpoints, Oracle CDC connections, and Databricks service principals. Storing these directly in Terraform files, environment variables, or Git is a critical security vulnerability. Key Vault provides a centralized, encrypted, access-controlled vault.
- **Databricks Integration Path:** In future steps, Azure Key Vault will back a Databricks secret scope, enabling notebooks and jobs to securely retrieve credentials at runtime via `dbutils.secrets.get()` without exposing values in code.
- **RBAC Authorization Model:** Using `rbac_authorization_enabled = true` enables Azure Role-Based Access Control for vault operations instead of legacy vault access policies, aligning with modern Azure security best practices and simplifying identity management.
- **Dev Environment Settings:** `soft_delete_retention_days = 7` and `purge_protection_enabled = false` provide a lightweight safety net appropriate for a development tier without the operational overhead of long retention windows.

### Files Created / Modified
- **[NEW]** [`infra/keyvault.tf`](file:///c:/scm-logistics-platform/infra/keyvault.tf): Declares the `data.azurerm_client_config.current` data source and the `azurerm_key_vault.kv` resource.

### Resources Defined
1. `data.azurerm_client_config.current`:
   - Data source retrieving the current Azure CLI tenant ID for Key Vault tenant binding.
2. `azurerm_key_vault.kv`:
   - Name: `kv-scm-platform-dev`
   - Resource Group: `rg-scm-platform-dev`
   - Location: `Central India` (`var.location`)
   - Tenant ID: dynamically resolved from `data.azurerm_client_config.current.tenant_id`
   - SKU: `standard`
   - Soft Delete Retention: `7` days
   - Purge Protection: `false` (dev environment)
   - RBAC Authorization: `true` (modern Azure RBAC model)
   - Tags: `project = "supply-chain-control-tower"`, `environment = "dev"`, `managed_by = "terraform"`

### Security Configuration
- **No secrets stored in code:** The Key Vault resource definition contains zero secret values. No API keys, passwords, connection strings, or tokens exist in any Terraform file.
- **RBAC-based access control:** Replaces legacy access policies with Azure IAM role assignments for fine-grained, auditable permissions.
- **Soft delete enabled:** Accidentally deleted secrets are recoverable for 7 days.

### Commands Used
```powershell
terraform -chdir=infra fmt
terraform -chdir=infra validate
terraform -chdir=infra plan
```

### Verification
1. **Terraform Format:** Code formatted canonically with `terraform fmt`.
2. **Terraform Validate:**
   ```text
   Success! The configuration is valid.
   ```
3. **Terraform Plan:**
   ```text
   Plan: 8 to add, 0 to change, 0 to destroy.
   # azurerm_resource_group.rg will be created
   # azurerm_storage_account.sa will be created
   # azurerm_storage_container.containers["checkpoints"] will be created
   # azurerm_storage_container.containers["landing"] will be created
   # azurerm_storage_container.containers["rejected"] will be created
   # azurerm_eventhub_namespace.evhns will be created
   # azurerm_eventhub.gps_events will be created
   # azurerm_key_vault.kv will be created
   ```

### Status
- **`terraform apply` Status:** **NOT APPLIED**. `terraform apply` was intentionally skipped. The Key Vault resource is **`CONFIGURED (PLANNED)`** in Terraform code and verified through dry-run plan, but has not been provisioned in Azure yet.


---

## Step 5 — Azure Databricks Workspace & Access Connector Foundation

### Objective
Deploy the Azure Databricks Workspace as the core analytics compute engine for the platform, alongside an Azure Databricks Access Connector to provide a managed identity for secure, passwordless access to Azure Data Lake Storage via Unity Catalog.

### Why We Changed It
- **Centralized Lakehouse Compute:** Azure Databricks is the primary environment for running data ingestion (Auto Loader), stream processing (Structured Streaming), and complex transformations on Delta Lake. It serves as the unified compute layer for Bronze, Silver, and Gold pipelines.
- **Premium SKU for Governance:** A `premium` SKU is required to support Unity Catalog, which provides centralized data governance, metadata management, and role-based access control.
- **Secure Data Access:** The Databricks Access Connector is an Azure resource that provides a System-Assigned Managed Identity. Instead of using fragile and insecure Service Principal client secrets or storage account keys, Unity Catalog will assume this identity to read/write from ADLS Gen2.

### Files Created / Modified
- **[NEW]** [`infra/databricks.tf`](file:///c:/scm-logistics-platform/infra/databricks.tf): Declares the `azurerm_databricks_workspace` and `azurerm_databricks_access_connector` resources.

### Resources Defined
1. `azurerm_databricks_workspace.dbw`:
   - Name: `dbw-scm-platform-dev`
   - Resource Group: `rg-scm-platform-dev`
   - Location: `Central India` (`var.location`)
   - SKU: `premium`
   - Managed Resource Group: `rg-dbw-scm-platform-dev-managed`
2. `azurerm_databricks_access_connector.dbac`:
   - Name: `dbac-scm-platform-dev`
   - Resource Group: `rg-scm-platform-dev`
   - Location: `Central India` (`var.location`)
   - Identity Type: `SystemAssigned`

### Security Configuration
- **No hardcoded credentials:** The Access Connector relies entirely on a System-Assigned Managed Identity, meaning Azure automatically rotates the underlying credentials.
- **No secrets in code:** No tokens or connection strings were created or stored.

### Commands Used
```powershell
terraform -chdir=infra fmt
terraform -chdir=infra validate
terraform -chdir=infra plan
```

### Verification
1. **Terraform Format:** Code formatted canonically with `terraform fmt`.
2. **Terraform Validate:**
   ```text
   Success! The configuration is valid.
   ```
3. **Terraform Plan:**
   ```text
   Plan: 10 to add, 0 to change, 0 to destroy.
   # azurerm_resource_group.rg will be created
   # azurerm_storage_account.sa will be created
   # azurerm_storage_container.containers["checkpoints"] will be created
   # azurerm_storage_container.containers["landing"] will be created
   # azurerm_storage_container.containers["rejected"] will be created
   # azurerm_eventhub_namespace.evhns will be created
   # azurerm_eventhub.gps_events will be created
   # azurerm_key_vault.kv will be created
   # azurerm_databricks_workspace.dbw will be created
   # azurerm_databricks_access_connector.dbac will be created
   ```

### Status
- **`terraform apply` Status:** **NOT APPLIED**. `terraform apply` was intentionally skipped. The Databricks resources are **`CONFIGURED (PLANNED)`** in Terraform code and verified through dry-run plan, but have not been provisioned in Azure yet.

---

## Step 6 — Initial Azure Foundation Deployment

### Objective
Deploy the validated Terraform foundation (10 resources) to Microsoft Azure. This establishes the physical cloud environment—including the Resource Group, ADLS Gen2 storage, Event Hubs, Key Vault, and Databricks Workspace—that will host the Medallion architecture.

### Why We Changed It
- The infrastructure configuration was previously only PLANNED and verified via static checks.
- Actual deployment is required to proceed with configuring Databricks Data Intelligence Platform features, Unity Catalog bindings, and Auto Loader pipelines.

### Commands Used
```powershell
terraform -chdir=infra plan -no-color
terraform -chdir=infra apply -no-color
terraform -chdir=infra state list
az group show --name rg-scm-platform-dev --output table
az databricks workspace show --resource-group rg-scm-platform-dev --name dbw-scm-platform-dev
```

### Verification
1. **Terraform Apply Result:**
   ```text
   Apply complete! Resources: 10 added, 0 changed, 0 destroyed.
   ```
2. **Terraform State Tracking:** The local `.terraform.tfstate` file now tracks 10 deployed resources.
3. **Azure Resource Group Verification:** Confirmed the `rg-scm-platform-dev` exists in the `centralindia` region.
4. **Databricks Workspace Verification:**
   - Workspace Name: `dbw-scm-platform-dev`
   - Workspace URL: `https://adb-7405618030182158.18.azuredatabricks.net`
   - Provisioning State: `Succeeded`

### Status
- **`terraform apply` Status:** **DEPLOYED**. The first actual Azure deployment has completed successfully.

## Step 6C — Databricks Declarative Automation Bundle (DAB) Foundation

### Objective
Create the initial Databricks Declarative Automation Bundle (DAB) structure. This bundle provides the Infrastructure-as-Code equivalent for Databricks-specific workloads (jobs, pipelines, notebooks) separate from the underlying Azure Terraform infrastructure.

### Why We Changed It
- **Separation of Concerns:** Terraform provisions the physical Azure infrastructure (Workspace, Storage). DAB provisions Databricks-internal data engineering resources.
- **Why DAB over manual creation:** DABs allow version-controlled, CI/CD-friendly deployments of Databricks resources without manual UI clicking.
- **databricks.yml:** Serves as the root configuration defining the bundle name, structure, and environment targets.
- **Authentication Approach:** Relies securely on the Databricks CLI OAuth profile (M2M or U2M) which doesn't require hardcoded secrets in the bundle configuration.

### Files Created / Modified
- **[NEW]** `databricks/databricks.yml`
- **[NEW]** `databricks/resources/.gitkeep`
- **[NEW]** `scm_platform/.gitkeep`
- **[NEW]** `databricks/tests/.gitkeep`

### Current Bundle Structure
```
databricks/
├── databricks.yml
├── resources/
│   └── .gitkeep
├── src/
│   └── .gitkeep
└── tests/
    └── .gitkeep
```

### Commands Used
```powershell
databricks bundle validate --target dev
```

### DEV Target Configuration
The `dev` target inside `databricks.yml` points directly to our newly deployed Azure Databricks workspace URL: `https://adb-7405618030182158.18.azuredatabricks.net` and defaults to `development` mode to prevent accidental overrides in production.

### Verification
1. **Bundle Validation:**
   ```text
   Name: scm-logistics-platform
   Target: dev
   Workspace:
     Host: https://adb-7405618030182158.18.azuredatabricks.net
     User: kushwahajigar070@gmail.com
     Path: /Workspace/Users/kushwahajigar070@gmail.com/.bundle/scm-logistics-platform/dev

   Validation OK!
   ```

### Status
- **Deployment Status:** **VALIDATED BUT NOT DEPLOYED**. Deployment was intentionally skipped to prepare the foundation without prematurely deploying empty assets.

## Step 7A — Azure RBAC Permission for Databricks Access Connector

### Objective
Grant the Databricks Access Connector's managed identity the required Azure Role-Based Access Control (RBAC) permission (`Storage Blob Data Contributor`) to access the ADLS Gen2 storage account.

### Why We Changed It
- **Why Storage Permissions?** Unity Catalog requires direct read/write access to the ADLS Gen2 containers (`landing`, `checkpoints`, `rejected`) to manage Delta tables and streaming logs.
- **Why Managed Identity?** Using the Databricks Access Connector's System-Assigned Managed Identity is infinitely more secure than configuring storage account access keys or generating Service Principal client secrets. It eliminates secret rotation overhead and reduces attack surfaces.
- **Azure RBAC:** Azure RBAC is the standard authorization system for managing access to Azure resources. We are assigning a built-in data-plane role rather than creating custom roles.

### Roles and Scopes
- **Principal Resource:** The Databricks Access Connector (`dbac-scm-platform-dev`), specifically its System-Assigned Managed Identity principal ID.
- **Target Scope:** The ADLS Gen2 Storage Account (`stscmplatformdev`).
- **Exact Role Selected:** `Storage Blob Data Contributor`. This role grants full read, write, and delete permissions to blob data within the storage account containers, which is exactly what Databricks needs.

### Files Created / Modified
- **[NEW]** `infra/rbac.tf`

### Commands Used
```powershell
terraform -chdir=infra fmt
terraform -chdir=infra validate
terraform -chdir=infra plan -no-color
```

### Verification
1. **Terraform Validate Result:**
   ```text
   Success! The configuration is valid.
   ```
2. **Terraform Plan Result:**
   ```text
   # azurerm_role_assignment.dbac_storage_contributor will be created
   Plan: 1 to add, 0 to change, 0 to destroy.
   ```
3. **Terraform Apply Result:**
   ```text
   Apply complete! Resources: 1 added, 0 changed, 0 destroyed.
   ```
4. **Terraform State Tracking:** The `.terraform.tfstate` explicitly tracked `azurerm_role_assignment.dbac_storage_contributor`.

### Status
- **Deployment Status:** **DEPLOYED**. The Azure RBAC assignment is live in Azure. The Databricks Access Connector managed identity now inherently has the `Storage Blob Data Contributor` rights over the ADLS Storage Account. Note that this step does not move or process any data, it only grants the security authorization. Unity Catalog governance (mapping these identities to data) is a separate layer that will be handled in a subsequent step.

---

## Template for Future Steps

Every subsequent implementation step will append to this document using the following standardized schema:

```markdown
## Step X — [Step Name]

### Objective
[Brief explanation of what this step aims to achieve]

### Why We Changed It
[Technical or business justification for the change]

### Files Created / Modified
- [NEW] path/to/new_file
- [MODIFY] path/to/modified_file

### Commands Used
```powershell
[Commands executed]
```

### Verification
[How correctness was validated: logs, linting, tests, plans]

### Result
[Actual outcome and whether any changes were applied]
```

