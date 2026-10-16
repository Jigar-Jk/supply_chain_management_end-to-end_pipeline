# Supply Chain Control Tower — End-to-End Logistics Visibility

An enterprise-grade, greenfield logistics intelligence platform engineered on **Azure Databricks**, **Delta Lake (Medallion Architecture)**, **Unity Catalog**, and modern infrastructure automation using **Terraform**.

---

## 📌 Project Overview & Business Problem

Modern global supply chains suffer from fragmented data silos across Warehouse Management Systems (WMS), Enterprise Resource Planning (ERP), third-party carrier APIs, and IoT GPS telemetry. This fragmentation results in:
- **Blind Spots & Latency:** Operations teams react hours or days after shipping exceptions occur.
- **Stockout & Delay Risks:** Inability to predict delivery bottlenecks and inventory shortages in real time.
- **Data Quality Inconsistencies:** High failure rates in downstream reporting caused by unvalidated source data.

The **Supply Chain Control Tower** centralizes real-time and batch logistics data, executes automated data quality gates, and delivers predictive operational insights (OTIF, delay risks, stockout forecasts) via a unified lakehouse.

---

## 🏛 High-Level Architecture

The platform architecture ingests multi-source data into an event-driven Medallion Lakehouse with quarantine replay mechanisms:

```
Sources (APIs, WMS, ERP CDC, IoT/GPS)
  │
  ▼
Landing (ADLS Gen2) / Azure Event Hubs  [PLANNED]
  │
  ▼
Bronze Layer (Raw Append-Only Delta)    [PLANNED]
  │
  ▼
Quality Gate (Validation Rules)         [PLANNED]
  ├── PASS ──> Silver Layer (Cleansed)  [PLANNED]
  └── FAIL ──> Quarantine Layer         [PLANNED]
                 │
                 ▼
               Reprocess Pipeline       [PLANNED]
  │
  ▼
Gold Layer (Aggregated KPIs & Marts)    [PLANNED]
  │
  ▼
Power BI / Databricks Genie             [PLANNED]
```

---

## 🛠 Technology Stack

| Layer / Capability | Technology | Status |
|---|---|---|
| **Infrastructure as Code** | [Terraform](https://www.terraform.io/) `>= 1.5.0` with `azurerm ~> 4.0` | **IMPLEMENTED (Foundation)** |
| **Cloud Provider** | Microsoft Azure (`rg-scm-platform-dev` Resource Group) | **IMPLEMENTED (Configured)** |
| **Object Storage** | Azure Data Lake Storage (ADLS) Gen2 (`stscmplatformdev`) | **DEPLOYED** |
| **Real-Time Ingestion** | Azure Event Hubs (`evhns-scm-platform-dev` / `gps-events`) | **DEPLOYED** |
| **Lakehouse Compute** | Azure Databricks Workspace (`dbw-scm-platform-dev`) | **DEPLOYED** |
| **Storage & ACID Engine**| Delta Lake (Medallion: Bronze ➔ Silver ➔ Gold) | `PLANNED` |
| **Data Governance** | Unity Catalog (Metastore, Catalogs, Lineage) | `PLANNED` |
| **Secret Management** | Azure Key Vault (`kv-scm-platform-dev`) | **DEPLOYED** |
| **Databricks Deployment** | Databricks Asset Bundles (DABs) | **CONFIGURED (PLANNED)** |
| **Schema Management** | Liquibase | `PLANNED` |
| **Version Control & CI/CD**| Git & Automated Pipelines | **IMPLEMENTED (Git Repo)** / `PLANNED (CI/CD)` |
| **BI & Analytics** | Power BI / Databricks Genie | `PLANNED` |

---

## 🚦 Current Implementation Status

> **Important:** This is the first actual Azure deployment. The Terraform infrastructure codebase has been fully verified and deployed to Azure.

### What is Currently Set Up & Deployed:
- **Repository & Hygiene:** Git configuration with strict `.gitignore` ignoring `.terraform/`, state files, and logs.
- **Terraform Foundation, Storage & Streaming (`infra/`):**
  - [`infra/providers.tf`](file:///c:/scm-logistics-platform/infra/providers.tf): Terraform `>= 1.5.0` with AzureRM `~> 4.0` provider pinning.
  - [`infra/variables.tf`](file:///c:/scm-logistics-platform/infra/variables.tf): Parameterized region (`Central India`), environment (`dev`), and project name (`scm-platform`).
  - [`infra/main.tf`](file:///c:/scm-logistics-platform/infra/main.tf): Root Azure Resource Group declaration (`rg-scm-platform-dev`).
  - [`infra/storage.tf`](file:///c:/scm-logistics-platform/infra/storage.tf): ADLS Gen2 Storage Account (`stscmplatformdev`) with Hierarchical Namespace enabled (`is_hns_enabled = true`) and three private containers (`landing`, `checkpoints`, `rejected`).
  - [`infra/eventhub.tf`](file:///c:/scm-logistics-platform/infra/eventhub.tf): Azure Event Hubs Namespace (`evhns-scm-platform-dev`) with the `gps-events` Event Hub.
  - [`infra/keyvault.tf`](file:///c:/scm-logistics-platform/infra/keyvault.tf): Azure Key Vault (`kv-scm-platform-dev`) with RBAC authorization enabled.
  - [`infra/databricks.tf`](file:///c:/scm-logistics-platform/infra/databricks.tf): Azure Databricks Workspace (`dbw-scm-platform-dev`, premium SKU) and Access Connector (`dbac-scm-platform-dev`, SystemAssigned identity). **Workspace URL:** `https://adb-7405618030182158.18.azuredatabricks.net`
  - [`infra/rbac.tf`](file:///c:/scm-logistics-platform/infra/rbac.tf): Azure RBAC assignment (`Storage Blob Data Contributor`) granting the Access Connector managed identity permissions to the ADLS Gen2 storage account. **(DEPLOYED)**
  - [`infra/envs/`](file:///c:/scm-logistics-platform/infra/envs): Empty folder structure prepared for future `.tfvars` environment files.
- **Databricks Assets Bundles (DAB) Foundation (`databricks/`):**
  - [`databricks/databricks.yml`](file:///c:/scm-logistics-platform/databricks/databricks.yml): Root configuration pointing the `dev` target to the deployed Databricks Workspace. Uses the CLI's existing OAuth profile. Validated successfully.
- **Terraform State & Verification:** Provider initialized, code formatted, static validation succeeded. The deployment (`terraform apply`) completed successfully. The Terraform state file now explicitly tracks these 10 deployed Azure resources.

---

## 🔮 Planned Major Components

The upcoming phases of the platform implementation include:
1. **Cloud Foundation Deployment:** Executing Terraform to deploy the Resource Group, ADLS Gen2 storage accounts, Event Hubs, and Azure Key Vault to Azure.
2. **Azure Databricks & Unity Catalog:** Provisioning the Azure Databricks workspace and configuring Unity Catalog metastore bindings and access service principals.
3. **Data Ingestion Pipelines:** Implementing Auto Loader batch/REST file ingestion and Structured Streaming CDC/IoT pipelines.
5. **Quality Gates & Quarantine:** Defining Delta Live Tables expectations, anomaly routing, and reprocessing workflows.
6. **Gold Aggregations & Dashboarding:** Building OTIF, stockout prediction, and delay risk data marts.

---

## 📚 Project Documentation Index

Detailed architectural and implementation documentation is available in the [`docs/`](file:///c:/scm-logistics-platform/docs) directory:

- [**`01-project-overview.md`**](file:///c:/scm-logistics-platform/docs/01-project-overview.md): Detailed business context, project objectives, 4 ingestion patterns, and business outcomes.
- [**`02-architecture.md`**](file:///c:/scm-logistics-platform/docs/02-architecture.md): Full end-to-end lakehouse architecture, component responsibilities, and quality gate designs.
- [**`03-implementation.md`**](file:///c:/scm-logistics-platform/docs/03-implementation.md): Chronological engineering implementation log (Step 1 details, verification outputs, and template for future steps).
- [**`04-infrastructure.md`**](file:///c:/scm-logistics-platform/docs/04-infrastructure.md): Deep-dive into the Terraform codebase, provider settings, variables, resource naming, and file lifecycle (`.terraform`, `.lock.hcl`, state).
- [**`05-data-flow.md`**](file:///c:/scm-logistics-platform/docs/05-data-flow.md): Specifications of the 4 planned ingestion pathways (REST API, Batch NDJSON, CDC, and GPS/RFID Streaming).
- [**`decisions.md`**](file:///c:/scm-logistics-platform/docs/decisions.md): Architecture Decision Records (ADRs) establishing standard operating rules.
