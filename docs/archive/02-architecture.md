# 02 — Platform Architecture

## Planned Architectural Flow

The Supply Chain Control Tower follows an event-driven Lakehouse Medallion architecture with automated quality gates and quarantine handling.

```
                    ┌──────────────────────────────┐
                    │      Upstream Sources        │
                    │ (APIs, WMS, ERP CDC, IoT/GPS)│
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │     Landing / Event Hubs     │ [PLANNED]
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │         Bronze Layer         │ [PLANNED]
                    │   (Raw, Append-Only Delta)   │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │       Quality Gate DLT       │ [PLANNED]
                    │   (Schema, Nulls, Ranges)    │
                    └──────┬────────────────┬──────┘
                           │                │
                   [PASS]  │                │  [FAIL]
                           ▼                ▼
         ┌────────────────────────┐  ┌────────────────────────┐
         │      Silver Layer      │  │    Quarantine Layer    │ [PLANNED]
         │ (Cleansed, Normalized) │  │  (Dead-Letter / Anomaly)│
         └───────────┬────────────┘  └──────────┬─────────────┘
                     │                          │
                     │                          ▼
                     │               ┌────────────────────────┐
                     │               │       Reprocess        │ [PLANNED]
                     │               │  (Remediation/Replay)  │
                     │               └────────────────────────┘
                     ▼
         ┌────────────────────────┐
         │       Gold Layer       │ [PLANNED]
         │ (Control Tower KPIs,   │
         │  Aggregates, Dimensions│
         └───────────┬────────────┘
                     │
                     ▼
         ┌────────────────────────┐
         │     Power BI / Genie   │ [PLANNED]
         │ (Executive Dashboards  │
         │   & Conversational BI) │
         └────────────────────────┘
```

---

## Architectural Component Roles

| Component | Status | Role & Responsibilities in Platform |
|---|---|---|
| **Terraform** | **IMPLEMENTED (Foundation)** | Declarative Infrastructure as Code (IaC) managing foundational Azure resources (Resource Groups, and upcoming Storage Accounts, Networking, Key Vault). Enforces consistent environments (dev, qa, prod) and eliminates manual configuration drift. |
| **Azure Resource Group** | **IMPLEMENTED (Configured)** | Logical boundary grouping all cloud assets for the project (`rg-scm-platform-dev`). Configured in code; awaiting deployment. |
| **ADLS Gen2** | `PLANNED` | Azure Data Lake Storage Gen2 provides scalable, hierarchical namespace object storage underlying the raw landing zones, external tables, and Delta lakehouse tiers. |
| **Azure Event Hubs** | `PLANNED` | Real-time event streaming broker ingesting high-throughput telemetry from GPS/RFID beacons and Debezium CDC feeds with low latency. |
| **Azure Databricks** | `PLANNED` | Central unified analytics engine hosting high-performance compute clusters for streaming ingestion, Delta Live Tables (DLT) data pipelines, and machine learning models. |
| **Delta Lake** | `PLANNED` | Open storage framework delivering ACID transactions, scalable metadata handling, time travel, schema enforcement, and unified streaming/batch processing on ADLS Gen2. |
| **Unity Catalog** | `PLANNED` | Centralized data governance layer enforcing fine-grained access control (table/column-level permissions), unified auditing, and end-to-end data lineage across workspaces. |
| **Azure Key Vault** | `PLANNED` | Cloud hardware security module (HSM) securing secrets, service principal credentials, storage keys, and connection strings without hardcoding in repositories. |
| **Databricks Asset Bundles (DABs)** | `PLANNED` | Tooling to package, test, deploy, and version control Databricks projects (notebooks, DLT pipelines, jobs, and workspace configs) through CI/CD pipelines. |
| **Liquibase** | `PLANNED` | Database change management and schema migration tool tracking, versioning, and deploying relational/SQL schema modifications safely across environments. |
| **Git / CI/CD** | `PLANNED` | The definitive source of truth for all infrastructure, data pipelines, schema definitions, and documentation, orchestrating automated testing, validation, and deployments. |

---

## Quality Gate & Quarantine Design (`PLANNED`)

Inbound supply chain data contains unexpected anomalies (e.g., negative transit times, missing order references, invalid coordinates). 

Rather than failing the entire pipeline:
1. **Validation Engine:** Expectation rules run automatically during Silver ingestion.
2. **Valid Records (`PASS`):** Processed and materialized directly into clean Silver Delta tables.
3. **Invalid Records (`FAIL`):** Route to a dedicated `Quarantine` storage location with an error timestamp, validation rule code, and original payload.
4. **Reprocess Pipeline:** Data stewards inspect quarantined records, correct root-cause upstream anomalies, and trigger an automated replay pipeline back through the quality gates into Silver.
