# Architecture Decision Records (ADRs)

This document records the architectural and engineering decisions established for the **Supply Chain Control Tower** project, detailing the context, rationale, and current status for each decision.

---

## Decision Index

| ID | Decision Summary | Status |
|---|---|---|
| **ADR-01** | Terraform will manage Azure infrastructure | **IMPLEMENTED (Foundation)** |
| **ADR-02** | Databricks Asset Bundles (DABs) will manage/deploy Databricks resources | `PLANNED` |
| **ADR-03** | Liquibase will manage version-controlled SQL/database schema changes | `PLANNED` |
| **ADR-04** | Git will be the definitive source of truth | **IMPLEMENTED** |
| **ADR-05** | Databricks UI used primarily for inspection/verification, not persistent source of truth | **IMPLEMENTED (SOP)** |
| **ADR-06** | The platform will adopt strict DEV → QA → PROD environment promotion | `PLANNED (Structure Initialized)` |

---

## ADR-01: Terraform Will Manage Azure Infrastructure

### Decision
All underlying Microsoft Azure cloud infrastructure (Resource Groups, Storage Accounts, Key Vaults, Event Hubs, Virtual Networks, IAM roles, and Databricks Workspaces) will be defined and provisioned exclusively via Terraform.

### Reason
- **Idempotency & State Tracking:** Terraform guarantees declarative, predictable state management and drift detection across environments.
- **Modularity & Standards:** Prevents ad-hoc manual configuration in the Azure Portal, enforcing consistent naming, governance tagging, and security baselines.
- **Auditability:** Every infrastructure modification is peer-reviewed and committed via code.

### Status
**IMPLEMENTED (Foundation)** — Core Terraform provider (`azurerm ~> 4.0`), variables, and Resource Group structure are implemented under `infra/`.

---

## ADR-02: Databricks Asset Bundles (DABs) Will Manage Databricks Resources

### Decision
Databricks-native assets (Delta Live Tables pipelines, Databricks workflows/jobs, notebook references, and cluster compute policies) will be defined, tested, and deployed using **Databricks Asset Bundles (DABs)** rather than raw Terraform resources or manual UI creation.

### Reason
- **Developer Experience & Tooling Alignment:** DABs provide first-class native support for Databricks lifecycle workflows, parameter substitutions, and testing.
- **Separation of Concerns:** Azure cloud resources belong to Terraform; Databricks analytical runtime assets belong to DABs.
- **CI/CD Integration:** Simplifies packaging and deploying assets seamlessly across environments via CLI (`databricks bundle deploy`).

### Status
`PLANNED` — Will be set up after Azure Databricks workspace and storage foundational layers are deployed.

---

## ADR-03: Liquibase Will Manage Version-Controlled SQL Schema Changes

### Decision
All relational DDL, database catalog alterations, schema migrations, and view definitions across the Lakehouse and Unity Catalog will be managed through **Liquibase** changelogs.

### Reason
- **Repeatable & Versioned Migrations:** Tracks which database changes have been applied to which environment using audit tables.
- **Rollback Capabilities:** Provides structured rollback mechanisms for schema evolution errors.
- **Prevents Drift:** Ensures schema changes are tested in DEV and promoted systematically through QA to PROD without manual SQL execution.

### Status
`PLANNED` — To be introduced when Unity Catalog schemas, tables, and views are declared.

---

## ADR-04: Git Will Be the Definitive Source of Truth

### Decision
No infrastructure, data pipeline, job configuration, or database schema will be altered directly in cloud environments or runtime portals. All changes must originate from version-controlled Git branches and be deployed through automated pipelines.

### Reason
- **Auditability & Compliance:** Provides a complete historical trail of what changed, when, why, and by whom.
- **Disaster Recovery:** Entire platform environments can be stood up from scratch from Git repositories.
- **Quality Control:** Enforces automated linting, security scanning, and peer reviews before production deployment.

### Status
**IMPLEMENTED** — Repository structure initialized with strict `.gitignore` and version control practices.

---

## ADR-05: Databricks UI Used Exclusively for Inspection & Verification

### Decision
The Databricks Web UI is designated solely as an observational and exploratory interface (for reviewing job execution logs, inspecting data profiles, tracking cluster metrics, and interactive debugging). It will **not** be used as the persistent source of truth for creating production jobs, pipelines, or table schemas.

### Reason
- **Eliminating Configuration Drift:** Resources built or modified directly in the UI are untracked and easily overwritten or lost.
- **Reproducibility:** Ensuring all deployable assets exist in repository code prevents "works on my workspace" syndrome.

### Status
**IMPLEMENTED (Standard Operating Procedure)** — Established as a non-negotiable project design rule.

---

## ADR-06: DEV → QA → PROD Environment Promotion

### Decision
The platform will utilize three strictly isolated environment tiers:
1. **DEV:** Active feature development, experimentation, and unit testing.
2. **QA:** Automated integration testing, end-to-end data pipeline validation, and user acceptance testing (UAT).
3. **PROD:** Mission-critical production operations, continuous ingestion, and executive reporting.

### Reason
- **Risk Mitigation:** Isolates experimental changes from live operations, ensuring analytics integrity and high availability.
- **Data Privacy & Compliance:** Restricts production data access while enabling synthetic or masked data testing in lower tiers.

### Status
`PLANNED (Structure Initialized)` — The `infra/envs/` directory is created to house future tier-specific variables (`dev.tfvars`, `qa.tfvars`, `prod.tfvars`).
