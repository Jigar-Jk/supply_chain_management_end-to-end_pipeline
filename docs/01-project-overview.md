# 01 — Project Overview

## Project Name
**Supply Chain Control Tower — End-to-End Logistics Visibility**

---

## Business Problem
Global modern supply chains face critical operational friction due to siloed systems, fragmented data, and delayed visibility across transit networks:
- **Disparate Data Silos:** Warehouse Management Systems (WMS), Enterprise Resource Planning (ERP), carrier tracking APIs, and IoT sensors operate on incompatible protocols, schemas, and cadences.
- **Blind Spots & Latency:** Operational teams react hours or days after shipping exceptions occur, leading to missed delivery windows, stockouts, and dissatisfied customers.
- **Data Quality Inconsistencies:** Inbound data frequently contains malformed schemas, duplicate messages, missing telemetry, or out-of-order events that pollute reporting systems if not rigorously validated.

---

## Project Objectives
The objective of this project is to build an automated, enterprise-grade **Supply Chain Control Tower** on Azure Databricks that unifies streaming and batch logistics data into a single source of truth governed by Unity Catalog.

Key objectives:
1. **Centralize Ingestion:** Ingest telemetry, transaction logs, warehouse milestones, and external context (e.g., weather/traffic) into a unified lakehouse.
2. **Enforce Medallion Architecture:** Transition data through Bronze (raw/append-only), Silver (validated/cleansed with quarantine handling), and Gold (business aggregates) layers.
3. **Automate Infrastructure & Deployment:** Provision cloud infrastructure predictably via Terraform and deploy Databricks assets via Databricks Asset Bundles (DABs).
4. **Ensure Strict Data Governance:** Enforce data access, cataloging, and lineage using Unity Catalog.

---

## Four Ingestion Patterns (PLANNED)

The platform is designed to ingest multi-modal supply chain datasets through four distinct ingestion patterns:

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   1. REST API   │       │ 2. Batch Files  │       │     3. CDC      │       │  4. Streaming   │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ Carrier Tracking│       │ Warehouse WMS   │       │ SQL Server OMS  │       │ GPS Trackers &  │
│ Weather Updates │       │ Inventory Dumps │       │ Order Changes   │       │ RFID Scanners   │
│                 │       │ (NDJSON files)  │       │ (Debezium)      │       │                 │
└────────┬────────┘       └────────┬────────┘       └────────┬────────┘       └────────┬────────┘
         │                         │                         │                         │
         ▼                         ▼                         ▼                         ▼
    [ADLS Gen2]               [ADLS Gen2]              [Event Hubs]              [Event Hubs]
```

1. **REST API Ingestion (`PLANNED`):**
   - **Sources:** External 3PL carrier tracking APIs, port terminal updates, and road/weather disruption feeds.
   - **Pattern:** Periodic pull/webhook landing into ADLS Gen2, ingested incrementally into Bronze using Databricks Auto Loader.
2. **Batch File Ingestion (`PLANNED`):**
   - **Sources:** Warehouse Management Systems (WMS) batch extracts and daily inventory status snapshots formatted as NDJSON or CSV.
   - **Pattern:** File drops to ADLS Gen2 landing containers, auto-detected and schema-inferred via Auto Loader into Bronze.
3. **Change Data Capture (CDC) (`PLANNED`):**
   - **Sources:** Transactional relational databases (the SQL Server OMS order-line and inventory tables).
   - **Pattern:** Streamed row-level insert, update, and delete events captured via Debezium into Azure Event Hubs, then processed via Structured Streaming into Bronze.
4. **Real-Time Streaming (`PLANNED`):**
   - **Sources:** High-frequency IoT sensors, container GPS beacons, and temperature/humidity trackers.
   - **Pattern:** Real-time event streams emitted directly to Azure Event Hubs and consumed via Databricks Structured Streaming into Bronze with low latency.

---

## Core Business Outcomes

By integrating these four patterns into the control tower, the business achieves five core capabilities:

- **Logistics Visibility:** End-to-end real-time tracking of shipments, carrier performance, and multimodal freight legs on a unified dashboard.
- **OTIF (On-Time, In-Full):** Continuous calculation and alerting for order fulfillment accuracy and punctuality against customer SLAs.
- **Inventory Visibility:** Near real-time stock level monitoring across distribution centers, regional hubs, and in-transit containers.
- **Stockout Risk Identification:** Early warning indicators triggered when distribution center inventory falls below safety thresholds relative to forecasted demand.
- **Delivery Delay Risk Forecasting:** Proactive prediction of transit delays using live GPS telemetry, weather impacts, and carrier velocity bottlenecks before customer deliveries fail.
