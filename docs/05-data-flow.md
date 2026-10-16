# 05 — Ingestion Data Flows (PLANNED)

> **Status Notice:** All data ingestion patterns and pipelines described in this document are **`PLANNED`**. No ingestion services, storage containers, Event Hubs, or Databricks pipelines have been provisioned or executed yet.

---

## Overview

The Supply Chain Control Tower design defines four distinct ingestion paths to ingest heterogeneous logistics data into the Bronze Delta layer:

```
Pattern 1: REST API (PLANNED)
[Carrier/Weather API] ──> [ADLS Gen2 Landing] ──> [Databricks Auto Loader] ──> [Bronze Delta Table]

Pattern 2: Batch Files (PLANNED)
[WMS NDJSON Drops]    ──> [ADLS Gen2 Landing] ──> [Databricks Auto Loader] ──> [Bronze Delta Table]

Pattern 3: Change Data Capture (PLANNED)
[Oracle ERP Database] ──> [Debezium CDC] ──> [Azure Event Hub] ──> [Structured Streaming] ──> [Bronze Delta Table]

Pattern 4: Real-Time Streaming (PLANNED)
[GPS/RFID Telemetry]  ──> [Azure Event Hub] ──> [Structured Streaming] ──> [Bronze Delta Table]
```

---

## 1. REST API Ingestion Flow (`PLANNED`)

### Purpose:
Ingests status updates from external logistics partners (3PL carriers) and external context feeds (e.g., weather alerts, highway route disruptions, port congestion).

### Planned Flow:
1. **Source:** Third-party carrier REST endpoints (JSON payloads) and open weather APIs.
2. **Landing Zone (ADLS Gen2):** An orchestrated scheduler triggers API extract functions/tasks that deposit raw JSON response payloads into a dedicated ADLS Gen2 landing container (`landing/carrier_api/`).
3. **Ingestion Engine (Databricks Auto Loader):** Uses cloud notification or directory listing to detect newly arrived files, managing schema evolution automatically.
4. **Target (Bronze):** Appends raw records with metadata attributes (`_ingest_timestamp`, `_source_file`) to the Bronze Delta table.

---

## 2. Batch File Ingestion Flow (`PLANNED`)

### Purpose:
Processes scheduled batch exports from enterprise Warehouse Management Systems (WMS), such as nightly physical inventory counts and outbound pick logs.

### Planned Flow:
1. **Source:** On-premise or cloud WMS extracting newline-delimited JSON (NDJSON) or compressed CSV files.
2. **Landing Zone (ADLS Gen2):** Files transferred via SFTP or automated storage sync into `landing/wms_batch/`.
3. **Ingestion Engine (Databricks Auto Loader):** Processes files incrementally using Auto Loader with schema inference and rescue columns for malformed lines.
4. **Target (Bronze):** Stores raw transactions in the Bronze WMS Delta table, preserving full historical fidelity.

---

## 3. Change Data Capture (CDC) Flow (`PLANNED`)

### Purpose:
Captures row-level operational changes (inserts, updates, deletes) in core relational transaction systems (e.g., Oracle ERP purchase orders, line items, and fulfillment tables) with minimal latency.

### Planned Flow:
1. **Source:** Oracle ERP transactional database transaction log.
2. **CDC Engine (Debezium):** Debezium reads database redo logs, transforming row-level mutations into structured change events.
3. **Messaging Broker (Azure Event Hubs):** Events stream directly into an Event Hub topic partition.
4. **Streaming Engine (Databricks Structured Streaming):** A continuous Spark Structured Streaming job reads the Event Hub stream with checkpointing.
5. **Target (Bronze):** Raw CDC envelopes (before/after state, operation type `c`/`u`/`d`, timestamp) append directly into the Bronze ERP CDC Delta table.

---

## 4. Real-Time Telemetry Streaming Flow (`PLANNED`)

### Purpose:
Captures high-frequency live geolocation and environmental telemetry from moving fleet assets, shipping containers, and yard RFID readers.

### Planned Flow:
1. **Source:** In-vehicle GPS trackers, cellular IoT asset tags, and warehouse RFID dock sensors emitting periodic heartbeat pings (lat, long, speed, temp, timestamp).
2. **Messaging Broker (Azure Event Hubs):** Ingests incoming sensor telemetry at high velocity via Kafka/AMQP endpoints.
3. **Streaming Engine (Databricks Structured Streaming):** Low-latency streaming readers consume incoming batches using micro-batching or continuous processing.
4. **Target (Bronze):** Pings append directly into the raw Bronze Telemetry Delta table for subsequent spatial analysis, geofencing, and ETA estimation.

---

## 5. Storage Container Mapping (Configured in `infra/storage.tf`)

| Storage Container | Role in Ingestion Data Flows |
|---|---|
| **`landing`** | Staging container for external file drops (Pattern 1 REST API JSON extracts and Pattern 2 WMS NDJSON files) prior to Auto Loader ingestion. |
| **`checkpoints`** | Dedicated state repository storing offset logs and schema metadata for Structured Streaming queries (Pattern 3 CDC, Pattern 4 Telemetry) and Auto Loader. |
| **`rejected`** | Quarantine storage receiving corrupt files, malformed JSON lines, and unparseable source payloads diverted before reaching Bronze Delta tables. |

---

## 6. Event Hub Mapping (Configured in `infra/eventhub.tf`)

| Event Hub Entity | Target Ingestion Flows | Architectural Configuration & Role |
|---|---|---|
| **`evhns-scm-platform-dev`** (Namespace) | Pattern 3 (CDC) & Pattern 4 (Streaming) | Standard SKU root streaming broker managing security, public endpoints, and throughput capacity (1 TU). |
| **`gps-events`** (Event Hub) | • **Pattern 4:** Real-time GPS beacons, speed, temperature, and RFID yard reads.<br>• **Pattern 3:** Inbound Oracle ERP row-mutation change events from Debezium. | • **8 Partitions:** Matches parallel Spark worker readers, prevents hot-spotting via key-based routing (`vehicle_id`, `shipment_id`).<br>• **7-Day Retention:** Full 168-hour buffer allowing cluster maintenance, crash recovery, and stream backfills. |

> **Status Notice:** Both the namespace and `gps-events` hub are configured in `eventhub.tf` as **`CONFIGURED (PLANNED)`**. Physical creation in Azure occurs only when `terraform apply` is executed in a subsequent deployment stage.

---

## 7. Compute & Access Mapping (Configured in `infra/databricks.tf`)

| Resource | Target Ingestion Flows | Architectural Configuration & Role |
|---|---|---|
| **`dbw-scm-platform-dev`** (Databricks Workspace) | All Patterns (1, 2, 3, 4) | Premium SKU compute environment hosting the Auto Loader jobs and Structured Streaming pipelines responsible for all Bronze data ingestion and subsequent Silver/Gold transformations. |
| **`dbac-scm-platform-dev`** (Access Connector) | All Patterns | System-Assigned Managed Identity that allows Databricks (via Unity Catalog) to securely authenticate with ADLS Gen2 `landing`, `checkpoints`, and `rejected` containers without hardcoded secrets. |

> **Status Notice:** Databricks resources are configured in `databricks.tf` as **`CONFIGURED (PLANNED)`**. Physical creation in Azure occurs only when `terraform apply` is executed in a subsequent deployment stage.

