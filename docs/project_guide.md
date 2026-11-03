# Logistics Visibility Control Tower: architecture and workflow guide

PS-02 · Azure Databricks greenfield lakehouse · Jigar Kushwaha · 29 October 2026

This guide walks through the whole project part by part: sources, layers, data quality, schema
management, KPIs, governance, infrastructure, CI/CD and the day-to-day workflows. Section 12 lists
the questions reviewers usually ask, with answers.

**Contents**

1. [Project at a glance](#1-project-at-a-glance)
2. [Data sources and ingestion](#2-data-sources-and-ingestion)
3. [Medallion layers and data model](#3-medallion-layers-and-data-model)
4. [Data quality](#4-data-quality)
5. [Schema management with Liquibase](#5-schema-management-with-liquibase)
6. [KPIs, dashboard and Genie](#6-kpis-dashboard-and-genie)
7. [Governance and security](#7-governance-and-security)
8. [Infrastructure as code](#8-infrastructure-as-code)
9. [Environments, CI/CD and jobs](#9-environments-cicd-and-jobs)
10. [End-to-end workflows](#10-end-to-end-workflows)
11. [Status and next steps](#11-status-and-next-steps)
12. [Likely questions](#12-likely-questions)

---

## 1. Project at a glance

### 1.1 The problem

A logistics company runs many warehouses, ships through many carriers and takes orders from thousands
of customers. Its data sits in separate systems: order management, warehouse management, carrier
tracking and truck GPS. Nobody sees one trusted picture of how deliveries are doing.

The control tower answers four daily questions in one place:

- **Are we delivering well?** OTIF % (on time and in full) and fill rate %.
- **Is stock healthy?** Stockout rate % and inventory turnover.
- **Where is the problem?** Worst warehouses and carriers against their targets.
- **What could go wrong next?** A delay-risk score for every shipment still on the road.

### 1.2 The solution in one paragraph

This is a medallion lakehouse on Azure Databricks.

1. Four kinds of sources land in **Bronze** exactly as received: REST APIs, batch files, database
   change data capture (CDC) and a GPS event stream.
2. **Silver** cleans and validates them into a galaxy schema of 3 facts and 6 dimensions. Every bad
   row goes to a quarantine table together with the reason it failed.
3. **Gold** turns Silver into KPI views and a daily mart, shown on an AI/BI dashboard.

Everything is code: infrastructure, tables, jobs, dashboard and permissions. CI/CD deploys it to
three environments: dev, qa and prod.

### 1.3 Architecture

```
SOURCES                         TRANSPORT                        LAKEHOUSE (Unity Catalog, Delta)
REST APIs (routes, weather,   ┐
  carrier shipments)          ├─ JSONL files → ADLS landing ─→ Auto Loader ─┐
Batch files (WMS stock,       │                                              │
  master data, orders)        ┘                                              ▼
SQL Server OMS ─ Debezium CDC ─→ Kafka / Event Hubs ─→ Structured Streaming ─→ BRONZE (raw)
Truck GPS / RFID ──────────────→ Kafka / Event Hubs ─→ Structured Streaming ─→   │
                                                                                  ▼
                                                    quality gate ─→ QUARANTINE ← reprocess
                                                                                  │
                                                                      SILVER (3 facts, 6 dims)
                                                                                  │
                                                                      GOLD (KPI views + mart)
                                                                                  │
                                                       AI/BI dashboard · Genie · alerts
```

### 1.4 Technology stack

| Area | Technology |
|---|---|
| Cloud | Microsoft Azure (Azure for Students subscription) |
| Compute and storage | Azure Databricks, ADLS Gen2, Delta Lake, Unity Catalog |
| Ingestion | Auto Loader (files), Spark Structured Streaming (Kafka / Event Hubs), Debezium (SQL Server CDC) |
| Transformations | PySpark package `scm_platform`, shipped as a Python wheel |
| Schema management | Liquibase-format changesets, applied by `scripts/apply_migrations.py` |
| Orchestration | Databricks Jobs, defined in a Databricks Asset Bundle (`databricks.yml`) |
| Infrastructure as code | Terraform (azurerm and databricks providers) |
| BI | Databricks AI/BI dashboard (as code), Genie (planned), Databricks SQL alerts |
| CI/CD | GitHub Actions: CI on every push and pull request, CD by manual dispatch |
| Local development | Docker (Kafka, Debezium, Spark), SQL Server on Windows |

### 1.5 Environments

One Databricks workspace holds three Unity Catalog catalogs, one per environment. Section 9.1
explains why this project uses catalogs instead of three workspaces.

| Catalog | Purpose | Who writes |
|---|---|---|
| `scm_platform_dev` | Development and the CD integration test | Developer, CD |
| `scm_platform_qa` | Pre-production check of each release | CD |
| `scm_platform_prod` | Production; a deploy needs reviewer approval | Only the `scm-pipeline` service principal |

Every catalog has the same five schemas:

| Schema | Contents |
|---|---|
| `bronze` | Raw data exactly as received, plus audit columns |
| `silver` | Clean, validated galaxy schema (facts and dimensions) |
| `gold` | KPI views, the daily mart, PII-masked directories |
| `quarantine` | `quarantine.records`: every rejected row with its reason |
| `ops` | Run log, SLA config, migration register, streaming checkpoints volume |

---

## 2. Data sources and ingestion

> **About the data.** The data is synthetic, because no real company systems were available.
>
> - The shapes, formats and failure cases follow real systems.
> - Two APIs are real: OpenRouteService and Open-Meteo.
> - SQL Server is a real database with real CDC.
>
> About 2% of rows are corrupted on purpose. Every corruption is listed in a manifest file in
> `data/_manifests/`, so the tests can check that the quality gate catches exactly those rows.

### 2.1 Four patterns, twelve feeds

| Pattern | Real-world system | In this project | Bronze table(s) |
|---|---|---|---|
| REST API | Routing service | OpenRouteService, real API, cached (`seed_api.py`, `normalize_api_data.py`). One real route; the other lanes are synthetic | `bronze.routes` |
| REST API | Weather service | Open-Meteo, real API, hourly forecast, cached | `bronze.weather` |
| REST API | Carrier tracking API | Mock carrier API (`services/carrier_api`) serving `generate_shipments.py` data, pulled by `scm-ingest-api` (see 2.6); uploaded as a file while the API step is off | `bronze.shipments` |
| Batch file | WMS nightly stock file | `generate_inventory.py`, one CSV file per night (header, comma, quoted where needed), as a WMS exports it | `bronze.inventory` |
| Batch file | ERP / WMS / OMS master data | `generate_master_data.py` | `bronze.warehouses`, `products`, `customers`, `carriers` |
| Batch file | OMS order file feed | `generate_orders.py` | `bronze.orders` |
| CDC | Order management database | SQL Server `scm_oms` + `simulate_oms_changes.py`, Debezium → Kafka | `bronze.orders_cdc`, `inventory_cdc`, `cdc_dlq` |
| Streaming | Truck GPS / RFID | `produce_gps.py` (~1,000-2,000 events/s) → Kafka / Event Hubs | `bronze.gps_events`, `gps_events_dlq` |

### 2.2 Where the data is stored

All data lives in one ADLS Gen2 storage account. Terraform creates its four containers.

| Container | Holds | Access |
|---|---|---|
| `landing` | Raw files, one folder per feed (`landing/orders/`, ...) | File feeds are uploaded by `up.ps1`; the pipeline reads them and writes only the carrier API pages (READ_FILES, WRITE_FILES) |
| `checkpoints` | Auto Loader progress, one folder per catalog and feed | Pipeline can read and write |
| `managed` | All Delta tables (Bronze, Silver, Gold), one folder per catalog | Managed by Unity Catalog |
| `rejected` | Reserved for rejected files | Not used by any job yet |

Streaming checkpoints (GPS and CDC) live in a Unity Catalog volume:
`/Volumes/<catalog>/ops/checkpoints`.

**How the WMS file reaches landing.** A warehouse system does not get access to the data lake. It
drops its nightly CSV on an SFTP server, and Azure Data Factory copies it in (`infra/wms_sftp.tf`,
`infra/adf.tf`):

```
WMS ──SFTP (user wms: create/write/list only)──→ SFTP drop  wms-drop/inbox/inventory_<night>.csv
                                                      │  01:30 UTC  ADF copy_wms_inventory
                                                      │   1. inbox/*.csv → landing/inventory/  (byte for byte)
                                                      │   2. same files → archive/, deleted from inbox/
                                                      ▼
                       landing/inventory/ → 02:30 SLA check → 03:00 Auto Loader → bronze.inventory
```

- **Who plays the WMS.** `scripts/up.ps1` uploads the generated CSV with `scripts/wms_sftp_upload.py`
  (paramiko, as the `wms` user, password from Key Vault into an environment variable, never an
  argument), then starts `copy_wms_inventory` and waits for it, so a session does not wait for 01:30.
- **Why ADF here.** A plain file transfer on a schedule, with a built-in SFTP connector and retries;
  if the WMS were on-premises, a self-hosted integration runtime would reach it. The carrier API
  has custom logic (cursor, `Retry-After`, watermark), so it is Python instead (2.6).
- **Secrets.** Azure generates each SFTP user's password; both go to Key Vault. ADF reads its
  password from Key Vault and writes the lake with its managed identity, on the landing container only.
- **Both steps take the same files:** `inbox/*.csv` modified before the run started, so a file that
  arrives mid-run is neither archived without landing nor lost.

### 2.3 File ingestion with Auto Loader

- **Only new files.** A checkpoint remembers which files were read, so a re-run never loads a file twice.
- **Explicit schema.** The schema is given in `DATASET_SCHEMAS` in `ingest_bronze.py`; it is not inferred.
- **Format per feed.** Every feed is NDJSON except the WMS stock file, which is CSV (`FEED_FORMATS`).
  CSV is read with a header row, comma, double quotes around values holding a comma or a quote, an
  empty cell as NULL and UTF-8 (`CSV_OPTIONS`); only `*.csv` files in the folder are picked up.
- **Rescue mode.** A field that does not fit the schema (a new column or a wrong type) goes to
  `_rescued_data` instead of failing the row. Silver's schema-drift rule then catches it.
- **One checkpoint per environment.** The checkpoint path contains the catalog name. Dev, qa and prod
  read the same landing files but keep separate progress.

**Bronze audit columns**

| Column | Meaning | Why |
|---|---|---|
| `_ingest_timestamp` | When the row reached Bronze | Freshness, SLA, daily DQ volume |
| `_source_file` | File path (`_metadata.file_path`) | Find the bad file behind a bad row |
| `_batch_id` | Job run that loaded the row (`{{job.run_id}}`) | Separate or roll back one run |
| `_rescued_data` | Fields that did not fit the contract | Detect schema drift without losing data |

**Bronze types**

- Dates and timestamps are stored as **strings**.
- Numbers are numeric (LONG, DOUBLE).
- Every column is nullable.

Because of this, a broken value such as `"2026-13-45"` stays in Bronze as received instead of
failing the load. Silver then quarantines it with a reason. A non-numeric value in a numeric column
goes to `_rescued_data`.

### 2.4 CDC: SQL Server order management

```
SQL Server scm_oms (Windows service, CDC + Agent on)
  → SQL Server CDC change tables (transaction log)
  → Debezium / Kafka Connect (Docker, reaches SQL Server via host.docker.internal:1433)
  → Kafka topic scm.scm_oms.dbo.order_lines / .inventory
  → stream_cdc_events.py (Structured Streaming) → bronze.orders_cdc / inventory_cdc / cdc_dlq
```

| CDC column | Why it is there |
|---|---|
| `record_key` | Primary key of the source row; groups all changes of one row |
| `op` | `r` snapshot, `c` insert, `u` update, `d` delete (a delete becomes a soft delete) |
| `before`, `after` | Row images as raw JSON text, so a new source column never breaks Bronze |
| `commit_lsn`, `change_lsn`, `event_serial_no` | Exact position in the SQL Server log |
| `source_position` | The three above combined into one sortable value: **the highest position wins** in Silver |
| `source_ts`, `captured_ts` | When the change happened and when Debezium captured it (lag) |
| `kafka_*` | Topic, partition, offset and timestamp: the replay position |

- **Duplicates and late events.** Duplicates have the same `source_position`, so Silver keeps one.
  A late, older event can never overwrite newer data.
- **Bad messages.** Messages that are not JSON go to `bronze.cdc_dlq` unchanged, with a reason. The
  stream keeps running.
- **Tested live.** Debezium was stopped and restarted, a stale event was replayed, and then 60,000
  rows were reconciled against SQL Server. They were identical.

### 2.5 Streaming: truck GPS / RFID

```
produce_gps.py (fleet on real routes, ~2.5% corrupted, 8% "dark" trucks reporting 3 h late)
  → topic gps-events (local Kafka: 4 partitions; Azure Event Hubs: 8 partitions, 7-day retention)
  → stream_gps_events.py
       not JSON → bronze.gps_events_dlq (verbatim)
       JSON     → dedup on event_id within a 10-minute watermark → bronze.gps_events
  → Silver: quality rules (lat/lon range, truck id, speed), latest clean ping per shipment
       → fact_shipment.last_lat / last_lon / last_ping_ts → Gold delay-risk GPS term
```

- **No cluster runs 24/7.** Job `scm_streaming` runs every 30 minutes with `availableNow`: it starts,
  drains what is waiting, and stops. Switching to continuous mode is one trigger setting.
- **Crash recovery.** Each sink has its own checkpoint with the committed Kafka offsets, so after a
  crash the job resumes exactly where it stopped.
- **Back-pressure.** `maxOffsetsPerTrigger` = 500,000 limits the batch size after an outage.
- **One code path.** The same code talks to local Kafka and to Azure Event Hubs, which speaks the
  Kafka protocol. Only the bootstrap address and the secret change.

### 2.6 REST API ingestion: the carrier tracking API

A real carrier API needs a commercial account, so `services/carrier_api` is a mock that behaves
like one: `X-API-Key`, `updated_since` (incremental), cursor pagination of up to 1,000, and random
429 (with `Retry-After`) and 500. The pipeline talks to it as it would to a real carrier; switching
later changes only the URL and the key.

```
Mock carrier API ←── GET /v1/shipments?updated_since=<watermark>&cursor=…  (X-API-Key)
                 ──→ pages ──→ scm-ingest-api ──→ landing/shipments/api_<run>_p0001.jsonl …
                                    │                      ↓
                                    │              Auto Loader → bronze.shipments (unchanged)
                                    └─ watermark = newest updated_at landed (ops.api_watermark)
```

| Piece | What it does |
|---|---|
| `scm_platform/api/client.py` | Shared client for every REST source: 429 waits for `Retry-After`, 5xx and timeouts back off exponentially (1, 2, 4, 8, 16 s, ±50 % jitter), other 4xx fail at once, pagination by cursor, optional requests per minute; the key never reaches logs |
| `scm_platform/api/watermark.py` | How far a source has read: the newest `updated_at` landed, never the run time, and never moving back. A JSON file locally, `ops.api_watermark` (Liquibase 017) on Databricks |
| `scm_platform/api/carrier.py` | One run: read the watermark, page through the API, write each page as one NDJSON file, save the new watermark only after the last page landed |
| `scm_platform/ingest_api.py` | Job entry point `scm-ingest-api`; the key comes from an environment variable, never an argument |

- **Failure.** A page that keeps failing stops the run before the watermark moves; the next run
  asks again from the old watermark. Pages that did land arrive twice, and Silver keeps the latest
  version per shipment (at-least-once delivery, idempotent Silver).
- **Switch.** In `scm_daily_pipeline`, `carrier_api_enabled` (a condition task) runs
  `fetch_carrier_api` only when `carrier_api_url` is set; `ingest_bronze_files` runs with
  `NONE_FAILED`, so it goes ahead when the fetch succeeded or was skipped, but not when it failed.
  The URL is `disabled` until the mock API runs in Azure, and shipments come from the uploaded file
  meanwhile.

### 2.7 Where each part runs today

| Component | Local | Azure |
|---|---|---|
| File feeds (orders, shipments, inventory, master data, routes, weather) | Yes | Yes. All dashboard data comes from these feeds |
| CDC (SQL Server → Debezium → Kafka → Bronze → Silver) | Yes, fully tested | Job and tables deployed. Debezium has never been pointed at Event Hubs, so `orders_cdc` is empty |
| GPS stream | Yes | Event hub and job deployed. The producer has never been run against Azure, so `last_ping_ts` is NULL |
| Carrier API (mock API → `scm-ingest-api` → landing) | Yes, tested end to end | Task, key secret and landing write permission deployed; switched off (`carrier_api_url = disabled`) until the mock API runs in Azure |

### 2.8 How Silver chooses the order source

The file feed and CDC carry the same orders: one OMS, two delivery paths. The file feed is the old
nightly extract; CDC is the new live change stream. Silver uses exactly one of them:

- If `bronze.orders_cdc` has at least one row, Silver uses CDC and picks the latest version of each
  row by `source_position`.
- Otherwise it uses the file feed `bronze.orders` and picks the latest version by `updated_at`.

The two feeds are not merged, for two reasons:

- Their version clocks (log position and timestamp) cannot be compared with each other.
- The file feed cannot express deletes, so a deleted order would come back.

**Switch-over.** Rows loaded from the file feed have `source_position` = NULL, so the first CDC load
overwrites them. The MERGE condition is
`target.source_position IS NULL OR source.source_position >= target.source_position`.

> **Known gaps**
>
> - There is no freshness alert when CDC goes quiet.
> - Nothing checks that the initial CDC snapshot is complete before Silver switches.
> - A row that exists only in the file feed stays in Silver after the switch.

---

## 3. Medallion layers and data model

| Layer | Rule | Example |
|---|---|---|
| Bronze | Keep everything exactly as received | `order_date = "2026-13-45"` is stored |
| Silver | Clean, validated, typed and joined; bad rows quarantined | That row goes to quarantine; good rows go to `fact_order` |
| Gold | Business answers; reads Silver only | OTIF of a warehouse yesterday = 82% |

### 3.1 Galaxy schema

Three business processes (orders, shipments and inventory) each have their own fact table, and the
facts share conformed dimensions. Several star schemas that share dimensions form a galaxy.

```
                 dim_customer (SCD2)
                        │
 dim_product (SCD2) ── fact_order ────────────── dim_date
        │                  │                        │
        │            dim_warehouse (SCD2)           │
        │              │            │               │
 fact_inventory_snapshot            fact_shipment ── dim_carrier (SCD1)
                                         │
                                    dim_route (SCD1)
```

| Fact | Grain (one row is...) | Source | Dimensions |
|---|---|---|---|
| `fact_order` | one order line (`order_id` + `order_line_number`) | orders (file or CDC) | customer, product, warehouse, date |
| `fact_shipment` | one shipment leg, plus the latest GPS ping | shipments, GPS | carrier, route, warehouse, date |
| `fact_inventory_snapshot` | one warehouse × product × night | WMS nightly file | warehouse, product, date |

| Dimension | Source | Type | Tracked attributes |
|---|---|---|---|
| `dim_customer` | customers | SCD2 | name, segment, country, city |
| `dim_product` | products | SCD2 | category, price, cost, ... |
| `dim_warehouse` | warehouses | SCD2 | name, city, capacity, ... |
| `dim_carrier` | carriers | SCD1 | contracted OTIF target, service level |
| `dim_route` | routes | SCD1 | distance, duration |
| `dim_date` | generated in code | static | day, month, quarter, weekend |

### 3.2 SCD1 and SCD2

- **SCD1 overwrites.** Only today's value matters, for example a carrier's current contract target.
- **SCD2 keeps history.** Each change adds a new row, so old facts keep pointing at the version that
  was true at the time.

Example: customer CUST-42 moves from Pune to Mumbai on 15 October.

| customer_sk | customer_id | city | effective_from | effective_to | is_current |
|---|---|---|---|---|---|
| a1b2... | CUST-42 | Pune | 2026-01-01 | 2026-10-15 | false |
| c3d4... | CUST-42 | Mumbai | 2026-10-15 | NULL | true |

How the SCD2 logic works:

- **Surrogate key.** `md5(natural key + effective_from)`. It is deterministic, so re-runs and all
  three environments produce the same keys.
- **Point-in-time lookup.** A fact joins the version that was valid on its own date
  (`effective_from <= date < effective_to`). An order of 10 October points at the Pune version.
- **First version open to the past.** The first version of each key also covers earlier dates, so
  facts older than the first load still find their dimension row.
- **Stale protection.** An incoming version that is not newer than the current one is ignored.
- **No empty versions.** A new version is created only when a tracked attribute changes, so an
  identical daily file creates nothing.

### 3.3 What Silver does for every fact

1. Build the grain key. For orders this is `order_id` + `order_line_number`.
2. Parse dates into helper columns with `try_to_timestamp`. A bad date becomes NULL instead of
   crashing the job. The original string is kept for the quarantine payload.
3. Keep the latest version of each key: by `updated_at` for the file feed and by `source_position`
   for CDC. Older versions are history; exact ties are duplicates.
4. Look up the dimension surrogate keys, point-in-time.
5. Apply the quality rules. Failing rows go to `quarantine.records` with the reason.
6. Cast the passing rows to their final types (`order_date_sk` INT, quantities, `total_price`,
   `is_deleted`).
7. MERGE into the Silver table on the surrogate key. Re-running never duplicates rows, so the write
   is idempotent.

### 3.4 Bronze to Silver mapping

| Bronze | Silver |
|---|---|
| warehouses / products / customers | `dim_warehouse` / `dim_product` / `dim_customer` (SCD2) |
| carriers / routes | `dim_carrier` / `dim_route` (SCD1) |
| orders (file) or orders_cdc | `fact_order` |
| shipments + gps_events | `fact_shipment` (with the last-ping columns) |
| inventory | `fact_inventory_snapshot` |
| inventory_cdc | `inventory_current` (live stock) |
| weather | `silver.weather` (helper table) |
| gps_events_dlq, cdc_dlq | Not read; storage only |

### 3.5 Gold

Gold reads Silver only. It holds:

- **Views.** They are always in sync with Silver and hold no copy of the data.
- **One materialized view**, `mart_logistics_kpi_daily`, with all KPIs per warehouse per day.

The mart's refresh is transactional, so readers never see a half-refreshed mart. On a day without a
given event, that KPI is NULL, not 0. Section 6 describes the KPIs.

---

## 4. Data quality

### 4.1 Where bad data goes

| Problem | Caught by | Goes to |
|---|---|---|
| A stream message is not JSON | Streaming job | `gps_events_dlq` / `cdc_dlq` |
| The file format changed (schema drift) | Auto Loader rescue + `RULE_SCHEMA_DRIFT` | Quarantine. The whole file is logged as REJECTED_FILE when 50% or more of its rows drift |
| The same key arrives twice | `RULE_DUPLICATE_KEY` | Quarantine |
| A wrong value (date, quantity, price, missing reference) | The feed's rule list | Quarantine, with rule code and reason |
| The row passes everything | | Silver fact or dimension |

### 4.2 Quality rules

Each rule is data with three parts:

- a code;
- a SQL condition that is TRUE when the row breaks the rule;
- a readable reason.

A row that breaks several rules is recorded under the first one in the list (list order =
priority). That way the dashboard shows one root cause per row.

```python
QualityRule("RULE_ORDER_QUANTITY_POSITIVE",
            "ordered_quantity <= 0",
            "concat('ordered_quantity must be > 0, got ', ordered_quantity)")
```

| Feed | Examples of rules |
|---|---|
| Orders | missing id, invalid or future date, quantity ≤ 0, negative price, promised before ordered, shipped > ordered, invalid status, unknown customer / product / warehouse |
| Shipments | missing id, unparseable time, delivery before dispatch, invalid status, delivered without a time, unknown carrier / route / warehouse |
| Inventory | missing ids, invalid or future snapshot date, on hand < 0, reserved outside 0..on hand, unknown warehouse / product |
| Master data | non-positive weight / volume, negative cost / price, OTIF target outside 0-100, bad coordinates |
| GPS | missing event / truck / shipment id, invalid time, coordinates out of range, impossible speed |
| All file feeds | `RULE_SCHEMA_DRIFT` first, then `RULE_DUPLICATE_KEY` |

### 4.3 The quarantine record

| Column | Content |
|---|---|
| `quarantine_id` | md5 of source, record id, rule and payload, so a re-run never quarantines a row twice |
| `original_payload` | The full source row as JSON (never edited) |
| `failure_reason`, `failed_rule` | Readable reason and rule code |
| `batch_id`, `source_system`, `ingested_at` | Which run, which system, when |
| `status` | PENDING → REPROCESSED or REJECTED |
| `source_feed`, `retry_count`, `corrected_payload` | Reprocessing lifecycle |

### 4.4 Reprocessing

Job `scm_reprocess` runs every 2 hours. It replays every PENDING row through the same transform and
rules as the daily pipeline, against today's dimensions. Dimension feeds go first, so a corrected
warehouse lets its facts heal in the same run.

| Outcome | When |
|---|---|
| REPROCESSED | The row passes now: a late dimension arrived, or a data steward wrote a fix into `corrected_payload` |
| PENDING, retry + 1 | The row still fails |
| REJECTED | The row still fails after 7 days, or it is a duplicate whose kept copy is already in Silver. It is kept for audit and never reaches Gold |

GPS pings and weather are not replayed: a ping fixed hours later says nothing about where the truck
is now.

### 4.5 WMS file SLA and last-known-good

- **Deadline.** The WMS stock file for a night is due by 02:00 UTC.
- **Check.** Job `scm_wms_sla_check` runs at 02:30, before the daily pipeline at 03:00. For each
  warehouse it reports ON_TIME, LATE, WAITING or SLA_MISS.
- **Carry forward.** On SLA_MISS it copies that warehouse's last good snapshot forward with
  `is_stale = TRUE`. Stockout and turnover still compute, and Gold shows
  `built_from_stale_data = TRUE`.
- **Late file.** When the late file arrives, the normal MERGE replaces the carried-forward rows.

### 4.6 Monitoring

- **Quarantine alert.** A Databricks SQL alert per catalog (created by Terraform) sends an e-mail
  when any feed quarantines 5% or more of today's rows. The normal level is about 2%: orders 2.06%,
  inventory 2.01%, shipments 1.80%.
- **Data Quality dashboard page.** Volume vs quarantined, quarantine by rule, reprocessing success
  and pipeline SLA status. The tile SQL lives in `dashboards/dq/` and is tested.
- **Job failure e-mails** on every job.
- **Tests** compare the quarantined rows with the generators' manifests: exactly the corrupted rows,
  under the right rule.

> **Known gaps**
>
> - The SLA tile shows RED on Azure, because the jobs do not yet write SUCCESS rows to
>   `ops.pipeline_run_log`.
> - The dead-letter tables are not monitored.
> - Drift detection is automatic only for file feeds (see 5.5).

### 4.7 Why not DLT expectations

DLT expectations can warn, drop or fail, and they only count violations. This project needs four
things they do not provide:

- every rejected row stored with its payload and reason;
- a PENDING → REPROCESSED / REJECTED lifecycle;
- one root cause per row;
- rules that run in local unit tests.

The rules are already SQL conditions, so they could be turned into expectations later if the
pipeline moves to DLT.

---

## 5. Schema management with Liquibase

### 5.1 Who owns what

| Object | Managed by |
|---|---|
| Catalogs, schemas, volumes, storage, external locations, grants, groups | Terraform |
| All tables (Bronze, Silver, quarantine, ops) and constraints | Liquibase changesets (`liquibase/changelog/changes/001-017`) |
| Gold views and the mart | Liquibase with `runOnChange` (`liquibase/changelog/gold/01-09`) |

### 5.2 How the runner works

The Liquibase CLI is not installed (`liquibase/liquibase.properties.example` shows how it would
connect to Databricks over JDBC, if it is ever used). Instead:

- The files follow the Liquibase format: a master changelog `db.changelog-master.xml` and
  `-- changeset author:id` markers.
- `scripts/apply_migrations.py` applies them through the SQL Statement API of the SQL warehouse.
- Each catalog has a register, `ops.schema_changelog`, with the checksum of every applied
  changeset. It plays the role of Liquibase's DATABASECHANGELOG table.

| Changeset state | Action |
|---|---|
| Not in the register | RUN, then record it |
| In the register, same checksum | SKIP |
| Changed, and it is `runOnChange` (a Gold view) | RE-RUN (CREATE OR REPLACE) and update the checksum |
| Changed, and it is a table changeset | EDITED: stop. An applied changeset must never be edited; write a new one |

- **Failure.** A failed statement stops that catalog, because later changesets build on earlier ones.
- **Dry run.** `--dry-run` prints the plan without changing anything. CD shows the prod plan before
  approval.
- **Login.** A CLI profile, OAuth as a service principal (in CD), or the Azure CLI token.

### 5.3 Where migrations run

Migrations never run inside a Databricks job. They are part of deployment:

- **`up.ps1` step 5** applies all three catalogs on a fresh workspace (66 changesets each).
- **CD** runs migrations first in every environment, then the bundle deploy, then the test run. The
  qa job ends with the prod dry-run plan.

If migrations are skipped, three things go wrong:

- A new column is silently dropped by the Silver writer, because it aligns data to the table schema.
- A Gold view that reads the new column fails.
- On a fresh workspace, jobs create tables without the designed types and comments.

### 5.4 Adding a new column end to end

1. **Ingest contract.** Add the field to `DATASET_SCHEMAS` in `ingest_bronze.py`.
2. **Changeset.** Add a new Liquibase changeset (for example 016) with
   `ALTER TABLE bronze.<feed> ADD COLUMNS (...)`, and the Silver table if needed.
3. **Silver code.** Carry the column through `transform_silver.py`, and add a rule if needed.
4. **Gold.** Update the Gold SQL file if a KPI uses the column. `runOnChange` redeploys it.
5. **Release.** Tests, pull request, CI, merge, then CD (migrate → deploy → run). Reprocess replays
   the quarantined rows.

### 5.5 How a new source column is detected

| Source | Where a new column stops | Data kept? | Detected? |
|---|---|---|---|
| File feeds | Auto Loader → `_rescued_data` → `RULE_SCHEMA_DRIFT` | Yes, in Bronze | Yes: quarantine + alert |
| APIs (routes, weather) | The normaliser maps only known fields | Only in the API cache | No |
| GPS | `from_json` with a fixed schema ignores extra fields | No | No |
| CDC | The SQL Server capture instance (a new one is needed), then the Silver decode | Yes, in the raw `after` JSON | No |

Rescue mode was chosen over `addNewColumns` on purpose. `addNewColumns` would:

- add the column to Bronze only, while Silver and Gold would still need code;
- raise no alert;
- turn a rename into a new column;
- make the real Bronze schema drift away from the Liquibase files.

### 5.6 Why Liquibase, when DLT and Auto Loader handle schemas

Auto Loader and DLT **handle** schema changes: they do not break. They do not **manage** them, which
means:

- deciding and reviewing each change;
- applying it in the same order to every environment;
- keeping a history.

| | Auto Loader | DLT | Liquibase (this project) |
|---|---|---|---|
| Covers | Bronze ingestion only | Its own tables only | All tables and views, including quarantine and ops |
| Same in dev / qa / prod | No (changes when a file arrives) | Through code | Yes: register + order |
| Review and history | No | Git only | Numbered changesets + register + dry-run |
| External writers (the reprocess MERGE) | n/a | Not allowed on DLT tables | Allowed |
| Local tests | Yes | No | Gold SQL is tested locally |

There is a trade-off. Liquibase means more work: a changeset for every change, and the schema also
lives in code. In a DLT project the table schema follows the query, so a new column flows through
automatically only where the query uses `SELECT *`. Renames and type changes often need a full
refresh.

---

## 6. KPIs, dashboard and Genie

### 6.1 KPI definitions

| KPI | Formula | Grain | Target |
|---|---|---|---|
| OTIF % | delivered shipments that were on time (actual ≤ planned) AND in full (shipped ≥ ordered) / delivered × 100 | warehouse × delivery date | 95 |
| Fill rate % | units shipped / units ordered × 100, over shipped lines; cancelled and soft-deleted lines excluded. A line fill rate is also reported | warehouse × order date | 98 |
| Stockout rate % | SKUs with available (on hand − reserved) ≤ 0 / SKUs × 100. An at-risk % (available ≤ safety stock) is also reported | warehouse × snapshot date | max 2 |
| Inventory turnover | trailing-30-day COGS / average inventory value × 365/30, plus days of inventory. Unit cost comes from the SCD2 product version | warehouse × snapshot date | |

### 6.2 Delivery delay risk

`vw_delivery_delay_risk` scores every shipment still in flight:

```
score (0-1) = 0.35 × weather       (HIGH 1.0, MODERATE 0.5, LOW 0.1)
            + 0.35 × carrier        (1 − on-time rate over the last 90 days; 0.8 if no history)
            + 0.20 × distance       (> 800 km 1.0, > 300 km 0.5, else 0.15)
            + 0.10 × GPS staleness  (no ping, or last ping older than 2 hours)
band: HIGH ≥ 0.45, MEDIUM ≥ 0.25, else LOW
```

- The weights are documented business assumptions (implementation guide §9.2), not a trained model.
- The 90-day window is anchored to the latest delivery in Silver, so the scores are reproducible.

### 6.3 Other Gold objects

- `vw_otif_by_carrier_daily`: each carrier's OTIF against its contracted target.
- `mart_logistics_kpi_daily`: materialized; all KPIs per warehouse per day, plus the warehouse name
  and city.
- `vw_customer_directory`, `vw_carrier_directory`: names and e-mails are masked as `***` unless the
  reader is in `pii_readers`.

### 6.4 Dashboard: Logistics Control Tower

The dashboard is a Databricks AI/BI dashboard defined in code:
`dashboards/control_tower.lvdash.json`. The bundle resource `databricks/resources/scm_dashboard.yml`
deploys it.

| Page | Widgets |
|---|---|
| Operations | Intro text; four counters with the targets in their titles (OTIF, fill rate, stockout, turnover); OTIF by day; lowest-OTIF warehouses; carriers vs contracted OTIF; high delay-risk shipments |
| Data Quality | Pipeline SLA status; volume vs quarantined (14 days); reprocessing success; quarantine by rule |

- **One file for every environment.** Queries name `schema.table` only, and the bundle's
  `dataset_catalog` picks the environment, so one file serves dev, qa and prod.
- **Gold only.** The dashboard reads Gold only; a test checks this.
- **Viewer rights.** `embed_credentials: false` means viewers query with their own rights, so grants
  and PII masking apply.
- **Tests.** 10 tests check the datasets, fields and widget spec versions, and that the DQ page runs
  the tested tile SQL.

### 6.5 Genie

Genie (shown in the UI as Genie Agents) turns a business question in plain language into SQL. It
runs on the SQL warehouse with the user's own rights.

Planned setup:

- Gold tables only, starting with the mart.
- Instructions with the KPI definitions.
- Trusted example queries.
- Created from a file in the repository with `databricks genie create-space`.

**Status: not created yet.**

---

## 7. Governance and security

### 7.1 Identities

| Identity | Type | Rights |
|---|---|---|
| `scm-pipeline` | Service principal | ALL_PRIVILEGES on the three catalogs. Prod jobs run as it (`run_as`), and CD logs in as it (OAuth) |
| `scm-analyst` | Service principal (the demo analyst persona) | USE_CATALOG, plus USE_SCHEMA and SELECT on Gold only |
| Deployer | Azure user | Owner of the catalogs |
| `pii_readers` | Workspace group, created empty | Members see real names and e-mails |

Grants go to service principals instead of groups. Unity Catalog grants to groups need account-level
groups, and only a Databricks account admin can create those. In a company, the same grants would go
to groups such as analysts and data-engineers.

### 7.2 Access

- **Three privileges to read a table:** USE_CATALOG, USE_SCHEMA and SELECT. A schema-level grant
  covers today's tables and future ones.
- **Analyst scope.** The analyst cannot read Bronze, Silver, quarantine or ops. Gold views run with
  their owner's rights, so the analyst sees view results without Silver access.
- **Other permissions.**
  - SQL warehouse: CAN_USE for the analyst and the pipeline.
  - Landing: READ_FILES and WRITE_FILES for the pipeline (it writes only the carrier API's pages).
  - Checkpoints: READ and WRITE.
  - Secret scope: READ for the pipeline.
  - Quarantine alert and query: CAN_RUN for the pipeline.
- **Authoritative grants.** `databricks_grants` resources are authoritative: a grant clicked in the
  UI is removed by the next `terraform apply`. Access changes only through a reviewed pull request.
- **Tested.** `tests/test_governance.py` parses `governance.tf` and fails CI on any unexpected grant.

### 7.3 PII masking

```sql
CASE WHEN is_member('pii_readers') THEN customer_name ELSE '***' END AS customer_name
```

- **Masked in Gold, not Silver.** The SCD2 merge compares the real Silver values; a masked value
  would look like a name change every night.
- **Still useful.** Segment and city stay visible, so customers can be counted without being
  identified.

### 7.4 Secrets

| Secret | How it is handled |
|---|---|
| Storage access | No account key anywhere. A Databricks access connector (managed identity) has a storage role, and the Unity Catalog external locations use it |
| Event Hubs connection string | Key Vault + Databricks secret scope, injected as `{{secrets/scm-platform/eventhub-connection-string}}` |
| CD OAuth secret | Piped from Terraform straight into a GitHub secret. It is never printed, stored in a variable or written to a file |
| Debezium SQL password | A user environment variable only, referenced as `${env:...}` |

### 7.5 Audit and lineage

- **Audit queries** in `dashboards/governance/` cover table reads, denied access and permission
  changes, read from `system.access.audit`. They are tested against a fake audit view.
- **Unity Catalog lineage** shows the table and column flow automatically: Bronze → Silver → Gold →
  dashboard.
- **Row-level lineage** comes from `_source_file`, `_batch_id`, `source_position`, the Kafka offsets
  and the quarantine payload.

### 7.6 Process controls

- `main` is protected: a pull request and the CI `test` check are required, and merges are squash
  merges.
- Prod deployment needs a reviewer's approval and is allowed only from `main` (GitHub environment).
- Prod jobs always run as `scm-pipeline`, whoever deploys. A test checks this.

---

## 8. Infrastructure as code

### 8.1 Terraform files

| File | Resources |
|---|---|
| `main.tf` | Resource group |
| `storage.tf` | ADLS Gen2 account; containers landing, checkpoints, managed, rejected |
| `databricks.tf` | Databricks workspace; access connector (managed identity) |
| `rbac.tf` | Storage role for the access connector; storage and Key Vault roles for the deployer |
| `unity_catalog.tf` | Storage credential, external locations, 3 catalogs, 15 schemas, checkpoint volumes |
| `sql_warehouse.tf` | SQL warehouse |
| `eventhub.tf` | Event Hubs namespace and the `gps-events` event hub (8 partitions, 7 days) |
| `keyvault.tf`, `secrets.tf` | Key Vault, Event Hubs auth rule and connection string, the generated carrier API key, Databricks secret scope and secrets |
| `governance.tf` | Service principals, grants, warehouse permission, secret ACL, pipeline OAuth secret, `pii_readers` |
| `alerts.tf` | Quarantine-rate query and alert per catalog, with run permissions for the pipeline |
| `wms_sftp.tf` | The WMS SFTP drop: a storage account with SFTP on, container `wms-drop`, local users `wms` and `adf`, their passwords in Key Vault |
| `adf.tf`, `adf/*.json` | Data Factory: Key Vault, SFTP and data lake connections, datasets, pipeline `copy_wms_inventory`, daily 01:30 UTC trigger, roles (Key Vault secrets, landing only) |
| `outputs.tf` | Workspace URL, warehouse id, storage name, Event Hubs namespace, alert ids, pipeline client id and the (sensitive) secret |

One `terraform apply` creates 91 resources. The settings for the student subscription are in
`infra/envs/student.tfvars`.

### 8.2 Session scripts

The stack is created at the start of each working session and destroyed at the end, so money is
spent only while working.

**scripts/up.ps1**

| Step | What happens |
|---|---|
| 1. Safety checks | Logged in; the subscription is Azure for Students; the previous session's Databricks managed resource group is gone |
| 2. terraform apply | Builds the stack after you type `yes`. Billing starts here |
| 3. Outputs | Reads the workspace URL, warehouse id, storage, Event Hubs and alert id; drops the secret from memory at once |
| 3b. GitHub values | Sets the CD secrets and variables with `gh`; the OAuth secret is piped from Terraform to GitHub |
| 4. Databricks login | Uses the az login token; an empty CLI config avoids stale profiles |
| 5. Tables | `apply_migrations.py` on dev, qa and prod |
| 6. Data | Generates fresh data dated today and uploads 8 feeds to `landing`, retrying while a new storage role activates. Plays the WMS for the ninth: puts the stock CSV on the SFTP drop (`scripts/wms_sftp_upload.py`, password from Key Vault), starts the Data Factory copy at once instead of waiting for 01:30, and waits for it to succeed |
| 7. Deploy | `databricks bundle deploy`: builds the wheel and creates 4 jobs and the dashboard |

```powershell
.\scripts\up.ps1                 # normal session
.\scripts\up.ps1 -SkipData       # keep the data already uploaded
.\scripts\up.ps1 -SkipGitHub     # this session will not use CD
. .\scripts\session_env.ps1      # in any other PowerShell window
.\scripts\down.ps1               # end of session: destroy everything
```

**scripts/down.ps1**

1. Checks the subscription.
2. Runs `terraform destroy` (you type `yes`).
3. Clears the local bundle cache.
4. Removes the old workspace URL and the CD secret from GitHub.
5. Waits up to 30 minutes until Azure has removed the Databricks managed resource group.

**Common problems**

| Symptom | Cause and fix |
|---|---|
| `...-managed from the last session still exists` | Azure is still deleting the old workspace. Wait 20-30 minutes |
| `cannot create service principal secret: User is not authorized` | Seen on a fresh workspace, where permissions activate a little later. Run apply (or `up.ps1`) again |
| `gh secret set ... HTTP 403` | The active `gh` account has no access to the repository. Run `gh auth switch --user Jigar-Jk` |
| `<feed> not allowed yet ... retry` | A new storage role is activating; the script retries by itself |
| Bundle deploy fails on a pip timeout | PyPI is slow while building the wheel. Run `up.ps1 -SkipData` again |
| `no such host` from the Databricks CLI | A stale `default_profile` in `~/.databrickscfg`. Use `session_env.ps1` (empty CLI config) |

---

## 9. Environments, CI/CD and jobs

### 9.1 Why one workspace with three catalogs

Unity Catalog isolates environments with catalogs, and the grants and `run_as` keep them apart.
Three workspaces would mean:

- three times the workspace setup and Terraform;
- three SQL warehouses;
- more vCPU quota than the student subscription allows (4 vCPUs per VM family, so one cluster at a
  time).

In a company with a bigger budget, prod often gets its own workspace for network and blast-radius
isolation, with the metastore shared. The bundle targets and the catalog names would stay the same.

### 9.2 What CD deploys to each environment

Every environment receives the same release. Only the values differ.

| Item | What it is | dev | qa | prod |
|---|---|---|---|---|
| Table and view changes | New Liquibase changesets via `apply_migrations.py` | `scm_platform_dev` | `scm_platform_qa` | `scm_platform_prod` |
| The code | The `scm_platform` wheel, built once per deploy from the commit | uploaded | uploaded | uploaded |
| Job definitions | 4 jobs, with tasks pointing at the wheel and the SQL files | `[dev <user>]` names, paused | paused | paused, `run_as` scm-pipeline |
| SQL files | `databricks/sql/*.sql` that the jobs run | synced | synced | synced |
| Dashboard | `control_tower.lvdash.json`, with `dataset_catalog` set per environment | yes | yes | yes |
| Workspace folder | Where the bundle files go | `/Workspace/Users/<you>/.bundle/.../dev` | `/Workspace/Shared/.bundle/.../qa` | `/Workspace/Shared/.bundle/.../prod` |
| Test run | `scm_daily_pipeline` | yes | no | no |

Some things are not deployed by CD:

- The data: landing files are uploaded by `up.ps1`.
- Infrastructure and grants: Terraform.
- Excluded folders (`sync.exclude` in `databricks.yml`): `data/`, `tests/`, `infra/`, `liquibase/`,
  `local/`, `cdc/`, `docs/`.

### 9.3 CI (every push and pull request)

1. Set up Python 3.11 and Java 17 (for local Spark).
2. Lint with `ruff check .`.
3. Run all tests: 357 tests in 27 files, fast tests and local Spark tests. They include bundle,
   dashboard, governance, migration runner and CD workflow checks.
4. Build the wheel and keep it as an artifact.

### 9.4 CD (manual dispatch on main)

```
build ──→ dev ───────────────────→ qa ──────────────────────────→ prod (reviewer approval)
          migrations (dev)          migrations (qa)                migrations (prod)
          bundle deploy             bundle deploy                  bundle deploy
          run scm_daily_pipeline    prod --dry-run migration plan
```

- **Login.** Every job logs in as `scm-pipeline` (OAuth machine-to-machine).
- **Secrets:** `DATABRICKS_HOST`, `DATABRICKS_CLIENT_SECRET`, `ALERT_EMAIL`.
- **Variables:** `DATABRICKS_CLIENT_ID`, `WAREHOUSE_ID`, `STORAGE_ACCOUNT`, `KAFKA_BOOTSTRAP`, and
  `QUARANTINE_ALERT_ID` per environment.
- **Safety:** `permissions: contents: read`; one CD run at a time (`concurrency: cd`); the
  Databricks CLI is pinned to 1.17.0.
- **Public logs.** The repository is public, so its Actions logs are public too. Anything sensitive
  is a secret, which GitHub masks.

### 9.5 Jobs

| Job | Schedule (UTC) | Tasks |
|---|---|---|
| ADF `copy_wms_inventory` | 01:30 daily | WMS CSV from the SFTP drop to landing/inventory/, then archive (Data Factory, not a Databricks job) |
| `scm_wms_sla_check` | 02:30 daily | check_wms_snapshot (carries stock forward on an SLA miss) |
| `scm_daily_pipeline` | 03:00 daily | carrier_api_enabled → fetch_carrier_api (only with a URL) → ingest_bronze_files → transform_silver → silver_quality_tests → refresh_gold; quarantine_rate_alert after transform_silver |
| `scm_streaming` | every 30 minutes | stream_gps and stream_cdc in parallel on one cluster |
| `scm_reprocess` | every 2 hours | reprocess_quarantine |

- **Paused schedules.** Schedules are paused in qa and prod (student budget, one-cluster quota) and
  in dev mode. Runs there are manual or by CD.
- **Job settings.** All jobs use single-node job clusters in Unity Catalog mode, allow one run at a
  time, and send an e-mail on failure.

---

## 10. End-to-end workflows

### 10.1 A working session on Azure

1. Run `az login`, then `.\scripts\up.ps1`. This creates the stack, the GitHub values, the tables,
   fresh data and the dev deploy.
2. Run CD from GitHub Actions: dev (deploy and test run) → qa (deploy and prod plan) → approve prod.
3. Or run the pipeline directly: `databricks bundle run -t dev scm_daily_pipeline`.
4. Open the dashboard, check Gold and capture evidence.
5. Run `.\scripts\down.ps1` to stop billing.

### 10.2 One night of data

1. Sources deliver files to `landing`. CDC and GPS events wait in Kafka / Event Hubs.
2. At 02:30 the WMS SLA check runs; missing stock files are carried forward and flagged.
3. At 03:00 the daily pipeline runs: Bronze (Auto Loader) → Silver (quality gate, SCD2, MERGE) → SQL
   quality tests → Gold refresh, then the quarantine alert.
4. Every 30 minutes, streaming drains GPS and CDC into Bronze.
5. Every 2 hours, reprocess heals or rejects quarantined rows.
6. Users read the dashboard (and later Genie) with their own rights.

### 10.3 Making a change

1. Create a feature branch from an up-to-date `main`.
2. Change code, SQL, changesets, dashboard or Terraform together, with tests.
3. Open a pull request. CI must pass; review; squash merge.
4. Run CD, check the prod plan, and approve prod.

---

## 11. Status and next steps

### 11.1 Verified

- A full CD run (build, dev, qa, prod) and a green daily pipeline running as the service principal.
- The migration register works live: a new Gold view was applied as `run 1, skipped 61`.
- The carrier OTIF view matches the dashboard: 1,696 rows, 19 carriers, OTIF 80.6% in the rehearsal.
- CDC end to end locally: snapshot, live changes, restart recovery, stale replay, and a
  reconciliation of 60,000 rows.
- About 2% quarantined per feed, matching the generator manifests.

### 11.2 Open items

- Jobs write SUCCESS to `ops.pipeline_run_log` (fixes the red SLA tile).
- Split the dashboard in two:
  - a business Control Tower with `embed_credentials: true`, and a test that it never reads the PII
    views;
  - a separate Data Quality monitor.

  The dashboard permissions also go into code.
- A Genie space as code, then live testing.
- Live governance checks: `SHOW GRANTS`, masking on and off, audit queries on the system tables.
- CDC and GPS on Azure: point Debezium and the GPS producer at Event Hubs.
- Smaller hardening:
  - DLQ monitoring;
  - a CDC freshness alert;
  - GPS raw payload and a key check;
  - an unknown-field log in the API normaliser;
  - a test that compares the Liquibase DDL with the ingest contract;
  - a guard at pipeline start that stops when migrations are pending.

---

## 12. Likely questions

**The data is synthetic. What do the numbers prove?**
They prove that the pipeline processes data correctly. Every corrupted row is listed in a manifest,
and the tests check that Silver quarantines exactly those rows. The KPIs are also computed
independently in Python and compared with the SQL. Connecting a real source only changes the landing
folder or the topic.

**Why keep Bronze as strings and nullable?**
Bronze must never lose data. Strict types would fail broken rows at load time, so nobody could see or
fix them. Typing and validation happen in Silver, with a reason for every rejection.

**Why CDC instead of copying the orders table every night?**
A full copy is heavy and slow, and it cannot show deletes. CDC sends only the changes, within
minutes, including deletes. It reads the transaction log instead of querying the tables.

**What if an event arrives twice or out of order?**
Silver keeps the highest `source_position` per key, and the MERGE only moves forward. Duplicates
collapse, and an old event never overwrites a newer one. This was tested live with a replayed stale
event.

**Why not Lakeflow Connect for CDC?**
- Its gateway runs in Azure and must reach SQL Server over the network; ours runs on a laptop.
- The gateway runs continuously, which the student budget cannot carry.
- Its ingestion pipeline needs serverless compute.
- We also wanted the ordering and recovery logic visible and tested.

In production, with a reachable source and a Databricks-only team, Lakeflow Connect is a strong
choice.

**Why not a DLT pipeline?**
- Tables are versioned by Liquibase, and the reprocess job MERGEs into them; DLT owns its tables.
- The logic is custom: SCD2 with stale protection, the CDC switch, the quarantine lifecycle and the
  carry-forward.
- The whole pipeline runs and is tested locally.

DLT would save code (APPLY CHANGES, expectations) and is a valid alternative.

**Why a galaxy schema?**
There are three business processes with different grains. One fact table would mix the grains and
double-count. Shared dimensions still let the processes be compared by warehouse, product and date.

**Why SCD2 for customers but SCD1 for carriers?**
Orders must stay with the customer version that was valid at the time: a customer who moved keeps
their old orders in the old city. The guide sets carriers and routes to SCD1, and switching is one
function call.

**Why md5 surrogate keys?**
They are deterministic: the same input gives the same key in every run and every environment, so
MERGE works. Auto-increment is hard in distributed Spark and would differ per run.

**What if the pipeline runs twice?**
- Auto Loader skips files it has already read.
- Silver MERGEs on surrogate keys.
- SCD2 creates no new version without a change.
- Gold views are always current.

The pipeline is idempotent.

**Why not drop bad rows?**
Dropped rows disappear without a trace, and some failures heal by themselves (for example, a late
dimension). Quarantine keeps the payload and the reason, and reprocess brings the rows back.

**Why one workspace instead of three?**
Unity Catalog isolates environments with catalogs, grants and `run_as`. Three workspaces would
triple the setup and the warehouses and exceed the student quota. In a company, prod often gets its
own workspace for extra isolation.

**What does CD actually deploy to each environment?**
The same release everywhere: the new schema changesets, the wheel built from the commit, the job
definitions, the SQL files and the dashboard. Only the catalog, the folder, the schedules and the
identity differ. Data and infrastructure are not part of CD.

**Who can see customer names?**
Only members of `pii_readers`. Everyone else sees `***` in Gold. The group starts empty, and the
analyst cannot read Silver at all.

**Why no storage keys?**
A key opens the whole account and leaves no audit trail. A managed identity with external locations
gives path-level access (landing is read-only), and every read is audited.

**What happens if someone adds a grant in the UI?**
The next `terraform apply` removes it, because the grants are authoritative. The permission-change
audit query shows who made the change.

**How is production protected?**
- `main` is protected, and CI must pass.
- Prod needs a reviewer's approval and deploys only from `main`.
- Prod jobs run as the service principal.
- A migration dry-run is shown before approval.
