# CDC: SQL Server OMS → Debezium → Bronze → Silver

Pattern 3 of the four ingestion patterns. Every insert, update and delete in the order-management
database reaches the lakehouse as a change event, so Silver mirrors the source without re-reading
whole tables. The sequence diagram is in [architecture.md §4](architecture.md#4-cdc-path-in-detail);
this page covers how the pieces fit, why they were built this way, and how to run the demo.

## 1. What flows where

| Step | Component | Files |
|---|---|---|
| Source tables | `scm_oms.dbo.order_lines`, `scm_oms.dbo.inventory` with primary keys (Debezium uses them as the message key, so all changes to one row stay in order on one partition) | [cdc/sqlserver/01_create_database.sql](../cdc/sqlserver/01_create_database.sql) |
| Capture | CDC enabled on both tables; change retention 7 days, matching Kafka / Event Hubs retention | [cdc/sqlserver/02_enable_cdc.sql](../cdc/sqlserver/02_enable_cdc.sql) |
| Least privilege | `debezium_cdc` login: read-only on `dbo` (snapshot) and `cdc` (changes), cannot write | [cdc/sqlserver/03_create_debezium_login.sql](../cdc/sqlserver/03_create_debezium_login.sql) |
| Connector | Debezium SQL Server connector: initial snapshot (`op = r`), then streaming (`c` / `u` / `d`). Topics `scm.scm_oms.dbo.<table>`. Password read from `${env:DEBEZIUM_SQL_PASSWORD}`, never stored in a file | [cdc/debezium/sqlserver-connector.json](../cdc/debezium/sqlserver-connector.json) |
| Bronze | One streaming query per topic → `bronze.orders_cdc`, `bronze.inventory_cdc` (append-only, envelope as columns, row images as raw JSON); non-JSON messages → `bronze.cdc_dlq` verbatim; tombstones skipped | [scm_platform/stream_cdc_events.py](../scm_platform/stream_cdc_events.py), [liquibase 011](../liquibase/changelog/changes/011-cdc-bronze-tables.sql) |
| Silver | Decode the images, drop redeliveries, keep the **highest `source_position` per key**, `op = d` → `is_deleted = true`; then the same quality gate as every feed → `fact_order`, `inventory_current` | [scm_platform/transform_silver.py](../scm_platform/transform_silver.py) (`decode_order_line_cdc`, `latest_cdc_rows`, `transform_inventory_current`), [liquibase 012](../liquibase/changelog/changes/012-cdc-silver-apply.sql) |
| Schedule | `scm_streaming` job, every 30 min, drains the topics (`--available-now`) and stops; offsets checkpointed in `/Volumes/<catalog>/ops/checkpoints/cdc` | [databricks/resources/scm_streaming.yml](../databricks/resources/scm_streaming.yml) |
| Freshness | `ops.sla_config` row `sqlserver_cdc`: max lag 45 min (30-min schedule plus margin), AMBER beyond that, RED at 2× | [liquibase 015](../liquibase/changelog/changes/015-streaming-sla-and-sqlserver-cdc-name.sql) |

## 2. Design decisions

- **`source_position` = commit LSN + change LSN + event serial, as one fixed-width string.** A
  greater string is a later change. Silver keeps the highest per key, so an event delivered late,
  replayed or re-sent after a crash can never overwrite newer data. The MERGE condition only moves
  forward.
- **Bronze does not deduplicate.** A redelivered event has the same position as the original, so
  Silver collapses it; Bronze stays a faithful log of what arrived.
- **Bronze stores row images as raw JSON.** A new source column never breaks ingestion; Silver
  applies each table's schema.
- **Soft deletes.** A delete keeps the last known values and sets `is_deleted`, so Gold history
  and audits still add up.
- **Bad values are not filtered at the source.** The OMS tables have no CHECK constraints on
  purpose, and the change simulator injects ~2 % bad events (negative quantity, misspelled status,
  over-shipment). They flow through CDC and the Silver quality gate quarantines them like any feed.
- **No cluster running 24/7.** The stream runs every 30 minutes with `availableNow`; the
  checkpoint means each run starts exactly where the last one stopped.
- **One business event = one transaction.** Shipping an order updates `order_lines` and
  `inventory` together, so both changes share a commit LSN.

## 3. Failure scenarios covered

| Scenario | What happens | Shown by |
|---|---|---|
| Debezium (Kafka Connect) down for a while | Changes stay in the CDC change tables (7-day retention). On restart Debezium resumes from its saved LSN bookmark (Kafka topic `connect_offsets`); nothing is lost or duplicated in Silver | demo step 6 below |
| Old event delivered after a newer one | Higher `source_position` wins; Silver keeps the newer state | demo step 7, `test_out_of_order_replay_cannot_overwrite_newer_state` |
| Same event delivered twice | Same position → collapsed, not quarantined | `test_redelivered_event_is_collapsed_not_quarantined` |
| Delete | Soft delete with the last values | `test_delete_becomes_soft_delete_with_last_known_values` |
| Bad value arrives via CDC | Quarantined by the quality gate | `test_bad_values_via_cdc_hit_the_quality_gate` |
| Message is not JSON | Kept verbatim in `bronze.cdc_dlq` | `test_garbage_goes_to_dead_letter_verbatim` |
| Spark job stops mid-run | Resumes from the checkpointed offsets | `scm_streaming` checkpoint volume |

Automated tests: [tests/test_stream_cdc_events.py](../tests/test_stream_cdc_events.py) (Bronze
parsing, positions, dead letters) and [tests/test_cdc_silver.py](../tests/test_cdc_silver.py)
(Silver apply), built on real Debezium messages captured in
[tests/fixtures_debezium_order_lines.json](../tests/fixtures_debezium_order_lines.json). They run
in CI without Docker or SQL Server.

## 4. Running the demo locally

### Prerequisites (one-time)

- **SQL Server** (default instance `MSSQLSERVER`) with **SQL Server Agent** running, plus
  `pip install -r requirements-cdc.txt` (`pyodbc`).
- **Docker Desktop** (Kafka, Kafka Connect with Debezium, Spark; sized for an ~8 GB laptop).
- Generated data in `data/`: see the README's local run.

1. **Enable TCP on port 1433** so Debezium in Docker can reach SQL Server. Run as Administrator:
   `cdc/sqlserver/enable_tcp.ps1`.
2. **Create the OMS database, enable CDC and create the read-only login.** Run the three scripts in
   [cdc/sqlserver/](../cdc/sqlserver/) in order (SSMS or `sqlcmd -E`). In `03`, replace the
   placeholder password when running it; do not save it in the file.
3. **Set the password as a user environment variable** (never in a file, never echoed), then open a
   new terminal so it is picked up:
   ```powershell
   [Environment]::SetEnvironmentVariable('DEBEZIUM_SQL_PASSWORD', (Read-Host -AsSecureString | ConvertFrom-SecureString -AsPlainText), 'User')
   ```
   On Windows PowerShell 5.1, which has no `-AsPlainText`, set it under *System Properties →
   Environment Variables* instead.

### Each run

All `docker compose` commands use `-f local/streaming/docker-compose.yml` (written `dc` below).

| # | Step | Command | Expected |
|---|---|---|---|
| 1 | Load the source | `python -m data_generators.load_sqlserver_oms --reset` | `order_lines: loaded N, skipped {...}` (null/duplicate keys skipped, other bad values kept) |
| 2 | Start Kafka + Kafka Connect | `dc up -d kafka kafka-init kafka-connect` | `scm-kafka-connect` healthy on port 8083 |
| 3 | Register the connector | `Invoke-RestMethod -Method Put -Uri http://localhost:8083/connectors/scm-oms-sqlserver/config -ContentType application/json -InFile cdc/debezium/sqlserver-connector.json` | `Invoke-RestMethod http://localhost:8083/connectors/scm-oms-sqlserver/status` shows `RUNNING` |
| 4 | Snapshot into Bronze | `dc run --rm cdc-stream`, then `dc run --rm cdc-stream-inspect` | Only `op r` rows; snapshot rows = source rows |
| 5 | Live changes | `python -m data_generators.simulate_oms_changes --events 2000 --tps 20`, then `dc run --rm cdc-stream` and inspect again | `c` / `u` / `d` rows appear after the snapshot |
| 6 | **Outage** | `docker stop scm-kafka-connect`, run the simulator again, `docker start scm-kafka-connect`, wait for `RUNNING`, then `dc run --rm cdc-stream` | Changes made during the outage arrive after the restart |
| 7 | **Out-of-order replay** | `python -m local.streaming.replay_stale_cdc_event`, then `dc run --rm cdc-stream` | Prints the key plus stale vs newest status; Silver keeps the newest |
| 8 | **Reconcile** | `python -m local.streaming.export_oms_state`, then `dc run --rm cdc-reconcile` | `RESULT: PASS - identical` (0 mismatched, missing, extra) and exit code 0 |
| 9 | Stop | `dc down` | Kafka topics are lost; Delta tables in `data/_local_lake/` stay |

After `dc down` the connector's LSN bookmark is gone (it lives in Kafka). For a clean rerun, start
again from step 1 and delete `data/_local_lake/bronze/*_cdc` and the CDC checkpoints first.
Last recorded result: snapshot, outage recovery and stale replay all ended in a 60k-row
reconciliation with **PASS - identical**.

## 5. On Databricks (Azure)

- Same code, run by the `scm_streaming` job with `--catalog` and a checkpoint volume instead of
  `--lake-path`.
- The Kafka bootstrap is the Event Hubs namespace (`<namespace>.servicebus.windows.net:9093`, bundle
  variable `kafka_bootstrap`). The connection string comes from the secret scope
  (`eventhub-connection-string`). Event Hubs speaks the Kafka protocol, so no code changes.
- **Not done yet:** pointing Kafka Connect at Event Hubs for a cloud run (remaining-work line 6).
  For the demo, CDC is shown on the local stack; the cloud job is deployed and drains whatever the
  topics hold.
- Silver reads `bronze.orders_cdc` when it exists and falls back to the `bronze.orders` file feed
  otherwise, so the daily pipeline works in an environment without CDC.

## 6. Known limitations

- Local only end to end; the cloud path is deployed, but no Debezium → Event Hubs run has been done.
- Schema changes in the source (`include.schema.changes = false`) are not propagated; a new source
  column appears in the raw `after` JSON and needs a Silver change to be used.
- `SIMPLE` recovery model on the local database is fine for CDC but not for production backups.
