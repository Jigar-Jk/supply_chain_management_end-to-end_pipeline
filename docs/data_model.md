# Data model (ER diagrams)

Built from the Liquibase migrations (`liquibase/changelog/changes/001`–`015`), i.e. the tables as
deployed. GitHub renders the diagrams below directly.

- **Diagram 1 – Silver galaxy schema:** 3 fact tables sharing 6 conformed dimensions, plus the
  real-time `inventory_current`. This is what Gold reads.
- **Diagram 2 – Quarantine and ops:** the tables behind the quality gate, reprocessing and the DQ tiles.

Notation: `PK` primary key, `FK` foreign key, `UK` natural (business) key.
`||--o{` = one-to-many. Dotted lines (`..`) are links on a natural key, not on a surrogate key.

## 1. Silver galaxy schema

```mermaid
erDiagram
    dim_customer ||--o{ fact_order : "customer_sk"
    dim_product ||--o{ fact_order : "product_sk"
    dim_warehouse |o--o{ fact_order : "origin_warehouse_sk"
    dim_date ||--o{ fact_order : "order_date_sk / promised_delivery_date_sk"

    dim_carrier ||--o{ fact_shipment : "carrier_sk"
    dim_route ||--o{ fact_shipment : "route_sk"
    dim_warehouse |o--o{ fact_shipment : "origin_warehouse_sk"
    dim_date ||--o{ fact_shipment : "dispatch_date_sk / planned_delivery_date_sk"

    dim_warehouse ||--o{ fact_inventory_snapshot : "warehouse_sk"
    dim_product ||--o{ fact_inventory_snapshot : "product_sk"
    dim_date ||--o{ fact_inventory_snapshot : "snapshot_date_sk"

    fact_order }o..o| fact_shipment : "order_id (one shipment per order)"
    dim_warehouse ||..o{ inventory_current : "warehouse_id"
    dim_product ||..o{ inventory_current : "product_id"

    dim_customer {
        string customer_sk PK "hash(customer_id, effective_from)"
        string customer_id UK "SQL Server OMS"
        string customer_name
        string customer_segment "ENTERPRISE, SMB, RETAIL"
        string country
        string city
        timestamp effective_from "SCD2"
        timestamp effective_to "SCD2, NULL = current"
        boolean is_current "SCD2"
        timestamp created_at
    }

    dim_product {
        string product_sk PK "hash(product_id, effective_from)"
        string product_id UK "SKU"
        string product_name
        string category
        double unit_weight_kg
        double unit_volume_cbm
        double unit_cost
        double unit_price
        timestamp effective_from "SCD2"
        timestamp effective_to "SCD2"
        boolean is_current "SCD2"
        timestamp created_at
    }

    dim_warehouse {
        string warehouse_sk PK "hash(warehouse_id, effective_from)"
        string warehouse_id UK "WMS"
        string warehouse_name
        string facility_type "REGIONAL_DC, HUB, FULFILLMENT_CENTER"
        string city
        string country
        double latitude
        double longitude
        double capacity_sqm
        timestamp effective_from "SCD2"
        timestamp effective_to "SCD2"
        boolean is_current "SCD2"
        timestamp created_at
    }

    dim_carrier {
        string carrier_sk PK "md5(carrier_id), SCD1"
        string carrier_id UK
        string carrier_name
        string service_level
        string transport_mode
        double contracted_otif_target "0-100"
        string contact_email
        boolean is_active
        timestamp effective_from
        timestamp effective_to
        boolean is_current
        timestamp created_at
    }

    dim_route {
        string route_sk PK "md5(route_id), SCD1"
        string route_id UK "ORS"
        double origin_latitude
        double origin_longitude
        double destination_latitude
        double destination_longitude
        double distance_meters
        double duration_seconds
        string routing_profile "driving-hgv"
        string source "openrouteservice"
        timestamp effective_from
        timestamp effective_to
        boolean is_current
        timestamp created_at
    }

    dim_date {
        int date_sk PK "YYYYMMDD"
        date calendar_date
        int year
        int quarter
        int month
        string month_name
        int day_of_month
        int day_of_week "1 = Monday"
        string day_name
        boolean is_weekend
        boolean is_holiday
    }

    fact_order {
        string order_line_sk PK "grain: one order line"
        string order_id UK
        int order_line_number UK
        string customer_sk FK
        string product_sk FK
        string origin_warehouse_sk FK
        int order_date_sk FK
        int promised_delivery_date_sk FK
        int ordered_quantity
        int shipped_quantity "fill rate"
        double unit_price
        double total_price
        string order_status "CREATED .. DELIVERED, CANCELLED"
        string customer_id
        string product_id
        string warehouse_id
        timestamp updated_at "freshness"
        boolean is_deleted "CDC soft delete"
        string source_position "CDC LSN, highest wins"
        timestamp created_at
    }

    fact_shipment {
        string shipment_leg_sk PK "grain: one shipment leg"
        string shipment_id UK
        int leg_sequence
        string order_id "links to fact_order"
        string carrier_sk FK
        string route_sk FK
        string origin_warehouse_sk FK
        int dispatch_date_sk FK
        int planned_delivery_date_sk FK
        timestamp dispatch_time
        timestamp planned_delivery_time
        timestamp actual_delivery_time "NULL in transit"
        double planned_transit_hours
        double actual_transit_hours
        string shipment_status
        int ordered_quantity
        int shipped_quantity
        int delay_minutes
        boolean is_on_time "OTIF"
        boolean is_in_full "OTIF"
        string weather_risk "LOW, MODERATE, HIGH"
        string truck_id "GPS stream"
        double last_lat "GPS stream"
        double last_lon "GPS stream"
        timestamp last_ping_ts "GPS stream"
        string carrier_id
        string route_id
        string warehouse_id
        string data_type
        timestamp updated_at
        timestamp created_at
    }

    fact_inventory_snapshot {
        string inventory_snapshot_sk PK "grain: warehouse x product x night"
        string warehouse_sk FK
        string product_sk FK
        int snapshot_date_sk FK
        date snapshot_date
        double quantity_on_hand
        double quantity_reserved
        double quantity_available "on_hand - reserved"
        double reorder_point
        double safety_stock
        boolean is_stockout_risk
        boolean is_stockout "stockout rate"
        boolean is_stale "carried forward after a WMS SLA miss"
        string warehouse_id
        string product_id
        timestamp recorded_at "WMS export time, newest wins"
        timestamp created_at
    }

    inventory_current {
        string inventory_current_sk PK "md5(warehouse_id, product_id)"
        string warehouse_id UK
        string product_id UK
        int quantity_on_hand
        int quantity_reserved
        int quantity_available
        int safety_stock
        int reorder_point
        timestamp updated_at
        boolean is_deleted "CDC soft delete"
        string source_position "CDC LSN"
        timestamp created_at
    }
```

### How to read it

| Fact | Grain (one row per…) | Feeds the KPIs |
|---|---|---|
| `fact_order` | order line | Fill rate (`shipped_quantity / ordered_quantity`), turnover |
| `fact_shipment` | shipment leg | OTIF (`is_on_time`, `is_in_full`), delivery-delay risk (GPS + weather) |
| `fact_inventory_snapshot` | warehouse × product × night | Stockout rate, turnover, `built_from_stale_data` |
| `inventory_current` | warehouse × product (latest) | Real-time stock from CDC |

- **Galaxy (fact constellation), not a single star:** three facts share the conformed dimensions
  (`dim_warehouse`, `dim_product` and `dim_date` serve several facts), so KPIs from different
  facts line up by warehouse, product and day.
- **SCD2 dimensions** (`customer`, `product`, `warehouse`) keep history: a fact points at the
  dimension *version* valid when the event happened (point-in-time lookup), so a warehouse
  renamed today does not rewrite last month's KPIs. **SCD1** (`carrier`, `route`) overwrite in place.
- **`dim_date`** keys are `YYYYMMDD` integers; `fact_order` and `fact_shipment` each reference it twice
  (role-playing dimension: order vs promised date, dispatch vs planned delivery date).
- **Soft deletes:** a row deleted in SQL Server stays in Silver with `is_deleted = true` (audit);
  Gold excludes it.
- **`silver.weather`** (hourly Open-Meteo forecasts) is a supporting table with no foreign keys; its
  signal reaches the facts as `fact_shipment.weather_risk`.
- `silver.routes` / `silver.shipments` from migration 002 are legacy tables the pipeline no longer writes.

## 2. Quarantine and ops

```mermaid
erDiagram
    quarantine_records }o..|| sla_config : "source_system"
    pipeline_run_log }o..|| sla_config : "source_system"

    quarantine_records {
        string quarantine_id PK
        string original_payload "never edited (audit)"
        string corrected_payload "steward correction, replayed instead"
        string failure_reason
        string failed_rule "e.g. RULE_ORDER_STATUS_INVALID"
        string batch_id "job run"
        string source_system "SQLSERVER_OMS, WMS, CARRIER_API, ORS"
        string source_feed "orders, inventory, shipments, warehouses, ..."
        timestamp ingested_at
        string status "PENDING, REPROCESSED, REJECTED"
        int retry_count
        timestamp last_retry_at
        timestamp resolved_at
        string resolved_by
        string resolution_note
    }

    sla_config {
        string source_system PK "wms_batch, carrier_api, sqlserver_cdc, gps_stream"
        string expected_by_time "HH:MM UTC, batch sources"
        int max_lag_minutes "AMBER above, RED above 2x"
        int quarantine_sla_days "PENDING older than this -> REJECTED"
    }

    pipeline_run_log {
        string run_id "job run"
        string source_system
        string step
        string layer "bronze, silver, gold"
        timestamp started_at
        timestamp ended_at
        bigint rows_read
        bigint rows_written
        bigint rows_quarantined
        string status "SUCCESS, FAILED, SLA_MISS"
        string error_message
    }
```

In Unity Catalog these are `quarantine.records`, `ops.sla_config` and `ops.pipeline_run_log`
(Mermaid entity names cannot contain a dot). The quarantine rows link back to Silver through
`source_feed`. A healed row is merged into that feed's Silver table and marked `REPROCESSED`.
