-- ============================================================================
-- Silver quality tests - task silver_quality_tests in scm_daily_pipeline
-- ============================================================================
-- The quality gate in transform_silver checks every ROW on its own (bad values -> quarantine).
-- These tests check every TABLE as a whole after the write: keys unique, SCD2 history valid,
-- every foreign key resolvable, business rules hold, quarantine rate normal, data fresh.
--
-- Each check counts its failing rows. If any count is above 0 the last statement calls
-- raise_error(): the task fails, refresh_gold is skipped (dashboards keep yesterday's correct
-- mart) and the failure e-mail lists every failing check by name.
--
-- Parameter: catalog (dev / qa / prod), passed by the job.
-- Tested locally by tests/test_silver_quality_tests.py (clean Silver passes, each break is caught).
-- ============================================================================

USE CATALOG IDENTIFIER(:catalog);

-- Thresholds. as_of is the reference time for the freshness checks.
DECLARE OR REPLACE VARIABLE as_of TIMESTAMP DEFAULT current_timestamp();
DECLARE OR REPLACE VARIABLE max_quarantine_pct DOUBLE DEFAULT 20.0;
DECLARE OR REPLACE VARIABLE min_rows_for_rate BIGINT DEFAULT 100;
DECLARE OR REPLACE VARIABLE max_source_age_hours INT DEFAULT 26;

CREATE OR REPLACE TEMPORARY VIEW silver_quality_results AS

-- ---------------------------------------------------------------- 1. uniqueness
-- A second row for the same key means a MERGE inserted instead of updating: KPIs count it twice.
SELECT 'uniqueness' AS category, 'fact_order.order_line_sk unique' AS check_name, count(*) AS failing_rows
FROM (SELECT order_line_sk FROM silver.fact_order GROUP BY order_line_sk HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'fact_order one row per order line', count(*)
FROM (SELECT order_id, order_line_number FROM silver.fact_order GROUP BY order_id, order_line_number HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'fact_shipment.shipment_leg_sk unique', count(*)
FROM (SELECT shipment_leg_sk FROM silver.fact_shipment GROUP BY shipment_leg_sk HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'fact_inventory_snapshot.inventory_snapshot_sk unique', count(*)
FROM (SELECT inventory_snapshot_sk FROM silver.fact_inventory_snapshot GROUP BY inventory_snapshot_sk HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'fact_inventory_snapshot one row per warehouse, product, night', count(*)
FROM (SELECT warehouse_id, product_id, snapshot_date FROM silver.fact_inventory_snapshot
      GROUP BY warehouse_id, product_id, snapshot_date HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'inventory_current.inventory_current_sk unique', count(*)
FROM (SELECT inventory_current_sk FROM silver.inventory_current GROUP BY inventory_current_sk HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'dim_customer.customer_sk unique', count(*)
FROM (SELECT customer_sk FROM silver.dim_customer GROUP BY customer_sk HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'dim_product.product_sk unique', count(*)
FROM (SELECT product_sk FROM silver.dim_product GROUP BY product_sk HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'dim_warehouse.warehouse_sk unique', count(*)
FROM (SELECT warehouse_sk FROM silver.dim_warehouse GROUP BY warehouse_sk HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'dim_carrier one row per carrier_id (SCD1)', count(*)
FROM (SELECT carrier_id FROM silver.dim_carrier GROUP BY carrier_id HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'dim_route one row per route_id (SCD1)', count(*)
FROM (SELECT route_id FROM silver.dim_route GROUP BY route_id HAVING count(*) > 1)
UNION ALL
SELECT 'uniqueness', 'dim_date.date_sk unique', count(*)
FROM (SELECT date_sk FROM silver.dim_date GROUP BY date_sk HAVING count(*) > 1)

-- ---------------------------------------------------------------- 2. SCD2 history
-- Exactly one current version per business key; versions never overlap; is_current agrees with
-- effective_to. Two current versions of a warehouse would double every join to it.
UNION ALL
SELECT 'scd2', 'dim_customer exactly one current version', count(*)
FROM (SELECT customer_id FROM silver.dim_customer GROUP BY customer_id HAVING count_if(is_current) <> 1)
UNION ALL
SELECT 'scd2', 'dim_product exactly one current version', count(*)
FROM (SELECT product_id FROM silver.dim_product GROUP BY product_id HAVING count_if(is_current) <> 1)
UNION ALL
SELECT 'scd2', 'dim_warehouse exactly one current version', count(*)
FROM (SELECT warehouse_id FROM silver.dim_warehouse GROUP BY warehouse_id HAVING count_if(is_current) <> 1)
UNION ALL
SELECT 'scd2', 'dim_customer versions do not overlap', count(*)
FROM (SELECT effective_to, lead(effective_from) OVER (PARTITION BY customer_id ORDER BY effective_from) AS next_from
      FROM silver.dim_customer)
WHERE next_from IS NOT NULL AND (effective_to IS NULL OR effective_to > next_from)
UNION ALL
SELECT 'scd2', 'dim_product versions do not overlap', count(*)
FROM (SELECT effective_to, lead(effective_from) OVER (PARTITION BY product_id ORDER BY effective_from) AS next_from
      FROM silver.dim_product)
WHERE next_from IS NOT NULL AND (effective_to IS NULL OR effective_to > next_from)
UNION ALL
SELECT 'scd2', 'dim_warehouse versions do not overlap', count(*)
FROM (SELECT effective_to, lead(effective_from) OVER (PARTITION BY warehouse_id ORDER BY effective_from) AS next_from
      FROM silver.dim_warehouse)
WHERE next_from IS NOT NULL AND (effective_to IS NULL OR effective_to > next_from)
UNION ALL
SELECT 'scd2', 'is_current matches open effective_to', count(*)
FROM (SELECT is_current, effective_to FROM silver.dim_customer
      UNION ALL SELECT is_current, effective_to FROM silver.dim_product
      UNION ALL SELECT is_current, effective_to FROM silver.dim_warehouse)
WHERE is_current <> (effective_to IS NULL)

-- ---------------------------------------------------------------- 3. referential integrity
-- Every surrogate key in a fact must exist in its dimension, or the row silently drops out of
-- the Gold joins (KPIs computed on fewer orders than really exist).
UNION ALL
SELECT 'referential', 'fact_order.customer_sk in dim_customer', count(*)
FROM silver.fact_order f LEFT ANTI JOIN silver.dim_customer d ON f.customer_sk = d.customer_sk
UNION ALL
SELECT 'referential', 'fact_order.product_sk in dim_product', count(*)
FROM silver.fact_order f LEFT ANTI JOIN silver.dim_product d ON f.product_sk = d.product_sk
UNION ALL
SELECT 'referential', 'fact_order.origin_warehouse_sk in dim_warehouse', count(*)
FROM silver.fact_order f LEFT ANTI JOIN silver.dim_warehouse d ON f.origin_warehouse_sk = d.warehouse_sk
WHERE f.origin_warehouse_sk IS NOT NULL
UNION ALL
SELECT 'referential', 'fact_order date keys in dim_date', count(*)
FROM silver.fact_order f
WHERE NOT EXISTS (SELECT 1 FROM silver.dim_date d WHERE d.date_sk = f.order_date_sk)
   OR (f.promised_delivery_date_sk IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM silver.dim_date d WHERE d.date_sk = f.promised_delivery_date_sk))
UNION ALL
SELECT 'referential', 'fact_shipment.carrier_sk in dim_carrier', count(*)
FROM silver.fact_shipment f LEFT ANTI JOIN silver.dim_carrier d ON f.carrier_sk = d.carrier_sk
UNION ALL
SELECT 'referential', 'fact_shipment.route_sk in dim_route', count(*)
FROM silver.fact_shipment f LEFT ANTI JOIN silver.dim_route d ON f.route_sk = d.route_sk
UNION ALL
SELECT 'referential', 'fact_shipment.origin_warehouse_sk in dim_warehouse', count(*)
FROM silver.fact_shipment f LEFT ANTI JOIN silver.dim_warehouse d ON f.origin_warehouse_sk = d.warehouse_sk
WHERE f.origin_warehouse_sk IS NOT NULL
UNION ALL
SELECT 'referential', 'fact_shipment date keys in dim_date', count(*)
FROM silver.fact_shipment f
WHERE NOT EXISTS (SELECT 1 FROM silver.dim_date d WHERE d.date_sk = f.dispatch_date_sk)
   OR NOT EXISTS (SELECT 1 FROM silver.dim_date d WHERE d.date_sk = f.planned_delivery_date_sk)
UNION ALL
SELECT 'referential', 'fact_inventory_snapshot.warehouse_sk in dim_warehouse', count(*)
FROM silver.fact_inventory_snapshot f LEFT ANTI JOIN silver.dim_warehouse d ON f.warehouse_sk = d.warehouse_sk
UNION ALL
SELECT 'referential', 'fact_inventory_snapshot.product_sk in dim_product', count(*)
FROM silver.fact_inventory_snapshot f LEFT ANTI JOIN silver.dim_product d ON f.product_sk = d.product_sk
UNION ALL
SELECT 'referential', 'fact_inventory_snapshot.snapshot_date_sk in dim_date', count(*)
FROM silver.fact_inventory_snapshot f LEFT ANTI JOIN silver.dim_date d ON f.snapshot_date_sk = d.date_sk

-- ---------------------------------------------------------------- 4. business rules
-- Rules the row gate already enforces, re-checked on the stored table (catches logic bugs and
-- MERGE mistakes), plus cross-column consistency the KPIs rely on.
UNION ALL
SELECT 'business_rule', 'fact_order 0 <= shipped_quantity <= ordered_quantity', count(*)
FROM silver.fact_order WHERE shipped_quantity < 0 OR shipped_quantity > ordered_quantity OR ordered_quantity <= 0
UNION ALL
SELECT 'business_rule', 'fact_order total_price = ordered_quantity * unit_price', count(*)
FROM silver.fact_order WHERE abs(total_price - ordered_quantity * unit_price) > 0.01
UNION ALL
SELECT 'business_rule', 'fact_inventory_snapshot stock not negative', count(*)
FROM silver.fact_inventory_snapshot WHERE quantity_on_hand < 0 OR quantity_reserved < 0
UNION ALL
SELECT 'business_rule', 'fact_inventory_snapshot available = on_hand - reserved', count(*)
FROM silver.fact_inventory_snapshot WHERE abs(quantity_available - (quantity_on_hand - quantity_reserved)) > 0.001
UNION ALL
SELECT 'business_rule', 'inventory_current stock not negative', count(*)
FROM silver.inventory_current WHERE NOT coalesce(is_deleted, false) AND quantity_on_hand < 0
UNION ALL
SELECT 'business_rule', 'fact_shipment planned delivery after dispatch', count(*)
FROM silver.fact_shipment WHERE planned_delivery_time < dispatch_time
UNION ALL
SELECT 'business_rule', 'fact_shipment DELIVERED has an actual delivery time', count(*)
FROM silver.fact_shipment WHERE shipment_status = 'DELIVERED' AND actual_delivery_time IS NULL

-- ---------------------------------------------------------------- 5. quarantine rate
-- A normal day quarantines a few percent. Far more means the source itself broke (wrong file,
-- schema change, master data missing): publishing the few rows that passed would be misleading.
-- Feeds with fewer than min_rows_for_rate rows today are skipped (one bad row is not 50 %).
-- The ~5 % early-warning level is DQ tile 01; this is the hard stop.
UNION ALL
SELECT 'quarantine_rate', concat('quarantine rate today <= ', max_quarantine_pct, '% (', p.source_feed, ')'),
       CASE WHEN 100.0 * coalesce(q.n, 0) / p.n > max_quarantine_pct THEN coalesce(q.n, 0) ELSE 0 END
FROM (SELECT source_feed, count(*) AS n FROM (
        SELECT 'orders' AS source_feed, _ingest_timestamp FROM bronze.orders
        UNION ALL SELECT 'orders', _ingest_timestamp FROM bronze.orders_cdc
        UNION ALL SELECT 'inventory', _ingest_timestamp FROM bronze.inventory
        UNION ALL SELECT 'shipments', _ingest_timestamp FROM bronze.shipments)
      WHERE to_date(_ingest_timestamp) = to_date(as_of)
      GROUP BY source_feed HAVING count(*) >= min_rows_for_rate) p
LEFT JOIN (SELECT source_feed, count(*) AS n FROM quarantine.records
           WHERE to_date(ingested_at) = to_date(as_of) GROUP BY source_feed) q
  ON q.source_feed = p.source_feed

-- ---------------------------------------------------------------- 6. freshness
-- The SLA tile shows whether the JOBS ran; these show whether new DATA arrived. A stalled
-- Debezium connector or carrier feed still lets every job "succeed" with nothing new.
-- An empty table fails too (max() is NULL): Silver wrote nothing.
UNION ALL
SELECT 'freshness', concat('fact_order newest source change within ', max_source_age_hours, ' h'),
       CASE WHEN coalesce(max(updated_at) >= as_of - make_interval(0, 0, 0, 0, max_source_age_hours), false) THEN 0 ELSE 1 END
FROM silver.fact_order
UNION ALL
SELECT 'freshness', concat('fact_shipment newest carrier update within ', max_source_age_hours, ' h'),
       CASE WHEN coalesce(max(updated_at) >= as_of - make_interval(0, 0, 0, 0, max_source_age_hours), false) THEN 0 ELSE 1 END
FROM silver.fact_shipment
UNION ALL
-- Tonight's snapshot (file due 02:00 on snapshot_date) must exist, real or carried forward by
-- scm_wms_sla_check at 02:30 (is_stale).
SELECT 'freshness', 'fact_inventory_snapshot has last night',
       CASE WHEN coalesce(max(snapshot_date) >= to_date(as_of), false) THEN 0 ELSE 1 END
FROM silver.fact_inventory_snapshot;

-- Shown in the task output: every check with its count.
SELECT category, check_name, failing_rows FROM silver_quality_results ORDER BY failing_rows DESC, category, check_name;

-- Fail the task (and skip refresh_gold) if anything failed, naming every failing check.
SELECT CASE WHEN count(*) > 0 THEN raise_error(concat(
         'Silver quality tests failed (', count(*), ' checks): ',
         array_join(collect_list(concat(check_name, ' [', failing_rows, ' rows]')), '; ')))
       END AS result
FROM silver_quality_results
WHERE failing_rows > 0;
