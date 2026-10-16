-- 03_create_debezium_login.sql - Read-only login for Debezium (least privilege).
-- Replace the placeholder when running; never commit a real password. The same password goes in
-- the DEBEZIUM_SQL_PASSWORD user environment variable, which docker-compose passes to Kafka Connect.

CREATE LOGIN debezium_cdc WITH PASSWORD = '<strong-password>', CHECK_POLICY = ON;
GO
-- Server level: Debezium's streaming phase reads server DMVs (e.g. whether SQL Server Agent runs).
-- SQL Server 2022+ split VIEW SERVER STATE; this read-only part is what it needs.
GRANT VIEW SERVER PERFORMANCE STATE TO debezium_cdc;
GO
USE scm_oms;
GO
CREATE USER debezium_cdc FOR LOGIN debezium_cdc;
ALTER ROLE cdc_reader ADD MEMBER debezium_cdc;     -- read change data through the CDC functions
GRANT SELECT ON SCHEMA::dbo TO debezium_cdc;       -- initial snapshot of the tables
GRANT SELECT ON SCHEMA::cdc TO debezium_cdc;       -- change tables + CDC metadata (cdc.change_tables, lsn_time_mapping)
GRANT VIEW DATABASE STATE TO debezium_cdc;         -- LSN / CDC metadata
GO

-- Check, connected AS debezium_cdc: the SELECTs return counts, the DELETE is denied (read-only)
-- USE scm_oms;
-- SELECT COUNT(*) FROM dbo.order_lines;
-- SELECT COUNT(*) FROM cdc.dbo_order_lines_CT;
-- DELETE FROM dbo.order_lines WHERE 1 = 0;
