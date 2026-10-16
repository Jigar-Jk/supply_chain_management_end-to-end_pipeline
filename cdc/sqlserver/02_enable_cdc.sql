-- 02_enable_cdc.sql - Enable change data capture on scm_oms. Needs SQL Server Agent running
-- (its capture job reads the transaction log into the cdc.*_CT change tables).
--
-- "Update mask evaluation will be disabled ... CLR configuration option is disabled" is harmless:
-- it only affects the optional net-changes update mask, which Debezium does not use.

USE scm_oms;
GO
EXEC sys.sp_cdc_enable_db;
GO
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo', @source_name = N'order_lines',
     @role_name = N'cdc_reader', @supports_net_changes = 1;   -- cdc_reader = gating role for change data
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo', @source_name = N'inventory',
     @role_name = N'cdc_reader', @supports_net_changes = 1;
GO

-- Keep changes 7 days (default 3), matching Kafka retention: an outage of up to 7 days
-- (Debezium or Kafka down) recovers with no data loss.
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;
GO

-- Check: is_cdc_enabled = 1, both tables tracked, capture + cleanup jobs listed
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'scm_oms';
SELECT name, is_tracked_by_cdc FROM sys.tables;
EXEC sys.sp_cdc_help_jobs;
