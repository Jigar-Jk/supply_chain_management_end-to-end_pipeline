-- 01_create_database.sql - SQL Server OMS source for CDC
-- Run as an admin on the DEFAULT instance (MSSQLSERVER).
--
-- Primary keys: Debezium uses them as the Kafka message key, so every change to one row lands
-- on the same partition and stays in order. No CHECK constraints on purpose: real sources hold
-- bad values, and the Silver quality gate must catch them after CDC.

CREATE DATABASE scm_oms;
GO
ALTER DATABASE scm_oms SET RECOVERY SIMPLE;   -- local dev; CDC works in SIMPLE recovery
GO
USE scm_oms;
GO

CREATE TABLE dbo.order_lines (
    order_id               VARCHAR(20)   NOT NULL,
    order_line_number      INT           NOT NULL,
    customer_id            VARCHAR(20)   NULL,
    product_id             VARCHAR(20)   NULL,
    warehouse_id           VARCHAR(20)   NULL,
    order_date             DATE          NULL,
    promised_delivery_date DATE          NULL,
    ordered_quantity       INT           NULL,
    shipped_quantity       INT           NULL,
    unit_price             DECIMAL(12,2) NULL,
    order_status           VARCHAR(20)   NULL,
    updated_at             DATETIME2(3)  NOT NULL CONSTRAINT df_order_lines_updated DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_order_lines PRIMARY KEY (order_id, order_line_number)
);

CREATE TABLE dbo.inventory (
    warehouse_id      VARCHAR(20)  NOT NULL,
    product_id        VARCHAR(20)  NOT NULL,
    quantity_on_hand  INT          NULL,
    quantity_reserved INT          NULL,
    safety_stock      INT          NULL,
    reorder_point     INT          NULL,
    updated_at        DATETIME2(3) NOT NULL CONSTRAINT df_inventory_updated DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_inventory PRIMARY KEY (warehouse_id, product_id)
);
GO
