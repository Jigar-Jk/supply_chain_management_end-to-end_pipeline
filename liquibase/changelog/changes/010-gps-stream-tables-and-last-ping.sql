-- liquibase formatted sql

-- ============================================================================
-- Changeset: 010-gps-stream-tables-and-last-ping
-- Description: Streaming ingestion pattern (GPS/RFID via Kafka / Azure Event Hubs):
--              1. bronze.gps_events      parseable events, append-only, deduplicated on event_id
--              2. bronze.gps_events_dlq  unparseable payloads, kept verbatim (dead letter)
--              3. silver.fact_shipment   latest truck position per shipment for delay risk
-- The streaming job (scm_platform/stream_gps_events.py) appends to tables 1 and 2; their
-- columns match its output exactly.
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:bronze-gps-events-v1
-- comment: Parseable GPS/RFID events as received, plus Kafka coordinates and audit columns
CREATE TABLE IF NOT EXISTS bronze.gps_events (
    event_id STRING COMMENT 'Producer-assigned unique event id; Bronze dedup key',
    event_type STRING COMMENT 'GPS (truck ping) or RFID (pallet scan)',
    truck_id STRING COMMENT 'Truck identifier (nullable: bad events are quarantined in Silver)',
    pallet_id STRING COMMENT 'Pallet scanned (RFID events only)',
    shipment_id STRING COMMENT 'Shipment the truck is carrying',
    lat DOUBLE COMMENT 'Latitude as received (not range-checked in Bronze)',
    lon DOUBLE COMMENT 'Longitude as received',
    speed_kmh DOUBLE COMMENT 'Speed as received',
    ts STRING COMMENT 'Event time as sent by the device (ISO 8601 UTC)',
    kafka_topic STRING COMMENT 'Source topic / Event Hub',
    kafka_partition INT COMMENT 'Source partition',
    kafka_offset BIGINT COMMENT 'Source offset (with partition: exact replay position)',
    kafka_timestamp TIMESTAMP COMMENT 'Broker append time; watermark column for dedup',
    event_ts TIMESTAMP COMMENT 'Parsed event time (NULL if ts is unparseable)',
    _ingest_timestamp TIMESTAMP COMMENT 'When the event was written to Bronze',
    _source_system STRING COMMENT 'gps_stream'
)
USING DELTA
COMMENT 'Bronze GPS/RFID telemetry from the streaming feed (append-only)';

-- changeset scm-platform-team:bronze-gps-events-dlq-v1
-- comment: Dead letter for payloads that are not JSON objects; nothing is dropped silently
CREATE TABLE IF NOT EXISTS bronze.gps_events_dlq (
    payload STRING COMMENT 'Message value exactly as received',
    kafka_topic STRING COMMENT 'Source topic / Event Hub',
    kafka_partition INT COMMENT 'Source partition',
    kafka_offset BIGINT COMMENT 'Source offset',
    kafka_timestamp TIMESTAMP COMMENT 'Broker append time',
    dlq_reason STRING COMMENT 'Why the message was dead-lettered, e.g. UNPARSEABLE_JSON',
    _ingest_timestamp TIMESTAMP COMMENT 'When the message was dead-lettered',
    _source_system STRING COMMENT 'gps_stream'
)
USING DELTA
COMMENT 'Dead-letter table for unparseable GPS/RFID messages';

-- changeset scm-platform-team:fact-shipment-last-ping-v3
-- comment: Latest truck position from clean GPS events; drives the delay-risk GPS staleness term
ALTER TABLE silver.fact_shipment ADD COLUMNS (
    truck_id STRING COMMENT 'Truck carrying the shipment, from its latest GPS/RFID event',
    last_lat DOUBLE COMMENT 'Latitude of the latest clean ping',
    last_lon DOUBLE COMMENT 'Longitude of the latest clean ping',
    last_ping_ts TIMESTAMP COMMENT 'Event time of the latest clean ping; NULL if none received'
);
