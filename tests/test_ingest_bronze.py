"""
test_ingest_bronze.py – Unit tests for the Bronze ingestion module.

Tests schema contracts, configuration builder, Unity Catalog 3-level naming,
pure DataFrame metadata transformations, and Auto Loader stream construction via mocks.

Run with:
    python -m unittest tests.test_ingest_bronze -v
"""

import unittest
from unittest.mock import MagicMock

# Import module under test
from scm_platform.ingest_bronze import (
    DATASET_SCHEMAS,
    IngestionConfig,
    PYSPARK_AVAILABLE,
    add_bronze_metadata,
    build_autoloader_stream,
    get_default_config,
    get_routes_schema,
    get_shipments_schema,
    get_weather_schema,
    start_bronze_stream_write,
)


class TestBronzeSchemas(unittest.TestCase):
    """Validate PySpark StructType schemas match Liquibase and JSONL definitions."""

    def setUp(self):
        if not PYSPARK_AVAILABLE:
            self.skipTest("PySpark is not installed locally.")

    def test_routes_schema_fields(self):
        schema = get_routes_schema()
        field_names = [f.name for f in schema.fields]
        expected_fields = [
            "route_id",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
            "distance_meters",
            "duration_seconds",
            "routing_profile",
            "source",
        ]
        self.assertEqual(field_names, expected_fields)
        # Bronze accepts bad records verbatim; the Silver quality gate rejects them
        for f in schema.fields:
            self.assertTrue(f.nullable)

    def test_weather_schema_fields(self):
        schema = get_weather_schema()
        field_names = [f.name for f in schema.fields]
        expected_fields = [
            "weather_record_id",
            "latitude",
            "longitude",
            "forecast_time_utc",
            "temperature_2m_c",
            "precipitation_mm",
            "wind_speed_10m_kmh",
            "visibility_m",
            "source",
        ]
        self.assertEqual(field_names, expected_fields)
        # Bronze accepts bad records verbatim; the Silver quality gate rejects them
        for f in schema.fields:
            self.assertTrue(f.nullable)

    def test_shipments_schema_fields(self):
        schema = get_shipments_schema()
        field_names = [f.name for f in schema.fields]
        expected_fields = [
            "shipment_id",
            "route_id",
            "carrier_id",
            "dispatch_time_utc",
            "planned_delivery_time_utc",
            "data_type",
            "order_id",
            "warehouse_id",
            "actual_delivery_time_utc",
            "shipment_status",
            "ordered_quantity",
            "shipped_quantity",
            "weather_risk",
            "updated_at",
        ]
        self.assertEqual(field_names, expected_fields)
        for f in schema.fields:
            self.assertTrue(f.nullable)

    def test_dataset_schemas_registry(self):
        self.assertEqual(
            set(DATASET_SCHEMAS.keys()),
            {"routes", "weather", "shipments", "warehouses", "products", "customers", "carriers", "orders", "inventory"},
        )


class TestIngestionConfig(unittest.TestCase):
    """Test configuration paths and naming conventions."""

    def test_default_config_routes(self):
        cfg = get_default_config("routes", storage_account="stscmplatformdev", catalog="scm_platform_dev")
        self.assertEqual(cfg.feed_name, "routes")
        self.assertEqual(cfg.landing_path, "abfss://landing@stscmplatformdev.dfs.core.windows.net/routes/")
        self.assertEqual(cfg.checkpoint_path, "abfss://checkpoints@stscmplatformdev.dfs.core.windows.net/bronze/routes/")
        self.assertEqual(cfg.full_target_table_name, "scm_platform_dev.bronze.routes")

    def test_default_config_weather(self):
        cfg = get_default_config("weather")
        self.assertEqual(cfg.full_target_table_name, "scm_platform_dev.bronze.weather")
        self.assertIn("weather", cfg.landing_path)

    def test_default_config_shipments(self):
        cfg = get_default_config("shipments")
        self.assertEqual(cfg.full_target_table_name, "scm_platform_dev.bronze.shipments")
        self.assertIn("shipments", cfg.landing_path)

    def test_unknown_feed_raises_value_error(self):
        with self.assertRaises(ValueError):
            get_default_config("unknown_feed")


class TestStreamConstructionWithMock(unittest.TestCase):
    """Verify Auto Loader and Delta Lake streaming invocations using mocks."""

    def test_build_autoloader_stream(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.readStream.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.schema.return_value = mock_reader

        mock_raw_df = MagicMock()
        mock_reader.load.return_value = mock_raw_df
        mock_raw_df.withColumn.return_value = mock_raw_df

        cfg = IngestionConfig(
            feed_name="routes",
            landing_path="abfss://landing@sa/routes/",
            checkpoint_path="abfss://cp@sa/routes/",
            target_table="routes",
        )
        mock_schema = MagicMock()

        result_df = build_autoloader_stream(mock_spark, cfg, mock_schema)

        # Verify Auto Loader format requested
        mock_spark.readStream.format.assert_called_with("cloudFiles")
        # Verify format=json
        mock_reader.option.assert_any_call("cloudFiles.format", "json")
        # Verify schemaLocation
        mock_reader.option.assert_any_call("cloudFiles.schemaLocation", "abfss://cp@sa/routes//schema")
        # Verify rescue column configured
        mock_reader.option.assert_any_call("cloudFiles.schemaRescueDataColumn", "_rescued_data")
        # Verify loaded from landing path
        mock_reader.load.assert_called_with("abfss://landing@sa/routes/")

    def test_start_bronze_stream_write(self):
        mock_stream_df = MagicMock()
        mock_writer = MagicMock()
        mock_stream_df.writeStream.format.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer

        cfg = IngestionConfig(
            feed_name="shipments",
            landing_path="abfss://landing@sa/shipments/",
            checkpoint_path="abfss://cp@sa/shipments/",
            target_table="shipments",
            catalog="scm_dev",
            schema="bronze",
        )

        start_bronze_stream_write(mock_stream_df, cfg, trigger_available_now=True)

        # Verify Delta Lake format
        mock_stream_df.writeStream.format.assert_called_with("delta")
        # Verify append mode (no overwrites)
        mock_writer.outputMode.assert_called_with("append")
        # Verify checkpointLocation configured
        mock_writer.option.assert_called_with("checkpointLocation", "abfss://cp@sa/shipments//data")
        # Verify cost-effective availableNow trigger
        mock_writer.trigger.assert_called_with(availableNow=True)
        # Verify written to Unity Catalog 3-level table
        mock_writer.toTable.assert_called_with("scm_dev.bronze.shipments")


class TestMainEntrypoint(unittest.TestCase):
    """Verify CLI argument parsing and execution dispatch in ingest_bronze.main."""

    from unittest.mock import patch

    @patch("sys.argv", ["ingest_bronze.py", "--storage-account", "test_sa", "--catalog", "test_cat", "--feed", "routes"])
    @patch("scm_platform.ingest_bronze.ingest_feed")
    @patch("scm_platform.ingest_bronze.SparkSession")
    def test_main_single_feed(self, mock_spark_cls, mock_ingest_feed):
        from scm_platform.ingest_bronze import main
        mock_spark = MagicMock()
        mock_spark_cls.builder.getOrCreate.return_value = mock_spark

        main()

        mock_ingest_feed.assert_called_once()
        call_args = mock_ingest_feed.call_args
        self.assertEqual(call_args[0][0], mock_spark)
        self.assertEqual(call_args[0][1], "routes")
        config = call_args[1]["config"]
        self.assertEqual(config.catalog, "test_cat")
        self.assertIn("test_sa", config.landing_path)

    @patch("sys.argv", ["ingest_bronze.py", "--feed", "all"])
    @patch("scm_platform.ingest_bronze.ingest_all")
    @patch("scm_platform.ingest_bronze.SparkSession")
    def test_main_all_feeds(self, mock_spark_cls, mock_ingest_all):
        from scm_platform.ingest_bronze import main
        mock_spark = MagicMock()
        mock_spark_cls.builder.getOrCreate.return_value = mock_spark

        main()

        mock_ingest_all.assert_called_once_with(
            mock_spark, storage_account="stscmplatformdev", catalog="scm_platform_dev", batch_id="manual"
        )

    def test_bronze_metadata_adds_all_audit_columns(self):
        """_ingest_timestamp, _source_file (from _metadata.file_path) and the job run id as _batch_id."""
        df = MagicMock()
        df.withColumn.return_value = df
        add_bronze_metadata(df, batch_id="run-123")
        names = [c.args[0] for c in df.withColumn.call_args_list]
        self.assertEqual(names, ["_ingest_timestamp", "_source_file", "_batch_id"])


if __name__ == "__main__":
    unittest.main()

