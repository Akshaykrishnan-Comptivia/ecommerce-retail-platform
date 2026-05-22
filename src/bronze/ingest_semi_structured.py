"""
Bronze ingestion for semi-structured data (JSON clickstream event logs).

Design:
  - spark.read.json() only (no CSV/TSV delimiters)
  - Preserve nested JSON structure as-is (no flattening in Bronze)
  - Optional _ingested_at metadata column only

Usage (Databricks notebook):
    from src.bronze.ingest_semi_structured import ingest_clickstream

    table = ingest_clickstream(spark, config_path="../config/pipeline_config.yaml")
"""

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def _load_config(config_path: str | None) -> dict:
    if config_path:
        with open(config_path, encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    return {
        "storage": {
            "raw_landing_zone": "/Volumes/ecommerce_catalog/bronze/raw_data",
        },
        "catalog": "ecommerce_catalog",
        "bronze": {
            "schema": "bronze",
            "tables": {"clickstream": "bronze_clickstream_json"},
        },
        "synthetic": {
            "clickstream": {
                "output_subpath": "synthetic/clickstream",
            },
        },
    }


def clickstream_raw_path(config: dict) -> str:
    """Landing-zone path for synthetic clickstream JSON files."""
    landing_zone = config["storage"]["raw_landing_zone"]
    subpath = config["synthetic"]["clickstream"]["output_subpath"]
    return f"{landing_zone}/{subpath}"


def clickstream_bronze_table(config: dict) -> str:
    """Fully qualified Unity Catalog bronze table name for clickstream."""
    return (
        f"{config['catalog']}."
        f"{config['bronze']['schema']}."
        f"{config['bronze']['tables']['clickstream']}"
    )


def ingest_clickstream(
    spark: SparkSession,
    source_path: str | None = None,
    config_path: str | None = None,
    mode: str = "overwrite",
) -> str:
    """
    Ingest raw clickstream JSON from the landing zone into a Bronze Delta table.

    Args:
        spark: Active Spark session.
        source_path: Override path to JSON files (default from config).
        config_path: Path to pipeline_config.yaml.
        mode: Delta write mode (default overwrite for dev).

    Returns:
        Fully qualified bronze table name.
    """
    config = _load_config(config_path)
    source_path = source_path or clickstream_raw_path(config)
    target_table = clickstream_bronze_table(config)

    print("=" * 60)
    print("Bronze Clickstream Ingestion (JSON)")
    print("=" * 60)
    print(f"  Source path : {source_path}")
    print(f"  Target table: {target_table}")

    df = spark.read.json(source_path)
    df = df.withColumn("_ingested_at", current_timestamp())

    row_count = df.count()
    df.write.format("delta").mode(mode).saveAsTable(target_table)

    print(f"  Rows ingested: {row_count:,}")
    print("Bronze clickstream ingestion complete.")
    return target_table
