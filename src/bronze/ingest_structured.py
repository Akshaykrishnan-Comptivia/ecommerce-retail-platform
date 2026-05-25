"""
Bronze ingestion for structured public datasets (CSV in Unity Catalog Volumes).

Loads Olist (Kaggle) and UCI Online Retail II from the landing zone into
Delta tables under ecommerce_catalog.bronze.

Usage (Databricks notebook):
    from src.bronze.ingest_structured import ingest_public_csv_sources

    tables = ingest_public_csv_sources(spark, config_path="../config/pipeline_config.yaml")
"""

from __future__ import annotations

import re

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp


def _load_config(config_path: str | None) -> dict:
    if config_path:
        with open(config_path, encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    return {
        "storage": {"raw_landing_zone": "/Volumes/ecommerce_catalog/bronze/raw_data"},
        "catalog": "ecommerce_catalog",
        "bronze": {
            "schema": "bronze",
            "public": {
                "olist": {
                    "landing_subpath": "marketplace/olist_brazilian_ecommerce",
                    "table_prefix": "bronze_",
                },
                "uci": {
                    "landing_subpath": "retail/uci_online_retail_ii",
                    "tables": {
                        "year_2009_2010": "bronze_uci_retail_2009_2010",
                        "year_2010_2011": "bronze_uci_retail_2010_2011",
                    },
                    "paths": {
                        "year_2009_2010": "Year_2009_2010/Year_2009_2010.csv",
                        "year_2010_2011": "Year_2010_2011/Year_2010_2011.csv",
                    },
                },
            },
        },
    }


def _get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except Exception:
        pass
    try:
        import IPython

        ip = IPython.get_ipython()
        if ip is not None and "dbutils" in ip.user_ns:
            return ip.user_ns["dbutils"]
    except Exception:
        pass
    raise RuntimeError(
        "dbutils is not available. Run bronze ingestion on Databricks with an active spark session."
    )


def _qualified_table(config: dict, table_name: str) -> str:
    return f"{config['catalog']}.{config['bronze']['schema']}.{table_name}"


def _landing_path(config: dict, subpath: str) -> str:
    base = config["storage"]["raw_landing_zone"].rstrip("/")
    return f"{base}/{subpath}"


def _sanitize_column_names(df: DataFrame) -> DataFrame:
    """Rename columns so Delta accepts them (e.g. UCI 'Customer ID' -> 'Customer_ID')."""
    for old_name in df.columns:
        new_name = re.sub(r"[,;{}()\n\t= ]+", "_", old_name).strip("_")
        if not new_name:
            new_name = "unnamed_column"
        if new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def ingest_csv_to_bronze(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    mode: str = "overwrite",
) -> int:
    """Read a single CSV path from a Volume and write to a Bronze Delta table."""
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
    )
    df = _sanitize_column_names(df)
    df = df.withColumn("_ingested_at", current_timestamp())

    row_count = df.count()
    df.write.format("delta").mode(mode).saveAsTable(target_table)
    return row_count


def ingest_olist_bronze(
    spark: SparkSession,
    config_path: str | None = None,
    mode: str = "overwrite",
) -> dict[str, str]:
    """Ingest all Olist CSV files from the landing zone into Bronze tables."""
    config = _load_config(config_path)
    olist_cfg = config["bronze"]["public"]["olist"]
    base_path = _landing_path(config, olist_cfg["landing_subpath"])
    prefix = olist_cfg.get("table_prefix", "bronze_")

    dbutils = _get_dbutils(spark)
    ingested: dict[str, str] = {}

    print("Bronze Olist ingestion")
    print(f"  Source base: {base_path}")

    for folder in dbutils.fs.ls(base_path):
        if not folder.isDir():
            continue
        folder_name = folder.name.rstrip("/")
        csv_path = None
        for entry in dbutils.fs.ls(folder.path):
            if entry.name.endswith(".csv"):
                csv_path = entry.path
                break
        if not csv_path:
            print(f"  -> SKIP (no CSV): {folder_name}")
            continue

        table_name = f"{prefix}{folder_name}"
        target = _qualified_table(config, table_name)
        try:
            rows = ingest_csv_to_bronze(spark, csv_path, target, mode=mode)
            print(f"  -> {table_name}: {rows:,} rows")
            ingested[folder_name] = target
        except Exception as exc:
            print(f"  -> FAILED {folder_name}: {exc}")

    return ingested


def ingest_uci_bronze(
    spark: SparkSession,
    config_path: str | None = None,
    mode: str = "overwrite",
) -> dict[str, str]:
    """Ingest UCI Online Retail II sheet CSVs into Bronze tables."""
    config = _load_config(config_path)
    uci_cfg = config["bronze"]["public"]["uci"]
    base_path = _landing_path(config, uci_cfg["landing_subpath"])
    table_map = uci_cfg["tables"]
    path_map = uci_cfg["paths"]

    ingested: dict[str, str] = {}

    print("Bronze UCI Online Retail II ingestion")
    print(f"  Source base: {base_path}")

    for key, table_short in table_map.items():
        relative = path_map[key]
        source_path = f"{base_path}/{relative}"
        target = _qualified_table(config, table_short)
        try:
            rows = ingest_csv_to_bronze(spark, source_path, target, mode=mode)
            print(f"  -> {table_short}: {rows:,} rows")
            ingested[key] = target
        except Exception as exc:
            print(f"  -> FAILED {table_short}: {exc}")

    return ingested


def ingest_public_csv_sources(
    spark: SparkSession,
    config_path: str | None = None,
    mode: str = "overwrite",
) -> dict[str, dict[str, str]]:
    """
    Ingest Olist and UCI public CSV datasets into Bronze Delta tables.

    Returns:
        {"olist": {dataset_folder: qualified_table}, "uci": {sheet_key: qualified_table}}
    """
    print("=" * 60)
    print("Bronze Public CSV Ingestion")
    print("=" * 60)

    results = {
        "olist": ingest_olist_bronze(spark, config_path=config_path, mode=mode),
        "uci": ingest_uci_bronze(spark, config_path=config_path, mode=mode),
    }

    print("Bronze public CSV ingestion complete.")
    return results
