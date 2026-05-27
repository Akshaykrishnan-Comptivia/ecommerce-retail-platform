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
                    "tables": {
                        "olist_orders_dataset": "bronze_orders_csv",
                        "olist_order_items_dataset": "bronze_order_items_csv",
                        "olist_products_dataset": "bronze_products_csv",
                        "olist_customers_dataset": "bronze_customers_csv",
                        "olist_sellers_dataset": "bronze_sellers_csv",
                        "olist_order_reviews_dataset": "bronze_reviews_csv",
                        "olist_geolocation_dataset": "bronze_geolocation_csv",
                        "olist_order_payments_dataset": "bronze_order_payments_csv",
                        "product_category_name_translation": "bronze_product_category_translation_csv",
                    },
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
    config = _load_config(config_path)
    olist_cfg = config["bronze"]["public"]["olist"]
    base_path = _landing_path(config, olist_cfg["landing_subpath"])
    table_map = olist_cfg["tables"]

    dbutils = _get_dbutils(spark)
    ingested: dict[str, str] = {}

    print("Bronze Olist ingestion")
    print(f"  Source base: {base_path}")

    for folder in dbutils.fs.ls(base_path):
        if not folder.isDir():
            continue
        folder_name = folder.name.rstrip("/")
        if folder_name not in table_map:
            print(f"  -> SKIP (no table mapping): {folder_name}")
            continue
        csv_path = None
        for entry in dbutils.fs.ls(folder.path):
            if entry.name.endswith(".csv"):
                csv_path = entry.path
                break
        if not csv_path:
            print(f"  -> SKIP (no CSV): {folder_name}")
            continue

        table_name = table_map[folder_name]
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
    print("=" * 60)
    print("Bronze Public CSV Ingestion")
    print("=" * 60)

    results = {
        "olist": ingest_olist_bronze(spark, config_path=config_path, mode=mode),
        "uci": ingest_uci_bronze(spark, config_path=config_path, mode=mode),
    }

    print("Bronze public CSV ingestion complete.")
    return results
