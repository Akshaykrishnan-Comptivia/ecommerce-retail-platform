# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

# COMMAND ----------

KAGGLE_USERNAME = "blessey.maria@comptivia.com"  # e.g. "your_kaggle_username" (not your email)
KAGGLE_KEY = "KGAT_fb2ae310e5115a870b3cbfdeb3abc5b8"  # e.g. "KGAT_..." from kaggle.com/settings
SETUP_KAGGLE = True

if SETUP_KAGGLE:
    if KAGGLE_USERNAME and KAGGLE_KEY:
        os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
        os.environ["KAGGLE_KEY"] = KAGGLE_KEY
        print("Kaggle credentials configured (environment variables).")
    else:
        print(
            "WARNING: Kaggle credentials not configured - "
            "olist_brazilian_ecommerce will fail."
        )
        print(
            "  Fix: set KAGGLE_USERNAME and KAGGLE_KEY in this cell, "
            "or set SETUP_KAGGLE = False to skip Olist only."
        )

# COMMAND ----------

import os

from src.ingestion.download_public_data import PublicDataDownloader

# Relative to notebooks/ when running from a Databricks Repo checkout.
config_path = os.path.normpath(os.path.join("..", "config", "pipeline_config.yaml"))
if not os.path.exists(config_path):
    print(f"Config not found at {config_path} - using built-in defaults.")
    config_path = None
else:
    print(f"Using config: {config_path}")

RUN_PUBLIC_DOWNLOADS = True

downloader = PublicDataDownloader(spark, config_path=config_path)
downloader.download_all()

# COMMAND ----------

if RUN_PUBLIC_DOWNLOADS:
    from src.bronze.ingest_structured import ingest_public_csv_sources
    from src.bronze.ingest_semi_structured import ingest_amazon_reviews

    bronze_public_tables = ingest_public_csv_sources(spark, config_path=config_path)
    amazon_bronze_table = ingest_amazon_reviews(spark, config_path=config_path)
    bronze_public_tables["amazon"] = {"all_beauty": amazon_bronze_table}
    print(f"Amazon Bronze table: {amazon_bronze_table}")

# COMMAND ----------

landing_zone = "/Volumes/ecommerce_catalog/bronze/raw_data"
print(f"Landing zone contents: {landing_zone}")
try:
    display(dbutils.fs.ls(landing_zone))
except Exception as e:
    print(f"Could not list volume contents: {e}")
    print("Ensure you have READ VOLUME permission on ecommerce_catalog.bronze.raw_data.")

if RUN_PUBLIC_DOWNLOADS:
    print("\nPublic Bronze tables (sample):")
    display(spark.table("ecommerce_catalog.bronze.bronze_orders_csv").limit(5))
    display(spark.table("ecommerce_catalog.bronze.bronze_uci_retail_2009_2010").limit(5))
    display(spark.table("ecommerce_catalog.bronze.bronze_amazon_reviews_tsv").limit(5))

print("\nIngestion download step complete!")
print("Next step: Run Bronze ingestion notebooks/modules to load raw files into Delta tables.")
