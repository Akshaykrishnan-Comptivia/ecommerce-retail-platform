# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Retail Platform — Run Ingestion
# MAGIC Downloads public e-commerce datasets into the Bronze landing zone.
# MAGIC
# MAGIC **TRAINEE GUIDE — Run this notebook AFTER `00_setup_catalog.py`.**
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Unity Catalog + Volume created (`ecommerce_catalog.bronze.raw_data`)
# MAGIC 2. Cluster has outbound internet access
# MAGIC 3. (Optional) Databricks secret scope `kaggle` with keys `username` and `key` for Olist
# MAGIC
# MAGIC **Public sources downloaded (optional):**
# MAGIC - Brazilian E-Commerce by Olist (Kaggle)
# MAGIC - Online Retail II (UCI)
# MAGIC - Amazon Customer Reviews 2023 (Hugging Face)
# MAGIC
# MAGIC **Synthetic clickstream (Faker):**
# MAGIC - Generate JSON → landing zone → `ecommerce_catalog.bronze.bronze_clickstream_json`

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import os

# TRAINEE NOTE — Kaggle authentication is required only for the Olist source.
# Create a secret scope named "kaggle" with keys "username" and "key", then
# uncomment the block below. UCI and Amazon sources work without Kaggle creds.
KAGGLE_SECRET_SCOPE = "kaggle"
SETUP_KAGGLE = True

if SETUP_KAGGLE:
    try:
        kaggle_dir = "/root/.kaggle"
        os.makedirs(kaggle_dir, exist_ok=True)
        kaggle_config = {
            "username": dbutils.secrets.get(scope=KAGGLE_SECRET_SCOPE, key="username"),
            "key": dbutils.secrets.get(scope=KAGGLE_SECRET_SCOPE, key="key"),
        }
        kaggle_path = os.path.join(kaggle_dir, "kaggle.json")
        with open(kaggle_path, "w") as f:
            json.dump(kaggle_config, f)
        os.chmod(kaggle_path, 0o600)
        print("Kaggle credentials configured.")
    except Exception as e:
        print(
            "WARNING: Kaggle credentials not configured — "
            "olist_brazilian_ecommerce will fail."
        )
        print(f"  Reason: {e}")
        print(
            "  Fix: create secret scope 'kaggle' with keys 'username' and 'key', "
            "or set SETUP_KAGGLE = False to skip."
        )

# COMMAND ----------

import os

# Relative to notebooks/ when running from a Databricks Repo checkout.
config_path = os.path.normpath(os.path.join("..", "config", "pipeline_config.yaml"))
if not os.path.exists(config_path):
    print(f"Config not found at {config_path} — using built-in defaults.")
    config_path = None
else:
    print(f"Using config: {config_path}")

# Set False to skip public downloads and run synthetic clickstream only.
RUN_PUBLIC_DOWNLOADS = True

if RUN_PUBLIC_DOWNLOADS:
    from src.ingestion.download_public_data import PublicDataDownloader

    downloader = PublicDataDownloader(spark, config_path=config_path)
    downloader.download_all()

# COMMAND ----------

from src.bronze.ingest_semi_structured import ingest_clickstream
from src.ingestion.generate_synthetic import SyntheticDataGenerator

generator = SyntheticDataGenerator(spark, config_path=config_path)
raw_path = generator.write_clickstream_raw()

bronze_clickstream_table = ingest_clickstream(
    spark,
    source_path=raw_path,
    config_path=config_path,
)

print(f"Bronze clickstream table ready: {bronze_clickstream_table}")

# COMMAND ----------

landing_zone = "/Volumes/ecommerce_catalog/bronze/raw_data"
print(f"Landing zone contents: {landing_zone}")
display(dbutils.fs.ls(landing_zone))

clickstream_raw = f"{landing_zone}/synthetic/clickstream"
print(f"\nClickstream raw JSON: {clickstream_raw}")
display(dbutils.fs.ls(clickstream_raw))

print(f"\nBronze clickstream row count:")
display(spark.table(bronze_clickstream_table).limit(10))
print(spark.table(bronze_clickstream_table).count())

print("\nIngestion complete!")
print("Next step: Run 02_run_silver.py to transform clickstream (sessionization).")
