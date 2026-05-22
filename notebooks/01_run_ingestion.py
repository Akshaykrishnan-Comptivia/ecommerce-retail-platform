# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Retail Platform — Run Ingestion
# MAGIC Downloads public e-commerce datasets into the Bronze landing zone.
# MAGIC
# MAGIC **TRAINEE GUIDE - Run this notebook AFTER `00_setup_catalog.py`.**
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Unity Catalog + Volume created (`ecommerce_catalog.bronze.raw_data`)
# MAGIC 2. Cluster has outbound internet access
# MAGIC 3. (Optional) Databricks secret scope `kaggle` with keys `username` and `key` for Olist
# MAGIC
# MAGIC **Sources downloaded:**
# MAGIC - Brazilian E-Commerce by Olist (Kaggle)
# MAGIC - Online Retail II (UCI)
# MAGIC - Amazon Customer Reviews 2023 (Hugging Face)

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import os

# TRAINEE NOTE - Kaggle authentication is required only for the Olist source.
# Create a secret scope named "kaggle" with keys "username" and "key".
# UCI and Amazon sources work without Kaggle creds.
KAGGLE_SECRET_SCOPE = "kaggle"
SETUP_KAGGLE = True

if SETUP_KAGGLE:
    try:
        os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get(
            scope=KAGGLE_SECRET_SCOPE, key="username"
        )
        os.environ["KAGGLE_KEY"] = dbutils.secrets.get(
            scope=KAGGLE_SECRET_SCOPE, key="key"
        )
        print("Kaggle credentials configured (environment variables).")
    except Exception as e:
        print(
            "WARNING: Kaggle credentials not configured - "
            "olist_brazilian_ecommerce will fail."
        )
        print(f"  Reason: {e}")
        print(
            "  Fix: create secret scope 'kaggle' with keys 'username' and 'key', "
            "or set SETUP_KAGGLE = False to skip."
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

downloader = PublicDataDownloader(spark, config_path=config_path)
downloader.download_all()

# COMMAND ----------

landing_zone = "/Volumes/ecommerce_catalog/bronze/raw_data"
print(f"Landing zone contents: {landing_zone}")
display(dbutils.fs.ls(landing_zone))

print("\nIngestion download step complete!")
print("Next step: Run Bronze ingestion notebooks/modules to load raw files into Delta tables.")
