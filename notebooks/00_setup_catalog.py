# Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce_catalog
# MAGIC MANAGED LOCATION 's3://databricks-storage-7474650621272587/unity-catalog/7474650621272587';

# COMMAND ----------

spark.sql("USE CATALOG ecommerce_catalog")
print("Using catalog: ecommerce_catalog")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce_catalog.bronze
# MAGIC COMMENT 'Raw landing zone - no transformations applied';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce_catalog.silver
# MAGIC COMMENT 'Cleansed, typed, deduplicated data with DQ flags';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce_catalog.gold
# MAGIC COMMENT 'Normalized star schema dimensions, facts, and data mart';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS ecommerce_catalog.bronze.raw_data
# MAGIC COMMENT 'Raw landing-zone files for synthetic and public source data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS ecommerce_catalog.bronze.checkpoints
# MAGIC COMMENT 'Checkpoint files for Bronze, Silver, and Gold processing';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN ecommerce_catalog;

# COMMAND ----------

spark.sql("USE CATALOG ecommerce_catalog")
schemas = spark.sql("SHOW SCHEMAS").collect()
print("Schemas created:")
for s in schemas:
    print(f"  - {s.databaseName}")

print("\nUnity Catalog setup complete!")
print("Next step: Run 01_run_ingestion.py to generate and ingest source data.")
