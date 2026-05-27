# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os
KAGGLE_USERNAME = 'blessey.maria@comptivia.com'
KAGGLE_KEY = 'KGAT_fb2ae310e5115a870b3cbfdeb3abc5b8'
SETUP_KAGGLE = True
if SETUP_KAGGLE:
    if KAGGLE_USERNAME and KAGGLE_KEY:
        os.environ['KAGGLE_USERNAME'] = KAGGLE_USERNAME
        os.environ['KAGGLE_KEY'] = KAGGLE_KEY
        print('Kaggle credentials configured (environment variables).')
    else:
        print('WARNING: Kaggle credentials not configured - olist_brazilian_ecommerce will fail.')
        print('  Fix: set KAGGLE_USERNAME and KAGGLE_KEY in this cell, or set SETUP_KAGGLE = False to skip Olist only.')
# COMMAND ----------
import os
config_path = os.path.normpath(os.path.join('..', 'config', 'pipeline_config.yaml'))
if not os.path.exists(config_path):
    print(f'Config not found at {config_path} - using built-in defaults.')
    config_path = None
else:
    print(f'Using config: {config_path}')
RUN_PUBLIC_DOWNLOADS = True
if RUN_PUBLIC_DOWNLOADS:
    from src.ingestion.download_public_data import PublicDataDownloader
    downloader = PublicDataDownloader(spark, config_path=config_path)
    downloader.download_all()
# COMMAND ----------
if RUN_PUBLIC_DOWNLOADS:
    from src.bronze.ingest_structured import ingest_public_csv_sources
    from src.bronze.ingest_semi_structured import ingest_amazon_reviews
    bronze_public_tables = ingest_public_csv_sources(spark, config_path=config_path)
    amazon_bronze_table = ingest_amazon_reviews(spark, config_path=config_path)
    bronze_public_tables['amazon'] = {'all_beauty': amazon_bronze_table}
    print(f'Amazon Bronze table: {amazon_bronze_table}')
# COMMAND ----------
from src.bronze.ingest_semi_structured import ingest_clickstream
from src.ingestion.generate_synthetic import SyntheticDataGenerator
generator = SyntheticDataGenerator(spark, config_path=config_path)
raw_path = generator.write_clickstream_raw()
bronze_clickstream_table = ingest_clickstream(spark, source_path=raw_path, config_path=config_path)
print(f'Bronze clickstream table ready: {bronze_clickstream_table}')
# COMMAND ----------
landing_zone = '/Volumes/ecommerce_catalog/bronze/raw_data'
print(f'Landing zone contents: {landing_zone}')
display(dbutils.fs.ls(landing_zone))
clickstream_raw = f'{landing_zone}/synthetic/clickstream'
print(f'\nClickstream raw JSON: {clickstream_raw}')
display(dbutils.fs.ls(clickstream_raw))
print(f'\nBronze clickstream row count:')
display(spark.table(bronze_clickstream_table).limit(10))
print(spark.table(bronze_clickstream_table).count())
if RUN_PUBLIC_DOWNLOADS:
    print('\nPublic Bronze tables (sample):')
    display(spark.table('ecommerce_catalog.bronze.bronze_orders_csv').limit(5))
    display(spark.table('ecommerce_catalog.bronze.bronze_uci_retail_2009_2010').limit(5))
    display(spark.table('ecommerce_catalog.bronze.bronze_amazon_reviews_tsv').limit(5))
print('\nIngestion complete!')
print('Next step: Run 02_run_silver.py to transform clickstream (sessionization).')
