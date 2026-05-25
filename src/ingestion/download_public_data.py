"""
Download public e-commerce datasets from Kaggle, UCI, and Amazon Reviews.
Stores raw files in the landing zone for Bronze layer ingestion.

TRAINEE GUIDE — Why download public data?
Real customer and transaction data is highly sensitive (PII, payment details).
This platform supplements synthetic data with REAL publicly available datasets:

1. Brazilian E-Commerce by Olist (Kaggle):
   - Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
   - What: 100K+ orders from a Brazilian marketplace (orders, customers, products, payments)
   - Format: Multiple CSV files (downloaded via Kaggle API)
   - Why useful: Real multi-table relational e-commerce schema with order lifecycle,
     payment types, and geographic customer data

2. Online Retail II (UCI Machine Learning Repository):
   - Source: https://archive.ics.uci.edu/dataset/502/online+retail+ii
   - What: 1M+ UK online retail transactions (2010–2011)
   - Format: XLSX inside a ZIP archive
   - Why useful: Classic retail transaction data with returns, quantities, and
     customer IDs for RFM and basket analysis

3. Amazon Customer Reviews 2023:
   - Source: https://amazon-reviews-2023.github.io/
   - What: 571M+ product reviews with ratings, text, and metadata
   - Format: JSONL (one JSON object per line, hosted on Hugging Face)
   - Why useful: Rich review text and ratings for sentiment analysis and
     recommendation system training

Data flow:
  Kaggle / UCI / Hugging Face → download → raw file → landing zone path
  Landing zone → Bronze ingestion (ingest_structured.py or ingest_semi_structured.py)

Usage (Databricks notebook):
    from src.ingestion.download_public_data import PublicDataDownloader
    downloader = PublicDataDownloader(spark, config)
    downloader.download_all()
"""

import os
import subprocess
import zipfile

import requests
import yaml
from pyspark.sql import SparkSession


class PublicDataDownloader:
    """Downloads public e-commerce datasets to raw landing zone.

    TRAINEE NOTE — Why a class with a config dict?
    The URLs and paths for public data sources can change (APIs are versioned,
    bulk files move). By putting all source definitions in a config dictionary
    (loaded from YAML or defaulting to hard-coded values), we can update source
    locations without changing any logic code.

    Config structure:
      storage.raw_landing_zone : Where to write downloaded files
      data_sources             : Dictionary of source_name → {url, format, domain}
    """

    AMAZON_REVIEWS_BASE_URL = (
        "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023"
        "/resolve/main/raw/review_categories/{category}.jsonl"
    )

    def __init__(self, spark: SparkSession, config_path: str = None):
        self.spark = spark
        if config_path:
            with open(config_path, "r") as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = self._default_config()
        self.landing_zone = self.config["storage"]["raw_landing_zone"]

    def _get_dbutils(self):
        """Return Databricks dbutils (required to stage local files into Volumes)."""
        try:
            from pyspark.dbutils import DBUtils

            return DBUtils(self.spark)
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
            "dbutils is not available. Run this downloader on Databricks with an active spark session."
        )

    def _copy_local_file_to_destination(self, local_path: str, volume_path: str) -> str:
        """Copy a driver-local file into the landing-zone Volume (no Spark file reads).

        TRAINEE NOTE - Why copy-only?
        On Databricks Serverless, spark.read.csv('/tmp/...') and even some Volume
        reads fail with INSUFFICIENT_PERMISSIONS (SELECT on local files).
        dbutils.fs.cp('file:/tmp/...', '/Volumes/...') lands raw files the Bronze
        layer can ingest later without Spark reading driver-local paths.
        """
        dbutils = self._get_dbutils()
        parent = os.path.dirname(volume_path)
        if parent:
            dbutils.fs.mkdirs(parent)

        file_uri = local_path if local_path.startswith("file:") else f"file:{local_path}"

        try:
            dbutils.fs.rm(volume_path)
        except Exception:
            pass

        dbutils.fs.cp(file_uri, volume_path)
        print(f"  -> Copied to Volume: {volume_path}")
        return volume_path

    def _normalize_pandas_for_export(self, pdf):
        """Coerce object columns (e.g. Invoice with nulls) for reliable CSV export."""
        pdf = pdf.copy()
        for col in pdf.columns:
            if pdf[col].dtype == object:
                pdf[col] = pdf[col].fillna("").astype(str)
        return pdf

    def _default_config(self):
        """Return hardcoded default config when no config file is provided.

        TRAINEE NOTE — When is this used?
        In Databricks notebooks the config YAML may not be easily accessible
        without additional setup. The default config provides sensible
        defaults so the downloader works out-of-the-box.

        Source breakdown:
          olist_brazilian_ecommerce : Kaggle dataset (multiple CSV files)
          uci_online_retail_ii      : UCI archive ZIP containing XLSX
          amazon_customer_reviews   : Amazon Reviews 2023 JSONL (Hugging Face)
        """
        return {
            "storage": {
                "raw_landing_zone": "/Volumes/ecommerce_catalog/bronze/raw_data",
            },
            "data_sources": {
                "olist_brazilian_ecommerce": {
                    "source_url": "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce",
                    "kaggle_dataset": "olistbr/brazilian-ecommerce",
                    "format": "kaggle",
                    "domain": "marketplace",
                },
                "uci_online_retail_ii": {
                    "source_url": "https://archive.ics.uci.edu/dataset/502/online+retail+ii",
                    "url": "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip",
                    "format": "zip",
                    "domain": "retail",
                    "inner_file": "online_retail_II.xlsx",
                },
                "amazon_customer_reviews": {
                    "source_url": "https://amazon-reviews-2023.github.io/",
                    "url": self.AMAZON_REVIEWS_BASE_URL.format(category="All_Beauty"),
                    "format": "jsonl",
                    "domain": "reviews",
                    "category": "All_Beauty",
                    "max_records": 100000,
                },
            },
        }

    def download_all(self):
        """Download all configured data sources.

        TRAINEE NOTE — Graceful degradation:
        Each source is downloaded in a try/except block. If one source fails
        (network timeout, missing Kaggle credentials, file moved), the error
        is logged and the loop continues to the next source.
        """
        print("=" * 60)
        print("Starting Public E-Commerce Data Download")
        print("=" * 60)

        for source_name, source_config in self.config["data_sources"].items():
            try:
                print(f"\nDownloading: {source_name}")
                self._download_source(source_name, source_config)
                print(f"  -> Completed: {source_name}")
            except Exception as e:
                print(f"  -> FAILED: {source_name} — {str(e)}")

        print("\n" + "=" * 60)
        print("Download Complete")
        print("=" * 60)

    def _download_source(self, source_name: str, source_config: dict):
        """Route a single source to the appropriate download method based on format.

        TRAINEE NOTE:
        The landing zone path follows the pattern:
          /Volumes/ecommerce_catalog/bronze/raw_data/{domain}/{source_name}/
          e.g. /Volumes/ecommerce_catalog/bronze/raw_data/marketplace/olist_brazilian_ecommerce/
        """
        fmt = source_config["format"]
        domain = source_config["domain"]
        output_dir = f"{self.landing_zone}/{domain}/{source_name}"

        if fmt == "kaggle":
            self._download_kaggle(source_config, output_dir, source_name)
        elif fmt == "zip":
            self._download_zip(source_config, output_dir, source_name)
        elif fmt == "jsonl":
            self._download_jsonl(source_config, output_dir, source_name)
        else:
            raise ValueError(f"Unsupported format '{fmt}' for source '{source_name}'")

    def _download_kaggle(self, source_config: dict, output_dir: str, source_name: str):
        """Download a Kaggle dataset and write CSV files to the landing zone.

        TRAINEE NOTE - Kaggle authentication:
        Kaggle requires API credentials before downloads work. On Databricks Serverless,
        set KAGGLE_USERNAME and KAGGLE_KEY environment variables (recommended), or
        place kaggle.json in a writable directory via KAGGLE_CONFIG_DIR.

        The Olist dataset is a ZIP containing multiple related CSV tables
        (orders, customers, products, payments, etc.) — typical of a
        normalized e-commerce schema.
        """
        kaggle_dataset = source_config["kaggle_dataset"]
        local_dir = f"/tmp/{source_name}"
        os.makedirs(local_dir, exist_ok=True)

        try:
            from kaggle.api.kaggle_api_extended import KaggleApi

            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(kaggle_dataset, path=local_dir, unzip=True)
        except ImportError:
            cmd = [
                "kaggle",
                "datasets",
                "download",
                "-d",
                kaggle_dataset,
                "-p",
                local_dir,
                "--unzip",
            ]
            subprocess.run(cmd, check=True, capture_output=True, text=True)

        csv_files = [f for f in os.listdir(local_dir) if f.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found after downloading {kaggle_dataset}")

        for csv_file in csv_files:
            table_name = os.path.splitext(csv_file)[0]
            local_path = os.path.join(local_dir, csv_file)
            volume_path = f"{output_dir}/{table_name}/{csv_file}"
            self._copy_local_file_to_destination(local_path, volume_path)
            try:
                import pandas as pd

                row_count = len(pd.read_csv(local_path))
                print(f"  -> {table_name}: {row_count} rows")
            except Exception:
                print(f"  -> {table_name}: copied")

    def _download_zip(self, source_config: dict, output_dir: str, source_name: str):
        """Download a ZIP archive, extract it, and write contents to the landing zone.

        TRAINEE NOTE — UCI Online Retail II:
        The UCI archive ships this dataset as a ZIP containing an XLSX workbook
        with two sheets (Year 2009-2010 and Year 2010-2011). We extract the
        archive locally, read each sheet, and write them as separate CSV folders
        in the landing zone.
        """
        url = source_config["url"]
        inner_file = source_config.get("inner_file")
        local_zip = f"/tmp/{source_name}.zip"
        extract_dir = f"/tmp/{source_name}_extracted"
        os.makedirs(extract_dir, exist_ok=True)

        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(local_zip, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        with zipfile.ZipFile(local_zip, "r") as zf:
            zf.extractall(extract_dir)

        if inner_file:
            inner_path = os.path.join(extract_dir, inner_file)
            if not os.path.exists(inner_path):
                matches = [
                    os.path.join(extract_dir, name)
                    for name in os.listdir(extract_dir)
                    if name.lower() == inner_file.lower()
                ]
                if not matches:
                    raise FileNotFoundError(f"Expected file '{inner_file}' not found in archive")
                inner_path = matches[0]
            self._write_xlsx_sheets(inner_path, output_dir, source_name)
            return

        for root, _, files in os.walk(extract_dir):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_name = os.path.splitext(filename)[0]
                if filename.lower().endswith(".csv"):
                    volume_path = f"{output_dir}/{relative_name}/{filename}"
                    self._copy_local_file_to_destination(local_path, volume_path)
                    print(f"  -> {relative_name}: copied")
                elif filename.lower().endswith((".xlsx", ".xls")):
                    self._write_xlsx_sheets(
                        local_path, f"{output_dir}/{relative_name}", source_name
                    )

    def _write_xlsx_sheets(self, xlsx_path: str, output_dir: str, source_name: str):
        """Read an Excel workbook sheet-by-sheet and land each sheet as CSV in the Volume.

        TRAINEE NOTE - Invoice column fix:
        Online Retail II mixes strings and nulls in columns like Invoice. Spark
        Arrow conversion fails on those object columns, so we normalize with pandas,
        export to CSV on the driver, then copy to the Volume (no Spark read/write).
        """
        try:
            import pandas as pd

            workbook = pd.ExcelFile(xlsx_path)
            for sheet_name in workbook.sheet_names:
                pdf = pd.read_excel(xlsx_path, sheet_name=sheet_name)
                pdf = self._normalize_pandas_for_export(pdf)
                safe_sheet = sheet_name.replace(" ", "_").replace("-", "_")
                local_csv = f"/tmp/{source_name}_{safe_sheet}.csv"
                pdf.to_csv(local_csv, index=False)
                volume_path = f"{output_dir}/{safe_sheet}/{safe_sheet}.csv"
                self._copy_local_file_to_destination(local_csv, volume_path)
                print(f"  -> {sheet_name}: {len(pdf)} rows")
        except ImportError as exc:
            raise ImportError(
                "Reading XLSX requires pandas and openpyxl. "
                "Install with: pip install pandas openpyxl"
            ) from exc

    def _download_jsonl(self, source_config: dict, output_dir: str, source_name: str):
        """Download JSONL review data and write to the landing zone.

        TRAINEE NOTE — Amazon Reviews 2023:
        Reviews are stored as JSONL (one JSON object per line). Files are hosted
        on Hugging Face under McAuley-Lab/Amazon-Reviews-2023. Individual
        category files range from ~80 MB to 20+ GB, so use a smaller category
        (e.g. All_Beauty) for development and set max_records to cap download size.

        stream=True downloads in chunks to avoid loading multi-GB files into
        memory on the driver node.
        """
        category = source_config.get("category", "All_Beauty")
        url = source_config.get("url") or self.AMAZON_REVIEWS_BASE_URL.format(category=category)
        max_records = source_config.get("max_records")

        local_path = f"/tmp/{source_name}.jsonl"
        records_written = 0

        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()

        with open(local_path, "w", encoding="utf-8") as out_file:
            for line in response.iter_lines():
                if not line:
                    continue
                if isinstance(line, bytes):
                    line = line.decode("utf-8")
                out_file.write(line + "\n")
                records_written += 1
                if max_records and records_written >= max_records:
                    break

        if records_written == 0:
            print(f"  -> No records downloaded for {source_name}")
            return

        volume_path = f"{output_dir}/{category}.jsonl"
        self._copy_local_file_to_destination(local_path, volume_path)
        print(f"  -> Category: {category}")
        print(f"  -> Total records downloaded: {records_written}")
