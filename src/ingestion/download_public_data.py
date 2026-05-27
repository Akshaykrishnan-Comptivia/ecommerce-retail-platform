import os
import shutil
import subprocess
import zipfile
import requests
import yaml
from pyspark.sql import SparkSession

class PublicDataDownloader:
    AMAZON_REVIEWS_BASE_URL = 'https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/{category}.jsonl'

    def __init__(self, spark: SparkSession, config_path: str=None):
        self.spark = spark
        if config_path:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = self._default_config()
        self.landing_zone = self.config['storage']['raw_landing_zone']

    def _copy_local_file_to_destination(self, local_path: str, volume_path: str) -> str:
        parent = os.path.dirname(volume_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        if os.path.exists(volume_path):
            os.remove(volume_path)
        with open(local_path, 'rb') as src:
            with open(volume_path, 'wb') as dest:
                shutil.copyfileobj(src, dest)
        print(f'  -> Copied to Volume: {volume_path}')
        return volume_path

    def _normalize_pandas_for_export(self, pdf):
        pdf = pdf.copy()
        for col in pdf.columns:
            if pdf[col].dtype == object:
                pdf[col] = pdf[col].fillna('').astype(str)
        return pdf

    def _default_config(self):
        return {'storage': {'raw_landing_zone': '/Volumes/ecommerce_catalog/bronze/raw_data'}, 'data_sources': {'olist_brazilian_ecommerce': {'source_url': 'https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce', 'kaggle_dataset': 'olistbr/brazilian-ecommerce', 'format': 'kaggle', 'domain': 'marketplace'}, 'uci_online_retail_ii': {'source_url': 'https://archive.ics.uci.edu/dataset/502/online+retail+ii', 'url': 'https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip', 'format': 'zip', 'domain': 'retail', 'inner_file': 'online_retail_II.xlsx'}, 'amazon_customer_reviews': {'source_url': 'https://amazon-reviews-2023.github.io/', 'url': self.AMAZON_REVIEWS_BASE_URL.format(category='All_Beauty'), 'format': 'jsonl', 'domain': 'reviews', 'category': 'All_Beauty', 'max_records': 100000}}}

    def download_all(self):
        print('=' * 60)
        print('Starting Public E-Commerce Data Download')
        print('=' * 60)
        for source_name, source_config in self.config['data_sources'].items():
            try:
                print(f'\nDownloading: {source_name}')
                self._download_source(source_name, source_config)
                print(f'  -> Completed: {source_name}')
            except Exception as e:
                print(f'  -> FAILED: {source_name} — {str(e)}')
        print('\n' + '=' * 60)
        print('Download Complete')
        print('=' * 60)

    def _download_source(self, source_name: str, source_config: dict):
        fmt = source_config['format']
        domain = source_config['domain']
        output_dir = f'{self.landing_zone}/{domain}/{source_name}'
        if fmt == 'kaggle':
            self._download_kaggle(source_config, output_dir, source_name)
        elif fmt == 'zip':
            self._download_zip(source_config, output_dir, source_name)
        elif fmt == 'jsonl':
            self._download_jsonl(source_config, output_dir, source_name)
        else:
            raise ValueError(f"Unsupported format '{fmt}' for source '{source_name}'")

    def _download_kaggle(self, source_config: dict, output_dir: str, source_name: str):
        kaggle_dataset = source_config['kaggle_dataset']
        local_dir = f'/tmp/{source_name}'
        os.makedirs(local_dir, exist_ok=True)
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(kaggle_dataset, path=local_dir, unzip=True)
        except ImportError:
            cmd = ['kaggle', 'datasets', 'download', '-d', kaggle_dataset, '-p', local_dir, '--unzip']
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        csv_files = [f for f in os.listdir(local_dir) if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError(f'No CSV files found after downloading {kaggle_dataset}')
        for csv_file in csv_files:
            table_name = os.path.splitext(csv_file)[0]
            local_path = os.path.join(local_dir, csv_file)
            volume_path = f'{output_dir}/{table_name}/{csv_file}'
            self._copy_local_file_to_destination(local_path, volume_path)
            try:
                import pandas as pd
                row_count = len(pd.read_csv(local_path))
                print(f'  -> {table_name}: {row_count} rows')
            except Exception:
                print(f'  -> {table_name}: copied')

    def _download_zip(self, source_config: dict, output_dir: str, source_name: str):
        url = source_config['url']
        inner_file = source_config.get('inner_file')
        local_zip = f'/tmp/{source_name}.zip'
        extract_dir = f'/tmp/{source_name}_extracted'
        os.makedirs(extract_dir, exist_ok=True)
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(local_zip, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        with zipfile.ZipFile(local_zip, 'r') as zf:
            zf.extractall(extract_dir)
        if inner_file:
            inner_path = os.path.join(extract_dir, inner_file)
            if not os.path.exists(inner_path):
                matches = [os.path.join(extract_dir, name) for name in os.listdir(extract_dir) if name.lower() == inner_file.lower()]
                if not matches:
                    raise FileNotFoundError(f"Expected file '{inner_file}' not found in archive")
                inner_path = matches[0]
            self._write_xlsx_sheets(inner_path, output_dir, source_name)
            return
        for root, _, files in os.walk(extract_dir):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_name = os.path.splitext(filename)[0]
                if filename.lower().endswith('.csv'):
                    volume_path = f'{output_dir}/{relative_name}/{filename}'
                    self._copy_local_file_to_destination(local_path, volume_path)
                    print(f'  -> {relative_name}: copied')
                elif filename.lower().endswith(('.xlsx', '.xls')):
                    self._write_xlsx_sheets(local_path, f'{output_dir}/{relative_name}', source_name)

    def _write_xlsx_sheets(self, xlsx_path: str, output_dir: str, source_name: str):
        try:
            import pandas as pd
            workbook = pd.ExcelFile(xlsx_path)
            for sheet_name in workbook.sheet_names:
                pdf = pd.read_excel(xlsx_path, sheet_name=sheet_name)
                pdf = self._normalize_pandas_for_export(pdf)
                safe_sheet = sheet_name.replace(' ', '_').replace('-', '_')
                local_csv = f'/tmp/{source_name}_{safe_sheet}.csv'
                pdf.to_csv(local_csv, index=False)
                volume_path = f'{output_dir}/{safe_sheet}/{safe_sheet}.csv'
                self._copy_local_file_to_destination(local_csv, volume_path)
                print(f'  -> {sheet_name}: {len(pdf)} rows')
        except ImportError as exc:
            raise ImportError('Reading XLSX requires pandas and openpyxl. Install with: pip install pandas openpyxl') from exc

    def _download_jsonl(self, source_config: dict, output_dir: str, source_name: str):
        category = source_config.get('category', 'All_Beauty')
        url = source_config.get('url') or self.AMAZON_REVIEWS_BASE_URL.format(category=category)
        max_records = source_config.get('max_records')
        local_path = f'/tmp/{source_name}.jsonl'
        records_written = 0
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(local_path, 'w', encoding='utf-8') as out_file:
            for line in response.iter_lines():
                if not line:
                    continue
                if isinstance(line, bytes):
                    line = line.decode('utf-8')
                out_file.write(line + '\n')
                records_written += 1
                if max_records and records_written >= max_records:
                    break
        if records_written == 0:
            print(f'  -> No records downloaded for {source_name}')
            return
        volume_path = f'{output_dir}/{category}.jsonl'
        self._copy_local_file_to_destination(local_path, volume_path)
        print(f'  -> Category: {category}')
        print(f'  -> Total records downloaded: {records_written}')
