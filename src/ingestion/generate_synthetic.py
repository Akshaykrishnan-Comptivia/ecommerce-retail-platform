"""
Generate synthetic e-commerce clickstream data with Faker.
Writes raw JSON event logs to the Unity Catalog landing zone for Bronze ingestion.

Data flow:
  Faker → Python event dicts → Spark DataFrame → JSON on Volume
  Volume → Bronze ingestion (ingest_semi_structured.py via spark.read.json())

Usage (Databricks notebook):
    from src.ingestion.generate_synthetic import SyntheticDataGenerator

    generator = SyntheticDataGenerator(spark, config_path="../config/pipeline_config.yaml")
    raw_path = generator.write_clickstream_raw()
"""

import uuid
from datetime import datetime, timedelta
from random import choice, randint

import yaml
from faker import Faker
from pyspark.sql import SparkSession

fake = Faker()

DEVICE_TYPES = ["mobile", "desktop", "tablet"]

# Typical funnel order within a session (subset used per session length).
SESSION_FUNNEL = ["page_view", "page_view", "add_to_cart", "checkout", "purchase"]


class SyntheticDataGenerator:
    """Generates synthetic clickstream JSON for the Bronze landing zone.

    Config keys (pipeline_config.yaml):
      storage.raw_landing_zone
      synthetic.clickstream.*
    """

    def __init__(self, spark: SparkSession, config_path: str | None = None):
        self.spark = spark
        if config_path:
            with open(config_path, encoding="utf-8") as handle:
                self.config = yaml.safe_load(handle)
        else:
            self.config = self._default_config()
        self.landing_zone = self.config["storage"]["raw_landing_zone"]
        self.clickstream_cfg = self.config["synthetic"]["clickstream"]

    def _default_config(self) -> dict:
        return {
            "storage": {
                "raw_landing_zone": "/Volumes/ecommerce_catalog/bronze/raw_data",
            },
            "synthetic": {
                "clickstream": {
                    "num_events": 1_000_000,
                    "output_subpath": "synthetic/clickstream",
                    "event_types": [
                        "page_view",
                        "add_to_cart",
                        "checkout",
                        "purchase",
                    ],
                    "num_users": 10_000,
                    "num_products": 500,
                    "num_sessions": 5_000,
                    "events_per_session_min": 3,
                    "events_per_session_max": 12,
                },
            },
        }

    def clickstream_output_path(self) -> str:
        subpath = self.clickstream_cfg["output_subpath"]
        return f"{self.landing_zone}/{subpath}"

    def generate_clickstream_events(self) -> list[dict]:
        """Build clickstream event records up to num_events.

        Events are grouped into sessions so session_id repeats and timestamps
        advance within a session (supports later sessionization in Silver).
        """
        cfg = self.clickstream_cfg
        target_events = cfg["num_events"]
        num_users = cfg.get("num_users", 10_000)
        num_products = cfg.get("num_products", 500)
        num_sessions = cfg.get("num_sessions", 5_000)
        min_events = cfg.get("events_per_session_min", 3)
        max_events = cfg.get("events_per_session_max", 12)
        allowed_types = cfg.get("event_types", SESSION_FUNNEL)

        events: list[dict] = []
        base_time = datetime(2024, 1, 1)

        for session_idx in range(num_sessions):
            if len(events) >= target_events:
                break

            session_id = f"sess_{session_idx + 1}"
            user_id = f"user_{randint(1, num_users)}"
            session_length = randint(min_events, max_events)
            funnel = [t for t in SESSION_FUNNEL if t in allowed_types]
            if not funnel:
                funnel = list(allowed_types)

            if session_length <= len(funnel):
                event_types = funnel[:session_length]
            else:
                extras = [choice(allowed_types) for _ in range(session_length - len(funnel))]
                event_types = funnel + extras

            session_start = base_time + timedelta(minutes=randint(0, 500_000))

            for offset, event_type in enumerate(event_types):
                if len(events) >= target_events:
                    break

                events.append(
                    {
                        "event_id": str(uuid.uuid4()),
                        "session_id": session_id,
                        "user_id": user_id,
                        "event_type": event_type,
                        "product_id": f"prod_{randint(1, num_products)}",
                        "timestamp": (
                            session_start + timedelta(seconds=offset * randint(15, 180))
                        ).isoformat(),
                        "device": choice(DEVICE_TYPES),
                        "referrer": fake.url() if choice([True, False]) else None,
                    }
                )

        # If sessions did not reach num_events, append standalone events.
        while len(events) < target_events:
            events.append(self._random_event(num_users, num_products, allowed_types, base_time))

        return events[:target_events]

    def _random_event(
        self,
        num_users: int,
        num_products: int,
        allowed_types: list[str],
        base_time: datetime,
    ) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "session_id": f"sess_{randint(1, 500_000)}",
            "user_id": f"user_{randint(1, num_users)}",
            "event_type": choice(allowed_types),
            "product_id": f"prod_{randint(1, num_products)}",
            "timestamp": (
                base_time + timedelta(minutes=randint(0, 500_000))
            ).isoformat(),
            "device": choice(DEVICE_TYPES),
            "referrer": fake.url() if choice([True, False]) else None,
        }

    def write_clickstream_raw(self, batch_size: int = 50_000) -> str:
        """Generate clickstream events and write JSON to the landing zone.

        Writes in batches to limit driver memory for large num_events values.
        """
        output_path = self.clickstream_output_path()
        target_events = self.clickstream_cfg["num_events"]
        print("=" * 60)
        print("Generating Synthetic Clickstream Data")
        print("=" * 60)
        print(f"  Target events : {target_events:,}")
        print(f"  Output path   : {output_path}")

        total_written = 0
        mode = "overwrite"

        while total_written < target_events:
            remaining = target_events - total_written
            chunk_target = min(batch_size, remaining)

            original_num_events = self.clickstream_cfg["num_events"]
            self.clickstream_cfg["num_events"] = chunk_target
            chunk_events = self.generate_clickstream_events()
            self.clickstream_cfg["num_events"] = original_num_events

            if not chunk_events:
                break

            df = self.spark.createDataFrame(chunk_events)
            df.write.mode(mode).json(output_path)
            mode = "append"

            total_written += len(chunk_events)
            print(f"  -> Wrote batch: {len(chunk_events):,} rows (total: {total_written:,})")

        print("\nSynthetic clickstream generation complete.")
        print(f"  Total events written: {total_written:,}")
        return output_path

    def generate_all(self) -> dict[str, str]:
        """Generate all configured synthetic sources (clickstream only for now)."""
        return {"clickstream": self.write_clickstream_raw()}
