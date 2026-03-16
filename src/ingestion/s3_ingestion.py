"""
S3 Data Ingestion Module
Handles ingestion from various sources into AWS S3 landing zone.
"""

import boto3
import logging
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from utils.logger import get_logger
from utils.config import load_config
from utils.aws_helpers import get_s3_client, upload_to_s3

logger = get_logger(__name__)


class S3Ingestion:
    """Handles data ingestion from source systems into S3."""

    def __init__(self, config: dict):
        self.config = config
        self.s3_client = get_s3_client(config)
        self.bucket = config["s3"]["bucket"]
        self.landing_prefix = config["s3"]["landing_prefix"]

    def ingest_csv(self, source_path: str, target_key: str) -> dict:
        """
        Ingest a CSV file into S3 landing zone.

        Args:
            source_path: Local or remote path to source CSV.
            target_key: S3 key (path) for the destination.

        Returns:
            Metadata dict with record count, file size, timestamp.
        """
        logger.info(f"Starting CSV ingestion from: {source_path}")

        try:
            df = pd.read_csv(source_path)
            record_count = len(df)

            # Partition by ingestion date
            date_partition = datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
            full_key = f"{self.landing_prefix}/{target_key}/{date_partition}/data.parquet"

            # Convert to parquet for efficiency
            local_parquet = f"/tmp/{Path(target_key).stem}.parquet"
            df.to_parquet(local_parquet, index=False)

            file_size = Path(local_parquet).stat().st_size

            upload_to_s3(self.s3_client, local_parquet, self.bucket, full_key)

            metadata = {
                "source": source_path,
                "target": f"s3://{self.bucket}/{full_key}",
                "record_count": record_count,
                "file_size_bytes": file_size,
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "status": "SUCCESS",
            }

            logger.info(f"Ingestion complete: {record_count} records → {full_key}")
            return metadata

        except Exception as e:
            logger.error(f"Ingestion failed: {e}", exc_info=True)
            raise

    def ingest_api(self, endpoint: str, target_key: str, params: Optional[dict] = None) -> dict:
        """
        Ingest data from a REST API endpoint into S3.

        Args:
            endpoint: API URL.
            target_key: S3 destination key.
            params: Optional query parameters.

        Returns:
            Metadata dict.
        """
        import requests

        logger.info(f"Ingesting from API: {endpoint}")

        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.json_normalize(data)

            record_count = len(df)
            date_partition = datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
            full_key = f"{self.landing_prefix}/{target_key}/{date_partition}/data.parquet"

            local_parquet = f"/tmp/{Path(target_key).stem}_api.parquet"
            df.to_parquet(local_parquet, index=False)

            upload_to_s3(self.s3_client, local_parquet, self.bucket, full_key)

            metadata = {
                "source": endpoint,
                "target": f"s3://{self.bucket}/{full_key}",
                "record_count": record_count,
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "status": "SUCCESS",
            }

            logger.info(f"API ingestion complete: {record_count} records")
            return metadata

        except Exception as e:
            logger.error(f"API ingestion failed: {e}", exc_info=True)
            raise

    def ingest_database(self, query: str, connection_string: str, target_key: str) -> dict:
        """
        Ingest data from a SQL database into S3.

        Args:
            query: SQL query to extract data.
            connection_string: SQLAlchemy connection string.
            target_key: S3 destination key.

        Returns:
            Metadata dict.
        """
        from sqlalchemy import create_engine

        logger.info(f"Ingesting from database. Target key: {target_key}")

        try:
            engine = create_engine(connection_string)
            df = pd.read_sql(query, engine)
            record_count = len(df)

            date_partition = datetime.utcnow().strftime("year=%Y/month=%m/day=%d")
            full_key = f"{self.landing_prefix}/{target_key}/{date_partition}/data.parquet"

            local_parquet = f"/tmp/{Path(target_key).stem}_db.parquet"
            df.to_parquet(local_parquet, index=False)

            upload_to_s3(self.s3_client, local_parquet, self.bucket, full_key)

            metadata = {
                "source": "database",
                "target": f"s3://{self.bucket}/{full_key}",
                "record_count": record_count,
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "status": "SUCCESS",
            }

            logger.info(f"Database ingestion complete: {record_count} records")
            return metadata

        except Exception as e:
            logger.error(f"Database ingestion failed: {e}", exc_info=True)
            raise


def main():
    parser = argparse.ArgumentParser(description="S3 Data Ingestion")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument("--source", required=True, help="Source path or URL")
    parser.add_argument("--target-key", required=True, help="S3 destination key")
    parser.add_argument("--source-type", default="csv", choices=["csv", "api", "db"])
    args = parser.parse_args()

    config = load_config(args.env)
    ingester = S3Ingestion(config)

    if args.source_type == "csv":
        metadata = ingester.ingest_csv(args.source, args.target_key)
    elif args.source_type == "api":
        metadata = ingester.ingest_api(args.source, args.target_key)

    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
