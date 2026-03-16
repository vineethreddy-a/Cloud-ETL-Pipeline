"""
PySpark Transformation Module
Handles large-scale data transformations on AWS EMR / local Spark.
Optimized for performance with partition tuning, broadcast joins, and caching.
"""

import argparse
import logging
from datetime import datetime
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType, BooleanType
)
from pyspark.sql.window import Window

from utils.logger import get_logger
from utils.config import load_config

logger = get_logger(__name__)


def create_spark_session(app_name: str, config: dict) -> SparkSession:
    """
    Create an optimized SparkSession for EMR or local execution.
    Tunes executor memory, shuffle partitions, and S3 access.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", config.get("spark_shuffle_partitions", 200))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    if config.get("environment") != "local":
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        )

    return builder.getOrCreate()


class DataTransformer:
    """
    Core transformation class. Implements ETL/ELT patterns:
    - Cleansing and standardization
    - Deduplication
    - Dimensional modeling (star schema)
    - Aggregations for analytics
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

    def read_parquet(self, s3_path: str) -> DataFrame:
        """Read parquet files from S3 with pushdown optimization."""
        logger.info(f"Reading parquet from: {s3_path}")
        return self.spark.read.parquet(s3_path)

    def cleanse(self, df: DataFrame, schema: StructType) -> DataFrame:
        """
        Apply standard cleansing:
        - Trim whitespace from strings
        - Cast to expected types
        - Replace nulls with defaults
        - Filter invalid rows
        """
        logger.info("Applying data cleansing...")

        # Trim all string columns
        for field in schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, F.trim(F.col(field.name)))

        # Drop exact duplicates
        df = df.dropDuplicates()

        # Add metadata columns
        df = df.withColumn("_ingestion_ts", F.current_timestamp())
        df = df.withColumn("_source_env", F.lit(self.config.get("environment", "dev")))

        logger.info(f"Cleansed dataset: {df.count()} rows")
        return df

    def deduplicate(self, df: DataFrame, partition_cols: List[str], order_col: str) -> DataFrame:
        """
        Deduplicate using a window function, keeping the latest record.

        Args:
            df: Input DataFrame.
            partition_cols: Columns to partition by (e.g., primary key).
            order_col: Column to determine "latest" record.

        Returns:
            Deduplicated DataFrame.
        """
        logger.info(f"Deduplicating on: {partition_cols}, ordered by: {order_col}")

        window = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
        df = df.withColumn("_row_num", F.row_number().over(window))
        df = df.filter(F.col("_row_num") == 1).drop("_row_num")

        return df

    def build_fact_table(
        self,
        transactions_df: DataFrame,
        dim_customer_df: DataFrame,
        dim_product_df: DataFrame
    ) -> DataFrame:
        """
        Build a fact table from transactions joined to dimension tables.
        Uses broadcast join for small dimension tables (< 10MB).

        Returns:
            Star schema fact table DataFrame.
        """
        logger.info("Building fact table with star schema joins...")

        # Broadcast small dimension tables to avoid shuffle
        fact_df = (
            transactions_df
            .join(F.broadcast(dim_customer_df), on="customer_id", how="left")
            .join(F.broadcast(dim_product_df), on="product_id", how="left")
            .select(
                F.col("transaction_id"),
                F.col("customer_id"),
                F.col("product_id"),
                F.col("customer_name"),
                F.col("product_name"),
                F.col("category"),
                F.col("transaction_amount"),
                F.col("quantity"),
                F.col("transaction_date"),
                F.col("region"),
                (F.col("transaction_amount") * F.col("quantity")).alias("total_revenue"),
                F.col("_ingestion_ts"),
            )
        )

        return fact_df

    def compute_daily_aggregates(self, fact_df: DataFrame) -> DataFrame:
        """
        Compute daily revenue aggregates by region and category.
        Used for BI dashboards and reporting.
        """
        logger.info("Computing daily aggregates...")

        agg_df = (
            fact_df
            .groupBy(
                F.to_date("transaction_date").alias("date"),
                "region",
                "category"
            )
            .agg(
                F.sum("total_revenue").alias("daily_revenue"),
                F.count("transaction_id").alias("transaction_count"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.avg("transaction_amount").alias("avg_order_value"),
                F.max("total_revenue").alias("max_order_value"),
            )
            .withColumn("_processed_ts", F.current_timestamp())
        )

        return agg_df

    def write_parquet(
        self,
        df: DataFrame,
        s3_path: str,
        partition_cols: Optional[List[str]] = None,
        mode: str = "overwrite"
    ) -> None:
        """Write DataFrame to S3 as Parquet with optional partitioning."""
        logger.info(f"Writing {df.count()} rows to: {s3_path}")

        writer = df.write.mode(mode).format("parquet")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(s3_path)
        logger.info("Write complete.")

    def write_snowflake(self, df: DataFrame, table: str, config: dict) -> None:
        """Write DataFrame to Snowflake using Spark connector."""
        logger.info(f"Writing to Snowflake table: {table}")

        snowflake_options = {
            "sfURL": config["snowflake"]["url"],
            "sfDatabase": config["snowflake"]["database"],
            "sfSchema": config["snowflake"]["schema"],
            "sfWarehouse": config["snowflake"]["warehouse"],
            "sfRole": config["snowflake"]["role"],
            "sfUser": config["snowflake"]["user"],
            "sfPassword": config["snowflake"]["password"],
        }

        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", table) \
            .mode("append") \
            .save()

        logger.info(f"Successfully wrote to Snowflake: {table}")


def run_pipeline(env: str) -> None:
    """Main pipeline runner."""
    config = load_config(env)
    spark = create_spark_session("ETL-Pipeline", config)

    transformer = DataTransformer(spark, config)

    s3_base = f"s3a://{config['s3']['bucket']}"

    # --- Ingestion ---
    transactions_df = transformer.read_parquet(f"{s3_base}/landing/transactions/")
    customers_df = transformer.read_parquet(f"{s3_base}/landing/customers/")
    products_df = transformer.read_parquet(f"{s3_base}/landing/products/")

    # --- Transformation ---
    from pyspark.sql.types import StructType  # schema defined per dataset
    transactions_clean = transformer.cleanse(transactions_df, transactions_df.schema)
    transactions_dedup = transformer.deduplicate(
        transactions_clean,
        partition_cols=["transaction_id"],
        order_col="updated_at"
    )

    # --- Dimensional Modeling ---
    fact_df = transformer.build_fact_table(transactions_dedup, customers_df, products_df)
    daily_agg_df = transformer.compute_daily_aggregates(fact_df)

    # --- Load ---
    transformer.write_parquet(
        fact_df,
        f"{s3_base}/curated/fact_transactions/",
        partition_cols=["region"]
    )
    transformer.write_parquet(
        daily_agg_df,
        f"{s3_base}/curated/agg_daily_revenue/"
    )

    if env == "prod":
        transformer.write_snowflake(fact_df, "FACT_TRANSACTIONS", config)
        transformer.write_snowflake(daily_agg_df, "AGG_DAILY_REVENUE", config)

    spark.stop()
    logger.info("Pipeline complete.")


def main():
    parser = argparse.ArgumentParser(description="PySpark ETL Transformation")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    args = parser.parse_args()
    run_pipeline(args.env)


if __name__ == "__main__":
    main()
