"""
AWS helper utilities for S3, Glue, Redshift, SNS, and Step Functions.
"""

import boto3
import logging
from typing import Optional

from utils.logger import get_logger

logger = get_logger(__name__)


def get_s3_client(config: dict):
    """Return a boto3 S3 client configured for the environment."""
    session = boto3.Session(
        aws_access_key_id=config.get("aws", {}).get("access_key_id"),
        aws_secret_access_key=config.get("aws", {}).get("secret_access_key"),
        region_name=config.get("aws", {}).get("region", "us-east-1"),
    )
    return session.client("s3")


def upload_to_s3(s3_client, local_path: str, bucket: str, key: str) -> None:
    """Upload a local file to S3."""
    logger.info(f"Uploading {local_path} → s3://{bucket}/{key}")
    s3_client.upload_file(local_path, bucket, key)
    logger.info("Upload complete.")


def download_from_s3(s3_client, bucket: str, key: str, local_path: str) -> None:
    """Download a file from S3 to local disk."""
    logger.info(f"Downloading s3://{bucket}/{key} → {local_path}")
    s3_client.download_file(bucket, key, local_path)


def list_s3_objects(s3_client, bucket: str, prefix: str) -> list:
    """List all objects in an S3 prefix."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]


def send_sns_alert(topic_arn: str, subject: str, message: str, region: str = "us-east-1") -> None:
    """Send an SNS alert for pipeline failures or warnings."""
    sns = boto3.client("sns", region_name=region)
    sns.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    logger.info(f"SNS alert sent: {subject}")


def get_glue_client(region: str = "us-east-1"):
    """Return a boto3 Glue client."""
    return boto3.client("glue", region_name=region)


def start_glue_job(glue_client, job_name: str, arguments: dict) -> str:
    """Start an AWS Glue job and return the run ID."""
    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments={f"--{k}": v for k, v in arguments.items()}
    )
    run_id = response["JobRunId"]
    logger.info(f"Started Glue job '{job_name}' with run ID: {run_id}")
    return run_id


def get_glue_job_status(glue_client, job_name: str, run_id: str) -> str:
    """Poll and return the status of a Glue job run."""
    response = glue_client.get_job_run(JobName=job_name, RunId=run_id)
    return response["JobRun"]["JobRunState"]
