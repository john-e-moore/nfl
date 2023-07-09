# Standard
import json
from typing import Optional
# External
import boto3
# Internal
from utils.logger import get_logger

logger = get_logger(__name__)

def check_file_exists(s3: boto3.resources.factory.s3.ServiceResource, bucket: str, key: str) -> bool:
    """Checks if an S3 object exists. Returns true/false."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

def fetch_file_size(s3: boto3.resources.factory.s3.ServiceResource, bucket: str, key: str) -> Optional[int]:
    """Reads the file size metadata for an S3 object
    and returns it as an integer (number of bytes)."""
    try:
        file_info = s3.head_object(Bucket=bucket, Key=key)
        return file_info['ContentLength']
    except Exception as e:
        logger.info(f"Error occurred while fetching file size: {e}")
        return None

def fetch_last_update_timestamp(s3: boto3.resources.factory.s3.ServiceResource, bucket: str, key: str, job_name: str) -> Optional[str]:
    """Reads the last updated metadata for an S3 object
    and returns it as a string."""
    try:
        file_info = s3.head_object(Bucket=bucket, Key=key)
        return str(file_info['LastModified'])
    except Exception as e:
        logger.info(f"Error occurred while fetching last update timestamp: {e}")
        return None

def write_last_update_timestamp(s3: boto3.resources.factory.s3.ServiceResource, bucket: str, key: str, job_name: str) -> None:
    """Writes a .txt file to S3 with the timestamp
    that another file in that folder was last updated (job_name)."""
    try:
        timestamp = fetch_last_update_timestamp(s3, bucket, key, job_name)
        if timestamp:
            s3.put_object(Body=timestamp, Bucket=bucket, Key=key)
    except Exception as e:
        logger.info(f"Error occurred while writing last update timestamp: {e}")
