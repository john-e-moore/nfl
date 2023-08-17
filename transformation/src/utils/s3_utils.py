# Standard
import json
from typing import Optional
# External
import zipfile
import shutil
import boto3
import pandas as pd
from io import StringIO, BytesIO
# Internal
from utils.logger import get_logger

logger = get_logger(__name__)

def check_file_exists(s3, bucket: str, key: str) -> bool:
    """Checks if an S3 object exists. Returns true/false."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

def fetch_file_size(s3, bucket: str, key: str) -> Optional[int]:
    """Reads the file size metadata for an S3 object
    and returns it as an integer (number of bytes)."""
    try:
        file_info = s3.head_object(Bucket=bucket, Key=key)
        return file_info['ContentLength']
    except Exception as e:
        logger.info(f"Error occurred while fetching file size: {e}")
        return None

def fetch_last_update_timestamp(s3, bucket: str, key: str, job_name: str) -> Optional[str]:
    """Reads the last updated metadata for an S3 object
    and returns it as a string."""
    try:
        file_info = s3.head_object(Bucket=bucket, Key=key)
        return str(file_info['LastModified'])
    except Exception as e:
        logger.info(f"Error occurred while fetching last update timestamp: {e}")
        return None

def write_last_update_timestamp(s3, bucket: str, key: str, job_name: str) -> None:
    """Writes a .txt file to S3 with the timestamp
    that another file in that folder was last updated (job_name)."""
    try:
        timestamp = fetch_last_update_timestamp(s3, bucket, key, job_name)
        if timestamp:
            s3.put_object(Body=timestamp, Bucket=bucket, Key=key)
    except Exception as e:
        logger.info(f"Error occurred while writing last update timestamp: {e}")

def write_df_to_s3(s3, df: pd.DataFrame, file_format: str, bucket: str, key: str) -> None:
    s3 = boto3.client('s3')

    if file_format == 'csv':
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

    elif file_format == 'json':
        json_buffer = StringIO()
        df.to_json(json_buffer, orient='records')
        s3.put_object(Bucket=bucket, Key=key, Body=json_buffer.getvalue())

    elif file_format == 'parquet':
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow')
        s3.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())

    elif file_format == 'zip':
        str_buffer = BytesIO()
        df.to_csv(str_buffer, index=False)
        # Use BytesIO to write zip file to buffer
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            # The arcname parameter avoids including the full path in the zip file
            zip_file.writestr(f'{key}.csv', str_buffer.getvalue())
        
        # Reset the position of the buffers
        str_buffer.seek(0)
        zip_buffer.seek(0)

        # Upload buffer content to S3
        s3.put_object(Bucket=bucket, Key=key, Body=zip_buffer.getvalue())

    else:
        raise ValueError("file_format must be 'csv', 'json', 'parquet', or 'zip'")
