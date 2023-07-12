# Standard
from typing import List
import time
# External
import nfl_data_py as nfl
# Internal
from utils.logger import get_logger
from utils.s3_utils import write_df_to_s3

logger = get_logger(__name__)

def fetch_pbp(years: List[int], format='json') -> None:
    """Gets play-by-play data from nfl_data_py and converts to JSON."""
    
    df = nfl.import_pbp_data(years=years, downcast=True, cache=False, alt_path=None)

    return df

def run_pbp_job(s3, s3_bucket: str, s3_base_key: str, years: List[int], file_format: str, dry_run=False) -> None:
    """Fetches play-by-play for each year and uploads to S3"""
    for year in years:
        # Fetch play-by-play data
        logger.info(f"Fetching play-by-play data for {year}.")
        df = fetch_pbp([int(year)], file_format)

        if not dry_run:
            # Each load will be a separate file
            s3_key = s3_base_key + f'/pbp/{str(year)}.{file_format}'
            logger.info(f"Uploading file to S3://{s3_bucket}/{s3_key}")
            write_df_to_s3(s3, df, file_format, s3_bucket, s3_key)
            logger.info("File uploaded to S3://{s3_bucket}/{s3_key}")
        else:
            logger.info("dry_run set to True; skipping S3 upload.")
        time.sleep(5)
        