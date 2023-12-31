# Standard
from typing import List
import time
# External
import nfl_data_py as nfl
import pandas as pd
# Internal
from utils.logger import get_logger
from utils.s3_utils import write_df_to_s3, check_file_exists
from utils.time_utils import get_current_nfl_season

logger = get_logger(__name__)

def fetch_pbp(years: List[int], retries: int) -> pd.DataFrame:
    """Gets play-by-play data from nfl_data_py."""

    for retry_count in range(retries):
        try:
            df = nfl.import_pbp_data(years=years, downcast=True, cache=False, alt_path=None)
        except Exception as e:
            logger.info(f"Exception fetching data. {retries - (retry_count + 1)} retries left.")
            logger.exception(e)
            logger.info("Sleeping for 10 seconds...")
            time.sleep(10)
    return df

def run_pbp_job(s3, s3_bucket: str, s3_key: str, years: List[int], retries: int, file_format: str, dry_run=False) -> None:
    """Loops through each week in the list of years and uploads to data lake if file is not present."""
    for year in years:
        # Fetch play-by-play data
        logger.info(f"Fetching play-by-play data for {year}.")
        df = fetch_pbp([int(year)], retries)

        if df.empty:
            logger.info(f"No data for {year}.")
            continue
        
        # Check if data for this year exists in S3 if not current year
        # If no, write to S3
        s3_key_full = s3_key + f'/pbp/pbp_{str(year)}.{file_format}'
        file_exists = check_file_exists(s3, s3_bucket, s3_key_full)

        # If current season, replace existing file
        is_current_season = year == get_current_nfl_season()
        if (not file_exists) or (is_current_season):
            if not dry_run:
                if (is_current_season) and (file_exists):
                    logger.info(f"Replacing file for current season {year}...")
                write_df_to_s3(s3, df, file_format, s3_bucket, s3_key_full)
                logger.info(f"File uploaded to S3://{s3_bucket}/{s3_key_full}")
            else:
                logger.info("dry_run set to True; skipping S3 upload.")
        else:
            logger.info(f"S3://{s3_bucket}/{s3_key_full} already exists.")
        
        # Sleep before hitting API again
        logger.info(f"{year} uploads complete. Sleeping for 5 seconds...")
        time.sleep(5)
        