# Standard
from typing import List
import time
# External
import nfl_data_py as nfl
import pandas as pd
# Internal
from utils.logger import get_logger
from utils.s3_utils import write_df_to_s3, check_file_exists

logger = get_logger(__name__)

def fetch_player_weekly(years: List[int]) -> pd.DataFrame:
    """Gets weekly player data from nfl_data_py."""
    
    try:
        df = nfl.import_weekly_data(years=years)
    except Exception as e:
        logger.info("Exception fetching data, trying again in 10 seconds. If second try fails, program will terminate.")
        logger.exception(e)
        logger.info("Sleeping for 10 seconds...")
        time.sleep(10)
        df = nfl.import_weekly_data(years=years)

    return df

def run_player_weekly_job(s3, s3_bucket: str, s3_key: str, years: List[int], file_format: str, dry_run=False) -> None:
    """Loops through each week in the list of years and uploads to data lake if file is not present."""
    for year in years:
        # Fetch weekly player data
        logger.info(f"Fetching weekly player data for {year}.")
        df = fetch_player_weekly([int(year)])

        if df.empty:
            logger.info(f"No data for {year}.")
            continue

        # Check if data for this week exists in S3
        # If no, write to S3
        s3_key_full = s3_key + f'/player_weekly/{str(year)}.{file_format}'
        file_exists = check_file_exists(s3, s3_bucket, s3_key_full)

        if not file_exists:
            if not dry_run:
                write_df_to_s3(s3, df, file_format, s3_bucket, s3_key_full)
                logger.info(f"File uploaded to S3://{s3_bucket}/{s3_key_full}")
            else:
                logger.info("dry_run set to True; skipping S3 upload.")
        else:
            logger.info(f"S3://{s3_bucket}/{s3_key_full} already exists.")
    
        # Sleep before hitting API again
        logger.info(f"{year} uploads complete. Sleeping for 5 seconds...")
        time.sleep(5)
            
            
        