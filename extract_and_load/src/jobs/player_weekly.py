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
    """Gets data for all years and does full replace if file is already in S3."""
    
    logger.info(f"Fetching weekly player data for {years}.")
    df = fetch_player_weekly([int(year) for year in years])

    if df.empty:
        logger.info(f"No player weekly data found.")
        return False

    # Check if data for this week exists in S3
    # If no, write to S3
    s3_key_full = s3_key + f'/player_weekly.{file_format}'

    if not dry_run:
        write_df_to_s3(s3, df, file_format, s3_bucket, s3_key_full)
        logger.info(f"File uploaded to S3://{s3_bucket}/{s3_key_full}")
    else:
        logger.info("dry_run set to True; skipping S3 upload.")
    
    logger.info(f"Weekly player data upload complete.")
    
            
            
        