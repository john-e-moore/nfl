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

def fetch_player_weekly(years: List[int], retries: int) -> pd.DataFrame:
    """Gets weekly player data from nfl_data_py."""
    
    for retry_count in range(retries):
        try:
            df = nfl.import_seasonal_data(years=years)
        except Exception as e:
            logger.info(f"Exception fetching data. {retries - (retry_count + 1)} retries left.")
            logger.exception(e)
            logger.info("Sleeping for 10 seconds...")
            time.sleep(10)
    return df

def run_player_weekly_job(s3, s3_bucket: str, s3_key: str, years: List[int], retries: int, file_format: str, dry_run=False) -> None:
    """Gets data for all years and does full replace if file is already in S3."""
    
    logger.info(f"Fetching weekly player data for {years}.")
    df = fetch_player_weekly([int(year) for year in years], retries)

    if df.empty:
        logger.info(f"No player weekly data found.")
        return False

    s3_key_full = s3_key + f'/player_weekly.{file_format}'

    if not dry_run:
        write_df_to_s3(s3, df, file_format, s3_bucket, s3_key_full)
        logger.info(f"File uploaded to S3://{s3_bucket}/{s3_key_full}")
    else:
        logger.info("dry_run set to True; skipping S3 upload.")
    
    logger.info(f"Weekly player data upload complete.")
    
            
            
        