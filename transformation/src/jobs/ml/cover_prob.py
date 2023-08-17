# External
import pandas as pd
import numpy as np
import pyarrow
import boto3
from typing import List
# Internal
from utils.logger import get_logger
from utils.s3_utils import write_df_to_s3, check_file_exists

# Pull play-by-play data from all years from S3 bronze (loop)
# trim unnecessary columns, calculate features, and concat all years
# Upload single parquet file to silver, and that file will get the model applied to it

## BELOW IS THE OLD EXTRACT_AND_LOAD JOB FOR THIS DATA
## CONTAINS THE CORRECT COLUMNS

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

"""
Columns needed via join:
- team record
- team preseason ELO (If I can't get this, team preseason win total should be ok)

maybe these aren't that necessary as long as I have spread and total for game?
"""

columns = [
    'away_team',
    'home_team',
    'away_score',
    'home_score',
    'week',
    'season_type', # filter for reg season
    'posteam', # offensive team
    'posteam_type', # 'home' or 'away'
    'defteam', # defensive team
    'side_of_field',
    'yardline_100',
    'half_seconds_remaining',
    'game_seconds_remaining',
    'qtr', # derive 'half' from this
    'down',
    'yrdln', # 'BAL 35'
    'ydstogo',
    'weather', # use to create yes/no for rain, temperature decile
    'wind',
    'temp',
    'location',
    'surface',
    'roof',
    'spread_line',
    'total',
    'play_type',
    'game_half',
    'game_id',
    'result',
    'score_differential',
    'posteam_timeouts_remaining',
    'defteam_timeouts_remaining'
]

def fetch_pbp(years: List[int], columns: List[str]) -> pd.DataFrame:
    """Gets play-by-play data from nfl_data_py."""
    
    try:
        df = nfl.import_pbp_data(years=years, columns=columns, downcast=True, cache=False, alt_path=None)
    except Exception as e:
        logger.info("Exception fetching data, trying again in 10 seconds. If second try fails, program will terminate.")
        logger.exception(e)
        logger.info("Sleeping for 10 seconds...")
        time.sleep(10)
        df = nfl.import_pbp_data(years=years, columns=columns, downcast=True, cache=False, alt_path=None)

    return df

def run_pbp_win_prob_job(s3, s3_bucket: str, s3_key: str, years: List[int], columns: List[str], file_format: str, dry_run=False) -> None:
    """Loops through each week in the list of years and uploads to data lake if file is not present."""
    
    # Check if data for this week exists in S3
    # If no, write to S3
    s3_key_full = s3_key + f'/pbp/win_prob_1999_2022.{file_format}'
    file_exists = check_file_exists(s3, s3_bucket, s3_key_full)

    if not file_exists:
        logger.info(f"years: {years}")
        logger.info(f"columns: {columns}")
        logger.info("Fetching data...")
        df = fetch_pbp(years=years, columns=columns)
        if not dry_run:
            write_df_to_s3(s3, df, file_format, s3_bucket, s3_key_full)
            logger.info(f"File uploaded to S3://{s3_bucket}/{s3_key_full}")
        else:
            logger.info("dry_run set to True; skipping S3 upload.")
    else:
        logger.info(f"S3://{s3_bucket}/{s3_key_full} already exists.")


    