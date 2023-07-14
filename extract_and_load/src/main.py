# Standard
import os
import argparse
from typing import List
# External
import boto3
# Internal
from utils.logger import get_logger
from utils.time_utils import get_current_nfl_season
from jobs.pbp import run_pbp_job

logger = get_logger(__name__)

def main(args):
    # Create boto3 client
    s3 = boto3.client('s3')
    
    # S3 data lake variables
    s3_bucket = os.getenv(key='S3_BUCKET')
    s3_key = os.getenv(key='S3_BRONZE_KEY')

    # Unpack args
    years = list(args.years)
    file_format = args.file_format
    data = args.data
    if args.dry_run:
        logger.info("Running dry mode -- no files will be uploaded.")
        dry_run = True
    else:
        dry_run = False

    # Run extract-and-load job
    """
    0. Pull play-by-play data for 'year'
    2. Loop through all possible weeks (1-17)
    3. Check to see if nfl/bronze/pbp/{year}/{week_x}.{file_format} exists
    4. If yes, continue
    5. If no, partition the dataframe and write that week's data to s3
    (no need to upload an extra file this way)
    """
    if data == 'pbp':
        run_pbp_job(s3, s3_bucket, s3_key, years, file_format, dry_run)
    # Add more data type jobs here

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract play-by-play data and send to S3 bronze layer.")
    parser.add_argument('-d', '--data', required=True, help='The type of data to be fetched. Only accepts 1 argument; run a separate container for other data types. Corresponds to the name of the .py file in jobs.')
    parser.add_argument('-y', '--years', nargs='+', required=True, default=get_current_nfl_season(), help='List of weeks to extract data for.')
    parser.add_argument('-f', '--file_format', default='json', help='File format to be uploaded to S3.')
    parser.add_argument('--dry_run', action='store_true', help='Run the script without making changes')    # Example: python main.py -y 2020 2021 2022 -f 'parquet'
    args = parser.parse_args()

    main(args)