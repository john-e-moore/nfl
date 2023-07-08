# Standard
import os
import json
import argparse
from typing import List
# External
import boto3
# Internal
from utils.logger import get_logger
from utils.nfl_data_utils import fetch_pbp

logger = get_logger(__name__)

def main(years: List[int]):
    # Create boto3 client
    s3 = boto3.client('s3')
    
    # S3 variables
    s3_bucket = os.getenv(key='S3_BUCKET')
    s3_base_key = os.getenv(key='S3_BRONZE_KEY')

    for year in years:
        # Fetch play-by-play data
        logger.info(f"Fetching play-by-play data for {year}.")
        json_data = fetch_pbp(year)

        # TODO:
        # Implement incremental load
        # Save last timestamp / date / whatever in .txt file
        # Much more cost effective to not pull the entire file
        
        # TODO: filename needs to have timestamp or datestamp
        # Each load will be a separate file
        s3_key = s3_base_key + f'/pbp/{str(year)}.json'
        logger.info(f"Uploading file to S3://{s3_bucket}/{s3_key}")
        s3.put_object(
            Body=json.dumps(json_data),
            Bucket=s3_bucket,
            Key=s3_key
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract play-by-play data and send to S3 bronze layer.")
    parser.add_argument('-y', '--years', nargs='+', required=True, help='List of years to extract data for')
    # Example: python main.py -y 2020 2021 2022
    args = parser.parse_args()

    main(args.years)