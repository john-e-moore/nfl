# Standard
import os
import argparse
from typing import List
# External
import boto3
# Internal
from utils.logger import get_logger
from jobs.pbp import run_job

logger = get_logger(__name__)

def main(args):
    # Create boto3 client
    s3 = boto3.client('s3')
    
    # S3 variables
    s3_bucket = os.getenv(key='S3_BUCKET')
    s3_base_key = os.getenv(key='S3_BRONZE_KEY')

    # Unpack args
    years = list(args.years)
    print(f"Years: {years}")
    file_format = args.file_format

    # Run extract-and-load job
    if args.dry_run:
        run_job(s3, s3_bucket, s3_base_key, years, file_format, dry_run=True)
    else:
        run_job(s3, s3_bucket, s3_base_key, years, file_format)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract play-by-play data and send to S3 bronze layer.")
    parser.add_argument('-y', '--years', nargs='+', required=True, help='List of years to extract data for.')
    parser.add_argument('-f', '--file_format', default='json', help='File format to be uploaded to S3.')
    parser.add_argument('--dry_run', action='store_true', help='Run the script without making changes')    # Example: python main.py -y 2020 2021 2022 -f 'parquet'
    args = parser.parse_args()

    main(args)