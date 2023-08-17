# Standard
import os
import argparse
from typing import List
# External
import boto3
import yaml
# Internal
from utils.logger import get_logger
from jobs.pbp import run_pbp_job
from jobs.player_weekly import run_player_weekly_job
from jobs.player_seasonal import run_player_seasonal_job
from jobs.rosters import run_rosters_job
from jobs.sc_lines import run_sc_lines_job
from jobs.win_totals import run_win_totals_job
from scrapers.draftkings.player_props import get_draftkings_player_props

logger = get_logger(__name__)

def main(args):
    # Import config
    with open("src/config.yml", "r") as config_stream:
        try:
            config = yaml.safe_load(config_stream)
        except yaml.YAMLError as exception:
            logger.info(exception)
    retries = config['jobs']['retries']

    # Create boto3 client
    s3 = boto3.client('s3')
    
    # S3 data lake variables
    s3_bucket = os.getenv(key='S3_BUCKET')
    s3_key = os.getenv(key='S3_BRONZE_KEY')

    # Unpack args
    years = args.years
    try:
        if years[0] == 'all':
            years = [x for x in range(1999,2023)]
    except TypeError as err:
        logger.info("No 'years' argument provided.")
    file_format = args.file_format
    data = args.data
    logger.info(f"Job: {data}\nYears: {years}\nFile format: {file_format}")
    
    if args.dry_run:
        logger.info("Running dry mode -- no files will be uploaded.")
        dry_run = True
    else:
        dry_run = False

    # Run extract-and-load job
    if data == 'pbp':
        run_pbp_job(s3, s3_bucket, s3_key, years, retries, file_format, dry_run)
    if data == 'player_weekly':
        run_player_weekly_job(s3, s3_bucket, s3_key, years, retries, file_format, dry_run)
    if data == 'player_seasonal':
        run_player_seasonal_job(s3, s3_bucket, s3_key, years, retries, file_format, dry_run)
    if data == 'rosters':
        run_rosters_job(s3, s3_bucket, s3_key, years, retries, file_format, dry_run)
    if data == 'sc_lines':
        run_sc_lines_job(s3, s3_bucket, s3_key, years, retries, file_format, dry_run)
    if data == 'win_totals':
        run_win_totals_job(s3, s3_bucket, s3_key, years, retries, file_format, dry_run)
    if data == 'dk_player_props':
        d = config['scrapers']['urls']['player_props']['draftkings']
        chromedriver_path = config['scrapers']['chromedriver_location']
        sleep_secs_range = config['scrapers']['sleep_secs_range']
        urls = [url for url in d.values()]
        get_draftkings_player_props(urls, chromedriver_path, sleep_secs_range)
    # Add more data type jobs here

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract play-by-play data and send to S3 bronze layer.")
    parser.add_argument('-d', '--data', required=True, help='The type of data to be fetched. Only accepts 1 argument; run a separate container for other data types. Corresponds to the name of the .py file in jobs.')
    parser.add_argument('-y', '--years', nargs='+', help='List of weeks to extract data for.')
    parser.add_argument('-f', '--file_format', default='json', help='File format to be uploaded to S3.')
    parser.add_argument('--dry_run', action='store_true', help='Run the script without making changes')
    args = parser.parse_args()

    main(args)