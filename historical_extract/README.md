Download historical play-by-play data from nfl-data-py and upload to S3. Supports .csv, .json, .parquet.

Must pass a .env file with S3_BUCKET and S3_BRONZE_KEY.

# Example run
docker run --env-file .env nfl_historical_extract --years 2021 --file_format parquet --dry_run
