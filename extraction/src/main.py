import os
import boto3
import json
from jobs.pbp import fetch_pbp

def main():
    # Create boto3 client
    s3 = boto3.client('s3')

    # Get play-by-play data for 2022
    years = [2022]
    json_data = fetch_pbp(years)

    # S3 variables
    s3_bucket = os.getenv(key='S3_BUCKET')
    s3_base_key = os.getenv(key='S3_BRONZE_KEY')

    # Upload to S3
    for year in years:
        s3_key = s3_base_key + f'/pbp/{str(year)}.json'
        print(f"Uploading to S3://{s3_bucket}/{s3_key}")
        s3.put_object(
            Body=json.dumps(json_data),
            Bucket=s3_bucket,
            Key=s3_key
        )

if __name__ == "__main__":
    main()