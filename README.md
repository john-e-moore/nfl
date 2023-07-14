## Project

Lakehouse ELT for NFL data. Experimentation with containers and CI/CD.

'extract_and_load' contains jobs that partition data and fill the bronze layer of our data lake on S3. Each job will be run as a separate service.

'transformation' will perform any necessary transformations and store that in a separate S3 directory (silver).

'load_to_warehouse' will process the data further and load it to a relational warehouse for end user use.


## Usage

Include a .env file in the project's base directory. This will be passed when a container is run. Format:

AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>
S3_BUCKET=<your_s3_bucket>
S3_BRONZE_KEY=<your_s3_key>

### extract_and_load

This script is designed to extract play-by-play data and send it to the S3 bronze layer. It supports several command-line flags to customize its behavior:

- `-d` or `--data`: (Required) This argument specifies the type of data to be fetched. The value provided should correspond to the name of the .py file in the `jobs` directory. For instance, for play-by-play data there is a file called `pbp.py`. You should pass `-d pbp` to fetch that type of data. Note that this argument only accepts one data type at a time. To fetch different types of data, you need to run separate containers.

- `-y` or `--years`: (Required) This argument takes a list of years for which to extract data. The years should be space-separated. For example, to fetch data for the years 2020 and 2021, you would pass `-y 2020 2021`. If not provided, it defaults to the current NFL season.

- `-f` or `--file_format`: This argument specifies the file format to be uploaded to S3. By default, it's set to `json`. Other acceptable formats are 'parquet' and 'csv'.

- `--dry_run`: If this flag is passed, the script will run without loading anything to S3.

### Examples

Here's an example command that fetches play-by-play data for the years 2020 and 2021 and stores it in parquet format:

```bash
docker run --env-file .env extract_and_load --data pbp --years 2020 2021 --file_format parquet
