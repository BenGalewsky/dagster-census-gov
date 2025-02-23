import re
import tempfile
from logging import Logger
from zipfile import ZipFile
import pyarrow
from pyarrow import fs, parquet
from dagster import AssetExecutionContext, Config, EnvVar, asset
from dagster_aws.s3 import S3Resource
import requests
from bs4 import BeautifulSoup


def zipped_files(url: str, s3_prefix: str, log: Logger):
    try:
        # Stream download the zip file
        log.info(f"Downloading zip file from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Create a temporary file to store the zip
        # We need this because ZipFile needs seek capability
        with tempfile.SpooledTemporaryFile(
                max_size=10 * 1024 * 1024) as tmp:  # 10MB max in memory
            # Stream the content in chunks to temp file
            log.info("Streaming zip file to temporary storage")
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    tmp.write(chunk)

            tmp.seek(0)

            # Process the zip file
            with ZipFile(tmp) as zip_file:
                # List all files in the zip
                for file_info in zip_file.infolist():
                    if file_info.filename.endswith('/'):  # Skip directories
                        continue

                    s3_key = f"{s3_prefix.rstrip('/')}/{file_info.filename.replace('.txt', '.parquet')}"
                    log.info(f"Processing {file_info.filename} into {s3_key}")

                    # Upload the file to S3
                    with zip_file.open(file_info) as file:
                        yield file
    except requests.exceptions.RequestException as e:
        log.error(f"Error downloading from URL: {str(e)}")
        raise


class TigerShapeFileConfig(Config):
    year: int = 2024
    geographic_entity: str = "STATE"
    s3_prefix: str = "shapefiles/tiger/"


def _get_zipfiles(url: str) -> list:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    return [
        url+"/"+a["href"]
        for a in soup.find_all(href=re.compile(".*.zip$"))
    ]

def save_expanded_zipfile(zipfile: str, s3_prefix: str, s3_client,  log: Logger) -> str:
        try:
            # Stream download the zip file
            log.info(f"Downloading zip file from {zipfile}")
            response = requests.get(zipfile, stream=True)
            response.raise_for_status()

            # Create a temporary file to store the zip
            # We need this because ZipFile needs seek capability
            with tempfile.SpooledTemporaryFile(
                    max_size=10 * 1024 * 1024) as tmp:  # 10MB max in memory
                # Stream the content in chunks to temp file
                log.info("Streaming zip file to temporary storage")
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        tmp.write(chunk)

                tmp.seek(0)

                # Process the zip file
                with ZipFile(tmp) as zip_file:
                    # List all files in the zip
                    for file_info in zip_file.infolist():
                        if file_info.filename.endswith('/'):  # Skip directories
                            continue

                        s3_key = f"{s3_prefix.rstrip('/')}/{file_info.filename}"
                        log.info(f"Processing {file_info.filename} into {s3_key}")

                        # Upload the file to S3
                        with zip_file.open(file_info) as file:
                            s3_client.upload_fileobj(
                                Fileobj=file,
                                Bucket=EnvVar("DEST_BUCKET").get_value(),
                                Key=s3_key)

        except requests.exceptions.RequestException as e:
            log.error(f"Error downloading from URL: {str(e)}")
            raise


@asset
def tiger_shapefile_asset(context: AssetExecutionContext,
                         config: TigerShapeFileConfig,
                          s3: S3Resource) -> None:
    url = f"https://www2.census.gov/geo/tiger/TIGER{config.year}/{config.geographic_entity}/"
    zipfiles = _get_zipfiles(url)
    context.log.info(f"Found {len(zipfiles)} zipfiles")
    s3_prefix = f"{config.s3_prefix.rstrip('/')}/{config.year}/{config.geographic_entity}"
    for zipfile in zipfiles:
        save_expanded_zipfile(zipfile, s3_prefix, s3.get_client(), context.log)
    return None

import pandas as pd
class GazetterConfig(Config):
    year: int = 2024
    geographic_entity: str = "STATE"
    s3_prefix: str = "shapefiles/gazetteer/"

def save_expanded_gazetteer(url: str, config: GazetterConfig, s3: S3Resource, log: Logger) -> str:
    try:
        # Upload the file to S3
        for file in zipped_files(url, config.s3_prefix, log):
            s3_key = f"{EnvVar('DEST_BUCKET').get_value()}/{config.s3_prefix.rstrip('/')}/{config.year}/{config.geographic_entity}"

            df = pd.read_csv(file, sep="\t")
            log.info(f"Processing {file.name} {df.shape} into {s3_key}")

            s3_fs = pyarrow.fs.S3FileSystem(
                access_key=s3.aws_access_key_id,
                secret_key=s3.aws_secret_access_key,
                endpoint_override=s3.endpoint_url)

            table = pyarrow.Table.from_pandas(df)

            # Write partitioned parquet file
            pyarrow.parquet.write_to_dataset(
                table,
                root_path=s3_key,
                partition_cols=[],
                filesystem=s3_fs,
                compression="snappy",
            )


    except requests.exceptions.RequestException as e:
        log.error(f"Error downloading from URL: {str(e)}")
        raise
@asset
def gazetteer(context: AssetExecutionContext,
              config: GazetterConfig,
              s3: S3Resource) -> None:
    url = f"https://www2.census.gov/geo/docs/maps-data/data/gazetteer/{config.year}_Gazetteer/{config.year}_Gaz_{config.geographic_entity.lower()}_national.zip"
    save_expanded_gazetteer(url, config, s3, context.log)
    return None