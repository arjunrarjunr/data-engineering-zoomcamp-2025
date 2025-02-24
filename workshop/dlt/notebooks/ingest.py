import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import pandas as pd
import os
import re


def my_ingest(
    base_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/",
    file_path="fhv_tripdata_{year}-{month}.csv.gz",
    years=[2019],
    months=range(1, 13),
    pipeline_name="nyc_taxi",
    destination_platform="bigquery",
    dataset_name="nyc_taxi_dataset",
    table_name="for_hire_vehicle_trips",
    write_disposition="append",
    loader_file_format="parquet",
    type_mapping={
        "dispatching_base_num": "object",
        "pickup_datetime": "timestamp",
        "dropOff_datetime": "timestamp",
        "PUlocationID": "Float64",
        "DOlocationID": "Float64",
        "SR_Flag": "Float64",
        "Affiliated_base_number": "object",
    },
):
    def my_paginate(
        base_url: str,
        file_path: str,
    ):
        """
        This source retrieves NYC taxi rides from the NYC Taxi & Limousine Commission's API.
        """
        client = RESTClient(
            base_url=base_url,
            paginator=PageNumberPaginator(base_page=1, total_path=None),
        )

        for page in client.paginate(file_path):
            yield page

    def clean_column_name(col):
        col = col.strip()  # Remove leading/trailing spaces
        col = col.lower()  # Convert to lowercase
        col = re.sub(r"[^a-z0-9]+", "_", col)  # Replace special characters with '_'
        return col

    def download_parquet(url):
        """Download and load Parquet data from a URL into a Pandas DataFrame."""
        print(f"Downloading {url}")
        os.system(f"wget {url} -O temp.csv.gz")
        df = pd.read_csv(
            filepath_or_buffer="temp.csv.gz",
            compression="gzip",
        )
        os.system("rm temp.csv.gz")
        print(f"Downloaded {df.shape[0]} rows and {df.shape[1]} columns")
        # Apply transformation to all columns
        df.columns = [clean_column_name(col) for col in df.columns]
        column_types = df.dtypes.apply(lambda x: x.name).to_dict()
        print(f"Column types: {column_types}")
        for k, v in type_mapping.items():
            print(k, v)
            if v != "timestamp":
                df[k] = df[k].astype(v)
            else:
                df[k] = pd.to_datetime(
                    df[k], format="%Y-%m-%d %H:%M:%S"
                ).dt.tz_localize(None)

        return df

    # Define the DLTHub pipeline
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination_platform,
        dataset_name=dataset_name,
    )
    # Ingest data into the pipeline

    for year in years:
        for month in months:
            year = str(year)
            month = str(month).zfill(2)
            URL = base_url + file_path.format(year=year, month=month)
            pipeline.run(
                download_parquet(URL),
                table_name=table_name,
                write_disposition=write_disposition,
                loader_file_format=loader_file_format,
            )


# my_ingest(
#     base_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/",
#     file_path="green_tripdata_{year}-{month}.csv.gz",
#     years=[2019, 2020],
#     months=range(1, 13),
#     pipeline_name="nyc_taxi",
#     destination_platform="bigquery",
#     dataset_name="nyc_taxi_dataset",
#     table_name="green_trips",
#     write_disposition="append",
#     loader_file_format="parquet",
#     type_mapping={
#         "vendorid": "Int64",
#         "lpep_pickup_datetime": "timestamp",
#         "lpep_dropoff_datetime": "timestamp",
#         "store_and_fwd_flag": "object",
#         "ratecodeid": "Int64",
#         "pulocationid": "Int64",
#         "dolocationid": "Int64",
#         "passenger_count": "Int64",
#         "trip_distance": "Float64",
#         "fare_amount": "Float64",
#         "extra": "Float64",
#         "mta_tax": "Float64",
#         "tip_amount": "Float64",
#         "tolls_amount": "Float64",
#         "ehail_fee": "Float64",
#         "improvement_surcharge": "Float64",
#         "total_amount": "Float64",
#         "payment_type": "Int64",
#         "trip_type": "Int64",
#         "congestion_surcharge": "Float64",
#     },
# )

my_ingest(
    base_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/",
    file_path="yellow_tripdata_{year}-{month}.csv.gz",
    years=[2019, 2020],
    months=range(1, 13),
    pipeline_name="nyc_taxi",
    destination_platform="bigquery",
    dataset_name="nyc_taxi_dataset",
    table_name="yellow_trips",
    write_disposition="append",
    loader_file_format="parquet",
    type_mapping={
        "vendorid": "Int64",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "Int64",
        "trip_distance": "Float64",
        "ratecodeid": "Int64",
        "store_and_fwd_flag": "object",
        "pulocationid": "Int64",
        "dolocationid": "Int64",
        "payment_type": "Int64",
        "fare_amount": "Float64",
        "extra": "Float64",
        "mta_tax": "Float64",
        "tip_amount": "Float64",
        "tolls_amount": "Float64",
        "improvement_surcharge": "Float64",
        "total_amount": "Float64",
        "congestion_surcharge": "Float64",
    },
)

my_ingest(
    base_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/",
    file_path="fhv_tripdata_{year}-{month}.csv.gz",
    years=[2019],
    months=range(1, 13),
    pipeline_name="nyc_taxi",
    destination_platform="bigquery",
    dataset_name="nyc_taxi_dataset",
    table_name="for_hire_vehicle_trips",
    write_disposition="append",
    loader_file_format="parquet",
    type_mapping={
        "dispatching_base_num": "object",
        "pickup_datetime": "timestamp",
        "dropoff_datetime": "timestamp",
        "pulocationid": "Float64",
        "dolocationid": "Float64",
        "sr_flag": "Float64",
        "affiliated_base_number": "object",
    },
)
