import os
import pandas as pd
from sqlalchemy import create_engine
import argparse


def main(args):
    """
    This script ingests data from a CSV file into a PostgreSQL database.

    Functions:
        main(args): Main function to handle the ingestion process.

    Arguments:
        --username: Database username.
        --password: Database password.
        --host: Database host.
        --port: Database port.
        --database: Database name.
        --ingest_file_url: URL of the file to ingest.

    Usage:
        python ingest.py --username <username> --password <password> --host <host> --port <port> --database <database> --ingest_file_url <url_to_file>

    Example:
        python ingest.py --username user --password pass --host localhost --port 5432 --database mydb --ingest_file_url http://example.com/data.csv.gz

    Comments:
        - Imports necessary libraries: pandas, sqlalchemy, and argparse.
        - Defines the main function to handle the ingestion process.
        - Parses command-line arguments.
        - Creates a connection URL for the PostgreSQL database.
        - Downloads the CSV file from the provided URL.
        - Reads the CSV file in chunks and ingests data into the database.
        - Converts datetime columns to appropriate format.
        - Uses `to_sql` method to insert data into the database.
    """
    # Assign the arguments to variables
    username = args.username
    password = args.password
    host = args.host
    port = args.port
    database = args.database
    ingest_file_url = args.ingest_file_url
    # Create the connection URL
    connection_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"

    connector = create_engine(connection_url)

    connector.connect()

    os.system(f"wget {ingest_file_url} -O ingest.csv.gz")

    df_iter = pd.read_csv(
        filepath_or_buffer="ingest.csv.gz",
        iterator=True,
        chunksize=100000,
        compression="gzip",
    )

    for iter_inx, df in enumerate(df_iter):
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
        if iter_inx == 0:
            df.to_sql(
                name="yellow_taxi_data", con=connector, index=False, if_exists="replace"
            )
        else:
            df.to_sql(
                name="yellow_taxi_data", con=connector, index=False, if_exists="append"
            )


if __name__ == "__main__":
    # Initialize the parser
    parser = argparse.ArgumentParser(
        description="Ingest data in csv.gz format to the postgres database"
    )

    # Add arguments
    parser.add_argument("--username", type=str, help="Database username")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--host", type=str, help="Database host")
    parser.add_argument("--port", type=str, help="Database port")
    parser.add_argument("--database", type=str, help="Database name")
    parser.add_argument("--ingest_file_url", type=str, help="url of the file to ingest")

    # Parse the arguments
    args = parser.parse_args()

    main(args)


