#python ingest.py --username <username> --password <password> --host <host> --port <port> --database <database> --ingest_file_url <url_to_file>
docker run -it \
    --rm \
    --network 2_docker_sql_dev-network ingest:latest \
    --username root \
    --password root \
    --host postgres_db_container \
    --port 5432 \
    --database ny_taxi \
    --ingest_file_url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
