import duckdb
import filelock
import dagster as dg
import pandas as pd
import kagglehub 
from kagglehub import KaggleDatasetAdapter
import shutil
import os

def serialize_duckdb_query(duckdb_path: str, sql: str):
    """Execute SQL statement with file lock to guarantee cross-process concurrency."""
    lock_path = f"{duckdb_path}.lock"
    with filelock.FileLock(lock_path):
        conn = duckdb.connect(duckdb_path)
        try:
            return conn.execute(sql)
        finally:
            conn.close()

def import_url_to_duckdb(data, duckdb_path: str, table_name: str):

    create_query = f"""
        create or replace table {table_name} as (
            select * from data
        )
    """

    serialize_duckdb_query(duckdb_path, create_query)

@dg.asset
def download_data():
    # Download the dataset (downloads entire dataset to cache)
    path = kagglehub.dataset_download("thedevastator/unlock-profits-with-e-commerce-sales-data")

    # Your destination folder
    destination_folder = os.path.expanduser("~/data")
    os.makedirs(destination_folder, exist_ok=True)

    # If you want only the specific CSV file
    specific_file = "Amazon Sale Report.csv"
    src_file = os.path.join(path, specific_file)
    dst_file = os.path.join(destination_folder, specific_file)

    if os.path.exists(src_file):
        shutil.copy2(src_file, dst_file)
        print(f"File saved to: {dst_file}")
    else:
        print(f"File '{specific_file}' not found. Available files:")
        print(os.listdir(path))

@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_amazon_data"])
def raw_amazon_data() -> None:
    
    download_data()
    data_path = "/data/Amazon Sale Report.csv"
    import_url_to_duckdb(
        data=data_path,
        duckdb_path="database/db.duckdb",
        table_name="raw_amazon_data",
    )

