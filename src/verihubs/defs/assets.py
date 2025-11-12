import duckdb
import filelock
import dagster as dg
import pandas as pd
import kagglehub 
from kagglehub import KaggleDatasetAdapter
import shutil
import os

DATASET_URL = "thedevastator/unlock-profits-with-e-commerce-sales-data"
DUCKDB_PATH = os.path.join("database", "verihubs.duckdb")

def serialize_duckdb_query(duckdb_path: str, sql: str):
    """Execute SQL statement with file lock"""
    lock_path = f"{duckdb_path}.lock"
    with filelock.FileLock(lock_path):
        conn = duckdb.connect(duckdb_path)
        try:
            return conn.execute(sql).fetchdf()
        finally:
            conn.close()

def import_url_to_duckdb(data_path: str, duckdb_path: str, table_name: str):

    create_query = f"""
        create or replace table {table_name} as (
            select * from read_csv_auto('{data_path}')
        )
    """

    serialize_duckdb_query(duckdb_path, create_query)

def download_data():
    # Download the dataset (downloads entire dataset to cache)
    path = kagglehub.dataset_download(DATASET_URL)

    # Your destination folder
    destination_folder = os.path.expanduser("data")
    os.makedirs(destination_folder, exist_ok=True)

    # If you want only the specific CSV file
    specific_file = "Amazon Sale Report.csv"
    src_file = os.path.join(path, specific_file)
    dst_file = os.path.join(destination_folder, specific_file)

    if os.path.exists(src_file):
        shutil.copy2(src_file, dst_file)
        print(f"File saved to: {dst_file}")
        return dst_file
    else:
        print(f"File '{specific_file}' not found. Available files:")
        print(os.listdir(path))
        return None

@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_amazon_data"])
def raw_amazon_data(context= dg.AssetExecutionContext) -> None:
    """Asset to download Amazon Sale Report.csv from kaggle and import the raw data into DuckDB."""

    context.log.info("Downloading Data from Kaggle this URL: https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data")
    data_path = download_data()
    if data_path is None or not os.path.exists(data_path):
        raise FileNotFoundError("Amazon Sale Report.csv could not be downloaded.")
    
    data_path = os.path.abspath(data_path)

    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

    try:
        import_url_to_duckdb(
        data_path=data_path,
        duckdb_path=DUCKDB_PATH,
        table_name="raw_amazon_data",
        )
        context.log.info(f"Successfully imported data to DuckDB at {DUCKDB_PATH}")
    except:
        context.log.error(f"Failed to import data to DuckDB")
    
@dg.asset(kinds={"duckdb"}, key=["target", "main", "monthly_category_sales"], deps=["raw_amazon_data"])
def monthly_category_sales(context= dg.AssetExecutionContext) -> None:
    """Asset to create monthly_category_sales table in DuckDB from raw_amazon_data."""

    create_query = """
        create or replace table monthly_category_sales as
        select 
            date_trunc('month', Date) as month,
            Category,
            sum(Amount) as total_amount
        from raw_amazon_data
        group by month, Category
        order by month, Category
    """

    try:
        serialize_duckdb_query(DUCKDB_PATH, create_query)
        context.log.info(f"Successfully created monthly_category_sales table in DuckDB at {DUCKDB_PATH}")
    except:
        context.log.error(f"Failed to create monthly_category_sales table in DuckDB")

@dg.asset(kinds={"duckdb"}, key=["target", "main", "daily_order_status"], deps=["raw_amazon_data"])
def daily_order_status(context= dg.AssetExecutionContext) -> None:
    """Asset to create daily_order_status table in DuckDB from raw_amazon_data."""
    
    create_query = """
        create or replace table daily_order_status as
        select 
            date_trunc('day', Date) as day,
            Status,
            count(distinct "Order ID") as order_count
        from raw_amazon_data
        group by day, Status
        order by day, Status
    """
    try:
        serialize_duckdb_query(DUCKDB_PATH, create_query)
        context.log.info(f"Successfully created daily_order_status table in DuckDB at {DUCKDB_PATH}")
    except:
        context.log.error(f"Failed to create daily_order_status table in DuckDB")

        