# Import Library

import duckdb
import filelock
import dagster as dg
import pandas as pd
import kagglehub 
from kagglehub import KaggleDatasetAdapter
import shutil
import os

# Global Variables
DATASET_URL = "thedevastator/unlock-profits-with-e-commerce-sales-data" # kaggle dataset URL
DUCKDB_PATH = os.path.join("database", "verihubs.duckdb") # duckdb file database path

def duckdb_execution(duckdb_path: str, sql: str):
    """Execute SQL statement"""
    conn = duckdb.connect(duckdb_path) # Connect to DuckDB database
    try:
        return conn.execute(sql).fetchdf() # execute SQL and fetch results as DataFrame
    finally:
        conn.close() # close the connection

def import_url_to_duckdb(data_path: str, duckdb_path: str, table_name: str):
    """Import CSV data from URL into DuckDB table."""

    create_query = f"""
        create or replace table {table_name} as (
            select * from read_csv_auto('{data_path}')
        )
    """

    duckdb_execution(duckdb_path, create_query)

def download_data():
    """Download specific file from Kaggle dataset and save to local directory."""

    # Download the dataset (downloads entire dataset)
    path = kagglehub.dataset_download(DATASET_URL)

    # Your destination folder
    destination_folder = os.path.expanduser("data")
    os.makedirs(destination_folder, exist_ok=True)

    # Select Specific File from the Zip
    specific_file = "Amazon Sale Report.csv"

    # source and destination file paths
    src_file = os.path.join(path, specific_file)
    dst_file = os.path.join(destination_folder, specific_file)

    # Copy the specific file from the downloaded dataset to the destination folder
    if os.path.exists(src_file):
        shutil.copy2(src_file, dst_file)
        print(f"File saved to: {dst_file}")
        return dst_file
    else:
        print(f"File '{specific_file}' not found. Available files:")
        print(os.listdir(path))
        return None

@dg.asset(kinds={"duckdb"}, key=["target", "main", "raw_amazon_data"]) # specified asset decorator and set key for the asset and target table_name
def raw_amazon_data(context= dg.AssetExecutionContext) -> None:
    """Asset to download Amazon Sale Report.csv from kaggle and import the raw data into DuckDB."""

    # log the download URL process
    context.log.info("Downloading Data from Kaggle this URL: https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data")
    data_path = download_data() # download the data

    # check if the data is successfully downloaded and raise issue if not
    if data_path is None or not os.path.exists(data_path):
        raise FileNotFoundError("Amazon Sale Report.csv could not be downloaded.")
    else:
        context.log.info(f"Data downloaded to: {data_path}") # otherwise log the data path
    
    # set the absolute data_path
    data_path = os.path.abspath(data_path)
    # set the duckdb directory
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

    # executing the import function to load data into duckdb
    try:
        import_url_to_duckdb(
            data_path=data_path,
            duckdb_path=DUCKDB_PATH,
            table_name="raw_amazon_data",
        )
        context.log.info(f"Successfully imported data to DuckDB at {DUCKDB_PATH}") # log success message
    except:
        context.log.error(f"Failed to import data to DuckDB") # log fail message
    
# specified asset decorator and set key for the asset and target table_name and set dependency to raw_amazon_data
@dg.asset(kinds={"duckdb"}, key=["target", "main", "monthly_category_sales"], deps=["raw_amazon_data"])
def monthly_category_sales(context= dg.AssetExecutionContext) -> None:
    """Asset to create monthly_category_sales table in DuckDB from raw_amazon_data."""

    # create query for the monthly sales by category
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
    # execute the query to create the monthly_category_sales table
    try:
        duckdb_execution(DUCKDB_PATH, create_query)
        context.log.info(f"Successfully created monthly_category_sales table in DuckDB at {DUCKDB_PATH}")
    except:
        context.log.error(f"Failed to create monthly_category_sales table in DuckDB")

# specified asset decorator and set key for the asset and target table_name and set dependency to raw_amazon_data
@dg.asset(kinds={"duckdb"}, key=["target", "main", "daily_order_status"], deps=["raw_amazon_data"])
def daily_order_status(context= dg.AssetExecutionContext) -> None:
    """Asset to create daily_order_status table in DuckDB from raw_amazon_data."""
    
    # create query for the daily order status
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

    # execute the query to create the daily_order_status table
    try:
        duckdb_execution(DUCKDB_PATH, create_query)
        context.log.info(f"Successfully created daily_order_status table in DuckDB at {DUCKDB_PATH}")
    except:
        context.log.error(f"Failed to create daily_order_status table in DuckDB")

        