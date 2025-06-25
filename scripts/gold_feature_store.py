import os
import argparse
from datetime import datetime
import pyspark
import glob

from utils.data_processing_gold_feature import process_feature_gold_table

def read_silver_table(table, silver_directory, spark, date_str=None):
    """
    Helper function to read all partitions of a silver table.
    If date_str (format: yyyy-mm-dd) is provided, convert to yyyy_mm_dd and only read the file containing that date in its name.
    If date_str is not provided, only read the first file found.
    For tables without date-specific partitions, read the main table file.
    """
    folder_path = os.path.join(silver_directory, table)
    all_files = glob.glob(os.path.join(folder_path, '*'))
    
    if date_str:
        # Convert yyyy-mm-dd to yyyy_mm_dd format for file matching
        formatted_date = date_str.replace('-', '_')
        
        # Different naming patterns for different tables
        if table == 'orders':
            pattern = f"silver_olist_orders_{formatted_date}.parquet"
        elif table == 'order_items':
            # This table doesn't have date-specific files, use main file
            pattern = "silver_olist_order_items.parquet"
        elif table == 'order_logistics':
            pattern = f"silver_olist_order_logistics_{formatted_date}.parquet"
        elif table == 'shipping_infos':
            pattern = f"silver_shipping_infos_{formatted_date}.parquet"
        elif table == 'delivery_history':
            pattern = f"silver_delivery_history_{formatted_date}.parquet"
        elif table == 'seller_performance':
            pattern = f"silver_seller_performance_{formatted_date}.parquet"
        elif table == 'concentration':
            pattern = f"silver_concentration_{formatted_date}.parquet"
        else:
            # Fallback: search for any file with the date
            pattern = f"*{formatted_date}*"
        
        # Find files matching the pattern
        if pattern.startswith('*') or pattern.endswith('*'):
            files_list = [f for f in all_files if formatted_date in os.path.basename(f)]
        else:
            files_list = [f for f in all_files if os.path.basename(f) == pattern]
        
        if not files_list:
            # For order_items which has no date-specific files, try to get the main file
            if table == 'order_items':
                main_files = [f for f in all_files if 'silver_olist_order_items.parquet' in os.path.basename(f)]
                if main_files:
                    files_list = main_files
                    print(f"Table '{table}' has no date-specific files, using main table file")
            
            if not files_list:
                print(f"Available files in {table}: {[os.path.basename(f) for f in all_files[:5]]}...")
                raise FileNotFoundError(f"No files found for table '{table}' with pattern '{pattern}' (date: '{date_str}')")
    else:
        if not all_files:
            raise FileNotFoundError(f"No files found for table '{table}'")
        files_list = [all_files[0]]
    
    print(f"Reading {len(files_list)} file(s) for table '{table}': {[os.path.basename(f) for f in files_list]}")
    df = spark.read.option("header", "true").parquet(*files_list)
    return df

if name == "main":
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("dev") \
        .master("local[*]") \
        .getOrCreate()

    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description='Process snapshot date from Airflow')
    parser.add_argument('--startdate', type=str, required=True, help='Snapshot date in YYYY-MM-DD format')
    args = parser.parse_args()
    snapshot_date_str = args.startdate

    
    ############################
    # GOLD
    ############################
    print("Building gold feature tables...")