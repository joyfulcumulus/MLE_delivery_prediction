import os
import argparse
from datetime import datetime
import pyspark
import glob

from utils.data_processing_gold_feature import process_feature_gold_table

def read_silver_table(table, silver_directory, spark, date_str=None):
    """
    Helper function to read all partitions of a silver table.
    If date_str (format: yyyy_mm_dd) is provided, only read the file containing that date in its name.
    If date_str is not provided, only read the first file found.
    """
    folder_path = os.path.join(silver_directory, table)
    all_files = glob.glob(os.path.join(folder_path, '*'))
    if date_str:
        files_list = [f for f in all_files if date_str in os.path.basename(f)]
        if not files_list:
            raise FileNotFoundError(f"No files found for table '{table}' with date '{date_str}'")
    else:
        if not all_files:
            raise FileNotFoundError(f"No files found for table '{table}'")
        files_list = [all_files[0]]
    df = spark.read.option("header", "true").parquet(*files_list)
    return df

if __name__ == "__main__":
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

    # Define paths
    silver_directory = "datamart/silver"
    gold_directory = "datamart/gold"
    os.makedirs(gold_directory, exist_ok=True)
    print(f"Gold root directory: {gold_directory}")

    formatted_date = snapshot_date_str.replace('-', '_')

    gold_feature_directory = "datamart/gold/feature_store/"
    if not os.path.exists(gold_feature_directory):
        os.makedirs(gold_feature_directory)

    
    try:
        orders_df = read_silver_table('orders', silver_directory, spark, date_str=formatted_date)
    except FileNotFoundError:
        print(f"Gold feature tables built successfully from start date: {formatted_date}")
        exit(0)
    
    items_df = read_silver_table('order_items', silver_directory, spark)
    logistic_df = read_silver_table('order_logistics', silver_directory, spark, date_str=formatted_date)
    orders_df = read_silver_table('orders', silver_directory, spark, date_str=formatted_date)
    shipping_df = read_silver_table('shipping_infos', silver_directory, spark, date_str=formatted_date)
    history_df = read_silver_table('delivery_history', silver_directory, spark, date_str=formatted_date)
    seller_perform_df = read_silver_table('seller_performance', silver_directory, spark, date_str=formatted_date)
    concentration_df = read_silver_table('concentration', silver_directory, spark)
    
        
    # Build gold tables
    y = process_feature_gold_table(snapshot_date_str, gold_directory, items_df, logistic_df, 
                               orders_df, shipping_df, history_df, seller_perform_df, concentration_df,spark)

    # Check for the rows ingested
    y_pdf = y.toPandas()
    y_count = y_pdf.shape[0]
    print(f"Number of rows in feature store: {y_pdf.shape[0]}")

    print(f"Gold feature tables built successfully from start date: {snapshot_date_str}")
