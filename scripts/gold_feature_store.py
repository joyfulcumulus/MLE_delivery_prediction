import os
import argparse
from datetime import datetime
import pyspark
import glob

from utils.data_processing_gold_feature import process_feature_gold_table

def read_silver_table(table, silver_directory, spark):
    """
    Helper function to read all partitions of a silver table
    """
    folder_path = os.path.join(silver_directory, table)
    files_list = [os.path.join(folder_path, os.path.basename(f)) for f in glob.glob(os.path.join(folder_path, '*'))]
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
    snapshot_date_str = [args.startdate]

    
    ############################
    # GOLD
    ############################
    print("Building gold feature tables...")

    # Define paths
    silver_directory = "datamart/silver"
    gold_directory = "datamart/gold"
    os.makedirs(gold_directory, exist_ok=True)
    print(f"Gold root directory: {gold_directory}")

    gold_feature_directory = "datamart/gold/feature_store/"
    if not os.path.exists(gold_feature_directory):
        os.makedirs(gold_feature_directory)

    cust_df = read_silver_table('customers', silver_directory, spark)
    geo_df = read_silver_table('geolocation', silver_directory, spark)
    items_df = read_silver_table('order_items', silver_directory, spark)\
    # # to be changed
    # logistic_df = read_silver_table('order_logistics', silver_directory, spark)
    prod_df = read_silver_table('products', silver_directory, spark)
    sellers_df = read_silver_table('sellers', silver_directory, spark)
    orders_df = read_silver_table('orders', silver_directory, spark)

    # Build gold tables
    y = process_feature_gold_table(snapshot_date_str, gold_directory, 
                          cust_df, geo_df, items_df, prod_df, sellers_df, orders_df, spark)
    # def process_feature_gold_table(snapshot_date_str, gold_directory, 
#                           cust_df, geo_df, items_df, logistic_df, prod_df, sellers_df, orders_df, spark)

    # Check for the rows ingested
    y_pdf = y.toPandas()
    y_count = y_pdf.shape[0]
    print(f"Number of rows in feature store: {y_pdf.shape[0]}")

    print(f"Gold feature tables built successfully from start date: {snapshot_date_str}")






