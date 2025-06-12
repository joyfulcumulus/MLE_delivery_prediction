import argparse
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

import utils.data_processing_bronze_table as bronze_processing


def main():
    print('\n\n---starting job---\n\n')
    
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("olist_bronze_processing") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # Create bronze root directory
    bronze_root = "datamart/bronze"
    os.makedirs(bronze_root, exist_ok=True)
    print(f"Bronze root directory: {bronze_root}")

    # Process all Olist datasets
    print("\nProcessing Olist datasets...")
    bronze_processing.process_olist_customers_bronze(bronze_root, spark)
    bronze_processing.process_olist_geolocation_bronze(bronze_root, spark)
    bronze_processing.process_olist_order_items_bronze(bronze_root, spark)
    bronze_processing.process_olist_order_payments_bronze(bronze_root, spark)
    bronze_processing.process_olist_order_reviews_bronze(bronze_root, spark)
    bronze_processing.process_olist_products_bronze(bronze_root, spark)
    bronze_processing.process_olist_sellers_bronze(bronze_root, spark)
    bronze_processing.process_product_cat_translation_bronze(bronze_root, spark)
    
    # Process orders with monthly partitioning
    bronze_processing.process_olist_orders_bronze(bronze_root, spark)

    # End spark session
    spark.stop()
    
    print('\n\n---completed job---\n\n')

# Older Code (From assignment 1), can remove once certain don't need
# if __name__ == "__main__":
#     # Setup argparse to parse command-line arguments
#     parser = argparse.ArgumentParser(description="Run Olist bronze processing job")
#     parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD date for orders partitioning")
    
#     args = parser.parse_args()
    
#     # Validate date format
#     try:
#         datetime.strptime(args.snapshotdate, "%Y-%m-%d")
#     except ValueError:
#         raise ValueError("Incorrect date format, should be YYYY-MM-DD")
    
#     # Call main with arguments
#     main(args.snapshotdate)

if __name__ == "__main__":
    # SIMPLIFIED PARSER (no arguments needed)
    parser = argparse.ArgumentParser(description="Run Olist bronze processing job")
    args = parser.parse_args()
    main()  # CALL WITHOUT ARGUMENTS