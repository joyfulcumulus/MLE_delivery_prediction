import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark

import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

import utils.data_processing_bronze_table as bronze_processing

def main():
    print('\n\n---starting job---')

    spark = None
    try:
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
        print("\nProcessing Olist raw data...")
        bronze_processing.process_olist_customers_bronze(bronze_root, spark)
        bronze_processing.process_olist_geolocation_bronze(bronze_root, spark)
        bronze_processing.process_olist_order_items_bronze(bronze_root, spark)
        bronze_processing.process_olist_order_reviews_bronze(bronze_root, spark)
        bronze_processing.process_olist_products_bronze(bronze_root, spark)
        bronze_processing.process_olist_sellers_bronze(bronze_root, spark)
        bronze_processing.process_product_cat_translation_bronze(bronze_root, spark)
        
        # Process orders with monthly partitioning
        bronze_processing.process_olist_orders_bronze(bronze_root, spark)

        print('\n\n---completed job---\n\n')

    finally:
        if spark is not None:
            spark.stop()
            print("\nSpark session stopped.\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate bronze store")
    args = parser.parse_args()
    main()  # No arguments required