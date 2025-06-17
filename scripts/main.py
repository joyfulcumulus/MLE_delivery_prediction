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
import utils.data_processing_silver_table as silver_processing

def main():
    print('\n\n---starting job---\n\n')

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
    
        # Create silver root directory
        silver_root = "datamart/silver"
        os.makedirs(silver_root, exist_ok=True)
        print(f"Silver root directory: {silver_root}")
        
        # Create all required output directories
        # Create silver directory to save customer data
        silver_cust_directory = "datamart/silver/customers/"
        if not os.path.exists(silver_cust_directory):
            os.makedirs(silver_cust_directory)
        
        # Create silver directory to save seller data
        silver_sell_directory = "datamart/silver/sellers/"
        if not os.path.exists(silver_sell_directory):
            os.makedirs(silver_sell_directory)
        
        # Create silver directory to save geolocation data
        silver_geo_directory = "datamart/silver/geolocation/"
        if not os.path.exists(silver_geo_directory):
            os.makedirs(silver_geo_directory)
        
        # Process all bronze tables into silver
        print("\nProcessing bronze tables...")
        silver_processing.process_silver_olist_customers("datamart/bronze/customers/",silver_cust_directory, spark)
        silver_processing.process_silver_olist_sellers("datamart/bronze/sellers/",silver_sell_directory, spark)
        silver_processing.process_silver_olist_geolocation("datamart/bronze/geolocation/",silver_geo_directory, spark)
        # add more below

        # End spark session
        spark.stop()

        print('\n\n---completed job---\n\n')

    finally:
        if spark is not None:
            spark.stop()
            print("\nSpark session stopped (under finally).\n")

if __name__ == "__main__":
    # SIMPLIFIED PARSER (no arguments needed)
    parser = argparse.ArgumentParser(description="Run Olist data processing job")
    args = parser.parse_args()
    main()  # CALL WITHOUT ARGUMENTS