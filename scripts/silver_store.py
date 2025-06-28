import argparse
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

import utils.data_processing_silver_table as silver_processing

if __name__ == "__main__":
    print('\n\n🔵🔵🔵---starting job---🔵🔵🔵\n\n')
    
    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description='Process snapshot date from Airflow for silver tables')
    parser.add_argument('--startdate', type=str, required=True, help='Snapshot date in YYYY-MM-DD format')
    args = parser.parse_args()
    snapshot_date_str = args.startdate

    spark = None
    try:
        # Initialize SparkSession
        spark = pyspark.sql.SparkSession.builder \
            .appName("olist_silver_processing") \
            .master("local[*]") \
            .getOrCreate()
        
        # Set log level to ERROR to hide warnings
        spark.sparkContext.setLogLevel("ERROR")

        # Create silver root directory
        silver_root = "datamart/silver"
        os.makedirs(silver_root, exist_ok=True)
        print(f"Silver root directory: {silver_root}")

        bronze_root = "datamart/bronze"
        
        # Create all required output directories (Silver tables first, then derived silver tables)
        # Customers
        silver_cust_directory = "datamart/silver/customers/"
        if not os.path.exists(silver_cust_directory):
            os.makedirs(silver_cust_directory)
        
        # Sellers
        silver_sell_directory = "datamart/silver/sellers/"
        if not os.path.exists(silver_sell_directory):
            os.makedirs(silver_sell_directory)
        
        # Geolocation
        silver_geo_directory = "datamart/silver/geolocation/"
        if not os.path.exists(silver_geo_directory):
            os.makedirs(silver_geo_directory)

        # Products
        silver_prod_directory = "datamart/silver/products/"
        if not os.path.exists(silver_prod_directory):
            os.makedirs(silver_prod_directory)

        # Order items
        silver_order_items_directory = "datamart/silver/order_items/"
        if not os.path.exists(silver_order_items_directory):
            os.makedirs(silver_order_items_directory)

        # Orders
        silver_orders_directory = "datamart/silver/orders/"
        if not os.path.exists(silver_orders_directory):
            os.makedirs(silver_orders_directory)

        # Shipping_Infos
        silver_shipping_infos_directory = "datamart/silver/shipping_infos/"
        if not os.path.exists(silver_shipping_infos_directory):
            os.makedirs(silver_shipping_infos_directory)

        # Order_Logistics
        silver_order_logistics_directory = "datamart/silver/order_logistics/"
        if not os.path.exists(silver_order_logistics_directory):
            os.makedirs(silver_order_logistics_directory)

        # Delivery_History
        silver_delivery_history_directory = "datamart/silver/delivery_history/"
        if not os.path.exists(silver_delivery_history_directory):
            os.makedirs(silver_delivery_history_directory)

        # Seller_Performance
        silver_seller_perf_directory = "datamart/silver/seller_performance/"
        if not os.path.exists(silver_seller_perf_directory):
            os.makedirs(silver_seller_perf_directory)

        # Concentration
        silver_concentration_directory = "datamart/silver/concentration/"
        if not os.path.exists(silver_concentration_directory):
            os.makedirs(silver_concentration_directory)
        
        # Process bronze tables into silver
        # Due to dependencies in orders table validation checks, customers, sellers, order_items processed first 
        # For tables with no partition (due to small size), meaningless to pass in snapshot_date_str, every day will overwrite with latest info
        print("\n🟡🟡🟡Creating Silver Tables🟡🟡🟡\n")
        silver_processing.process_silver_olist_customers("datamart/bronze/customers/",silver_cust_directory, spark)
        print("\n")
        silver_processing.process_silver_olist_sellers("datamart/bronze/sellers/",silver_sell_directory, spark)
        print("\n")
        silver_processing.process_silver_olist_geolocation("datamart/bronze/geolocation/",silver_geo_directory, spark)
        print("\n")
        silver_processing.process_silver_olist_products("datamart/bronze/products/",silver_prod_directory, spark)
        print("\n")
        silver_processing.process_silver_olist_order_items("datamart/bronze/order_items/",silver_order_items_directory, spark)
        print("\n")
        silver_processing.process_silver_olist_orders("datamart/bronze/orders/",silver_orders_directory, spark, date_str=snapshot_date_str)
        print("\n")
        
        print("\n🟡🟡🟡Creating Derived Silver Tables🟡🟡🟡\n")
        silver_processing.process_silver_shipping_infos(silver_shipping_infos_directory, spark, date_str=snapshot_date_str)
        print("\n")
        silver_processing.process_silver_order_logistics(silver_order_logistics_directory, spark, date_str=snapshot_date_str)
        print("\n")
        silver_processing.process_silver_delivery_history(silver_delivery_history_directory, spark, date_str=snapshot_date_str)
        print("\n")

        # # Derived Silver Tables that are dependent on other Derived Silver Tables
        silver_processing.process_silver_seller_performance(bronze_root, silver_root, spark, date_str=snapshot_date_str)
        print("\n")
        silver_processing.process_silver_concentration(silver_root, spark, date_str=snapshot_date_str)

        print('\n\n🔵🔵🔵---completed job---🔵🔵🔵\n\n')

    finally:
        if spark is not None:
            spark.stop()
            print("\nSpark session stopped.\n")
