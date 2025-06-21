import os
import glob
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from pyspark.sql.functions import date_format, to_date
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

def _read_write_bronze(csv_path: str,
                       out_dir: str,
                       out_name: str,
                       spark):
    """
    Helper that reads `csv_path`, writes `out_dir/out_name.parquet`,
    cleans up extra files, and returns the DataFrame.
    """
    df = spark.read.option("header", True).csv(csv_path)
    print(f"loaded {csv_path}  →  {df.count():,d} rows")

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{out_name}.parquet")

    # Write as Parquet
    (df.write
       .mode("overwrite")
       .parquet(out_path))
    print("saved bronze:", out_path)
    
    # Clean up extra files (_SUCCESS and .crc files)
    for file in glob.glob(os.path.join(out_path, "_SUCCESS")):
        os.remove(file)
    for file in glob.glob(os.path.join(out_path, "*.crc")):
        os.remove(file)
    for file in glob.glob(os.path.join(out_path, ".*.crc")):  # For hidden .crc files
        os.remove(file)
    
    return df

# Process Olist Datasets and save as Bronze Tables ------------------------------------------------------------

# Process Customers
def process_olist_customers_bronze(bronze_root: str, spark):
    return _read_write_bronze(
        "data/olist_customers_dataset.csv",
        os.path.join(bronze_root, "customers"),
        "bronze_olist_customers",
        spark)
# -------------------------------------------------------------------------------------------------------------

# Process Geolocation
def process_olist_geolocation_bronze(bronze_root: str, spark):
    return _read_write_bronze(
        "data/olist_geolocation_dataset.csv",
        os.path.join(bronze_root, "geolocation"),
        "bronze_olist_geolocation",
        spark)
# -------------------------------------------------------------------------------------------------------------

# Process Order_Items
def process_olist_order_items_bronze(bronze_root: str, spark):
    return _read_write_bronze(
        "data/olist_order_items_dataset.csv",
        os.path.join(bronze_root, "order_items"),
        "bronze_olist_order_items",
        spark)
# -------------------------------------------------------------------------------------------------------------

# Process Order_Payments
def process_olist_orders_bronze(bronze_root, spark):
    # Read source data
    df = spark.read.csv("data/olist_orders_dataset.csv", header=True, inferSchema=True)
    
    # Convert timestamp and create snapshot_date as yyyy_mm_dd string
    df = df.withColumn("order_purchase_timestamp", col("order_purchase_timestamp").cast("timestamp"))
    df = df.withColumn("snapshot_date", date_format(col("order_purchase_timestamp"), "yyyy_MM_dd"))
    
    # Extract distinct days (as strings)
    days = df.select("snapshot_date").distinct().collect()
    day_list = [row.snapshot_date for row in days]
    
    # Create output directory
    output_path = os.path.join(bronze_root, "orders")
    os.makedirs(output_path, exist_ok=True)
    
    for day_str in day_list:
        # Filter data for current day
        daily_df = df.filter(col("snapshot_date") == day_str)
        
        # Extract day, month, and year from the string
        day_part, month_part, year_part = day_str.split('_')
        filename = f"bronze_olist_orders_{day_part}_{month_part}_{year_part}.csv"
        final_filepath = os.path.join(output_path, filename)
        
        # Create temporary directory
        temp_dir = os.path.join(output_path, f"temp_{day_str}")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Write to temporary directory
        daily_df.coalesce(1).write.csv(temp_dir, mode="overwrite", header=True)
        
        # Find the generated CSV file
        csv_files = glob.glob(os.path.join(temp_dir, "*.csv"))
        if not csv_files:
            print(f"Warning: No CSV file found in {temp_dir}")
            shutil.rmtree(temp_dir)
            continue
            
        # Move the CSV file to final location
        shutil.move(csv_files[0], final_filepath)
        
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
        
        # Print status
        count = daily_df.count()
        print(f"Day {day_str}: {count} rows")
        print(f"Saved to: {final_filepath}")
    
    return df
# -------------------------------------------------------------------------------------------------------------

# Process Products
def process_olist_products_bronze(bronze_root: str, spark):
    return _read_write_bronze(
        "data/olist_products_dataset.csv",
        os.path.join(bronze_root, "products"),
        "bronze_olist_products",
        spark)
# -------------------------------------------------------------------------------------------------------------

# Process Sellers
def process_olist_sellers_bronze(bronze_root: str, spark):
    return _read_write_bronze(
        "data/olist_sellers_dataset.csv",
        os.path.join(bronze_root, "sellers"),
        "bronze_olist_sellers",
        spark)
# -------------------------------------------------------------------------------------------------------------

# Process Product_Category_Translation
def process_product_cat_translation_bronze(bronze_root: str, spark):
    return _read_write_bronze(
        "data/product_category_name_translation.csv",
        os.path.join(bronze_root, "category_translation"),
        "bronze_product_category_translation",
        spark)
# -------------------------------------------------------------------------------------------------------------

# Process Order_Reviews
def process_olist_order_reviews_bronze(bronze_root: str, spark):
    """
    Reads the olist order reviews CSV and writes it out as a bronze Parquet table.
    """
    return _read_write_bronze(
        "data/olist_order_reviews_dataset.csv",
        os.path.join(bronze_root, "order_reviews"),
        "bronze_olist_order_reviews",
        spark
    )
# -------------------------------------------------------------------------------------------------------------