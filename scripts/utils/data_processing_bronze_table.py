import os
import glob
import shutil
import csv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark

import pyspark.sql.functions as F
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
    print("----> saved bronze:", out_path)
    
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



def process_olist_orders_bronze(bronze_root, spark, target_date_str):


    formatted = target_date_str.replace("-", "_")
    date_formatted= formatted
    print("date input NOW", date_formatted)

    # # Read bronze order table of specific date_str
    # # date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y_%m_%d") # Convert "YYYY-MM-DD" from airflow to "YYYY_MM_DD"
    # partition_name = f"bronze_olist_orders_{date_formatted}.csv"
    # filepath = os.path.join(bronze_directory, partition_name)

    # # Check if file exists
    # if not os.path.exists(filepath):
    #     print(f"[SKIP] No orders csv found for date: {date_formatted}")
    #     return None  # Early return
    
    # # If file is found, proceed to read CSV
    # df = spark.read.option("header", True).option("inferSchema", True).csv(filepath)





    # Read source data
    df = spark.read.csv("data/olist_orders_dataset.csv", header=True, inferSchema=True)
    
    # Convert timestamp and create snapshot_date as yyyy_mm_dd string
    df = df.withColumn("order_purchase_timestamp", col("order_purchase_timestamp").cast("timestamp"))
    df = df.withColumn("snapshot_date", date_format(col("order_purchase_timestamp"), "yyyy_MM_dd"))
    
    # # Extract distinct days (as strings)
    # days = df.select("snapshot_date").distinct().collect()
    # day_list = [row.snapshot_date for row in days]
    
    # Create output directory
    output_path = os.path.join(bronze_root, "orders")
    os.makedirs(output_path, exist_ok=True)
    
    
    # Filter data for current day
    daily_df = df.filter(col("snapshot_date") == date_formatted)
    
    # Extract day, month, and year from the string
    # day_part, month_part, year_part = day_str.split('_')
    filename = f"bronze_olist_orders_{date_formatted}.csv"
    final_filepath = os.path.join(output_path, filename)
    
    # Create temporary directory
    temp_dir = os.path.join(output_path, f"temp_{date_formatted}")
    os.makedirs(temp_dir, exist_ok=True)
    
    # Write to temporary directory
    daily_df.coalesce(1).write.csv(temp_dir, mode="overwrite", header=True)
    
    # Find the generated CSV file
    csv_files = glob.glob(os.path.join(temp_dir, "*.csv"))
    if not csv_files:
        print(f"Warning: No CSV file found in {temp_dir}")
        shutil.rmtree(temp_dir)
        
        
    # Move the CSV file to final location
    shutil.move(csv_files[0], final_filepath)
    
    # Clean up temporary directory
    shutil.rmtree(temp_dir)
    
    # Print status
    count = daily_df.count()
    print(f"Day {date_formatted}: {count} rows")
    print(f"----------> Saved to: {final_filepath}")
    
    return df










# # Process Orders
# def process_olist_orders_bronze(bronze_root, spark, target_date_str):
#     """
#     Write ONE daily CSV for `target_date_str` (YYYY-MM-DD).
#     If the day has no orders, output a header-only CSV.
#     Returns the full DataFrame (with snapshot_date column).
#     """
#     print('🟢🟢🟢🟢🟢 STARTING TO READ ORDERS TABLE🟢🟢🟢🟢🟢')
#     print("date input as airflow:", target_date_str)
#     # 1. Read full source file
#     df = spark.read.csv(
#         "data/olist_orders_dataset.csv",
#         header=True,
#         inferSchema=True
#     )
#     print('🔴🔴🔴🔴df_order table count: ',df.count())
#     # 2. Add snapshot_date (yyyy-MM-dd)
#     df = (
#         df.withColumn(
#             "order_purchase_timestamp",
#             col("order_purchase_timestamp").cast("timestamp")
#         )
#         .withColumn(
#             "snapshot_date",
#             date_format(col("order_purchase_timestamp"), "yyyy_MM_dd")
#         )
#     )

    
#     formatted = target_date_str.replace("-", "_")
#     target_date_str= formatted
#     print("date input NOW", target_date_str)

#     print('🔴🔴🔴🔴df_order table count AFTER ADDING SNPASHOT DATE COLUMN: ',df.count())
#     # 3. Filter for the requested date
#     daily_df = df.filter(col("snapshot_date") == target_date_str)


#     if daily_df.count() == 0:
#         print(f"⚠️ No orders found for {target_date_str}. Writing empty header-only CSV.")
#         return
#     df = daily_df

#     print('🔴🔴🔴🔴df_order table count AFTER FTILER TO SNAPSHOT DATE: ',df.count())
#     output_path = os.path.join(bronze_root, "orders")
#     os.makedirs(output_path, exist_ok=True)

#     # year, month, day = target_date_str
#     filename = f"bronze_olist_orders_{target_date_str}.csv"
#     final_filepath = os.path.join(output_path, filename)

#     # Write as Parquet
#     # (df.write
#     #    .mode("overwrite")
#     #    .parquet(final_filepath))
#     # print("----> saved bronze:", final_filepath)

#     (df.write
#         .mode("overwrite")    # or "append"
#         .option("header", True)   # optional: include column names
#         .csv(final_filepath))

#     print("----> saved bronze as CSV:", final_filepath)


#     # temp_dir = os.path.join(output_path, f"temp_{target_date_str}")
#     # os.makedirs(temp_dir, exist_ok=True)

#     # # Write (may be empty). Header=True ensures header row *if* Spark emits a file
#     # daily_df.coalesce(1).write.csv(temp_dir, mode="overwrite", header=True)

#     # csv_files = glob.glob(os.path.join(temp_dir, "*.csv"))

#     # if csv_files:
#     #     # Normal case: Spark produced a part-*.csv file
#     #     shutil.move(csv_files[0], final_filepath)
#     # else:
#     #     # Edge case: no data → Spark created only _SUCCESS (no CSV).
#     #     # Manually create header-only CSV so downstream jobs still find a file.
#     #     with open(final_filepath, "w", newline="") as f:
#     #         writer = csv.writer(f)
#     #         writer.writerow(daily_df.columns)        # header only
#     #     print(f"Empty day: wrote header-only file {final_filepath}")

#     # Clean up temp dir
#     shutil.rmtree(temp_dir, ignore_errors=True)

#     # Row count for logging (safe even if zero)
    
#     count = df.count()
    
#     print(f"{target_date_str}: {count} rows -> {final_filepath}")
#     print('🔴🔴🔴🔴count OF rows in DATAFRAM returned: ',df.count())

    
#     return df


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
    return _read_write_bronze(
        "data/olist_order_reviews_dataset.csv",
        os.path.join(bronze_root, "order_reviews"),
        "bronze_olist_order_reviews",
        spark
    )
# -------------------------------------------------------------------------------------------------------------