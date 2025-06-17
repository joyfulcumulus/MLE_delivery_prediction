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
import argparse

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, BooleanType


def process_silver_olist_customers(bronze_directory, silver_directory, spark):
    
    # connect to bronze table
    partition_name = "bronze_olist_customers.parquet"
    filepath = bronze_directory + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())

    # clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "customer_id": StringType(),
        "customer_unique_id": StringType(),
        "customer_zip_code_prefix": StringType(),
        "customer_city": StringType(),
        "customer_state": StringType(),
    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))

    # Check customer_id duplicates (total rows - distinct ids)
    total_rows = df.count()
    distinct_rows = df.select("customer_id").distinct().count()
    duplicates_customer_id = total_rows - distinct_rows
    print(f"Number of duplicated 'customer_id': {duplicates_customer_id}")

    # Add missing leading zero
    df = df.withColumn(
        "customer_zip_code_prefix",
        F.lpad(col("customer_zip_code_prefix"), 5, "0")
    )
    
    # save silver table - IRL connect to database to write
    partition_name = "silver_olist_customers.parquet"
    filepath = silver_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    
    return df

def process_silver_olist_sellers(bronze_directory, silver_directory, spark):
    
    # connect to bronze table
    partition_name = "bronze_olist_sellers.parquet"
    filepath = bronze_directory + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())

    # clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "seller_id": StringType(),
        "seller_zip_code_prefix": StringType(),
        "seller_city": StringType(),
        "seller_state": StringType(),
    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))

    # Check seller_id duplicates (total rows - distinct ids)
    total_rows = df.count()
    distinct_rows = df.select("seller_id").distinct().count()
    duplicates_seller_id = total_rows - distinct_rows
    print(f"Number of duplicated 'seller_id': {duplicates_seller_id}")

    # Add missing leading zero
    df = df.withColumn(
        "seller_zip_code_prefix",
        F.lpad(col("seller_zip_code_prefix"), 5, "0")
    )
    
    # save silver table - IRL connect to database to write
    partition_name = "silver_olist_sellers.parquet"
    filepath = silver_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    
    return df

def process_silver_olist_geolocation(bronze_directory, silver_directory, spark):
    
    # connect to bronze table
    partition_name = "bronze_olist_geolocation.parquet"
    filepath = bronze_directory + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())

    # clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "geolocation_zip_code_prefix": StringType(),
        "geolocation_lat": FloatType(),
        "geolocation_lng": FloatType(),
        "geolocation_city": StringType(),
        "geolocation_state": StringType(),
    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))

    # Add missing leading zero
    df = df.withColumn(
        "geolocation_zip_code_prefix",
        F.lpad(col("geolocation_zip_code_prefix"), 5, "0")
    )

    # Deduplicate zipcodes by just taking the centroid (mean of lat,lng)
    df_dedupe = df.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng")
    )
    
    # save silver table - IRL connect to database to write
    partition_name = "silver_olist_geolocation.parquet"
    filepath = silver_directory + partition_name
    df_dedupe.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    
    return df_dedupe