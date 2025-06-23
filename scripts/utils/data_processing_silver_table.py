import argparse
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, trim, when, row_number, count, date_add, to_date, lit, datediff
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, DoubleType, TimestampType, LongType, StructType, StructField

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

def process_silver_olist_products(bronze_directory, silver_directory, spark):
    
    # connect to bronze table
    partition_name = "bronze_olist_products.parquet"
    filepath = bronze_directory + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())

    # Rename columns due to spelling mistakes 
    df = df.withColumnRenamed("product_name_lenght", "product_name_length") \
           .withColumnRenamed("product_description_lenght", "product_description_length")

    
    # clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "product_id": StringType(),
        "product_category_name": StringType(),
        "product_name_length": DoubleType(),
        "product_description_length": DoubleType(),
        "product_photos_qty": DoubleType(),
        "product_weight_g": DoubleType(),
        "product_length_cm": DoubleType(),
        "product_height_cm": DoubleType(),
        "product_width_cm": DoubleType(),
    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))

    # Inputting missing values as NaN
    df = df.fillna({"product_category_name": "NaN"})
    df = df.fillna({"product_name_length": float('nan')}) 
    df = df.fillna({"product_description_length": float('nan')}) 
    df = df.fillna({"product_photos_qty": float('nan')}) 
    
    # Check product_id duplicates (total rows - distinct ids)
    total_rows = df.count()
    distinct_rows = df.select("product_id").distinct().count()
    duplicates_product_id = total_rows - distinct_rows
    print(f"Number of duplicated 'product_id': {duplicates_product_id}")
    
    # Merge Product Category translation Table with Products table
    # Load the bronze table  
    df_cat_trans = spark.read.parquet("datamart/bronze/category_translation/bronze_product_category_translation.parquet")
    
    df = df.join(df_cat_trans, on='product_category_name', how='left')

    # Rename original cat name column to cat name portuguese
    df = df.withColumnRenamed("product_category_name", "product_category_name_portuguese")

    # Reorder columns for easy visualization
    
    desired_order = [
    "product_id",
    "product_category_name_portuguese",
    "product_category_name_english",
    "main_category",
    "sub_category",
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
    ]

    df = df.select(desired_order)


    # save silver table - IRL connect to database to write
    partition_name = "silver_olist_products.parquet"
    filepath = silver_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    
    return df

def process_silver_olist_order_items(bronze_directory, silver_directory, spark):
    
    # connect to bronze table
    partition_name = "bronze_olist_order_items.parquet"
    filepath = bronze_directory + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())

    
    # clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "order_id": StringType(),
        "order_item_id": LongType(),
        "product_id": StringType(),
        "seller_id": StringType(),
        "shipping_limit_date": TimestampType(),
        "price": DoubleType(),
        "freight_value": DoubleType(),
    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))

    
    # Checking for invalid seller IDs
    # Load df_sellers from SILVER <<<<<<----------------------------------<<<<<<<<<<<<<
    df_sellers = spark.read.parquet("datamart/silver/sellers/silver_olist_sellers.parquet")  

    # Get distinct valid seller IDs
    valid_seller_ids_df = df_sellers.select("seller_id").distinct()
    
    # Perform a left anti join to find sellers with invalid seller_id
    invalid_orders = df.join(valid_seller_ids_df, on="seller_id", how="left_anti")
    
    # Count how many invalid seller IDs there are
    invalid_seller_count = invalid_orders.count()

    # Conditionally drop invalid orders
    if invalid_seller_count > 0:
        initial_count = df.count()
        print("Dropping orders with invalid seller_id...")
        df = df.join(valid_seller_ids_df, on="seller_id", how="inner")
        final_count = df.count()
        dropped_count = initial_count - final_count
        print(f"Dropped {dropped_count} rows")
        
    else:
        print("All seller ids are valid — no need to drop!!")

    
    # save silver table - IRL connect to database to write
    partition_name = "silver_olist_order_items.parquet"
    filepath = silver_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    
    return df

def process_silver_olist_orders(bronze_directory, silver_directory, spark, date_str): # date_str replaces partition_name
    # Read bronze order table of specific date_str
    date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y_%m_%d") # Convert "YYYY-MM-DD" from airflow to "YYYY_MM_DD"
    partition_name = f"bronze_olist_orders_{date_formatted}.csv"
    filepath = os.path.join(bronze_directory, partition_name)

    # Check if file exists
    if not os.path.exists(filepath):
        print(f"[SKIP] No orders csv found for date: {date_formatted}")
        return None  # Early return
    
    # If file is found, proceed to read CSV
    df = spark.read.option("header", True).option("inferSchema", True).csv(filepath)
    print('loaded from:', filepath, 'row count:', df.count())

    # Clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "order_id": StringType(),
        "customer_id": StringType(),
        "order_status": StringType(),
        "order_purchase_timestamp": TimestampType(),
        "order_approved_at": TimestampType(),
        "order_delivered_carrier_date": TimestampType(),
        "order_delivered_customer_date": TimestampType(),
        "order_estimated_delivery_date": TimestampType(),
    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))

    # Removing Invalid order ids
    # Load the SILVER table  
    df_order_items = spark.read.parquet("datamart/silver/order_items/silver_olist_order_items.parquet") 
    
    # Get distinct order IDs that exist in order items
    valid_order_ids_df = df_order_items.select("order_id").distinct()
    
    # Keep only orders that exist in df_order_items
    df_orders_clean = df.join(valid_order_ids_df, on="order_id", how="inner")
    
    # Count how many were dropped
    dropped_orders = df.count() - df_orders_clean.count()
    print(f"Dropped {dropped_orders} orders with no items.")

    df = df_orders_clean


    # Checking for invalid customer IDs
    # Load df_customers from SILVER   
    df_customers = spark.read.parquet("datamart/silver/customers/silver_olist_customers.parquet")  

    # Get distinct valid customer IDs
    valid_customer_ids_df = df_customers.select("customer_id").distinct()
    
    # Perform a left anti join to find orders with invalid customer_id
    invalid_orders = df.join(valid_customer_ids_df, on="customer_id", how="left_anti")
    
    # Count how many invalid customer IDs there are
    invalid_customer_count = invalid_orders.count()

    # Conditionally drop invalid orders
    if invalid_customer_count > 0:
        initial_count = df.count()
        print("Dropping orders with invalid customer_id...")
        df = df.join(valid_customer_ids_df, on="customer_id", how="inner")
        final_count = df.count()
        dropped_count = initial_count - final_count
        print(f"Dropped {dropped_count} rows")
        
    else:
        print("All customer ids are valid — no need to drop!!")


    # Enforcing enum for order statuses
    # Define valid statuses 
    valid_statuses = {
        "created",
        "approved",
        "processing",
        "invoiced",
        "shipped",
        "delivered",
        "canceled",
        "unavailable"
    }
    
    # Clean and standardize the `order_status` column
    df = df.withColumn("order_status", trim(lower(col("order_status"))))
    
    # dentify invalid statuses (those NOT in the valid_statuses set)
    invalid_statuses_df = df.filter(~col("order_status").isin(list(valid_statuses)))
    
    # Print the unique invalid statuses
    invalid_statuses_list = invalid_statuses_df.select("order_status").distinct().rdd.flatMap(lambda x: x).collect()

    if invalid_statuses_list:
        print(f"Invalid statuses found: {invalid_statuses_list}")
    else:
        print("No invalid status found!!")

    # Adding snapshot date column
    df = df.withColumn("snapshot_date", to_date(lit(date_formatted), "yyyy_MM_dd"))

    # save 
    parquet_name = f"silver_olist_orders_{date_formatted}.parquet"
    output_path = os.path.join(silver_directory, parquet_name)
    df.write.mode("overwrite").parquet(output_path)
    print("-----> saved to:", output_path)

    return df