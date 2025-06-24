import glob
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


## DERIVED TABLES









def process_silver_seller_performance(bronze_directory, silver_directory, spark, date_str):
    date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y_%m_%d")
    target_date   = date_str  # YYYY-MM-DD 

    # reviews (bronze table)
    reviews_path = os.path.join(bronze_directory,"order_reviews","bronze_olist_order_reviews.parquet")

    if not os.path.exists(reviews_path):
        print(f"[SKIP] No order reviews parquet found at: {reviews_path}")
        return None

    df_reviews = (spark.read.parquet(reviews_path).withColumn("review_score_f", F.col("review_score").cast(DoubleType())))
    print(f"loaded reviews: {df_reviews.count()} rows")

    # items (silver)
    items_path = os.path.join(silver_directory,"order_items","silver_olist_order_items.parquet")
    if not os.path.exists(items_path):
        print(f"[SKIP] No order_items parquet found at: {items_path}")
        return None

    df_items = (spark.read.parquet(items_path).select("order_id", "seller_id"))
    print(f"loaded items: {df_items.count()} rows")

    # delivery history (silver)
    delivery_dir = os.path.join(silver_directory, "delivery_history")
    all_delivs = sorted(glob.glob(os.path.join(delivery_dir, "silver_delivery_history_*.parquet")))

    # split into prior (< date_formatted) and today
    prior = [p for p in all_delivs
             if os.path.basename(p).replace("silver_delivery_history_", "").replace(".parquet", "")
             < date_formatted]
    today = os.path.join(delivery_dir, f"silver_delivery_history_{date_formatted}.parquet")

    # if neither prior nor today exist, skip
    if not prior and not os.path.exists(today):
        print(f"[SKIP] No delivery history files for date: {date_formatted}")
        return None

    # load prior
    if prior:
        df_hist_delivery = (
            spark.read.parquet(*prior)
                 .select("order_id", "miss_delivery_sla")
                 .withColumn("miss_sla_f", F.col("miss_delivery_sla").cast(DoubleType()))
                 .select("order_id", "miss_sla_f")
        )
    else:
        schema = StructType([
            StructField("order_id",  StringType(), True),
            StructField("miss_sla_f", DoubleType(), True),
        ])
        df_hist_delivery = spark.createDataFrame([], schema)

    # load today
    if os.path.exists(today):
        df_today_delivery = (
            spark.read.parquet(today)
                .select("order_id", "miss_delivery_sla")
                .withColumn("miss_sla_f", F.col("miss_delivery_sla").cast(DoubleType()))
                .select("order_id", "miss_sla_f")
        )
        print(f"loaded delivery_history: {df_hist_delivery.count()} prior rows, {df_today_delivery.count()} today’s rows")
    else:
        print(f"[SKIP] No delivery file for today ({date_formatted}), proceeding with only prior data")
        # create an empty DF with the same schema so union still works
        df_today_delivery = spark.createDataFrame([], df_hist_delivery.schema)
    
    df_delivery = df_hist_delivery.unionByName(df_today_delivery)
    print(f"combined delivery: {df_delivery.count()} rows")

    # orders (silver): split prior + today
    orders_dir = os.path.join(silver_directory, "orders")
    order_file = os.path.join(orders_dir, f"silver_olist_orders_{date_formatted}.parquet")

    all_orders = sorted(glob.glob(os.path.join(orders_dir, "silver_olist_orders_*.parquet")))
    prior_orders = [p for p in all_orders
                    if os.path.basename(p).replace("silver_olist_orders_", "").replace(".parquet", "")
                    < date_formatted]

    if not prior_orders and not os.path.exists(order_file):
        print(f"[SKIP] No orders parquet for date: {date_formatted}")
        return None

    # load prior orders
    if prior_orders:
        df_hist_orders = (
            spark.read.parquet(*prior_orders)
                 .select("order_id", "snapshot_date", "order_purchase_timestamp", "order_delivered_customer_date")
                 .filter(F.col("snapshot_date") < F.lit(target_date))
        )
    else:
        order_schema = StructType([
            StructField("order_id",                     StringType(), True),
            StructField("snapshot_date",                StringType(), True),
            StructField("order_purchase_timestamp",     TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
        ])
        df_hist_orders = spark.createDataFrame([], order_schema)

    # load today’s orders
    if os.path.exists(order_file):
        df_today_orders = (
            spark.read.parquet(order_file)
                .select("order_id", "snapshot_date", "order_purchase_timestamp", "order_delivered_customer_date")
                .filter(F.col("snapshot_date") == F.lit(target_date))
        )
        print(f"loaded orders: {df_hist_orders.count()} prior rows, {df_today_orders.count()} today’s rows")
    else:
        print(f"[SKIP] No orders file for today ({date_formatted}), proceeding with only prior orders")
        # create an empty DataFrame with the same schema so the union still works
        df_today_orders = spark.createDataFrame([], df_hist_orders.schema)

    # now safe to union
    df_orders = df_hist_orders.unionByName(df_today_orders)

    # enrich & compute processing_time
    df_joined = (
        df_orders
        .join(df_items,    "order_id", "inner")
        .join(df_reviews,  "order_id", "left")
        .join(df_delivery, "order_id", "left")
        .withColumn("processing_time",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp").cast(DoubleType()))
    )

    # rolling window (exclude current row)
    w = Window.partitionBy("seller_id") \
              .orderBy("order_purchase_timestamp") \
              .rowsBetween(Window.unboundedPreceding, -1)

    df_with_hist = (
        df_joined
        .withColumn("hist_avg_delay_rate",
            F.avg(F.when(F.col("snapshot_date") < F.lit(target_date), F.col("miss_sla_f"))).over(w)
        )
        .withColumn("hist_avg_processing_time",
            F.avg(F.when(F.col("snapshot_date") < F.lit(target_date), F.col("processing_time"))).over(w)
        )
        .withColumn("hist_avg_review_score",
            F.avg(F.when(F.col("snapshot_date") < F.lit(target_date), F.col("review_score_f"))).over(w)
        )
    )

    # aggregate only today & save
    df_performance = (
        df_with_hist
        .filter(F.col("snapshot_date") == F.lit(target_date))
        .groupBy("snapshot_date", "seller_id")
        .agg(
            F.avg("hist_avg_review_score").alias("avg_rating"),
            F.avg("hist_avg_delay_rate").alias("avg_delay_rate"),
            F.avg("hist_avg_processing_time").alias("avg_processing_time")
        )
    )

    out_partition = f"silver_seller_performance_{date_formatted}.parquet"
    out_path      = os.path.join(silver_directory, "seller_performance", out_partition)
    df_performance.write.mode("overwrite").parquet(out_path)
    print('saved to:', out_path)

    return df_performance


def process_silver_concentration(silver_directory, spark, date_str):
    # Convert "YYYY-MM-DD" to "YYYY_MM_DD"
    date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y_%m_%d")

    # build the shipping_infos parquet path
    partition_name = f"silver_shipping_infos_{date_formatted}.parquet"
    filepath = os.path.join(silver_directory, "shipping_infos", partition_name)

    # Check if file exists
    if not os.path.exists(filepath):
        print(f"[SKIP] No shipping infos parquet found for date: {date_formatted}")
        return None  # Early return

    # read the shipping infos
    shipping_info_df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', shipping_info_df.count())
    
    # Define combinations for each region
    combos = [
        ("customer", "customer_city",  "city"),
        ("customer", "customer_state", "state"),
        ("seller",   "seller_city",    "city"),
        ("seller",   "seller_state",   "state"),
    ]
    
    concentration_dfs = []
    for actor, col_name, level in combos:
        region_df = (
            shipping_info_df
            .select("snapshot_date", F.col(col_name).alias("region"))
            .withColumn("type", F.lit(actor))
            .withColumn("granularity_level", F.lit(level))
        )
        
        count_df = (
            region_df
            .groupBy("snapshot_date", "type", "granularity_level", "region")
            .agg(F.count("*").alias("region_count"))
        )
        
        window_spec = Window.partitionBy("snapshot_date", "type", "granularity_level")
        pct_df = (
            count_df
            .withColumn("total_count", F.sum("region_count").over(window_spec))
            .withColumn("concentration", F.expr("region_count / total_count * 100"))
            .select("snapshot_date", "granularity_level", "type", "region", "concentration")
        )
        
        concentration_dfs.append(pct_df)
    
    # Union all
    final_concentration_df = concentration_dfs[0]
    for part_df in concentration_dfs[1:]:
        final_concentration_df = final_concentration_df.unionByName(part_df)
    
    # save silver table
    out_partition = f"silver_concentration_{date_formatted}.parquet"
    out_path      = os.path.join(silver_directory, "concentration", out_partition)
    final_concentration_df.write.mode("overwrite").parquet(out_path)
    print('saved to:', out_path)
    
    return final_concentration_df