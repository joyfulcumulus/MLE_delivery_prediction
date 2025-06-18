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
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType



def read_silver_table(table, silver_directory, spark):
    """
    Helper function to read all partitions of a silver table
    """
    folder_path = os.path.join(silver_directory, table)
    files_list = [os.path.join(folder_path, os.path.basename(f)) for f in glob.glob(os.path.join(folder_path, '*'))]
    df = spark.read.option("header", "true").parquet(*files_list)
    return df

# Define paths
silver_directory = "datamart/silver"
gold_directory = "datamart/gold"
os.makedirs(gold_directory, exist_ok=True)
print(f"Gold root directory: {gold_directory}")

gold_feature_directory = "datamart/gold/feature_store/"
if not os.path.exists(gold_feature_directory):
    os.makedirs(gold_feature_directory)

# cust_df = read_silver_table('orders', silver_directory, spark)
# geo_df = read_silver_table('orders', silver_directory, spark)
# items_df = read_silver_table('orders', silver_directory, spark)\
# # to be changed
# logistic_df = read_silver_table('orders', silver_directory, spark)
# prod_df = read_silver_table('orders', silver_directory, spark)
# sellers_df = read_silver_table('orders', silver_directory, spark)
orders_df = read_silver_table('orders', silver_directory, spark)

parser = argparse.ArgumentParser(description='Process snapshot date from Airflow')
parser.add_argument('--startdate', type=str, required=True, help='Snapshot date in YYYY-MM-DD format')
args = parser.parse_args()
snapshot_date_str = args.startdate


############################
# Pipeline
############################

def process_feature_gold_table(snapshot_date_str, silver_directory, gold_directory, orders_df, spark):
# def process_feature_gold_table(snapshot_date_str, silver_directory, gold_directory, 
#                           cust_df, geo_df, items_df, logistic_df, prod_df, sellers_df, orders_df, spark):
  
    orders_df = orders_df.withColumn(
        'order_purchase_timestamp',
        F.to_date(col('order_purchase_timestamp'))
    )
    
    # # connect to silver table - orders
    # partition_name = "silver_olist_orders_" + snapshot_date_str.replace('-','_') + '.parquet'
    # filepath = silver_directory + partition_name
    # orders_df = spark.read.parquet(filepath)
    # print('loaded from:', filepath, 'row count:', orders_df.count())

    
    # Filter orders_df to only include rows where order_purchase_timestamp matches snapshot_date_str
    df = orders_df.filter(col('order_purchase_timestamp') == snapshot_date_str)[['customer_id', 'order_purchase_timestamp']]

    # Save gold table - output only for the given snapshot date
    partition_name = f"gold_feature_store_{snapshot_date_str.replace('-','_')}.parquet"
    filepath = os.path.join(gold_directory, partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    print('Feature gold table processing completed for snapshot date:', snapshot_date_str)
