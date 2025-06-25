import os
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
from pyspark.sql.functions import udf
from pyspark.sql.functions import dayofweek, date_format, to_date, when, month
from pyspark.sql.types import DoubleType
import math

def process_feature_gold_table(snapshot_date_str, gold_directory, items_df, logistic_df, 
                                   orders_df, shipping_df, history_df, seller_perform_df, concentration_df,spark):

    # orders_df
    df = orders_df
    df = df.select('order_id', 'customer_id', 'order_status')

    # join items_df
    df = df.join(items_df, on='order_id', how='left')
    df = df.drop('shipping_limit_date', 'order_item_id', 'price', 'freight_value', 'snapshot_date')
    df = df.dropDuplicates(['order_id'])
    
    # join logistic_df
    df = df.join(logistic_df, on='order_id', how='left')
    df = df.drop('order_purchase_timestamp', 'main_category', 'sub_category', 'snapshot_date')

    # join shipping_df
    df = df.join(shipping_df, on='order_id', how='left')
    df = df.drop('order_purchase_timestamp', 'customer_zip_code_prefix',
       'customer_city', 'customer_state', 'customer_lat', 'customer_lng',
       'seller_zip_code_prefix', 'seller_lat', 'seller_lng', 'same_zipcode')
    
    # join history_df
    df = df.join(history_df, on='order_id', how='left')
    df = df.drop('order_purchase_timestamp', 'approval_duration', 
                 'processing_duration', 'ship_duration', 'miss_delivery_sla', 'snapshot_date')

    # join seller_perform_df
    df = df.join(seller_perform_df, on='seller_id', how='left')

    # make new columns: day_of_week, season
    df = df.withColumn("date", to_date("snapshot_date", "yyyy-MM-dd"))
    # 1: Sunday
    df = df.withColumn("day_of_week", dayofweek("date"))

    df = df.withColumn("month", month("date"))
    df = df.withColumn("season",
        when(month("date").isin([7, 8, 9]), "Winter")
        .when(month("date").isin([10, 11, 12]), "Spring")
        .when(month("date").isin([1, 2, 3]), "Summer")
        .otherwise("Autumn")
    )
    
    # join concentration_df
    df = df.join(concentration_df, on='snapshot_date', how='left')
    df = df.drop('granularity_level', 'type', 'region')

    # drop unused columns
    df = df.drop('customer_id', 'product_id', 'seller_id', 'date', 'month')
    df = df.dropDuplicates(['order_id'])
    
    # Save gold table - output only for the given snapshot date
    partition_name = f"{snapshot_date_str.replace('-','_')}.parquet"
    filepath = os.path.join(gold_directory + "/feature_store/" + partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    print('Feature gold table processing completed for snapshot date:', snapshot_date_str)

    return df