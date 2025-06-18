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
    filepath = os.path.join(gold_directory + "/feature_store/" + partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    print('Feature gold table processing completed for snapshot date:', snapshot_date_str)

    return df
