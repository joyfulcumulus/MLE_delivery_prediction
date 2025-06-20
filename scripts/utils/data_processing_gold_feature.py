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
from pyspark.sql.types import DoubleType
import math

def process_feature_gold_table(snapshot_date_str, gold_directory, items_df, logistic_df, 
                                   orders_df, spark):
# def process_feature_gold_table(snapshot_date_str, gold_directory, 
#                           cust_df, geo_df, items_df, prod_df, sellers_df, orders_df, spark):
# def process_feature_gold_table(snapshot_date_str, gold_directory, 
#                       cust_df, geo_df, items_df, logistic_df, prod_df, sellers_df, orders_df, 
#                       shipping_df, history_df, seller_perform_df, concentration_df, spark):

    # orders_dff
    df = orders_df
    df = df.select('order_id', 'customer_id', 'order_status')

    # join items_df
    df = df.join(items_df, on='order_id', how='left')
    df = df.drop('shipping_limit_date', 'order_item_id', 'price', 'freight_value')
    df = df.dropDuplicates(['order_id'])
    
    # join logistic_df
    df = df.join(logistic_df, on='order_id', how='left')
    df = df.drop('order_id', 'order_purchase_timestamp', 'main_category', 'sub_category', 'snapshot_date')

    
    # Save gold table - output only for the given snapshot date
    partition_name = f"{snapshot_date_str.replace('-','_')}.parquet"
    filepath = os.path.join(gold_directory + "/feature_store/" + partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    print('Feature gold table processing completed for snapshot date:', snapshot_date_str)

    return df
