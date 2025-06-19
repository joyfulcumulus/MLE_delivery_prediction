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


def process_feature_gold_table(snapshot_date_str, gold_directory, cust_df, geo_df, sellers_df, orders_df, spark):
# def process_feature_gold_table(snapshot_date_str, gold_directory, 
#                           cust_df, geo_df, items_df, logistic_df, prod_df, sellers_df, orders_df, spark):
  
    orders_df = orders_df.withColumn(
        'order_purchase_timestamp',
        F.to_date(col('order_purchase_timestamp'))
    )
    
    # define distance between customers and sellers
    
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    haversine_udf = udf(haversine, DoubleType())
    
    order_cust_seller = orders_df.select('order_id', 'customer_id', 'seller_id')

    cust_zip = cust_df.select(col('customer_id'), col('zipprefix').alias('cust_zipprefix'))
    seller_zip = sellers_df.select(col('seller_id'), col('zipprefix').alias('seller_zipprefix'))

    geo_cust = geo_df.select(col('zipprefix').alias('cust_zipprefix'), 
                             col('lat').alias('cust_lat'), col('lon').alias('cust_lon'))
    geo_seller = geo_df.select(col('zipprefix').alias('seller_zipprefix'), 
                               col('lat').alias('seller_lat'), col('lon').alias('seller_lon'))

    df = order_cust_seller \
        .join(cust_zip, on='customer_id', how='left') \
        .join(seller_zip, on='seller_id', how='left') \
        .join(geo_cust, on='cust_zipprefix', how='left') \
        .join(geo_seller, on='seller_zipprefix', how='left')

    df = df.withColumn(
        'customer_seller_distance_km',
        haversine_udf('cust_lat', 'cust_lon', 'seller_lat', 'seller_lon')
    )

    filtered_orders_df = orders_df.filter(col('order_purchase_timestamp') == snapshot_date_str)
    df = filtered_orders_df.join(
        df.select('order_id', 'customer_seller_distance_km'),
        on='order_id',
        how='left'
    )
    
    # Filter orders_df to only include rows where order_purchase_timestamp matches snapshot_date_str
    df = orders_df.filter(col('order_purchase_timestamp') == snapshot_date_str)[['customer_id', 'order_purchase_timestamp']]

    # Save gold table - output only for the given snapshot date
    partition_name = f"gold_feature_store_{snapshot_date_str.replace('-','_')}.parquet"
    filepath = os.path.join(gold_directory + "/feature_store/" + partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    print('Feature gold table processing completed for snapshot date:', snapshot_date_str)

    return df
