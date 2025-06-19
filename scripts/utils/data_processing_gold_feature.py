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


def process_feature_gold_table(snapshot_date_str, gold_directory, 
                          cust_df, geo_df, items_df, prod_df, sellers_df, orders_df, spark):

    # orders_df
    orders_df = orders_df.withColumn(
        'order_purchase_timestamp',
        F.to_date(col('order_purchase_timestamp'))
    )
    df = orders_df.filter(col('order_purchase_timestamp') == F.lit(snapshot_date_str))
    df = df.select('order_id', 'customer_id', 'order_status', 'order_purchase_timestamp')

    # join items_df
    df = df.join(items_df, on='order_id', how='left')
    df = df.drop('shipping_limit_date')

    # join products_df
    df = df.join(prod_df, on='product_id', how='left')

    # group orders by order_id
    df = df.groupBy('order_id').agg(
        F.first('customer_id').alias('customer_id'),
        F.first('product_id').alias('product_id'),
        F.first('seller_id').alias('seller_id'),
        F.count('*').alias('total_qty'),
        F.sum('price').alias('total_price'),
        F.max('freight_value').alias('total_freight_value'),
        F.sum('product_weight_g').alias('total_weight_g'),
        F.sum(F.col('product_length_cm') * F.col('product_height_cm') * F.col('product_width_cm')).alias('total_volume_cm3'),
        F.first('order_purchase_timestamp').alias('order_purchase_timestamp'),
        F.first('order_status').alias('order_status')
    )

    df = df.withColumn(
        'total_density',
        F.col('total_weight_g') / F.col('total_volume_cm3')
    )

    df = df.drop('product_id', 'order_id', 'customer_id', 'seller_id', 'order_purchase_timestamp')
    
    # Save gold table - output only for the given snapshot date
    partition_name = f"{snapshot_date_str.replace('-','_')}.parquet"
    filepath = os.path.join(gold_directory + "/feature_store/" + partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)
    print('Feature gold table processing completed for snapshot date:', snapshot_date_str)

    return df
