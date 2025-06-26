
import os
import glob
import pyspark
import pyspark.sql.functions as F
from tqdm import tqdm
from pyspark.sql.functions import col, date_add, when,to_date
from pyspark.sql.types import StringType

def read_silver_table(table, silver_directory, spark):
    """
    Helper function to read all partitions of a silver table
    """
    folder_path = os.path.join(silver_directory, table)
    files_list = [os.path.join(folder_path, os.path.basename(f)) for f in glob.glob(os.path.join(folder_path, '*'))]
    df = spark.read.option("header", "true").parquet(*files_list)
    return df

############################
# Label Store
############################
def build_label_store(sla, df, date_str):
    """
    Function to build label store
    """
    ####################
    # Create labels
    ####################

    # get customer at mob
    df = df.filter((col("order_status") == 'delivered') & (col("snapshot_date") == date_str))

    # get label
    df = df.withColumn("order_purchase_date", to_date(col("order_purchase_timestamp")))
    df = df.withColumn("snapshot_date", df["snapshot_date"].cast(StringType()))
    df = df.withColumn("miss_delivery_sla", when(col("order_delivered_customer_date") > date_add(col("order_purchase_date"), sla), 1).otherwise(0))

    # select columns to save
    df = df.select("order_id", "miss_delivery_sla", "snapshot_date")

    return df

############################
# Pipeline
############################

def process_gold_label(silver_directory, gold_directory, partitions_list, spark):
    """
    Wrapper function to build all gold tables
    """
    # Read silver tables
    orders_df = read_silver_table('orders', silver_directory, spark)

    # Build label store
    print("Building label store...")
    for date_str in tqdm(partitions_list, total=len(partitions_list), desc="Saving labels"):
        df_label = build_label_store(14, orders_df, date_str)
        partition_name = date_str.replace('-','_') + '.parquet'
        label_filepath = os.path.join(gold_directory, 'label_store', partition_name)
        df_label.write.mode('overwrite').parquet(label_filepath)

    print("Label store Completed")

    return df_label