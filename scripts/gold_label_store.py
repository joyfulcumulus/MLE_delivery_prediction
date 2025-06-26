import os
import argparse
from datetime import datetime
import pyspark
from tqdm import tqdm


from utils.data_processing_gold_label_table import process_gold_label

if __name__ == "__main__":
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("dev") \
        .master("local[*]") \
        .getOrCreate()

    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--startdate", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    # Generate partitions
    start_date_str = [args.startdate]

    ############################
    # GOLD
    ############################
    print("Building gold feature tables...")
    # Create gold datalake
    silver_directory = "datamart/silver"
    gold_directory = "datamart/gold"

    if not os.path.exists(gold_directory):
        os.makedirs(gold_directory)

    # Build gold tables
    y = process_gold_label(silver_directory, gold_directory, start_date_str, spark)

    # Check for the rows ingested
    y_pdf = y.toPandas()
    y_count = y_pdf.shape[0]
    print(f"Number of rows in label store: {y_pdf.shape[0]}")

    print(f"Gold feature tables built successfully from start date: {start_date_str}")

    ############################
    # GOLD
    ############################