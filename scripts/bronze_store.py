# import argparse
# import os
# import pandas as pd
# import matplotlib.pyplot as plt
# import numpy as np
# from datetime import datetime, timedelta
# from dateutil.relativedelta import relativedelta
# import pyspark

# import pyspark.sql.functions as F
# from pyspark.sql.functions import col
# from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


# import utils.data_processing_bronze_table as bronze_processing
# from bronze_processing import process_olist_orders_bronze


"""
bronze_store.py
Run all bronze-layer ingestions, including orders for a single target day.
Usage: python bronze_store.py YYYY-MM-DD
       python bronze_store.py          # defaults to today
"""

import argparse
import os
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession

# --- Import bronze helpers -------------------------------------------------
# Treat utils/ as a package; make sure project root is on PYTHONPATH.
from utils import data_processing_bronze_table as bronze_processing
# (We will call functions with bronze_processing.<name>)

# ---------------------------------------------------------------------------
def main(target_date_str: str):
    print("\n\n--- starting bronze job for", target_date_str, "---")

    # 1. SparkSession --------------------------------------------------------
    spark: SparkSession = (
        SparkSession.builder
        .appName("olist_bronze_processing")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 2. Output root -----------------------------------------------------
        bronze_root = "datamart/bronze"
        os.makedirs(bronze_root, exist_ok=True)
        print("Bronze root:", bronze_root)

        # 3. Run all non-order tables ---------------------------------------
        print("\nProcessing Olist raw data …")
        bronze_processing.process_olist_customers_bronze(bronze_root, spark)
        bronze_processing.process_olist_geolocation_bronze(bronze_root, spark)
        bronze_processing.process_olist_order_items_bronze(bronze_root, spark)
        bronze_processing.process_olist_order_reviews_bronze(bronze_root, spark)
        bronze_processing.process_olist_products_bronze(bronze_root, spark)
        bronze_processing.process_olist_sellers_bronze(bronze_root, spark)
        bronze_processing.process_product_cat_translation_bronze(bronze_root, spark)

        # 4. Orders – single day only ---------------------------------------
        bronze_processing.process_olist_orders_bronze(
            bronze_root=bronze_root,
            spark=spark,
            target_date_str=target_date_str
        )

        print("\n\n--- completed bronze job ---\n")
    finally:
        spark.stop()
        print("Spark session stopped.\n")

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate bronze store for one day")
    parser.add_argument(
        "target_date",
        nargs="?",                                   # optional positional
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Date to process (YYYY-MM-DD). Defaults to today if omitted."
    )
    args = parser.parse_args()
    main(args.target_date)
