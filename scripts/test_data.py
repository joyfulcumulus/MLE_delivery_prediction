import pandas as pd
import os

# Test reading a silver table file to check if data exists
silver_dir = "datamart/silver"

# Check orders table for specific date
orders_file = os.path.join(silver_dir, "orders", "silver_olist_orders_2017_12_04.parquet")
print(f"Checking file: {orders_file}")
print(f"File exists: {os.path.exists(orders_file)}")

try:
    df = pd.read_parquet(orders_file)
    print(f"Orders dataframe shape: {df.shape}")
    print(f"Orders columns: {list(df.columns)}")
    print("\nFirst 5 rows:")
    print(df.head())
except Exception as e:
    print(f"Error reading orders file: {e}")

# Check other silver tables
for table in ['order_items', 'order_logistics', 'shipping_infos', 'delivery_history', 'seller_performance', 'concentration']:
    table_dir = os.path.join(silver_dir, table)
    if os.path.exists(table_dir):
        files = os.listdir(table_dir)
        print(f"\n{table} files count: {len(files)}")
        if files:
            # Try to read first file
            try:
                first_file = os.path.join(table_dir, files[0])
                test_df = pd.read_parquet(first_file)
                print(f"  Sample file shape: {test_df.shape}")
                print(f"  Sample file columns: {list(test_df.columns)}")
            except Exception as e:
                print(f"  Error reading sample file: {e}")
    else:
        print(f"\n{table}: directory does not exist")
