import pandas as pd
import os

# Test specific date files
date_str = "2017_12_04"
silver_dir = "datamart/silver"

tables_to_check = [
    'orders', 'order_items', 'order_logistics', 
    'shipping_infos', 'delivery_history', 'seller_performance', 'concentration'
]

print(f"Checking data for date: {date_str}")
print("=" * 50)

for table in tables_to_check:
    table_dir = os.path.join(silver_dir, table)
    if not os.path.exists(table_dir):
        print(f"{table}: Directory does not exist")
        continue
        
    # Find files with the specific date
    files = os.listdir(table_dir)
    matching_files = [f for f in files if date_str in f]
    
    if not matching_files:
        print(f"{table}: No files for date {date_str}")
        # Show first few available files
        available_files = files[:3]
        print(f"  Available files (first 3): {available_files}")
        continue
    
    print(f"{table}: Found {len(matching_files)} file(s)")
    
    # Read the first matching file
    file_path = os.path.join(table_dir, matching_files[0])
    try:
        df = pd.read_parquet(file_path)
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        if df.shape[0] == 0:
            print(f"  ⚠️  WARNING: File is empty!")
        else:
            print(f"  ✓ Has data")
    except Exception as e:
        print(f"  ❌ Error reading file: {e}")
    print()
