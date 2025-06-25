import pandas as pd
import os

def check_data_for_date(date_str):
    """Check if data exists for all required tables for a given date"""
    
    formatted_date = date_str.replace('-', '_')
    silver_dir = "datamart/silver"
    
    print(f"Checking data availability for date: {date_str}")
    print("=" * 50)
    
    tables_and_patterns = [
        ('orders', f'silver_olist_orders_{formatted_date}.parquet'),
        ('order_items', 'silver_olist_order_items.parquet'),  # No date-specific files
        ('order_logistics', f'silver_olist_order_logistics_{formatted_date}.parquet'),
        ('shipping_infos', f'silver_shipping_infos_{formatted_date}.parquet'),
        ('delivery_history', f'silver_delivery_history_{formatted_date}.parquet'),
        ('seller_performance', f'silver_seller_performance_{formatted_date}.parquet'),
        ('concentration', f'silver_concentration_{formatted_date}.parquet'),
    ]
    
    all_available = True
    
    for table, pattern in tables_and_patterns:
        table_dir = os.path.join(silver_dir, table)
        file_path = os.path.join(table_dir, pattern)
        
        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path)
                print(f"✓ {table}: {df.shape[0]} rows, {df.shape[1]} columns")
            except Exception as e:
                print(f"❌ {table}: File exists but error reading: {e}")
                all_available = False
        else:
            print(f"❌ {table}: File not found - {pattern}")
            all_available = False
    
    print(f"\nAll data available: {'✓' if all_available else '❌'}")
    return all_available

if __name__ == "__main__":
    # Check the dates from the Airflow logs
    check_data_for_date("2016-09-15")
    print()
    check_data_for_date("2016-09-16")
    print()
    check_data_for_date("2017-12-04")
