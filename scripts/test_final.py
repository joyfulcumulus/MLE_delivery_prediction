import pandas as pd
import os
from datetime import datetime

def test_gold_feature_creation():
    """Test the gold feature creation logic using pandas instead of Spark"""
    
    date_str = "2017-12-04"
    formatted_date = date_str.replace('-', '_')
    silver_dir = "datamart/silver"
    
    print(f"Testing gold feature creation for date: {date_str}")
    print("=" * 60)
    
    # Read all required tables
    print("Reading silver tables...")
    
    # Orders
    orders_file = os.path.join(silver_dir, "orders", f"silver_olist_orders_{formatted_date}.parquet")
    orders_df = pd.read_parquet(orders_file)
    print(f"Orders: {orders_df.shape[0]} rows")
    
    # Order items (single file)
    items_file = os.path.join(silver_dir, "order_items", "silver_olist_order_items.parquet")
    items_df = pd.read_parquet(items_file)
    print(f"Order items: {items_df.shape[0]} rows")
    
    # Other date-specific tables
    logistic_file = os.path.join(silver_dir, "order_logistics", f"silver_olist_order_logistics_{formatted_date}.parquet")
    logistic_df = pd.read_parquet(logistic_file)
    print(f"Order logistics: {logistic_df.shape[0]} rows")
    
    shipping_file = os.path.join(silver_dir, "shipping_infos", f"silver_shipping_infos_{formatted_date}.parquet")
    shipping_df = pd.read_parquet(shipping_file)
    print(f"Shipping infos: {shipping_df.shape[0]} rows")
    
    history_file = os.path.join(silver_dir, "delivery_history", f"silver_delivery_history_{formatted_date}.parquet")
    history_df = pd.read_parquet(history_file)
    print(f"Delivery history: {history_df.shape[0]} rows")
    
    seller_file = os.path.join(silver_dir, "seller_performance", f"silver_seller_performance_{formatted_date}.parquet")
    seller_perform_df = pd.read_parquet(seller_file)
    print(f"Seller performance: {seller_perform_df.shape[0]} rows")
    
    concentration_file = os.path.join(silver_dir, "concentration", f"silver_concentration_{formatted_date}.parquet")
    concentration_df = pd.read_parquet(concentration_file)
    print(f"Concentration: {concentration_df.shape[0]} rows")
    
    print(f"\nStarting feature engineering...")
    
    # Start with orders
    df = orders_df[['order_id', 'customer_id', 'order_status']].copy()
    print(f"After selecting order columns: {df.shape[0]} rows")
    
    # Join items
    df = df.merge(items_df, on='order_id', how='left')
    drop_cols = [col for col in ['shipping_limit_date', 'order_item_id', 'price', 'freight_value', 'snapshot_date'] if col in df.columns]
    df = df.drop(columns=drop_cols)
    df = df.drop_duplicates(subset=['order_id'])
    print(f"After joining items: {df.shape[0]} rows")
    
    # Join logistics
    df = df.merge(logistic_df, on='order_id', how='left')
    drop_cols = [col for col in ['order_purchase_timestamp', 'main_category', 'sub_category', 'snapshot_date'] if col in df.columns]
    df = df.drop(columns=drop_cols)
    print(f"After joining logistics: {df.shape[0]} rows")
    
    # Join shipping
    df = df.merge(shipping_df, on='order_id', how='left')
    drop_cols = [col for col in ['order_purchase_timestamp', 'customer_zip_code_prefix',
                                'customer_city', 'customer_state', 'customer_lat', 'customer_lng',
                                'seller_zip_code_prefix', 'seller_lat', 'seller_lng', 'same_zipcode'] if col in df.columns]
    df = df.drop(columns=drop_cols)
    print(f"After joining shipping: {df.shape[0]} rows")
    
    # Join history
    df = df.merge(history_df, on='order_id', how='left')
    drop_cols = [col for col in ['order_purchase_timestamp', 'approval_duration', 
                                'processing_duration', 'ship_duration', 'miss_delivery_sla', 'snapshot_date'] if col in df.columns]
    df = df.drop(columns=drop_cols)
    print(f"After joining history: {df.shape[0]} rows")
    
    # Join seller performance
    df = df.merge(seller_perform_df, on='seller_id', how='left')
    print(f"After joining seller performance: {df.shape[0]} rows")
    
    # Add date features
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    df['day_of_week'] = date_obj.weekday() + 1  # 1=Monday, 7=Sunday (different from Spark)
    df['month'] = date_obj.month
    
    # Add season
    month = date_obj.month
    if month in [7, 8, 9]:
        season = "Winter"
    elif month in [10, 11, 12]:
        season = "Spring"
    elif month in [1, 2, 3]:
        season = "Summer"
    else:
        season = "Autumn"
    df['season'] = season
    print(f"After adding date features: {df.shape[0]} rows")
    
    # Join concentration
    df['snapshot_date'] = date_str
    df = df.merge(concentration_df, on='snapshot_date', how='left')
    drop_cols = [col for col in ['granularity_level', 'type', 'region'] if col in df.columns]
    df = df.drop(columns=drop_cols)
    print(f"After joining concentration: {df.shape[0]} rows")
    
    # Drop unused columns
    drop_cols = [col for col in ['customer_id', 'product_id', 'seller_id', 'month', 'snapshot_date'] if col in df.columns]
    df = df.drop(columns=drop_cols)
    print(f"After dropping unused columns: {df.shape[0]} rows")
    
    print(f"\nFinal feature DataFrame:")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    if df.shape[0] > 0:
        print(f"\nFirst 5 rows:")
        print(df.head())
        
        # Save as test output
        output_file = f"test_gold_features_{formatted_date}.parquet"
        df.to_parquet(output_file, index=False)
        print(f"\nSaved test output to: {output_file}")
    else:
        print("\n⚠️  WARNING: Final DataFrame is empty!")
    
    return df

if __name__ == "__main__":
    test_gold_feature_creation()
