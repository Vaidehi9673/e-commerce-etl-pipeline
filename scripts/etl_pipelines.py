import sys
print(f"CRITICAL DEBUG: Running Python from -> {sys.executable}")

import pandas as pd
from sqlalchemy import create_engine, text
import os

# ==========================================
# 1. EXTRACTION
# ==========================================
def extract_data(file_path):
    print("--- Phase 1: EXTRACTION ---")
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded {len(df)} rows from {file_path}.")
        return df
    except Exception as e:
        print(f"Extraction Error: {e}")
        return None

# ==========================================
# 2. CLEANING
# ==========================================
def clean_data(df):
    print("\n--- Phase 2: CLEANING ---")
    
    # 1. Explicitly rename columns to match our SQL database exactly
    df = df.rename(columns={
        'OrderID': 'order_id',
        'OrderDate': 'order_date',
        'CustomerID': 'customer_id',
        'CustomerName': 'customer_name',
        'ProductID': 'product_id',
        'ProductName': 'product_name',
        'Category': 'category',
        'Brand': 'brand',
        'Quantity': 'quantity',
        'UnitPrice': 'unit_price',
        'Discount': 'discount',
        'Tax': 'tax',
        'ShippingCost': 'shipping_cost',
        'TotalAmount': 'total_amount',
        'PaymentMethod': 'payment_method',
        'OrderStatus': 'order_status',
        'City': 'city',
        'State': 'state',
        'Country': 'country',
        'SellerID': 'seller_id'
    })
    
    # 2. Transform date to 'yyyy-mm-dd hh:mm:ss' format
    df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # 3. Trim whitespace from all string columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if isinstance(x, pd.Series) else x)

    # 4. Standardize Text
    df['category'] = df['category'].str.title()
    df['city'] = df['city'].str.title()

    print("Cleaning complete: Columns explicitly renamed, whitespace stripped, and dates formatted.")
    return df

# ==========================================
# 3. VALIDATION
# ==========================================
def validate_data(df):
    print("\n--- Phase 3: VALIDATION ---")
    initial_count = len(df)
    
    # Expanded Null Checks
    critical_cols = [
        'customer_id', 'product_id', 'order_id', 'customer_name', 
        'product_name', 'total_amount', 'order_date', 'unit_price', 'seller_id'
    ]
    df = df.dropna(subset=critical_cols)
    
    # Uniqueness Check
    df = df.drop_duplicates(subset=['order_id'])
        
    print(f"Validation complete. {len(df)} rows passed integrity checks out of {initial_count}.")
    return df

# ==========================================
# 4. TRANSFORMATION
# ==========================================
def transform_data(df, clean_data_path):
    print("\n--- Phase 4: TRANSFORMATION (Star Schema) ---")
    
    # Transform date to 'yyyy-mm-dd hh:mm:ss' format
    df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # --- Domain & Range Business Rules ---
    df = df[(df['quantity'] > 0) & (df['unit_price'] >= 0)]
    
    # Transform discount range to percentage decimal
    if df['discount'].max() > 1:
        df['discount'] = df['discount'] / 100
    print("Business rules applied (Quantity, Price, and Discount transformations).")

    # Create Dimensions and Fact tables
    dim_customer = df[['customer_id', 'customer_name', 'city', 'state', 'country']].drop_duplicates(subset=['customer_id'])
    dim_product = df[['product_id', 'product_name', 'category', 'brand']].drop_duplicates(subset=['product_id'])
    
    fact_sales = df[['order_id', 'order_date', 'customer_id', 'product_id', 'seller_id', 
                     'quantity', 'unit_price', 'discount', 'tax', 'shipping_cost', 
                     'total_amount', 'payment_method', 'order_status']]

    # Save cleaned audit file
    os.makedirs(os.path.dirname(clean_data_path), exist_ok=True)
    df.to_csv(clean_data_path, index=False)
    print(f"Clean audit file saved to {clean_data_path}")

    return dim_customer, dim_product, fact_sales

# ==========================================
# 5. LOADING
# ==========================================
def load_data(dim_customer, dim_product, fact_sales, db_url):
    print("\n--- Phase 5: LOADING ---")
    try:
        engine = create_engine(db_url)
        
        # 1. Check if data already exists in the warehouse
        with engine.connect() as connection:
            # Query the fact table to see if it has rows
            result = connection.execute(text("SELECT COUNT(*) FROM fact_sales"))
            row_count = result.scalar()
            
            if row_count > 0:
                print(f"\n WARNING: The database already contains {row_count} records in 'fact_sales'.")
                user_choice = input("Do you want to delete the existing data and reload it? (yes/no): ").strip().lower()
                
                if user_choice in ['yes', 'y']:
                    print("\nClearing existing data from tables...")
                    # Temporarily disable Foreign Key checks so MySQL allows us to truncate
                    connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
                    connection.execute(text("TRUNCATE TABLE fact_sales;"))
                    connection.execute(text("TRUNCATE TABLE dim_customer;"))
                    connection.execute(text("TRUNCATE TABLE dim_product;"))
                    connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
                    connection.commit() # Save the deletion
                    print("Tables successfully truncated. Proceeding with fresh load...")
                else:
                    print("\nLoading process aborted by user. Existing data was kept safely.")
                    return # This completely stops the function right here
        
        # 2. Proceed with loading the new data
        print("\nLoading dim_customer...")
        dim_customer.to_sql('dim_customer', engine, if_exists='append', index=False)
        
        print("Loading dim_product...")
        dim_product.to_sql('dim_product', engine, if_exists='append', index=False)
        
        print("Loading fact_sales...")
        fact_sales.to_sql('fact_sales', engine, if_exists='append', index=False)
        
        print("\nSUCCESS! Data Warehouse is fully populated.")
        
    except Exception as e:
        print(f"Loading Error: {e}")

# ==========================================
# ORCHESTRATOR (The Main Engine)
# ==========================================
def main():
    # 1. Dynamically find the main project folder (local_etl_project)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 2. Build the paths safely
    raw_data_path = os.path.join(BASE_DIR, 'data', 'raw_data', 'Amazon.csv')
    clean_data_path = os.path.join(BASE_DIR, 'data', 'processed_data', 'amazon_cleaned.csv')
        
    db_url = 'mysql+mysqlconnector://root:''@localhost:3306/amazon_warehouse'
    
    # Execute the Pipeline starting with extraction
    raw_df = extract_data(raw_data_path)
    
    #executing cleaning, validation checks, transformation and loading process.
    if raw_df is not None:
        cleaned_df = clean_data(raw_df)
        validated_df = validate_data(cleaned_df)
        dim_cust, dim_prod, fact_sales = transform_data(validated_df, clean_data_path)
        
        load_data(dim_cust, dim_prod, fact_sales, db_url)

if __name__ == "__main__":
    main()


#source .venv/bin/activate
