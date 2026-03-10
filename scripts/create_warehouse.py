import mysql.connector 
from mysql.connector import Error

def create_database_and_tables():
    # 1. Update this with your local MySQL password
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '' 
    }

    try:
        # Connect to MySQL server
        print("Connecting to MySQL...")
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Create the database
        cursor.execute("CREATE DATABASE IF NOT EXISTS amazon_warehouse;")
        cursor.execute("USE amazon_warehouse;")
        print("Database 'amazon_warehouse' is ready.")

        # 1. Create Dimension Table: dim_customer
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_id VARCHAR(50) PRIMARY KEY,
                customer_name VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(100),
                country VARCHAR(100)
            );
        """)
        print("Table 'dim_customer' created.")

        # 2. Create Dimension Table: dim_product
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_product (
                product_id VARCHAR(50) PRIMARY KEY,
                product_name TEXT,
                category VARCHAR(100),
                brand VARCHAR(100)
            );
        """)
        print("Table 'dim_product' created.")

        # 3. Create Fact Table: fact_sales
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                order_id VARCHAR(50) PRIMARY KEY,
                order_date DATE,
                customer_id VARCHAR(50),
                product_id VARCHAR(50),
                seller_id VARCHAR(50),
                quantity INT,
                unit_price DECIMAL(10, 2),
                discount DECIMAL(5, 2),
                tax DECIMAL(10, 2),
                shipping_cost DECIMAL(10, 2),
                total_amount DECIMAL(10, 2),
                payment_method VARCHAR(50),
                order_status VARCHAR(50),
                FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
                FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
            );
        """)
        print("Table 'fact_sales' created.")

        connection.commit()
        print("\nSuccess! The Star Schema infrastructure is perfectly created.")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    create_database_and_tables()