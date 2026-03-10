# E-Commerce ETL Pipeline & Local Data Warehouse

## Project Overview
This project demonstrates an end-to-end Extract, Transform, Load (ETL) pipeline built in Python. It extracts raw e-commerce sales data, performs rigorous data cleaning and validation, transforms the flat file into a normalized Star Schema, and loads it into a local MySQL Data Warehouse.

## Tech Stack
* Language: Python (Pandas)
* Database: MySQL
* Libraries: SQLAlchemy, mysql-connector-python
* Architecture: Star Schema (Fact and Dimension tables)

## Pipeline Architecture
1. Extraction: Pulled raw flat-file dataset into memory.
2. Cleaning: Standardized column naming, formatted datetime objects, and stripped whitespace.
3. Validation: Enforced data integrity by checking for nulls in critical fields, verifying primary key uniqueness, and applying business domain rules.
4. Transformation: Split the flat DataFrame into normalized Dimension and Fact tables to eliminate data redundancy.
5. Loading: Safely pushed the structured data into a MySQL relational database, handling existing data truncation and foreign key constraints.

## Database Design (Star Schema)
The warehouse is designed for analytical querying with the following structure:
* dim_customer : customer_id (PK), customer_name, city, state, country
* dim_product : product_id (PK), product_name, category, brand
* fact_sales : order_id (PK), foreign keys (customer, product), financial metrics (quantity, unit_price, discount, total_amount)