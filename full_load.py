import psycopg2
import pandas as pd
from psycopg2 import sql

host = "localhost" 
port = "5432"
user = "postgres"
password = "password"

db_name = "auto_sales"
table_name = "full_orders"
csv_file = "data/auto_sales_data.csv"

def ensure_database():
    try:
        conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error ensuring database: {e}")

def create_table():
    try:
        conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ORDERNUMBER INT,
                QUANTITYORDERED INT,
                PRICEEACH NUMERIC,
                ORDERLINENUMBER INT,
                SALES NUMERIC,
                ORDERDATE DATE,
                DAYS_SINCE_LASTORDER INT,
                STATUS VARCHAR(50),
                PRODUCTLINE VARCHAR(50),
                MSRP INT,
                PRODUCTCODE VARCHAR(50),
                CUSTOMERNAME VARCHAR(100),
                PHONE VARCHAR(50),
                ADDRESSLINE1 VARCHAR(100),
                CITY VARCHAR(50),
                POSTALCODE VARCHAR(20),
                COUNTRY VARCHAR(50),
                CONTACTLASTNAME VARCHAR(50),
                CONTACTFIRSTNAME VARCHAR(50),
                DEALSIZE VARCHAR(50),
                PRIMARY KEY(ORDERNUMBER,ORDERLINENUMBER)
            )
        """)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_data():
    try:
        data = pd.read_csv(csv_file)
        data['ORDERDATE'] = pd.to_datetime(data['ORDERDATE'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

        conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()
        
        for _, row in data.iterrows():
            insert_query = sql.SQL(f"""
                INSERT INTO {table_name} (
                    ORDERNUMBER, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER,
                    SALES, ORDERDATE, DAYS_SINCE_LASTORDER, STATUS, PRODUCTLINE,
                    MSRP, PRODUCTCODE, CUSTOMERNAME, PHONE, ADDRESSLINE1, CITY,
                    POSTALCODE, COUNTRY, CONTACTLASTNAME, CONTACTFIRSTNAME, DEALSIZE
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """)
            cursor.execute(insert_query, tuple(row))
        
        conn.commit()
        print("Data inserted successfully.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting data: {e}")

if __name__ == "__main__":
    ensure_database()
    create_table()
    insert_data()
