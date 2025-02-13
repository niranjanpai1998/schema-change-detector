import psycopg2
import pandas as pd
from psycopg2 import sql
from psycopg2 import sql, OperationalError
import sys

# PostgreSQL connection details
host = "localhost"
port = "5432"
user = "postgres"
password = "password"
DEFAULT_DATABASE = "postgres"

db_name = "auto_sales"
table_name = "orders"
csv_file = sys.argv[1]
primary_key_list = ["ordernumber", "orderlinenumber"]

def connect_postgres(dbname=DEFAULT_DATABASE):
    user = 'postgres'
    password = 'password'  
    host = 'localhost'
    
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            dbname=dbname,
            host=host
        )
        print("Connection to PostgreSQL database was successful.")
        return connection

    except OperationalError as e:
        print(f"The error '{e}' occurred.")
        return None

def infer_pg_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATE"
    elif pd.api.types.is_object_dtype(dtype):
        return "VARCHAR(255)"
    else:
        return "TEXT"  # Fallback for other types

def generate_create_table_query(dataframe):
    columns = dataframe.dtypes
    column_definitions = []
    for column_name, dtype in columns.items():
        pg_type = infer_pg_type(dtype)
        column_definitions.append(f"{column_name} {pg_type}")
    column_definitions.append(f"PRIMARY KEY ({','.join(primary_key_list)})")
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)});"
    return create_table_query

def ensure_database():
    try:
        conn = connect_postgres(DEFAULT_DATABASE)
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")
        
        cursor.close()

    except Exception as e:
        print(f"Error ensuring database: {e}")

def create_table(conn, dataframe):
    try:
        cursor = conn.cursor()
        
        create_table_query = generate_create_table_query(dataframe)
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
        
        cursor.close()

    except Exception as e:
        print(f"Error creating table: {e}")

def check_column_mismatch(conn, dataframe):

    cursor = conn.cursor()
        
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
    existing_columns = {col[0].lower() for col in cursor.fetchall()}  # Convert to lowercase for comparison
    print('Existing columns - ', existing_columns)
        
    input_columns = {item.lower() for item in dataframe.columns}
    print('Input columns - ', input_columns)
        
    missing_columns_sink = input_columns - existing_columns
    missing_columns_source = existing_columns - input_columns
    print(f"Missing columns in Destination: {missing_columns_sink}")
    print(f"Missing columns in Source: {missing_columns_source}")

    return missing_columns_source, missing_columns_sink

    


def handle_column_mismatch_in_destination(conn, dataframe, missing_columns_sink):
    
    if len(missing_columns_sink)>0:

        cursor = conn.cursor()
        
        try:
            for column in missing_columns_sink:
                actual_column_name = next(col for col in dataframe.columns if col.lower() == column)
                print(f"Adding missing column: {actual_column_name}")
                
                pg_type = infer_pg_type(dataframe[actual_column_name].dtype)
                print(f'pg_type for {actual_column_name}: {pg_type}')
                
                alter_table_query = sql.SQL(f"ALTER TABLE {table_name} ADD COLUMN {actual_column_name} {pg_type};")
                cursor.execute(alter_table_query)
                print(f"Added missing column: {actual_column_name} with type {pg_type}")
            
            conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error handling column mismatch: {e}")

def handle_column_mismatch_in_source(dataframe, missing_columns_source):

    if len(missing_columns_source) > 0:
        for column in missing_columns_source:
            print(f"Column '{column}' exists in the table but is missing in the source dataframe.")
            dataframe[column] = None

            print(f"Added missing column '{column}' to the dataframe with NULL values.")

    return dataframe

def insert_data(conn, dataframe):
    try:
        cursor = conn.cursor()
        
        columns = ", ".join(dataframe.columns)
        placeholders = ", ".join(["%s"] * len(dataframe.columns))
        insert_query = sql.SQL(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ") #ON CONFLICT (ORDERNUMBER) DO NOTHING
        
        for _, row in dataframe.iterrows():
            cursor.execute(insert_query, tuple(row))
        
        conn.commit()
        print("Data inserted successfully.")
        
        cursor.close()

    except Exception as e:
        print(f"Error inserting data: {e}")

if __name__ == "__main__":

    data = pd.read_csv(csv_file)
    
    
    ensure_database()

    conn = connect_postgres(db_name)

    create_table(conn, data)

    missing_columns_source, missing_columns_sink = check_column_mismatch(conn, data)

    if len(missing_columns_sink)>0 or len(missing_columns_source)>0:
        handle_column_mismatch_in_destination(conn, data, missing_columns_sink)
    if len(missing_columns_sink)>0 or len(missing_columns_source)>0:
        data = handle_column_mismatch_in_source(data, missing_columns_source)

    insert_data(conn, data)

    conn.close()
