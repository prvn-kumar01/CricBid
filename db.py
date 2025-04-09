import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote

# Database connection details
db_name = 'cricket_bid'
db_user = 'postgres'
db_password = 'postgres@sql'
db_host = 'localhost'
db_port = '5432'

# Encode password to handle special characters
encoded_password = quote(db_password)

# Step 1: Create Database if it doesn't exist
def create_database():
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
        if cursor.fetchone() is None:
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")

# Step 2: Upload CSV files to PostgreSQL
def upload_csv_to_postgres(file_path, table_name):
    try:
        # Connect to the database using SQLAlchemy with encoded password
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}')
        print("Connected to database using SQLAlchemy.")
        
        # Read the CSV file with encoding error handling
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='ISO-8859-1')

        # Check if DataFrame is empty
        if df.empty:
            print(f"Warning: '{file_path}' is empty. Skipping upload.")
            return

        # Upload to PostgreSQL
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"'{table_name}' uploaded successfully with {len(df)} rows.")
    except Exception as e:
        print(f"Error uploading '{table_name}': {e}")

# Step 3: Execute the functions
if __name__ == "__main__":
    create_database()

    # File paths and table names
    file_table_mapping = {
        'Datasets/IPL matches (2008-2024).csv': 'matches',
        'Datasets/IPL_Auction_Data_2008_2025.csv': 'auction_data',
        'Datasets/IPLPlayerAuctionData_C.csv': 'player_auction',
        'Datasets/Top Batsman.csv': 'top_batsman',
        'Datasets/Top Bowlers.csv': 'top_bowlers'
    }

    for file_path, table_name in file_table_mapping.items():
        upload_csv_to_postgres(file_path, table_name)
