import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

def main():
    """
    Main function to ingest all CSV data into a PostgreSQL database.
    """
    # --- 1. Get Database Credentials from Environment Variables ---
    # This is secure because the secrets are managed by Codespaces, not in the code.
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    host = 'localhost' # Since the script and DB are in the same Codespace
    port = 5432
    db_name = os.environ.get('POSTGRES_DB')

    if not all([user, password, db_name]):
        print("❌ Error: Database credentials are not set in environment variables.")
        return

    print("✅ Credentials loaded.")

    # --- 2. Create SQLAlchemy Database Engine with URL-encoded credentials ---
    # The engine manages the connection to the database.
    engine = create_engine(
        f'postgresql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db_name}'
    )
    
    # --- 3. Create the 'raw' Schema if it Doesn't Exist ---
    with engine.connect() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        connection.commit() # Make sure to commit the schema creation
    print("✅ 'raw' schema ensured.")

    # --- 4. Find, Read, and Ingest Each CSV File ---
    data_dir = 'data'
    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(data_dir, filename)

            # Create a clean table name by removing the common .csv suffix,
            # 'olist_' prefix, and '_dataset' suffix. This handles all
            # filename conventions in the dataset.
            table_name = filename.removesuffix('.csv')
            table_name = table_name.removeprefix('olist_')
            table_name = table_name.removesuffix('_dataset')

            print(f"🔄 Processing {filename} -> raw.{table_name}...")

            # Read the CSV into a pandas DataFrame
            df = pd.read_csv(file_path)

            # Convert date columns to datetime objects
            for col in df.columns:
                if 'timestamp' in col or '_date' in col:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # Load the DataFrame into the PostgreSQL table
            # 'if_exists='replace'' makes the script re-runnable.
            df.to_sql(table_name, engine, schema='raw', if_exists='replace', index=False)

            print(f"✅ Successfully loaded {table_name}.")

if __name__ == '__main__':
    main()