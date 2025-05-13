import pandas as pd
import psycopg2
import os
import argparse
from sqlalchemy import create_engine # For df.to_sql, easier than manual INSERTs

def load_parquet_to_postgres(parquet_path, table_name, source_dataset_id):
    """
    Reads a Parquet file and loads its data into a PostgreSQL table.
    """
    # Read connection details from environment variables
    db_host = os.getenv('POSTGRES_HOST', 'db') # 'db' is the service name in docker-compose
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')

    if not all([db_name, db_user, db_password]):
        print("Error: Database credentials (POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD) not set in environment.")
        return False

    print(f"Loading Parquet file: {parquet_path} into table: {table_name}")

    try:
        df = pd.read_parquet(parquet_path)
        print(f"Read {len(df)} rows from {parquet_path}")

        if df.empty:
            print("Parquet file is empty. Nothing to load.")
            return True

        # Add the source_dataset_id column
        df['source_dataset_id'] = source_dataset_id
        
        # ---->>>> Convert all DataFrame column names to lowercase <<<<----
        df.columns = [col.lower() for col in df.columns]
        # Now df will have 'linear_index', 'value', 'status_code', etc.

        # Create SQLAlchemy engine
        # postgresql+psycopg2://user:password@host:port/dbname
        engine_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(engine_url)

        print(f"Writing data to PostgreSQL table {table_name}...")
        # 'append' will add new rows.
        # 'if_exists' can be 'fail', 'replace', or 'append'.
        # 'index=False' means don't write the DataFrame index as a column.
        # 'chunksize' can improve performance for very large DataFrames.
        df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=10000)

        print(f"Successfully loaded data from {parquet_path} to {table_name}")
        return True

    except FileNotFoundError:
        print(f"Error: Parquet file not found at {parquet_path}")
        return False
    except ImportError:
        print("Error: SQLAlchemy or psycopg2 not installed. Please install them.")
        return False
    except Exception as e:
        print(f"An error occurred during loading: {e}")
        # Consider more specific error handling for database errors (e.g., psycopg2.Error)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data from a Parquet file into PostgreSQL.")
    parser.add_argument(
        "--parquet-file",
        required=True,
        help="Path to the input Parquet file."
    )
    parser.add_argument(
        "--table-name",
        default="stg_eurostat_health_data",
        help="Name of the target PostgreSQL table."
    )
    parser.add_argument(
        "--dataset-id",
        required=True,
        help="The Eurostat dataset ID (e.g., HLTH_CD_ANR) to be stored in source_dataset_id column."
    )
    args = parser.parse_args()

    # Ensure SQLAlchemy is in your requirements.txt for the app image
    # `pandas`, `pyarrow`, `psycopg2-binary`, `sqlalchemy`
    
    success = load_parquet_to_postgres(args.parquet_file, args.table_name, args.dataset_id)
    if success:
        print("Load process completed successfully.")
    else:
        print("Load process failed.")
        exit(1) # Exit with error code if script fails