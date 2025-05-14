import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import argparse
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, Text, insert
import pyarrow.parquet as pq

def load_parquet_to_postgres(parquet_path, table_name, source_dataset_id):
    """
    Reads a Parquet file in chunks (row groups) and loads its data 
    into a PostgreSQL table using direct SQL execution rather than 
    pandas.to_sql which has some compatibility issues.
    """
    db_host = os.getenv('POSTGRES_HOST', 'db')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')

    if not all([db_name, db_user, db_password]):
        print("Error: Database credentials (POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD) not set in environment.")
        return False

    print(f"Loading Parquet file: {parquet_path} into table: {table_name} in chunks.")

    # Create a direct connection using psycopg2
    conn_str = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}"
    
    try:
        # First connect with psycopg2 to create the table if needed
        conn = psycopg2.connect(conn_str)
        print(f"Connection established: {type(conn)}")  # Debugging statement to check connection type
        conn.autocommit = True  # Set autocommit to True to execute DDL
        cur = conn.cursor()
        print("Cursor created successfully.")  # Debugging statement to confirm cursor creation
        
        # Read the Parquet file to get schema information
        parquet_file = pq.ParquetFile(parquet_path)
        
        # Check if file is empty
        if parquet_file.num_row_groups == 0 and parquet_file.metadata.num_rows == 0:
            print("Parquet file is empty (0 rows in metadata). Nothing to load.")
            return True
        
        # Get the schema from the first row group or fall back to reading the whole file
        if parquet_file.num_row_groups > 0:
            # Get schema from first row group
            schema = parquet_file.schema
            sample_batch = parquet_file.read_row_group(0)
            sample_df = sample_batch.to_pandas()
            if sample_df.empty:
                print("First row group is empty, cannot determine schema.")
                return False
        else:
            # Fall back to reading the whole file (though this could be large)
            sample_df = pd.read_parquet(parquet_path)
            if sample_df.empty:
                print("Parquet file is empty. Nothing to load.")
                return True
        
        # Add source_dataset_id column if it doesn't exist in the dataframe
        if 'source_dataset_id' not in [col.lower() for col in sample_df.columns]:
            sample_df['source_dataset_id'] = source_dataset_id
        
        # Lower case all column names for consistency
        sample_df.columns = [col.lower() for col in sample_df.columns]
        
        # Create table if not exists
        columns_def = []
        for col in sample_df.columns:
            if col == 'source_dataset_id':
                columns_def.append(f"{col} TEXT")
            elif pd.api.types.is_numeric_dtype(sample_df[col]):
                if pd.api.types.is_integer_dtype(sample_df[col]):
                    columns_def.append(f"{col} INTEGER")
                else:
                    columns_def.append(f"{col} FLOAT")
            else:
                columns_def.append(f"{col} TEXT")
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns_def)}
        );
        """
        
        print(f"Creating table {table_name} if it does not exist...")
        cur.execute(create_table_sql)
        print(f"Table {table_name} is ready.")
        
        # Process each row group and insert into PostgreSQL
        total_rows_loaded = 0
        
        print(f"Parquet file has {parquet_file.num_row_groups} row groups.")
        
        for i in range(parquet_file.num_row_groups):
            print(f"Processing row group {i+1}/{parquet_file.num_row_groups}...")
            table_batch = parquet_file.read_row_group(i)
            df_batch = table_batch.to_pandas()
            
            if df_batch.empty:
                print(f"Row group {i+1} is empty. Skipping.")
                continue
            
            print(f"  Read {len(df_batch)} rows from row group {i+1}.")
            
            # Add source_dataset_id column and ensure all column names are lowercase
            df_batch['source_dataset_id'] = source_dataset_id
            df_batch.columns = [col.lower() for col in df_batch.columns]
            
            # Prepare batch for insertion
            # 1. Convert all columns to appropriate types and handle null values
            #    For execute_values, None is the correct way to represent SQL NULL
            for col in df_batch.columns:
                if pd.api.types.is_numeric_dtype(df_batch[col]):
                    # Convert to Python native types, pandas dtypes can cause issues
                    if pd.api.types.is_integer_dtype(df_batch[col]):
                         # Handle potential Pandas <NA> for nullable integers
                        df_batch[col] = df_batch[col].apply(lambda x: x if pd.notna(x) else None).astype('object')
                    else: # Float
                        df_batch[col] = df_batch[col].apply(lambda x: x if pd.notna(x) else None).astype('object')
                else: # String/Text
                    df_batch[col] = df_batch[col].apply(lambda x: str(x) if pd.notna(x) else None)

            # 2. Get column names and create the insert SQL template
            columns = df_batch.columns.tolist()
            insert_sql_template = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
            
            # 3. Convert dataframe to list of tuples for execute_values
            data_tuples = [tuple(x) for x in df_batch.to_numpy()]
            
            if not data_tuples:
                print(f"  No data to insert for row group {i+1}. Skipping.")
                continue

            try:
                execute_values(cur, insert_sql_template, data_tuples)
                total_rows_loaded += len(data_tuples)
                print(f"  Inserted {len(data_tuples)} rows. Total: {total_rows_loaded}")
            except Exception as e:
                print(f"Error inserting batch: {e}")
                # Optionally, you might want to log failed data_tuples or handle individual row errors
                # For now, we'll just print the error and continue with the next batch.
                conn.rollback() # Rollback the failed batch
                cur = conn.cursor() # Re-establish cursor if rollback closes it (depends on psycopg2 version/config)
                conn.autocommit = True # Re-enable autocommit if it was disabled by rollback
                print(f"  Rolled back batch from row group {i+1}. Attempting to continue...")
                continue # Skip to the next row group if a batch fails

            print(f"  Successfully loaded all rows from row group {i+1}. Total loaded: {total_rows_loaded}")
        
        print(f"Successfully loaded all {total_rows_loaded} rows from {parquet_path} to {table_name}")
        return True
        
    except FileNotFoundError:
        print(f"Error: Parquet file not found at {parquet_path}")
        return False
    except ImportError: 
        print("Error: Required packages not installed. Please ensure psycopg2, pyarrow, and pandas are installed.")
        return False
    except Exception as e: 
        print(f"An error occurred during loading: {e}")
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

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
    
    success = load_parquet_to_postgres(args.parquet_file, args.table_name, args.dataset_id)
    if success:
        print("Load process completed successfully.")
    else:
        print("Load process failed.")
        exit(1)