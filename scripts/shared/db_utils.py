import psycopg2
from psycopg2.extras import RealDictCursor # If you need to fetch results as dicts
import os
import logging # Optional
from datetime import datetime # For testing type conversion in get_dataset_last_processed_info_db

logger = logging.getLogger(__name__) # Optional

def get_db_connection_config(use_airflow_connection=True, airflow_conn_id="postgres_eurostat") -> dict:
    """
    Gets database connection parameters.
    Prioritizes Airflow connection, then environment variables, then defaults.
    """
    if use_airflow_connection:
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            pg_hook = PostgresHook(postgres_conn_id=airflow_conn_id)
            db_conn = pg_hook.get_conn() # This is a psycopg2 connection object
            conn_config = db_conn.get_dsn_parameters()
            db_conn.close() # Close the connection obtained just for config
            # Map DSN parameters to psycopg2.connect() kwargs
            # Common DSN params: host, port, dbname (for database), user, password
            return {
                'host': conn_config.get('host'),
                'port': conn_config.get('port'),
                'database': conn_config.get('dbname'),
                'user': conn_config.get('user'),
                'password': conn_config.get('password'),
            }
        except ImportError:
            logger.warning("Airflow components not available. Falling back to env vars for DB config.")
        except Exception as e:
            logger.warning(f"Failed to get DB config from Airflow connection '{airflow_conn_id}': {e}. Falling back.")
    
    # Fallback to environment variables or defaults
    return {
        'host': os.environ.get('POSTGRES_HOST', 'localhost'),
        'port': int(os.environ.get('POSTGRES_PORT', 5432)), # Default PG port
        'database': os.environ.get('POSTGRES_DB', 'eurostat_data'),
        'user': os.environ.get('POSTGRES_USER', 'eurostat_user'),
        'password': os.environ.get('POSTGRES_PASSWORD', 'mysecretpassword')
    }

def update_processed_dataset_log_db(dataset_id: str, 
                                 source_data_updated_at: str | None, 
                                 dataset_title: str | None, 
                                 airflow_run_id: str, 
                                 remarks: str = "") -> bool:
    """
    Inserts or updates a record in the processed_dataset_log table.
    Returns True on success, False on failure.
    """
    conn_config = get_db_connection_config()
    # Ensure source_data_updated_at is None if it's an empty string or not a valid date string
    # The database column is TIMESTAMP WITH TIME ZONE, so it needs to be a valid timestamp or NULL.
    # psycopg2 can handle ISO 8601 strings directly for timestamp with time zone.
    if isinstance(source_data_updated_at, str) and not source_data_updated_at.strip():
        source_data_updated_at = None
        
    sql = """
    INSERT INTO processed_dataset_log (
        dataset_id, source_data_updated_at, dataset_title, 
        last_processed_airflow_run_id, processing_remarks, last_processed_at_utc
    )
    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
    ON CONFLICT (dataset_id) DO UPDATE SET
        source_data_updated_at = EXCLUDED.source_data_updated_at,
        dataset_title = EXCLUDED.dataset_title,
        last_processed_at_utc = CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
        last_processed_airflow_run_id = EXCLUDED.last_processed_airflow_run_id,
        processing_remarks = EXCLUDED.processing_remarks;
    """
    try:
        # In Airflow context, it's better to use PostgresHook for connection management
        # but for a generic shared util, direct psycopg2 is also an option if Airflow is not available.
        # The get_db_connection_config tries to use Airflow hook first.
        
        with psycopg2.connect(**conn_config) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (dataset_id, source_data_updated_at, dataset_title, 
                                  airflow_run_id, remarks))
        logger.info(f"Successfully logged/updated {dataset_id} in processed_dataset_log")
        return True
    except Exception as e:
        logger.error(f"Error updating processed_dataset_log for {dataset_id}: {e}")
        return False

# Add other db utility functions here as needed, e.g.:
def get_dataset_last_processed_info_db(dataset_id: str) -> dict | None:
    """
    Retrieves the last processed info for a given dataset_id.
    Returns a dictionary with 'source_data_updated_at' etc., or None if not found.
    """
    conn_config = get_db_connection_config()
    sql = """
    SELECT dataset_id, source_data_updated_at, dataset_title, 
           last_processed_at_utc, last_processed_airflow_run_id, processing_remarks
    FROM processed_dataset_log
    WHERE dataset_id = %s;
    """
    try:
        with psycopg2.connect(**conn_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur: # Use RealDictCursor
                cur.execute(sql, (dataset_id,))
                row = cur.fetchone()
                if row:
                    # Convert datetime objects to ISO format strings if they aren't already
                    if row.get('source_data_updated_at') and isinstance(row['source_data_updated_at'], datetime):
                       row['source_data_updated_at'] = row['source_data_updated_at'].isoformat()
                    if row.get('last_processed_at_utc') and isinstance(row['last_processed_at_utc'], datetime):
                       row['last_processed_at_utc'] = row['last_processed_at_utc'].isoformat()
                    return dict(row)
                return None
    except Exception as e:
        logger.error(f"Error fetching processed info for {dataset_id}: {e}")
        return None

if __name__ == '__main__':
    # For local testing of this utility
    logging.basicConfig(level=logging.INFO)
    
    # Test Case 1: Update/Insert
    test_dataset_id = "test_dataset_001"
    logger.info(f"Attempting to update/insert {test_dataset_id}...")
    success = update_processed_dataset_log_db(
        dataset_id=test_dataset_id,
        source_data_updated_at="2024-01-15T10:00:00Z",
        dataset_title="Test Dataset Alpha",
        airflow_run_id="manual_test_run_1",
        remarks="Local test update"
    )
    if success:
        logger.info(f"Update/Insert for {test_dataset_id} successful.")
    else:
        logger.error(f"Update/Insert for {test_dataset_id} FAILED.")

    # Test Case 2: Retrieve
    logger.info(f"Attempting to retrieve info for {test_dataset_id}...")
    info = get_dataset_last_processed_info_db(test_dataset_id)
    if info:
        logger.info(f"Retrieved info for {test_dataset_id}: {info}")
    else:
        logger.error(f"Could not retrieve info for {test_dataset_id}.")

    # Test Case 3: Update existing
    logger.info(f"Attempting to update existing {test_dataset_id} again...")
    success_update = update_processed_dataset_log_db(
        dataset_id=test_dataset_id,
        source_data_updated_at="2024-01-16T12:30:00Z", # Newer date
        dataset_title="Test Dataset Alpha (Revised)",
        airflow_run_id="manual_test_run_2",
        remarks="Local test re-update"
    )
    if success_update:
        logger.info(f"Second update for {test_dataset_id} successful.")
        info_after_update = get_dataset_last_processed_info_db(test_dataset_id)
        logger.info(f"Retrieved info after second update: {info_after_update}")
    else:
        logger.error(f"Second update for {test_dataset_id} FAILED.")
        
    # Test case with None for source_data_updated_at
    test_dataset_id_null_date = "test_dataset_002_nulldate"
    logger.info(f"Attempting to update/insert {test_dataset_id_null_date} with null source date...")
    success_null = update_processed_dataset_log_db(
        dataset_id=test_dataset_id_null_date,
        source_data_updated_at=None, # Test None
        dataset_title="Test Dataset Beta (No Source Date)",
        airflow_run_id="manual_test_run_3",
        remarks="Local test null source date"
    )
    if success_null:
        logger.info(f"Update/Insert for {test_dataset_id_null_date} successful.")
        info_null = get_dataset_last_processed_info_db(test_dataset_id_null_date)
        logger.info(f"Retrieved info for {test_dataset_id_null_date}: {info_null}")
    else:
        logger.error(f"Update/Insert for {test_dataset_id_null_date} FAILED.") 