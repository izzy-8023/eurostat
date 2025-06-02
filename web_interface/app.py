#!/usr/bin/env python3
"""
Simplified Eurostat Dataset Web Interface

A lightweight Flask web application for browsing and processing Eurostat datasets.
"""
# Web UI
from flask import Flask, render_template, request, jsonify, redirect, url_for
import requests
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import traceback
import subprocess

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Flask app configuration
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')

# Database configuration
DB_CONFIG = {
    'host': os.environ.get('POSTGRES_HOST', 'localhost'),
    'port': int(os.environ.get('POSTGRES_PORT', 5433)),
    'database': os.environ.get('POSTGRES_DB', 'eurostat_data'),
    'user': os.environ.get('POSTGRES_USER', 'eurostat_user'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'mysecretpassword')
}

# Airflow API configuration
AIRFLOW_API_URL = os.environ.get('AIRFLOW_API_URL', 'http://localhost:8080/api/v2')
AIRFLOW_USERNAME = os.environ.get('AIRFLOW_USERNAME', 'admin')
AIRFLOW_PASSWORD = os.environ.get('AIRFLOW_PASSWORD', 'admin')

def get_db_connection():
    """Get database connection"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def get_dataset_stats():
    """Get health dataset statistics"""
    conn = get_db_connection()
    if not conn:
        return {'total_datasets': 0, 'processed_datasets': 0, 'recent_jobs': 0}
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Count processed health datasets (tables in public schema starting with 'hlth_')
            cur.execute("SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'hlth_%'")
            processed_count = cur.fetchone()['count']
            
            # Get total health datasets from health_datasets.csv
            health_csv_path = Path(__file__).parent.parent / 'health_datasets.csv'
            total_count = 0
            if health_csv_path.exists():
                try:
                    import pandas as pd
                    df = pd.read_csv(health_csv_path)
                    total_count = len(df)
                except Exception as e:
                    logger.error(f"Failed to read health datasets CSV: {e}")
                    total_count = 490  # Fallback to known count
            
            return {
                'total_datasets': total_count,
                'processed_datasets': processed_count,
                'recent_jobs': 0
            }
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        return {'total_datasets': 0, 'processed_datasets': 0, 'recent_jobs': 0}
    finally:
        conn.close()

def get_available_datasets():
    """Get available datasets from catalog"""
    catalog_path = Path(__file__).parent.parent / 'eurostat_full_catalog.json'
    if not catalog_path.exists():
        return []
    
    try:
        with open(catalog_path, 'r') as f:
            catalog = json.load(f)
            
        datasets = []
        items = catalog.get('link', {}).get('item', [])
        
        for item in items:
            if item.get('class') == 'dataset':
                extension = item.get('extension', {})
                dataset_id = extension.get('id', '')
                
                datasets.append({
                    'id': dataset_id,
                    'title': item.get('label', dataset_id),
                    'description': extension.get('description', ''),
                    'category': 'eurostat',
                    'last_updated': extension.get('last_update', ''),
                    'processed': check_if_processed(dataset_id)
                })
        
        return datasets
    except Exception as e:
        logger.error(f"Failed to load catalog: {e}")
        return []

def check_if_processed(dataset_id):
    """Check if dataset has been processed"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            # Check if the dataset table exists in public schema (using lowercase dataset_id)
            cur.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{dataset_id.lower()}')")
            return cur.fetchone()[0]
    except Exception as e:
        return False
    finally:
        conn.close()

def get_health_datasets():
    """Get health-related datasets with processed status"""
    health_csv_path = Path(__file__).parent.parent / 'health_datasets.csv'
    if not health_csv_path.exists():
        return []
    
    try:
        import pandas as pd
        df = pd.read_csv(health_csv_path)
        
        # Map CSV columns to expected format
        datasets = []
        for _, row in df.iterrows():
            dataset_id = row['ID']
            
            # Format the last updated date
            last_updated = row.get('UpdateDataDate', '')
            if last_updated:
                try:
                    # Parse the ISO format date and convert to readable format
                    from datetime import datetime
                    dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    last_updated = dt.strftime('%Y-%m-%d')
                except:
                    # If parsing fails, keep original or use empty
                    last_updated = last_updated[:10] if len(last_updated) >= 10 else ''
            
            datasets.append({
                'id': dataset_id,
                'title': row['Label'],
                'category': 'health',
                'description': row['Label'],  # Use label as description
                'created_date': row.get('CreatedDate', ''),
                'last_updated': last_updated,
                'processed': check_if_processed(dataset_id)  # Add processed status
            })
        
        return datasets
    except Exception as e:
        logger.error(f"Failed to load health datasets: {e}")
        return []

# Routes
@app.route('/')
def index():
    """Main dashboard"""
    stats = get_dataset_stats()
    return render_template('index.html', stats=stats)

@app.route('/datasets')
def datasets():
    """Dataset browser - Health datasets only"""
    search = request.args.get('search', '')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    # Only show health datasets
    all_health_datasets = get_health_datasets()
    
    # Filter by search if provided
    if search:
        filtered_datasets = [
            d for d in all_health_datasets 
            if search.lower() in d.get('title', '').lower() or 
               search.lower() in d.get('id', '').lower()
        ]
    else:
        filtered_datasets = all_health_datasets
    
    # Calculate pagination
    total_datasets = len(filtered_datasets)
    total_pages = (total_datasets + per_page - 1) // per_page
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    # Get datasets for current page
    datasets_page = filtered_datasets[start_idx:end_idx]
    
    # Pagination info
    pagination = {
        'page': page,
        'per_page': per_page,
        'total': total_datasets,
        'total_pages': total_pages,
        'has_prev': page > 1,
        'has_next': page < total_pages,
        'prev_num': page - 1 if page > 1 else None,
        'next_num': page + 1 if page < total_pages else None
    }
    
    return render_template('datasets.html', 
                         datasets=datasets_page,
                         pagination=pagination,
                         search_term=search)

@app.route('/dataset/<dataset_id>')
def dataset_detail(dataset_id):
    """Dataset detail view"""
    dataset_info = {}
    
    # First try to get info from health datasets CSV
    health_csv_path = Path(__file__).parent.parent / 'health_datasets.csv'
    if health_csv_path.exists():
        try:
            import pandas as pd
            df = pd.read_csv(health_csv_path)
            health_dataset = df[df['ID'] == dataset_id]
            
            if not health_dataset.empty:
                row = health_dataset.iloc[0]
                # Format the last updated date
                last_updated = row.get('UpdateDataDate', '')
                if last_updated:
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                        last_updated = dt.strftime('%Y-%m-%d')
                    except:
                        last_updated = last_updated[:10] if len(last_updated) >= 10 else ''
                
                dataset_info = {
                    'id': dataset_id,
                    'title': row['Label'],
                    'description': row['Label'],
                    'category': 'health',
                    'last_updated': last_updated
                }
        except Exception as e:
            logger.error(f"Failed to load health dataset info: {e}")
    
    # If not found in health datasets, try catalog
    if not dataset_info:
        catalog_path = Path(__file__).parent.parent / 'eurostat_full_catalog.json'
        if catalog_path.exists():
            try:
                with open(catalog_path, 'r') as f:
                    catalog = json.load(f)
                    items = catalog.get('link', {}).get('item', [])
                    
                    # Find the dataset by ID
                    for item in items:
                        if item.get('class') == 'dataset':
                            extension = item.get('extension', {})
                            if extension.get('id') == dataset_id:
                                # Format the last updated date
                                last_updated = extension.get('last_update', '')
                                if last_updated:
                                    try:
                                        from datetime import datetime
                                        dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                                        last_updated = dt.strftime('%Y-%m-%d')
                                    except:
                                        last_updated = last_updated[:10] if len(last_updated) >= 10 else ''
                                
                                dataset_info = {
                                    'id': dataset_id,
                                    'title': item.get('label', dataset_id),
                                    'description': extension.get('description', ''),
                                    'category': 'eurostat',
                                    'last_updated': last_updated
                                }
                                break
            except Exception as e:
                logger.error(f"Failed to load dataset info: {e}")
    
    # Check if processed
    processed = check_if_processed(dataset_id)
    
    # Get table info if processed
    table_info = None
    if processed:
        table_info = get_table_info(dataset_id.lower())
    
    return render_template('dataset_detail.html', 
                         dataset_id=dataset_id,
                         dataset_info=dataset_info,
                         processed=processed,
                         table_info=table_info)

def get_table_info(table_name):
    """Get table information"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get column info
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            columns = cur.fetchall()
            
            # Get row count
            cur.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            row_count = cur.fetchone()['count']
            
            return {
                'columns': columns,
                'row_count': row_count
            }
    except Exception as e:
        logger.error(f"Failed to get table info: {e}")
        return None
    finally:
        conn.close()

@app.route('/process')
def process_datasets():
    """Dataset selection for processing with pagination"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    search = request.args.get('search', '', type=str)
    
    # Get all health datasets
    all_health_datasets = get_health_datasets()
    
    # Filter by search if provided
    if search:
        filtered_datasets = [
            d for d in all_health_datasets 
            if search.lower() in d.get('title', '').lower() or 
               search.lower() in d.get('id', '').lower()
        ]
    else:
        filtered_datasets = all_health_datasets
    
    # Calculate pagination
    total_datasets = len(filtered_datasets)
    total_pages = (total_datasets + per_page - 1) // per_page
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    # Get datasets for current page
    datasets_page = filtered_datasets[start_idx:end_idx]
    
    # Pagination info
    pagination = {
        'page': page,
        'per_page': per_page,
        'total': total_datasets,
        'total_pages': total_pages,
        'has_prev': page > 1,
        'has_next': page < total_pages,
        'prev_num': page - 1 if page > 1 else None,
        'next_num': page + 1 if page < total_pages else None
    }
    
    return render_template('process.html', 
                         datasets=datasets_page,
                         pagination=pagination,
                         search_term=search)

@app.route('/api/trigger_processing', methods=['POST'])
def trigger_processing():
    """Trigger the dynamic processing pipeline using Docker exec"""
    try:
        data = request.get_json()
        dataset_ids = data.get('dataset_ids', [])
        
        if not dataset_ids:
            return jsonify({'error': 'No datasets selected'}), 400
        
        # Generate unique run ID
        dag_run_id = f"manual__{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Create configuration JSON
        config_json = json.dumps({'dataset_ids': dataset_ids})
        
        # Use Docker exec to trigger the DAG via Airflow CLI
        cmd = [
            'docker', 'exec', 'eurostat-airflow-scheduler-1',
            'airflow', 'dags', 'trigger',
            'enhanced_eurostat_processor',
            '--run-id', dag_run_id,
            '--conf', config_json
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            return jsonify({
                'success': True,
                'run_id': dag_run_id,
                'message': f'Processing started for {len(dataset_ids)} datasets',
                'output': result.stdout
            })
        else:
            return jsonify({
                'error': f'Failed to trigger processing: {result.stderr}',
                'output': result.stdout
            }), 500
            
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Command timed out'}), 500
    except Exception as e:
        logger.error(f"Failed to trigger processing: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trigger_rss_dag', methods=['POST'])
def trigger_rss_dag():
    """Trigger the Eurostat Health RSS Monitor DAG using Docker exec"""
    try:
        # Generate unique run ID for the RSS DAG
        dag_run_id = f"manual_rss_monitor__{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Docker exec command to trigger the RSS monitor DAG
        # Assumes 'eurostat-airflow-scheduler-1' is the correct container name.
        # Consider making this configurable if it changes often.
        cmd = [
            'docker', 'exec', 'eurostat-airflow-scheduler-1',
            'airflow', 'dags', 'trigger',
            'eurostat_health_rss_monitor_dag', # Target DAG ID
            '--run-id', dag_run_id
            # No specific --conf needed for this DAG trigger from UI initially
            # but could be added if we want to pass params, e.g., force full scan.
        ]
        
        logger.info(f"Attempting to trigger RSS DAG with command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            logger.info(f"Successfully triggered RSS DAG. Run ID: {dag_run_id}. Output: {result.stdout}")
            return jsonify({
                'success': True,
                'run_id': dag_run_id,
                'message': 'Eurostat Health RSS Monitor DAG triggered successfully.',
                'output': result.stdout
            })
        else:
            logger.error(f"Failed to trigger RSS DAG. Return Code: {result.returncode}. Stderr: {result.stderr}. Stdout: {result.stdout}")
            return jsonify({
                'error': f'Failed to trigger RSS Monitor DAG: {result.stderr or result.stdout}',
                'output': result.stdout,
                'stderr': result.stderr
            }), 500
            
    except subprocess.TimeoutExpired:
        logger.error("Command timed out while trying to trigger RSS DAG.")
        return jsonify({'error': 'Command timed out while triggering RSS Monitor DAG'}), 500
    except Exception as e:
        logger.error(f"An unexpected error occurred while triggering RSS DAG: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500

@app.route('/topic_marts')
def topic_marts():
    """View topic marts"""
    conn = get_db_connection()
    marts = []
    mart_files_info = []
    
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur: # Use RealDictCursor for all ops in this block
                schemas_to_check = ['dbt_prod', 'public']
                all_found_tables = []

                for schema in schemas_to_check:
                    try:
                        logger.info(f"DEBUG: Current schema variable for query: '{schema}', type: {type(schema)}")
                        logger.debug(f"Attempting to list marts in schema: '{schema}' with RealDictCursor.")
                        
                        # Query with LIKE pattern as a parameter (the successful approach)
                        sql_query = "SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema = %s AND table_name LIKE %s ORDER BY table_name"
                        like_pattern = 'mart_%'
                        sql_params = (schema, like_pattern)
                        
                        logger.info("Executing SQL with RealDictCursor: %s with params: %s", sql_query, sql_params)
                        cur.execute(sql_query, sql_params)
                        
                        logger.debug(f"Query executed for schema '{schema}'. Fetching all results...")
                        
                        # fetchall() with RealDictCursor returns a list of dictionaries directly
                        tables_in_schema = cur.fetchall()
                        
                        if tables_in_schema:
                            all_found_tables.extend(tables_in_schema)
                        logger.debug(f"Found {len(tables_in_schema) if tables_in_schema else 0} tables in schema '{schema}'.")

                    except Exception as e_list_tables:
                        logger.warning(f"Could not list tables for schema {schema} with RealDictCursor. Error: {e_list_tables}\nTraceback: {traceback.format_exc()}")
                        continue # Try next schema
                
                logger.info(f"Found a total of {len(all_found_tables)} potential mart tables across all checked schemas.")

                # Process `all_found_tables` (which are already list-of-dicts)
                # The main `cur` is already a RealDictCursor, so it can be used for row counts.
                for table_info_row in all_found_tables: # table_info_row is a dict from RealDictCursor
                    try:
                        table_name = table_info_row.get('table_name')
                        table_schema_from_row = table_info_row.get('table_schema')

                        if not table_name or not table_schema_from_row:
                            logger.error(f"Invalid table_info_row (missing name or schema from DB query result): {table_info_row}")
                            continue
                        
                        logger.info(f"Processing collected table: {table_schema_from_row}.{table_name}")
                        
                        row_count = 0
                        try:
                            # Use the main `cur` (RealDictCursor) for row count fetching
                            cur.execute(f"SELECT COUNT(*) as count FROM \"{table_schema_from_row}\".\"{table_name}\"")
                            count_result = cur.fetchone()
                            logger.debug(f"Row count result for {table_schema_from_row}.{table_name}: {count_result}")
                            if count_result and 'count' in count_result:
                                row_count = count_result['count']
                            else:
                                logger.warning(f"Could not get 'count' from count_result for {table_schema_from_row}.{table_name}. Result was: {count_result}")
                                row_count = 0
                        except Exception as e_count:
                            logger.error(f"Failed to get row count for {table_schema_from_row}.{table_name}. Error: {e_count}\nTraceback: {traceback.format_exc()}")
                            row_count = 0
                        
                        marts.append({
                            'name': table_name,
                            'schema': table_schema_from_row,
                            'display_name': table_name.replace('mart_', '').replace('_', ' ').title(),
                            'row_count': row_count,
                            'full_name': f"{table_schema_from_row}.{table_name}"
                        })
                    except Exception as e_process_row:
                        logger.error(f"Error processing an individual table data row '{table_info_row}'. Error: {e_process_row}\nTraceback: {traceback.format_exc()}")
                        continue
            
            mart_files_info = get_mart_files_status()
                
        except Exception as e_outer:
            logger.error(f"Major error in topic_marts function. Error: {e_outer}\nTraceback: {traceback.format_exc()}")
            if not mart_files_info:
                mart_files_info = get_mart_files_status()
        finally:
            if conn:
                conn.close()
    
    else:
        logger.error("Database connection was not established. Skipping DB operations for topic marts.")
        mart_files_info = get_mart_files_status()
    
    return render_template('topic_marts.html', marts=marts, mart_files=mart_files_info)

def get_mart_files_status():
    """Get status of mart files in dbt models directory"""
    mart_files = []
    mart_dir = Path(__file__).parent.parent / 'dbt_project' / 'models' / 'marts'
    
    if mart_dir.exists():
        for sql_file in mart_dir.glob('mart_*.sql'):
            mart_files.append({
                'filename': sql_file.name,
                'name': sql_file.stem,
                'display_name': sql_file.stem.replace('mart_', '').replace('_', ' ').title(),
                'size': sql_file.stat().st_size,
                'modified': sql_file.stat().st_mtime
            })
    
    return mart_files

@app.route('/api/query_mart/<mart_name>')
def query_mart(mart_name):
    """Query a topic mart or dataset table"""
    limit = request.args.get('limit', 100, type=int)
    schema = request.args.get('schema', None)  # Allow schema to be specified
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            schemas_to_try = [schema] if schema else ['dbt_prod', 'public']
            
            success = False
            for schema_name in schemas_to_try:
                if not schema_name:
                    continue
                    
                try:
                    # Use proper schema qualification
                    qualified_table = f"{schema_name}.{mart_name}"
                    cur.execute(f"SELECT * FROM {qualified_table} LIMIT %s", (limit,))
                    rows = cur.fetchall()
                    schema_used = schema_name
                    success = True
                    break
                except Exception as e:
                    logger.debug(f"Failed to query {qualified_table}: {e}")
                    continue
            
            if not success:
                return jsonify({'error': f'Table {mart_name} not found in any schema'}), 404
            
            # Convert to list of dicts
            data = [dict(row) for row in rows]
            
            return jsonify({
                'success': True,
                'data': data,
                'count': len(data),
                'schema': schema_used,
                'table': mart_name
            })
    except Exception as e:
        logger.error(f"Failed to query table {mart_name}: {e}")
        return jsonify({'error': f'Query failed: {str(e)}'}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001) 