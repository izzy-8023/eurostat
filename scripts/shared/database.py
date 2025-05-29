#!/usr/bin/env python3
"""
Shared Database Module for Eurostat Pipeline

Centralizes all database connection and schema inspection functionality
to eliminate redundancy across multiple scripts.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class EurostatDatabase:
    """Centralized database connection and schema management for Eurostat pipeline"""
    
    def __init__(self, db_config: Optional[Dict[str, str]] = None):
        """
        Initialize database connection configuration
        
        Args:
            db_config: Optional database configuration dict. If None, uses environment variables.
        """
        self.config = db_config or {
            'host': os.getenv('EUROSTAT_POSTGRES_HOST', 'eurostat_postgres_db'),
            'port': os.getenv('EUROSTAT_POSTGRES_PORT', '5432'),
            'database': os.getenv('EUROSTAT_POSTGRES_DB', 'eurostat_data'),
            'user': os.getenv('EUROSTAT_POSTGRES_USER', 'eurostat_user'),
            'password': os.getenv('EUROSTAT_POSTGRES_PASSWORD', 'mysecretpassword')
        }
    
    def get_connection(self):
        """Get a database connection"""
        return psycopg2.connect(**self.config)
    
    def get_table_schema(self, table_name: str) -> List[Tuple[str, str]]:
        """
        Get table schema from database
        
        Args:
            table_name: Name of the table to inspect
            
        Returns:
            List of tuples (column_name, data_type)
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        AND table_schema = 'public'
                        ORDER BY ordinal_position
                    """, (table_name,))
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return []
    
    def get_table_columns_detailed(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get detailed column information for a table (returns dict format)
        
        Args:
            table_name: Name of the table to inspect
            
        Returns:
            List of dicts with column information
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = %s 
                        ORDER BY ordinal_position
                    """, (table_name,))
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Failed to get detailed columns for table {table_name}: {e}")
            return []
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    """, (table_name,))
                    return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to check if table {table_name} exists: {e}")
            return False
    
    def get_all_tables(self) -> List[str]:
        """
        Get list of all tables in the public schema
        
        Returns:
            List of table names
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        ORDER BY table_name
                    """)
                    return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get table list: {e}")
            return []

# Convenience function for backward compatibility
def get_db_connection():
    """Legacy function for backward compatibility"""
    db = EurostatDatabase()
    return db.get_connection()

def get_table_columns(table_name: str):
    """Legacy function for backward compatibility"""
    db = EurostatDatabase()
    return db.get_table_columns_detailed(table_name)

def get_table_schema(table_name: str):
    """Legacy function for backward compatibility"""
    db = EurostatDatabase()
    return db.get_table_schema(table_name) 