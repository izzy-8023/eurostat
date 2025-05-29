#!/usr/bin/env python3
"""
Test script for Topic-Based Mart functionality

This script demonstrates how to use the generated topic marts for analysis.
"""

import psycopg2
import os
import pandas as pd
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class TopicMartAnalyzer:
    """Analyze data from topic-based marts"""
    
    def __init__(self, db_config: Dict[str, str] = None):
        """Initialize with database configuration"""
        self.db_config = db_config or {
            'host': os.getenv('POSTGRES_HOST', 'eurostat_postgres_db'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'eurostat_data'),
            'user': os.getenv('POSTGRES_USER', 'eurostat_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')
        }
    
    def get_available_marts(self) -> List[str]:
        """Get list of available topic marts"""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'dbt_prod' 
                        AND table_name LIKE 'mart_%'
                        ORDER BY table_name
                    """)
                    return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get available marts: {e}")
            return []
    
    def analyze_absence_from_work(self) -> Dict[str, any]:
        """Analyze absence from work data across different dimensions"""
        mart_name = "mart_absence_from_work_due_to_personal_health_problems"
        
        analyses = {}
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                # 1. Overall summary by source dataset
                query1 = f"""
                    SELECT 
                        source_dataset_id,
                        COUNT(*) as total_records,
                        COUNT(DISTINCT geo_code) as countries,
                        COUNT(DISTINCT time_code) as time_periods,
                        AVG(value) as avg_value,
                        MAX(value) as max_value
                    FROM dbt_prod.{mart_name}
                    WHERE value IS NOT NULL
                    GROUP BY source_dataset_id
                    ORDER BY source_dataset_id
                """
                analyses['dataset_summary'] = pd.read_sql(query1, conn)
                
                # 2. Analysis by disability level (only available in HLTH_EHIS_AW1D)
                query2 = f"""
                    SELECT 
                        lev_limit_code,
                        lev_limit_name,
                        COUNT(*) as records,
                        AVG(value) as avg_absence_rate,
                        COUNT(DISTINCT geo_code) as countries
                    FROM dbt_prod.{mart_name}
                    WHERE lev_limit_code IS NOT NULL 
                    AND value IS NOT NULL
                    GROUP BY lev_limit_code, lev_limit_name
                    ORDER BY avg_absence_rate DESC
                """
                analyses['by_disability'] = pd.read_sql(query2, conn)
                
                # 3. Analysis by education level (only available in HLTH_EHIS_AW1E)
                query3 = f"""
                    SELECT 
                        isced11_code,
                        isced11_name,
                        COUNT(*) as records,
                        AVG(value) as avg_absence_rate,
                        COUNT(DISTINCT geo_code) as countries
                    FROM dbt_prod.{mart_name}
                    WHERE isced11_code IS NOT NULL 
                    AND value IS NOT NULL
                    GROUP BY isced11_code, isced11_name
                    ORDER BY avg_absence_rate DESC
                """
                analyses['by_education'] = pd.read_sql(query3, conn)
                
                # 4. Analysis by urbanization (only available in HLTH_EHIS_AW1U)
                query4 = f"""
                    SELECT 
                        deg_urb_code,
                        deg_urb_name,
                        COUNT(*) as records,
                        AVG(value) as avg_absence_rate,
                        COUNT(DISTINCT geo_code) as countries
                    FROM dbt_prod.{mart_name}
                    WHERE deg_urb_code IS NOT NULL 
                    AND value IS NOT NULL
                    GROUP BY deg_urb_code, deg_urb_name
                    ORDER BY avg_absence_rate DESC
                """
                analyses['by_urbanization'] = pd.read_sql(query4, conn)
                
                # 5. Cross-dimensional analysis (common dimensions)
                query5 = f"""
                    SELECT 
                        sex_code,
                        sex_name,
                        age_code,
                        age_name,
                        COUNT(*) as records,
                        AVG(value) as avg_absence_rate,
                        COUNT(DISTINCT source_dataset_id) as datasets_available
                    FROM dbt_prod.{mart_name}
                    WHERE value IS NOT NULL
                    GROUP BY sex_code, sex_name, age_code, age_name
                    HAVING COUNT(DISTINCT source_dataset_id) >= 2  -- Available in multiple datasets
                    ORDER BY avg_absence_rate DESC
                    LIMIT 10
                """
                analyses['cross_dimensional'] = pd.read_sql(query5, conn)
                
        except Exception as e:
            logger.error(f"Failed to analyze absence from work data: {e}")
            analyses['error'] = str(e)
        
        return analyses
    
    def analyze_fruit_vegetables_consumption(self) -> Dict[str, any]:
        """Analyze fruit and vegetables consumption data"""
        mart_name = "mart_daily_consumption_of_fruit_and_vegetables"
        
        analyses = {}
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                # 1. Overall summary by source dataset
                query1 = f"""
                    SELECT 
                        source_dataset_id,
                        COUNT(*) as total_records,
                        COUNT(DISTINCT geo_code) as countries,
                        AVG(value) as avg_consumption_rate
                    FROM dbt_prod.{mart_name}
                    WHERE value IS NOT NULL
                    GROUP BY source_dataset_id
                    ORDER BY source_dataset_id
                """
                analyses['dataset_summary'] = pd.read_sql(query1, conn)
                
                # 2. Top countries by consumption rate
                query2 = f"""
                    SELECT 
                        geo_code,
                        geo_name,
                        AVG(value) as avg_consumption_rate,
                        COUNT(DISTINCT source_dataset_id) as datasets_available
                    FROM dbt_prod.{mart_name}
                    WHERE value IS NOT NULL
                    AND geo_code NOT IN ('EU27_2020', 'EU28', 'TOTAL')  -- Exclude aggregates
                    GROUP BY geo_code, geo_name
                    ORDER BY avg_consumption_rate DESC
                    LIMIT 10
                """
                analyses['top_countries'] = pd.read_sql(query2, conn)
                
                # 3. Gender differences
                query3 = f"""
                    SELECT 
                        sex_code,
                        sex_name,
                        AVG(value) as avg_consumption_rate,
                        COUNT(*) as records
                    FROM dbt_prod.{mart_name}
                    WHERE value IS NOT NULL
                    AND sex_code != 'T'  -- Exclude total
                    GROUP BY sex_code, sex_name
                    ORDER BY avg_consumption_rate DESC
                """
                analyses['by_gender'] = pd.read_sql(query3, conn)
                
        except Exception as e:
            logger.error(f"Failed to analyze fruit and vegetables data: {e}")
            analyses['error'] = str(e)
        
        return analyses
    
    def print_analysis_results(self, analysis_name: str, results: Dict[str, any]):
        """Print analysis results in a formatted way"""
        print(f"\n{'='*60}")
        print(f"ANALYSIS: {analysis_name.upper()}")
        print(f"{'='*60}")
        
        if 'error' in results:
            print(f"❌ Error: {results['error']}")
            return
        
        for section_name, df in results.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                print(f"\n📊 {section_name.replace('_', ' ').title()}:")
                print("-" * 40)
                print(df.to_string(index=False))
            elif isinstance(df, pd.DataFrame):
                print(f"\n📊 {section_name.replace('_', ' ').title()}: No data available")

def main():
    """Main function to run topic mart analysis"""
    logging.basicConfig(level=logging.INFO)
    
    analyzer = TopicMartAnalyzer()
    
    # Get available marts
    print("🔍 Checking available topic marts...")
    available_marts = analyzer.get_available_marts()
    
    if not available_marts:
        print("❌ No topic marts found in the database")
        return
    
    print(f"✅ Found {len(available_marts)} topic marts:")
    for mart in available_marts:
        print(f"  - {mart}")
    
    # Analyze absence from work if available
    if "mart_absence_from_work_due_to_personal_health_problems" in available_marts:
        print("\n🏥 Analyzing absence from work data...")
        absence_results = analyzer.analyze_absence_from_work()
        analyzer.print_analysis_results("Absence from Work Analysis", absence_results)
    
    # Analyze fruit and vegetables consumption if available
    if "mart_daily_consumption_of_fruit_and_vegetables" in available_marts:
        print("\n🍎 Analyzing fruit and vegetables consumption data...")
        fruit_veg_results = analyzer.analyze_fruit_vegetables_consumption()
        analyzer.print_analysis_results("Fruit & Vegetables Consumption Analysis", fruit_veg_results)
    
    print(f"\n{'='*60}")
    print("✅ Topic mart analysis completed!")
    print(f"{'='*60}")

if __name__ == "__main__":
    main() 