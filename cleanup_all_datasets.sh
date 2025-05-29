#!/bin/bash

echo "🧹 Starting comprehensive dataset cleanup..."

# 1. Remove downloaded JSON datasets
echo "📁 Cleaning Data_Directory..."
rm -f Data_Directory/*.json
rm -f Data_Directory/.DS_Store
echo "✓ Removed JSON datasets from Data_Directory"

# 2. Remove processed output files
echo "📁 Cleaning Output_Directory..."
rm -f Output_Directory/*.csv
rm -f Output_Directory/*.json
rm -f Output_Directory/*.sql
rm -f Output_Directory/.DS_Store
echo "✓ Removed processed files from Output_Directory"

# 3. Remove Parquet files
echo "📁 Cleaning Output_Parquet_Directory..."
rm -rf Output_Parquet_Directory/*
echo "✓ Removed Parquet files"

# 4. Remove temporary download directories (if any)
echo "📁 Cleaning temporary directories..."
find . -name "*temp_processor_downloads*" -type d -exec rm -rf {} + 2>/dev/null || true
find . -name "*temp_test_data*" -type d -exec rm -rf {} + 2>/dev/null || true
echo "✓ Removed temporary directories"

# 5. Clean up dbt artifacts and reset dbt project
echo "📁 Cleaning dbt artifacts and resetting dbt project..."
rm -rf dbt_project/target/*
rm -rf dbt_project/logs/*

# Reset dbt models to fresh state
echo "Resetting dbt models to fresh state..."
rm -f dbt_project/models/staging/stg_*.sql
rm -f dbt_project/models/marts/mart_*.sql
rm -f dbt_project/models/facts/fact_*.sql
rm -f dbt_project/models/dimensions/dim_*.sql

# Reset schema files to minimal structure
cat > dbt_project/models/staging/sources.yml << 'EOF'
version: 2

sources:
  - name: eurostat_raw
    description: "Raw Eurostat data tables"
    schema: public
    tables: []
      # Tables will be auto-generated when datasets are processed
EOF

cat > dbt_project/models/marts/schema.yml << 'EOF'
version: 2

models: []
  # Models will be auto-generated when marts are created
EOF

cat > dbt_project/models/dimensions/dimensions.yml << 'EOF'
version: 2

models: []
  # Dimension models will be auto-generated when datasets are processed
EOF

cat > dbt_project/models/facts/facts.yml << 'EOF'
version: 2

models: []
  # Fact models will be auto-generated when datasets are processed
EOF

# Remove any auto-generated staging files
rm -f dbt_project/models/staging/schema_output.dbt 2>/dev/null || true

echo "✓ Cleaned dbt artifacts and reset project to fresh state"

# 6. Remove database tables (both main DB and Airflow metadata)
echo "🗄️  Cleaning database tables..."

# Clean main eurostat database
echo "Cleaning main eurostat_data database..."
docker exec eurostat_postgres_db psql -U eurostat_user -d eurostat_data -c "
-- Drop all tables in public schema that start with hlth
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'hlth%')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Dropped table: %', r.tablename;
    END LOOP;
END
\$\$;

-- Drop all tables in dbt_prod schema
DROP SCHEMA IF EXISTS dbt_prod CASCADE;
CREATE SCHEMA dbt_prod;

-- List remaining tables
SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('public', 'dbt_prod') ORDER BY schemaname, tablename;
"

echo "✓ Cleaned database tables"

# 7. Reset Airflow variables (optional - comment out if you want to keep them)
echo "🔄 Resetting Airflow variables..."

# Get the correct Airflow scheduler container name dynamically
SCHEDULER_CONTAINER=$(docker ps --format "{{.Names}}" | grep scheduler | head -1)

if [ -n "$SCHEDULER_CONTAINER" ]; then
    docker exec "$SCHEDULER_CONTAINER" airflow variables delete processed_hlth_rss_ids 2>/dev/null || true
    docker exec "$SCHEDULER_CONTAINER" airflow variables delete all_hlth_dataset_details 2>/dev/null || true
    docker exec "$SCHEDULER_CONTAINER" airflow variables delete eurostat_target_datasets 2>/dev/null || true
    echo "✓ Reset Airflow variables"
    
    # 8. Clear Airflow DAG runs and task instances for cleanup
    echo "🔄 Clearing Airflow DAG run history..."
    docker exec "$SCHEDULER_CONTAINER" airflow db clean --clean-before-timestamp $(date -d '1 hour ago' -u +%Y-%m-%dT%H:%M:%S) --yes 2>/dev/null || true
    echo "✓ Cleaned Airflow history"
else
    echo "⚠️  Could not find Airflow scheduler container - skipping Airflow cleanup"
fi

# 9. Show final status
echo ""
echo "📊 Final Status:"
echo "=================="

echo "📁 Data_Directory contents:"
ls -la Data_Directory/ | grep -v "^total"

echo ""
echo "📁 Output_Directory contents:"
ls -la Output_Directory/ | grep -v "^total"

echo ""
echo "📁 Output_Parquet_Directory contents:"
ls -la Output_Parquet_Directory/ | grep -v "^total"

echo ""
echo "🗄️  Database tables:"
docker exec eurostat_postgres_db psql -U eurostat_user -d eurostat_data -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('public', 'dbt_prod') ORDER BY schemaname, tablename;" 2>/dev/null || echo "Could not connect to database"

echo ""
echo "🎉 Cleanup completed! You now have a fresh start."
echo ""
echo "Next steps:"
echo "1. You can now run your DAGs with new dataset selections"
echo "2. All previous downloads and processed data have been removed"
echo "3. Database is clean and ready for new data" 