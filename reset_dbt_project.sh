#!/bin/bash

echo "🔄 Starting comprehensive dbt project reset..."

# 1. Remove all generated models (keep only core structure)
echo "📁 Cleaning generated dbt models..."

# Remove all staging models (these are auto-generated)
echo "Removing staging models..."
rm -f dbt_project/models/staging/stg_*.sql
echo "✓ Removed staging models"

# Remove all mart models (these are auto-generated)
echo "Removing mart models..."
rm -f dbt_project/models/marts/mart_*.sql
echo "✓ Removed mart models"

# Remove all fact models (these are auto-generated)
echo "Removing fact models..."
rm -f dbt_project/models/facts/fact_*.sql
echo "✓ Removed fact models"

# Remove all dimension models (these are auto-generated)
echo "Removing dimension models..."
rm -f dbt_project/models/dimensions/dim_*.sql
echo "✓ Removed dimension models"

# 2. Clean up schema files (keep structure but remove auto-generated content)
echo "📁 Cleaning schema files..."

# Reset staging schema.yml to minimal structure
cat > dbt_project/models/staging/sources.yml << 'EOF'
version: 2

sources:
  - name: eurostat_raw
    description: "Raw Eurostat data tables"
    schema: public
    tables: []
      # Tables will be auto-generated when datasets are processed
EOF

# Reset marts schema.yml to minimal structure  
cat > dbt_project/models/marts/schema.yml << 'EOF'
version: 2

models: []
  # Models will be auto-generated when marts are created
EOF

# Reset dimensions schema.yml to minimal structure
cat > dbt_project/models/dimensions/dimensions.yml << 'EOF'
version: 2

models: []
  # Dimension models will be auto-generated when datasets are processed
EOF

# Reset facts schema.yml to minimal structure
cat > dbt_project/models/facts/facts.yml << 'EOF'
version: 2

models: []
  # Fact models will be auto-generated when datasets are processed
EOF

# Remove any auto-generated staging files
rm -f dbt_project/models/staging/schema_output.dbt 2>/dev/null || true

echo "✓ Reset all schema files to minimal structure"

# 3. Clean all dbt artifacts
echo "📁 Cleaning dbt artifacts..."
rm -rf dbt_project/target/*
rm -rf dbt_project/logs/*
rm -f dbt_project/dbt_packages.yml 2>/dev/null || true
rm -rf dbt_project/dbt_packages/ 2>/dev/null || true
echo "✓ Cleaned dbt artifacts"

# 4. Reset any custom macros (if auto-generated)
echo "📁 Cleaning auto-generated macros..."
rm -f dbt_project/macros/generate_*.sql 2>/dev/null || true
rm -f dbt_project/macros/auto_*.sql 2>/dev/null || true
echo "✓ Cleaned auto-generated macros"

# 5. Clean seeds directory
echo "📁 Cleaning seeds..."
rm -f dbt_project/seeds/*.csv 2>/dev/null || true
echo "✓ Cleaned seeds"

# 6. Clean tests directory of auto-generated tests
echo "📁 Cleaning auto-generated tests..."
rm -f dbt_project/tests/test_*.sql 2>/dev/null || true
echo "✓ Cleaned auto-generated tests"

# 7. Reset models_disabled directory
echo "📁 Cleaning disabled models..."
rm -f dbt_project/models_disabled/*.sql 2>/dev/null || true
echo "✓ Cleaned disabled models"

# 8. Show final dbt project structure
echo ""
echo "📊 Final dbt project structure:"
echo "================================"

echo "📁 Models directory:"
find dbt_project/models -name "*.sql" -o -name "*.yml" | sort

echo ""
echo "📁 Other dbt files:"
ls -la dbt_project/ | grep -E "\.(yml|yaml)$" | grep -v "^total"

echo ""
echo "🎉 dbt project reset completed!"
echo ""
echo "Your dbt project now has:"
echo "✅ Clean models directories (no auto-generated models)"
echo "✅ Minimal schema files ready for new content"
echo "✅ Clean artifacts and logs"
echo "✅ Ready for fresh model generation"
echo ""
echo "Next steps:"
echo "1. Run your data pipeline to generate new staging models"
echo "2. Auto-generated marts will be created based on new data"
echo "3. Schema files will be populated automatically" 