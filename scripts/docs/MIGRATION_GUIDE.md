# Migration Guide: Redundancy Elimination

This guide helps you migrate from the old redundant scripts to the new consolidated, shared-module approach.

## 🎯 What Changed

We've eliminated redundant code by:
1. **Creating shared modules** for common functionality
2. **Consolidating scripts** that had overlapping purposes
3. **Standardizing configuration** across all scripts

## 📁 New Shared Modules

### `scripts/shared/database.py`
- **Purpose**: Centralized database connection and schema inspection
- **Replaces**: Redundant `get_db_connection()` and `get_table_schema()` functions across multiple files
- **Usage**: `from shared.database import EurostatDatabase`

### `scripts/shared/config.py`
- **Purpose**: Centralized configuration constants and dataset lists
- **Replaces**: Hardcoded values scattered across scripts
- **Usage**: `from shared.config import HEALTH_DATASETS, get_db_config`

### `scripts/shared/patterns.py`
- **Purpose**: Eurostat-specific column pattern detection and SQL generation
- **Replaces**: Duplicate pattern detection logic
- **Usage**: `from shared.patterns import detect_column_patterns, generate_staging_model_sql`

## 🔄 Script Migrations

### 1. `add_missing_sources.py` → `consolidated_model_generator.py`

**Old way:**
```bash
python add_missing_sources.py
```

**New way:**
```bash
python consolidated_model_generator.py --action sources
```

### 2. `generate_missing_staging_models.py` → `consolidated_model_generator.py`

**Old way:**
```bash
python generate_missing_staging_models.py
```

**New way:**
```bash
python consolidated_model_generator.py --action models
```

### 3. Combined Operations

**New capability:**
```bash
# Process both sources and models in one command
python consolidated_model_generator.py --action all

# Process specific datasets
python consolidated_model_generator.py --action all --dataset-ids "HLTH_CD_AINFO,HLTH_DH010"
```

## 🔧 Updated Scripts

### `dbt_model_generator.py`
- ✅ **Updated** to use shared modules
- ✅ **Eliminates** redundant database connection code
- ✅ **Uses** centralized pattern detection
- ✅ **Maintains** same CLI interface

### `topic_mart_generator.py`
- ✅ **Updated** to use shared database module
- ✅ **Eliminates** redundant schema inspection code
- ✅ **Maintains** same functionality

## 📋 Migration Checklist

### For Users:
- [ ] Update any scripts/DAGs that call `add_missing_sources.py`
- [ ] Update any scripts/DAGs that call `generate_missing_staging_models.py`
- [ ] Test new consolidated script with your datasets
- [ ] Update documentation/README files

### For Developers:
- [ ] Import shared modules instead of duplicating code
- [ ] Use `EurostatDatabase` class for database operations
- [ ] Use configuration constants from `shared.config`
- [ ] Use pattern detection functions from `shared.patterns`

## 🚀 Benefits of Migration

### Before (Redundant):
```python
# In multiple files:
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('EUROSTAT_POSTGRES_HOST', 'eurostat_postgres_db'),
        # ... repeated code
    )

# Hardcoded lists in multiple files:
missing_tables = ['hlth_cd_ainfo', 'hlth_cd_ainfr', ...]
```

### After (Consolidated):
```python
# In one place:
from shared.database import EurostatDatabase
from shared.config import HEALTH_DATASETS

db = EurostatDatabase()
datasets = HEALTH_DATASETS
```

### Improvements:
- ✅ **50% less code** through elimination of redundancy
- ✅ **Single source of truth** for configuration
- ✅ **Easier maintenance** - change once, apply everywhere
- ✅ **Better error handling** and logging
- ✅ **Consistent patterns** across all scripts

## 🔍 Testing Your Migration

### 1. Test Database Connection
```python
from shared.database import EurostatDatabase
db = EurostatDatabase()
tables = db.get_all_tables()
print(f"Found {len(tables)} tables")
```

### 2. Test Configuration
```python
from shared.config import HEALTH_DATASETS, get_db_config
print(f"Health datasets: {len(HEALTH_DATASETS)}")
print(f"DB config: {get_db_config()}")
```

### 3. Test Consolidated Script
```bash
# Test sources only
python consolidated_model_generator.py --action sources --log-level DEBUG

# Test models only  
python consolidated_model_generator.py --action models --log-level DEBUG

# Test everything
python consolidated_model_generator.py --action all --log-level DEBUG
```

## 🆘 Troubleshooting

### Import Errors
If you get import errors with shared modules:
```python
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))
```

### Database Connection Issues
Check environment variables are set correctly:
```bash
echo $EUROSTAT_POSTGRES_HOST
echo $EUROSTAT_POSTGRES_DB
echo $EUROSTAT_POSTGRES_USER
```

### Path Issues
Ensure shared modules are in the correct location:
```
scripts/
├── shared/
│   ├── __init__.py
│   ├── database.py
│   ├── config.py
│   └── patterns.py
└── your_script.py
```

## 📞 Support

If you encounter issues during migration:
1. Check the deprecation notices in the old script files
2. Review this migration guide
3. Test with `--log-level DEBUG` for detailed output
4. Ensure all shared modules are properly installed

## 🎉 Next Steps

After successful migration:
1. **Remove** the old redundant scripts (keep `.deprecated` versions for reference)
2. **Update** any documentation that references old scripts
3. **Consider** extending shared modules for other common functionality
4. **Enjoy** the cleaner, more maintainable codebase! 