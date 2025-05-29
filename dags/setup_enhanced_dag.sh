#!/bin/bash

# 🚀 Enhanced Eurostat DAG Setup Script
# This script configures all necessary Airflow variables for the enhanced DAG

echo "🚀 Setting up Enhanced Eurostat DAG..."
echo "=================================="

# Core Configuration Variables
echo "📋 Setting core configuration variables..."

airflow variables set use_shared_health_datasets true
echo "✅ use_shared_health_datasets = true"

airflow variables set enhanced_batch_size 2000
echo "✅ enhanced_batch_size = 2000"

airflow variables set enable_streaming_load true
echo "✅ enable_streaming_load = true"

airflow variables set auto_generate_marts true
echo "✅ auto_generate_marts = true"

# Fallback datasets (used when shared health datasets are disabled)
airflow variables set eurostat_target_datasets '["hlth_cd_ainfo", "hlth_dh010"]'
echo "✅ eurostat_target_datasets = [\"hlth_cd_ainfo\", \"hlth_dh010\"]"

# Performance tuning variables
echo ""
echo "⚡ Setting performance tuning variables..."

airflow variables set max_parallel_downloads 5
echo "✅ max_parallel_downloads = 5"

airflow variables set max_parallel_loads 3
echo "✅ max_parallel_loads = 3"

# Optional debug mode (disabled by default)
airflow variables set enhanced_debug_mode false
echo "✅ enhanced_debug_mode = false"

echo ""
echo "🎯 Enhanced DAG setup completed!"
echo "=================================="
echo ""
echo "📊 Configuration Summary:"
echo "- Shared health datasets: ENABLED"
echo "- Batch size: 2000 rows"
echo "- Streaming load: ENABLED"
echo "- Auto-generate marts: ENABLED"
echo "- Debug mode: DISABLED"
echo ""
echo "🚀 You can now trigger the enhanced DAG:"
echo "   airflow dags trigger enhanced_eurostat_processor"
echo ""
echo "📖 For more options, see: ENHANCED_DAG_SETUP_GUIDE.md"
echo ""

# Verify variables were set correctly
echo "🔍 Verifying configuration..."
echo "=================================="

echo "use_shared_health_datasets: $(airflow variables get use_shared_health_datasets 2>/dev/null || echo 'NOT SET')"
echo "enhanced_batch_size: $(airflow variables get enhanced_batch_size 2>/dev/null || echo 'NOT SET')"
echo "enable_streaming_load: $(airflow variables get enable_streaming_load 2>/dev/null || echo 'NOT SET')"
echo "auto_generate_marts: $(airflow variables get auto_generate_marts 2>/dev/null || echo 'NOT SET')"

echo ""
echo "✅ Setup verification completed!"
echo ""
echo "🎉 Enhanced Eurostat DAG is ready to use!" 