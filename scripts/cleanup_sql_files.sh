#!/bin/bash

# Log file for cleanup operations
LOG_FILE="/opt/airflow/logs/cleanup.log"

# Function to log messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Directories to clean
DIRS_TO_CLEAN=(
    "/opt/airflow/dbt_project/models/marts"
    "/opt/airflow/dbt_project/models/facts"
    "/opt/airflow/dbt_project/models/dimensions"
)

log_message "Starting SQL files cleanup..."

# Clean each directory
for dir in "${DIRS_TO_CLEAN[@]}"; do
    if [ -d "$dir" ]; then
        log_message "Cleaning directory: $dir"
        # Count files before cleanup
        file_count=$(find "$dir" -name "*.sql" | wc -l)
        
        # Remove SQL files
        find "$dir" -name "*.sql" -type f -delete
        
        log_message "Removed $file_count SQL files from $dir"
    else
        log_message "Directory does not exist: $dir"
    fi
done

log_message "Cleanup completed" 