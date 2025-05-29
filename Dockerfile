# Enhanced Eurostat Pipeline Dockerfile
# Optimized for the cleaned-up project structure with shared modules

FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
# - curl is needed for SourceData.py data downloads
# - libpq-dev is needed for psycopg2 (PostgreSQL connections)
# - build-essential for any packages that need compilation
# - docker.io for Docker CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    build-essential \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the image
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the enhanced scripts with shared modules
COPY scripts/ ./scripts/
# This now includes:
# - 8 core enhanced scripts
# - shared/ directory with centralized modules
# - docs/ directory with documentation
# - backups/ directory (minimal, cleaned up)

# Create necessary directories for the enhanced pipeline
RUN mkdir -p \
    Data_Directory \
    Output_Directory \
    temp_eurostat_downloads \
    dbt_project \
    logs

# Set Python path to include scripts and shared modules
ENV PYTHONPATH="${PYTHONPATH}:/app/scripts:/app/scripts/shared"

# Create a non-root user for security (commented out for simplicity with docker.sock)
# RUN useradd --create-home --shell /bin/bash eurostat
# RUN chown -R eurostat:eurostat /app
# USER eurostat

# Default command (can be overridden in docker-compose.yml)
# CMD ["python", "scripts/SourceData.py", "--help"]


