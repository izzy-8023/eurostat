# Start with a slim Python base image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
# - curl is needed because SourceData.py uses it via subprocess
# - build-essential and other dev libraries might be needed for some Python packages (especially those with C extensions)
#   though for pandas/pyarrow, the pre-built wheels often suffice.
# - libpq-dev is needed for psycopg2
RUN apt-get update && apt-get install -y \
    curl \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the image
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python scripts into the image
COPY scripts/ ./scripts/  
# Copies the entire local 'scripts' dir to /app/scripts/

# Make directories that your scripts expect (optional, can be created at runtime too)
# These are relative to WORKDIR /app
RUN mkdir -p Data_Directory Output_Parquet_Directory Output_Directory temp_eurostat_downloads

# (Optional) You can set a default command, but for Airflow,
# you'll usually specify the command in the DAG task.
# CMD ["python", "SourceData.py", "--help"]


