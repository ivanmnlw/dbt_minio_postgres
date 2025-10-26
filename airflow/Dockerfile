# Use the official Airflow image with Python 3.11
FROM apache/airflow:latest-python3.11

# Switch to root to install additional system packages
USER root

# Install dependencies
RUN sudo apt-get update \
    && apt-get install -y --no-install-recommends \
    git \
    gcc \
    python3-distutils

# Switch back to airflow user
USER airflow

# Set profiles.yml environment variable
ENV DBT_PROFILES_DIR=/dbt_s3

RUN python -m pip install --upgrade pip

# Install Airflow and required providers
RUN pip install --no-cache-dir \
    dbt-postgres==1.9.1