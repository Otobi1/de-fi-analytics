FROM ubuntu:latest
LABEL authors="tobi"

ENTRYPOINT ["top", "-b"]

# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set environment variables to optimize Python behavior
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies required by dbt and other Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Install dbt and the necessary adapter (e.g., dbt-bigquery)
RUN pip install dbt-bigquery

# Copy the requirements file and install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Set the working directory inside the container
WORKDIR /app

# Copy the rest of the application code into the container
COPY . /app

# Copy dbt profiles configuration
# It's recommended to manage sensitive information like credentials securely
# You might use environment variables or secret management tools instead
COPY profiles.yml /root/.dbt/profiles.yml

# Set environment variables for dbt
ENV DBT_PROFILES_DIR=/root/.dbt

# Expose port if your application requires it (optional)
# EXPOSE 8080
# Define the entry point for the container
# Replace 'ingest_data.py' with your actual data ingestion script
CMD ["python", "ingest_data.py"]
