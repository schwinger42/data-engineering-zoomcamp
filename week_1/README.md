# Week 1: Docker & SQL

## Homework
This week's homework involves:
1. Understanding Docker.
2. Setting up Docker Compose for PostgreSQL and pgAdmin.
3. Writing SQL queries to analyze NYC Taxi data.

## Instructions
1. Run the `docker-compose.yaml` file to start the PostgreSQL and pgAdmin services:
   ```bash
   docker-compose up -d
    ```
2. Install Python Dependencies: Activate your environment and install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3. Run the Data Loading Script: Execute the Python script to download, extract, and load data into PostgreSQL:
    ```bash
    python scripts/load_data.py
    ```
4. Validate Data: Use the SQL queries in sql/validate_data.sql to verify the data:
   ```bash
   psql -h localhost -U postgres -d dataeng -f sql/validate_data.sql
   ```