# Crypto Airflow Project

This project implements a data pipeline for ingesting cryptocurrency hourly bar data using Apache Airflow, containerized with Docker.

# Download PowerBI #2 Dashboard to see the Dashboard built!


## Project Structure

- `crypto_airflow/`: Main application directory
  - `dags/`: Airflow DAGs
    - `crypto_ingestion_dag.py`: DAG for scheduling crypto data ingestion
  - `scripts/`: Python scripts for data processing
    - `ingest_hourly_bars.py`: Script to fetch and process hourly crypto bars
  - `Dockerfile`: Docker configuration for the Airflow environment
  - `docker-compose.yml`: Docker Compose setup for running Airflow

- `Project#1.py`: Main Python script (copy available as Project#1-copy.py)

## Setup and Running

1. Ensure Docker and Docker Compose are installed.

2. Navigate to the `crypto_airflow` directory.

3. Run the following command to start the Airflow services:
   ```
   docker-compose up -d
   ```

4. Access the Airflow web UI at `http://localhost:8080`.

5. The DAG `crypto_ingestion_dag` will be available to trigger or schedule.

## Requirements

- Docker
- Docker Compose
- Python 3.8+ (for local development)

## Data Source

The project ingests hourly bar data for cryptocurrencies. Ensure API keys or data sources are configured in the scripts as needed.

## Contributing

Feel free to submit issues or pull requests for improvements.
