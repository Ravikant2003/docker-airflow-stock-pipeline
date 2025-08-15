from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import logging
import sys

# Add scripts directory to path
sys.path.append('/scripts')
from fetch_stock_data import fetch_stock_data, update_database

# Airflow logging
logger = logging.getLogger("airflow.task")

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def run_data_pipeline():
    symbols = ["IBM", "AAPL", "GOOGL"]  # Add more symbols if needed
    
    for symbol in symbols:
        logger.info(f"Fetching data for {symbol}")
        data = fetch_stock_data(symbol)
        
        if data:
            success = update_database(data, symbol)
            if success:
                logger.info(f"Database updated successfully for {symbol}")
            else:
                logger.error(f"Failed to update database for {symbol}")
        else:
            logger.warning(f"No data returned from API for {symbol}")

# Define the DAG
with DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetches and stores stock data from API',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stocks'],
) as dag:

    run_pipeline = PythonOperator(
        task_id='run_stock_data_pipeline',
        python_callable=run_data_pipeline
    )

    run_pipeline
