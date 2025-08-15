import os
import requests
import json
import logging
from datetime import datetime
import psycopg2
from psycopg2 import sql
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    # Connect to default Airflow DB first
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("AIRFLOW_DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    conn.autocommit = True
    cur = conn.cursor()
    # Create the stocks database if it doesn't exist
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{os.getenv('DB_NAME')}';")
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {os.getenv('DB_NAME')};")
        logger.info(f"Database {os.getenv('DB_NAME')} created.")
    cur.close()
    conn.close()
    
    # Connect to the stocks DB
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def fetch_stock_data(symbol="IBM", retries=3, wait_sec=15):
    API_KEY = os.getenv("API_KEY")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
            
            if "Note" in data:  # Rate limit warning
                logger.warning(f"API limit reached: {data['Note']}")
                time.sleep(wait_sec)
                continue
            
            return data.get("Time Series (Daily)", {})
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed ({attempt+1}/{retries}): {str(e)}")
            time.sleep(wait_sec)
        except json.JSONDecodeError:
            logger.error("Invalid JSON response")
            time.sleep(wait_sec)
    
    logger.error(f"Failed to fetch data for {symbol} after {retries} attempts")
    return None

def update_database(data, symbol="IBM"):
    if not data:
        logger.warning("No data to process")
        return False

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                symbol VARCHAR(10),
                date DATE PRIMARY KEY,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT
            )
        """)
        
        # Prepare upsert query
        query = sql.SQL("""
            INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """)
        
        for date_str, values in data.items():
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d").date()
                record = (
                    symbol,
                    date,
                    float(values["1. open"]),
                    float(values["2. high"]),
                    float(values["3. low"]),
                    float(values["4. close"]),
                    int(values["5. volume"])
                )
                cur.execute(query, record)
            except (ValueError, KeyError) as e:
                logger.error(f"Data processing error for {date_str}: {str(e)}")
        
        conn.commit()
        logger.info(f"Inserted/updated {len(data)} records")
        return True
        
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
