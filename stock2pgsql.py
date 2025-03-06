from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_store_stock():
    # Financial Modeling Prep demo API for AAPL stock.
    # This API returns a list with a single quote object.
    api_url = "https://financialmodelingprep.com/api/v3/quote/AAPL?apikey=demo"
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an error for bad responses
    data = response.json()  # Expected to be a list, e.g., [{"symbol": "AAPL", "price": 150.0, ...}]
    if not data:
        raise ValueError("No data returned from API")
    
    # Get the first quote from the returned list.
    quote = data[0]
    symbol = quote.get("symbol", "UNKNOWN")
    price = quote.get("price", 0)
    
    # Connect to PostgreSQL using the internal cluster DNS name.
    conn = psycopg2.connect(
        host="postgres-stock.postgreSQL.svc.cluster.local",
        port=5432,
        database="stockdb",
        user="admin",
        password="admin"
    )
    cur = conn.cursor()
    
    # Create the stock table if it doesn't exist.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            price NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert the stock data.
    cur.execute("INSERT INTO stock (symbol, price) VALUES (%s, %s)", (symbol, price))
    conn.commit()
    
    cur.close()
    conn.close()

with DAG(
    'stock_data_ingestion',
    default_args=default_args,
    description='Fetch stock data from an API and load it into PostgreSQL',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
) as dag:

    task_fetch_and_store = PythonOperator(
        task_id='fetch_and_store_stock',
        python_callable=fetch_and_store_stock
    )

    task_fetch_and_store
