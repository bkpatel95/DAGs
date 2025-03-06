import random
import string
import psycopg2
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_table_and_insert_data():
    # Database connection parameters.
    conn = psycopg2.connect(
        host="postgres-stock.postgreSQL.svc.cluster.local",
        port=5432,
        database="stockdb",
        user="admin",
        password="admin"
    )
    cur = conn.cursor()

    # Generate 10 random column names (each 8 lowercase letters preceded by 'col_').
    columns = [ "col_" + ''.join(random.choices(string.ascii_lowercase, k=8)) for _ in range(10) ]
    print("Generated columns:", columns)

    # Define table name.
    table_name = "random_table"

    # Drop table if it exists (for idempotence).
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Build the CREATE TABLE statement.
    col_defs = ", ".join([ f"{col} INTEGER" for col in columns ])
    create_table_sql = f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {col_defs});"
    cur.execute(create_table_sql)
    conn.commit()
    print("Table created.")

    # Prepare for batch insertion.
    total_rows = 1_000_000
    batch_size = 10_000
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"
    print("Beginning data insertion...")

    # Insert rows in batches.
    for start in range(0, total_rows, batch_size):
        batch_data = []
        for _ in range(batch_size):
            # Generate a tuple of 10 random digits (0-9).
            row = tuple(random.randint(0, 9) for _ in range(10))
            batch_data.append(row)
        cur.executemany(insert_sql, batch_data)
        conn.commit()
        print(f"Inserted rows {start+1} to {start+len(batch_data)}")
    
    cur.close()
    conn.close()
    print("Data insertion complete.")

with DAG(
    'create_random_table',
    default_args=default_args,
    description='Create a PostgreSQL table with random columns and insert one million rows of random digits',
    schedule_interval=None,  # Run on demand
    catchup=False,
) as dag:

    task_create_and_insert = PythonOperator(
        task_id='create_table_and_insert_data',
        python_callable=create_table_and_insert_data
    )

    task_create_and_insert
