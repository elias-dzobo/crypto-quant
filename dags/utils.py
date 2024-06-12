import requests 
from psycopg2 import sql
import logging 


logging.basicConfig()

# Create a session for HTTP requests
session = requests.Session()
session.headers.update({
    'Content-Type': 'application/json',
    'User-Agent': 'Python http.client'
})

def get_json_response(url):
    """Utility function to send GET request and return JSON response."""
    try:
        response = session.get(url)
        logging.info(f"status {response.raise_for_status()}")  # Raises HTTPError for bad responses
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None
    
def check_and_create_table(conn, table_name):
    with conn.cursor() as cur:
        # Check if table exists
        cur.execute(sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """), [table_name])
        table_exists = cur.fetchone()[0]

        # Create table if it doesn't exist
        if not table_exists:
            cur.execute(sql.SQL("""
                CREATE TABLE {} (
                    time TIMESTAMPTZ PRIMARY KEY,
                    low float,
                    high float,
                    open float,
                    close float,
                    volume float
                );
            """).format(sql.Identifier(table_name)))
            conn.commit()
            logging.info(f"Table '{table_name}' created.")
        else:
            logging.info(f"Table '{table_name}' already exists.")

# Function to insert values into the table
def insert_values(conn, table_name, values):
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO {} (time, low, high, open, close, volume) VALUES (to_timestamp(%s) AT TIME ZONE 'UTC', %s, %s, %s, %s, %s) 
        """).format(sql.Identifier(table_name))
        cur.executemany(insert_query, values)
        conn.commit()
        print(f"{cur.rowcount} records inserted into '{table_name}'.")
