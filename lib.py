import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc
import streamlit as st 
import psycopg2
from dotenv import dotenv_values
import logging
from sqlalchemy import create_engine
import json 
import datetime 

env = dotenv_values('./.env')

def datetime_to_unix(dt):
    """
    Converts a datetime string in ISO format to a Unix timestamp.

    Args:
    dt (str): The datetime string in ISO 8601 format (e.g., '2024-06-12T00:00:00+00:00').

    Returns:
    int: The Unix timestamp.
    """
    # Parse the datetime string to a datetime object
    #dt_obj = datetime.datetime.fromisoformat(dt)

    # Convert the datetime object to a timestamp
    timestamp = int(dt.timestamp())

    return timestamp

def zones(data):
    # Load historical price data
    #data = pd.read_csv(token_data, parse_dates=['time'])

    #print('here')

    #data['time'] = data['time'].apply(datetime_to_unix)

    data['time'] = pd.to_datetime(data['time']) 
    data.set_index('time', inplace=True)

    # Convert the date to numerical format for matplotlib
    data['time'] = mdates.date2num(data.index.to_pydatetime())

    # Create a new DataFrame for OHLC data
    ohlc = data[['time', 'open', 'high', 'low', 'close']].copy()

    # Function to identify supply and demand zones
    def identify_zones(data, window=10):
        data['Demand_Zone'] = np.nan
        data['Supply_Zone'] = np.nan
        
        for i in range(window, len(data)-window):
            if data['low'][i] == min(data['low'][i-window:i+window]):
                data.at[data.index[i], 'Demand_Zone'] = data['low'][i]
            if data['high'][i] == max(data['high'][i-window:i+window]):
                data.at[data.index[i], 'Supply_Zone'] = data['high'][i]
        
        return data

    # Identify zones
    data = identify_zones(data)

    # Create supply and demand zones
    supply_zones = data[['time', 'Supply_Zone']].dropna()
    demand_zones = data[['time', 'Demand_Zone']].dropna()

    # Plotting
    fig, ax = plt.subplots(figsize=(12, 8))

    # Plot candlestick chart
    candlestick_ohlc(ax, ohlc.values, width=0.6, colorup='green', colordown='red', alpha=0.8)

    # Plot demand zones
    for i in range(len(demand_zones)):
        ax.fill_between(
            [demand_zones['time'].values[i] - 5, demand_zones['time'].values[i] + 5], 
            demand_zones['Demand_Zone'].values[i] * 0.99, 
            demand_zones['Demand_Zone'].values[i] * 1.01, 
            color='cyan', alpha=0.5
        )

    # Plot supply zones
    for i in range(len(supply_zones)):
        ax.fill_between(
            [supply_zones['time'].values[i] - 5, supply_zones['time'].values[i] + 5], 
            supply_zones['Supply_Zone'].values[i] * 0.99, 
            supply_zones['Supply_Zone'].values[i] * 1.01, 
            color='red', alpha=0.5
        )

    # Formatting
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.xticks(rotation=45)
    plt.title('Supply and Demand Zones')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.grid()
    plt.tight_layout()

    # Show plot
    st.pyplot(fig) 

def serialize_datetime(obj): 
    if isinstance(obj, datetime.datetime): 
        return obj.isoformat() 
    raise TypeError("Type not serializable")


def main(table):
    #query = f"SELECT * FROM {table}"
    #engine = create_engine('postgres+psycopg2://airflow:airflow@postgres:5432/airflow')
    try:
        conn = psycopg2.connect(
        host=env.get('DB_HOST'),
        database=env.get('DB_NAME'),
        user=env.get('DB_USER'),
        password=env.get('DB_PASSWORD'),
        port = env.get('DB_PORT')
    )
        cur = conn.cursor()

        check_query = f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name = %s
        """

        cur.execute(check_query, (table,))
        result = cur.fetchall()

        

        if not result:
            raise Exception(f"Table '{table}' does not exist in the database.")

        # Query to select all data from the specified table with schema
        schema, table = result[0]

        
        query = f'SELECT * FROM "{schema}"."{table}"'

        cur.execute(query)
        rows = cur.fetchall()

        # Get column names from the cursor
        colnames = [desc[0] for desc in cur.description]

        # Convert data to a list of dictionaries
        data = [dict(zip(colnames, row)) for row in rows]

        # Convert the list of dictionaries to a JSON object
        #json_data = json.dumps(data, indent=4, default=serialize_datetime)



        # Initialize empty lists for each key
        time_list = []
        low_list = []
        high_list = []
        open_list = []
        close_list = []
        volume_list = []

        # Iterate over the list of dictionaries and collect the values
        for entry in data:
            time_list.append(entry["time"])
            low_list.append(entry["low"])
            high_list.append(entry["high"])
            open_list.append(entry["open"])
            close_list.append(entry["close"])
            volume_list.append(entry["volume"])
            #print(entry)
            



        # Create a new dictionary using the collected lists
        converted_dict = {
            "time": time_list,
            "low": low_list,
            "high": high_list,
            "open": open_list,
            "close": close_list,
            "volume": volume_list
        }

        df = pd.DataFrame(converted_dict)
        # Close the database connection
        cur.close()

        conn.close()
        
        return df
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None