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

env = dotenv_values('./.env')


def zones(data):
    # Load historical price data
    #data = pd.read_csv(token_data, parse_dates=['time'])
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


def main(table):
    #query = f"SELECT * FROM {table}"
    engine = create_engine('postgres://airflow:airflow@localhost:5432/airflow')
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

        df = pd.read_sql_query(query, engine)
            
        # Close the database connection
        cur.close()

        conn.close()
        
        return df
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None