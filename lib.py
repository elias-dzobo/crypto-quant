import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc
import streamlit as st 


def zones(token_data):
    # Load historical price data
    data = pd.read_csv(token_data, parse_dates=['time'])
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


def main():
    pass 