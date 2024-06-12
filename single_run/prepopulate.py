from data_processor.utils import * 
import datetime
import psycopg2
from dotenv import dotenv_values


env = dotenv_values('./.env')

print(env.get('DB_HOST'))
def get_historical_data(token):
    end = datetime.datetime.now()
    start = end - datetime.timedelta(days=90)
    end = end.strftime('%Y-%m-%d')
    start = start.strftime('%Y-%m-%d')
    url = f'https://api.exchange.coinbase.com/products/{token}-USD/candles?start={start}&end={end}&granularity=86400' 
    data = get_json_response(url)
    return data


def save_to_postgres(token, data):
    conn = psycopg2.connect(
        host=env.get('DB_HOST'),
        database=env.get('DB_NAME'),
        user=env.get('DB_USER'),
        password=env.get('DB_PASSWORD'),
        port = env.get('DB_PORT')
    )
    data = [tuple(v) for v in data]
    check_and_create_table(conn, token)
    insert_values(conn, token, data)
    conn.close()

tokens = ['INJ', 'QNT', 'STORJ', 'VELO', 'SOL', 'JTO', 'ICP', 'SHIB', 'AUCTION', 'OCEAN', 'BONK', 'TIME', 'BTC', 'JUP', 'ILV']

for token in tokens:
    data = get_historical_data(token)
    #print(data)
    save_to_postgres(token.lower(), data) 

