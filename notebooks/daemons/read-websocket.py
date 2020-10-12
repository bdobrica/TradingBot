#!/usr/bin/env python3
import json
import pandas as pd
import pika
import websocket
from config import app_config
from rabbitmq import Publisher

params = pika.ConnectionParameters(host='localhost')
publisher = Publisher(params)
publisher['queue'] = 'database'
publisher['routing_key'] = 'database.save'

df = pd.DataFrame(columns = ['price', 'symbol', 'stamp', 'volume'])

def on_message(ws, message):
    global df
    json_data = json.loads(message)
    for item in json_data['data']:
        df = df.append({
            'price': item['p'],
            'symbol': item['s'],
            'stamp': item['t'],
            'volume': item['v']
        }, ignore_index = True)
        
    if df.shape[0] > 100:
        message = {
            'table_name': 'transactions',
            'table_desc': df.to_dict()
        }
        publisher.publish(message)
        df.drop(df.index, inplace=True)

def on_error(ws, error):
    if df.shape[0] > 0:
        message = {
            'table_name': 'transactions',
            'table_desc': df.to_dict()
        }
        publisher.publish(message)
        df.drop(df.index, inplace=True)

def on_close(ws):
    if df.shape[0] > 0:
        message = {
            'table_name': 'transactions',
            'table_desc': df.to_dict()
        }
        publisher.publish(message)
        df.drop(df.index, inplace=True)

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

if __name__ == '__main__':
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        'wss://ws.finnhub.io?token={api.token}'.format(api = app_config.api),
        on_message = on_message,
        on_error = on_error,
        on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
