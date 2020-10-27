#!/usr/bin/env python3
import datetime
import pika
import json
import pandas as pd
from rabbitmq import Subscriber
from rabbitmq import Publisher
from config import app_config
from db import mk_schema, OrderStatus

from sqlalchemy import create_engine, MetaData

meta = MetaData()
mk_schema(meta)
engine = create_engine('{db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))
meta.create_all(engine)

class CheckProfitSubscriber(Subscriber):
    def __setitem__(self, key, value):
        if key == 'publisher':
            self.publisher = value
        else:
            super().__setitem__(key, value)
    
    def __getitem__(self, key):
        if key == 'publisher':
            return self.publisher
        else:
            super().__getitem__(key)
    
    def _make_sell_order(self, orders):
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp())
        message = {
            'stamp': current_stamp,
            'type': 'sell',
            'orders': orders.to_dict()
        }
        self.publisher['routing_key'] = 'orders.make'
        self.publisher.publish(message)
    
    def on_message_callback(self, basic_delivery, properties, body):
        body_object = json.loads(body)
        if 'stamp' not in body_object:
            return
        check_stamp = body_object['stamp']
        if 'active_orders' not in body_object:
            return
        active_orders = body_object['active_orders']
        if active_orders > 0:
            return
        if 'budget' not in body_object:
            return
        budget = body_objects['budget']
        if 'portfolio' not in body_objects:
            return
        portfolio = pd.DataFrame.from_dict(body_objects['portfolio'])
        if 'prices' not in body_objects:
            return
        prices = pd.DataFrame.from_dict(body_objects['prices'])
        if portfolio.shape[0] == 0:
            return
        
        orders = pd.DataFrame(columns = ['symbol', 'volume'])
        for _, row in portfolio.iterrows():
            symbol, commission, buy_value, volume, buy_stamp = row
            symbol_data = prices[prices['symbol'] == symbol]
            if symbol_data.shape[0] == 0:
                continue
            _, sell_price, sell_stamp = symbol_data
            
            if buy_stamp + app_config.sell.cooldown >=  sell_stamp:
                continue
            
            cogs = buy_value + commission
            sales = sell_prices * volum
            margin = (sales - cogs) / sales
            
            if margin >= app_config.sell.margin:
                orders = orders.append({
                    'symbol': symbol,
                    'volume': volume
                }, ignore_index = True)

        if orders.shape[0] > 0:
            self._make_sell_order(orders)
                
params = pika.ConnectionParameters(host='localhost')
subscriber = CheckProfitSubscriber(params)
subscriber['queue'] = 'requested'
subscriber['routing_key'] = 'requested.profit'
publisher = Publisher(params)
publisher['queue'] = 'orders'
subscriber['publisher'] = publisher

if __name__ == '__main__':
    subscriber.run()