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

class CheckTrendsSubscriber(Subscriber):
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
    
    def _make_buy_order(self, orders):
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp())
        message = {
            'stamp': current_stamp,
            'type': 'buy',
            'orders': orders.to_dict()
        }
        self.publisher['routing_key'] = 'orders.make'
        self.publisher.publish(message)
    
    def _compute_trend(self, transactions):
        pass
    
    def _distribute_budget(self, orders, budget):
        pass
    
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
        budget = body_object['budget']
        if budget['amount'] <= 0:
            return
        if 'symbols' not in body_object:
            return
        symbols = pd.DataFrame.from_dict(body_object['symbols'])
        if symbols.shape[0] == 0:
            return
        if 'transactions' not in body_object:
            return
        transactions = pd.DataFrame.from_dict(body_object['transactions'])
        if transactions.shape[0] == 0:
            return
        
        orders = pd.DataFrame(columns = ['symbol', 'volume', 'trend'])
        for symbol in symbols.iterrows():
            symbol_transactions = transactions[transactions['symbol'] == symbol]
            trend = self._compute_trend(symbol_transactions)
            orders = orders.append({
                'symbol': symbol,
                'volume': 0,
                'trend': trend
            }, ignore_index = True)
        
        self._distribute_budget(orders, budget)
        orders = orders[orders['volume'] > 0]
        if orders.shape[0] > 0:
            self._make_buy_orders(orders)
            
params = pika.ConnectionParameters(host='localhost')
subscriber = CheckTrendsSubscriber(params)
subscriber['queue'] = 'requested'
subscriber['routing_key'] = 'requested.trends'
publisher = Publisher(params)
publisher['queue'] = 'orders'
subscriber['publisher'] = publisher

if __name__ == '__main__':
    subscriber.run()