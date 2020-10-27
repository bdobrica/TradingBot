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

class OrdersSubscriber(Subscriber):
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
    
    def _make_sell_order(self, symbols):
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
        budget = body_objects['budget']
        if 'portfolio' not in body_objects:
            return
        portfolio = pd.DataFrame.from_dict(body_objects['portfolio'])
        if 'prices' not in body_objects:
            return
        prices = pd.DataFrame.from_dict(body_objects['prices'])
        
params = pika.ConnectionParameters(host='localhost')
subscriber = CheckProfitSubscriber(params)
subscriber['queue'] = 'orders'
subscriber['routing_key'] = 'orders.make'
publisher = Publisher(params)
publisher['queue'] = 'database'
subscriber['publisher'] = publisher

if __name__ == '__main__':
    subscriber.run()