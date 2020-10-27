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

class DbSubscriber(Subscriber):
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
    
    def _send_profits(self):
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
        orders = pd.read_sql('select\
            count(*) as active_orders\
        from\
            orders\
        where\
            stamp <= %(stamp)s and\
            status in %(status)s;',
            con = engine,
            params = {
                'stamp': current_stamp,
                'status': [OrderStatus.PENDING, OrderStatus.PARTIAL]
            }
        )
        budget = pd.read_sql('select\
            amount,\
            stamp\
        from\
            budget\
        order by\
            stamp desc\
        limit 1;', con = engine)
        portfolio = pd.read_sql('select\
            symbol,\
            sum(commission) as commission,\
            sum(price * volume) as value,\
            sum(volume) as volume\
            max(stamp) as stamp\
        from\
            portfolio\
        group by\
            symbol;',
            con = engine)
        prices = pd.read_sql('select\
            A.symbol,\
            A.price,\
            A.stamp\
        from\
            transactions A inner join\
            (select\
                symbol,\
                max(stamp) as stamp\
            from\
                transactions\
            group by\
                symbol\
            ) B on (A.symbol = B.symbol and A.stamp = B.stamp);',
            con = engine)
        message = {
            'stamp': current_stamp,
            'active_orders': orders['active_orders'][0],
            'budget': {
                'amount': budget['amount'][0],
                'stamp': budget['stamp'][0]
            },
            'portfolio': portfolio.to_dict(),
            'prices': prices.to_dict()
        }
        self.publisher['routing_key'] = 'requested.profit'
        self.publisher.publish(message)
        
    def _send_trends(self, lookahead, lookbehind):
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
        begin_stamp = current_stamp - lookbehind * 1000
        end_stamp = current_stamp - lookahead * 1000
        orders = pd.read_sql('select\
            count(1) as active_orders\
        from\
            orders\
        where\
            stamp <= %(stamp)s and\
            status in %(status)s;',
            con = engine,
            params = {
                'stamp': current_stamp,
                'status': [OrderStatus.PENDING, OrderStatus.PARTIAL]
            }
        )
        budget = pd.read_sql('select\
            amount,\
            stamp\
        from\
            budget\
        order by\
            stamp desc\
        limit 1;', con = engine)
        symbols = pd.read_sql('select\
            symbol\
        from\
            transactions\
        where\
            stamp >= %(begin)s and\
            stamp < %(end)s\
        group by\
            symbol;',
            con = engine,
            params = {
                'begin': begin_stamp,
                'end': end_stamp
            }
        )
        transactions = pd.read_sql('select\
            id,\
            price,\
            symbol,\
            stamp,\
            volume\
        from\
            transactions\
        where\
            stamp >= %(begin)s and\
            stamp < %(end)s;',
            con = engine,
            params = {
                'begin': begin_stamp,
                'end': end_stamp
            }
        )
        message = {
            'stamp': current_stamp,
            'active_orders': orders['active_orders'][0],
            'budget': {
                'amount': budget['amount'][0],
                'stamp': budget['stamp'][0]
            },
            'symbols': symbols.to_dict(),
            'transactions': transactions.to_dict()
        }
        self.publisher['routing_key'] = 'requested.trends'
        self.publisher.publish(message)
    
    def _send_orders(self, lookahead):
        # first, substract the lookahead time from the current time,
        # cause this is the delay between a order is made and when it is processed
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
        order_stamp = current_stamp - lookahead * 1000
        orders = pd.read_sql('select\
            id,\
            price,\
            symbol,\
            stamp,\
            volume,\
            status\
        from\
            orders\
        where\
            stamp <= %(stamp)s and\
            status in %(status)s;',
            con = engine,
            params = {
                'stamp': order_stamp,
                'status': [OrderStatus.PENDING, OrderStatus.PARTIAL]
            }
        )
        message = {
            'stamp': current_stamp,
            'orders': orders.to_dict()
        }
        self.publisher['routing_key'] = 'requested.orders'
        self.publisher.publish(message)
    
    def on_message_callback(self, basic_delivery, properties, body):
        body_object = json.loads(body)
        if 'type' not in body_object:
            return
        request_type = body_object['type']
        if 'params' in body_object:
            params = body_object['params']
        else:
            params = {} 
        
        if request_type == 'profit':
            self._send_profits()
        elif request_type == 'trends':
            if 'lookahead' in params:
                lookahead = params['lookahead']
            else:
                lookahead = 15 * 60
            if 'lookbehind' in params:
                lookbehind = params['lookbehind']
            else:
                lookbehind = 60 * 60
            self._send_trends(lookahead, lookbehind)
        elif request_type == 'orders':
            if 'lookahead' in params:
                lookahead = params['lookahead']
            else:
                lookahead = 15 * 60
            self._send_orders(lookahead)

params = pika.ConnectionParameters(host='localhost')
subscriber = DbSubscriber(params)
subscriber['queue'] = 'database'
subscriber['routing_key'] = 'database.read'
publisher = Publisher(params)
publisher['queue'] = 'requested'
subscriber['publisher'] = publisher

if __name__ == '__main__':
    subscriber.run()