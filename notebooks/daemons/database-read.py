#!/usr/bin/env python
import pika
import json
import pandas as pd
from rabbitmq import Subscriber

from sqlalchemy import create_engine, MetaData, Table, Column, Index
from sqlalchemy.types import BigInteger, Float, String, DateTime, Float
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

# adds the word IGNORE after INSERT in sqlalchemy
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kwords):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kwords)

meta = MetaData()
table = Table(
    'transactions', meta, 
    Column('id', BigInteger, primary_key = True),
    Column('price', Float), 
    Column('symbol', String(32)),
    Column('time', DateTime),
    Column('stamp', BigInteger),
    Column('volume', Float)
)
_ = Index('symbol', table.c.symbol)
_ = Index('symbol_stamp', table.c.symbol, table.c.stamp, unique = True)

engine = create_engine('mysql://root:raspberry@localhost/tradingbot')
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
        pass
    def _send_trends(self, lookahead, lookbehind):
        pass
    def _send_orders(self, lookahead):
        pass
    
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
        
if __name__ == '__main__':
    subscriber.run()