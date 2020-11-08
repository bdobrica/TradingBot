#!/usr/bin/env python3
import datetime
import pika
import json
import pandas as pd
from rabbitmq import Subscriber
from rabbitmq import Publisher
from config import app_config
from db import DatabaseSchema, OrderStatus

from sqlalchemy import create_engine, MetaData

meta = MetaData()
db_schema = DatabaseSchema(meta)
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
    
    def _lock(self):
        self.locked = True
    
    def _unlock(self):
        self.locked = False
    
    def _is_locked(self):
        return hasattr(self, 'locked') and self.locked
    
    def _get_active_orders(self, lookahead):
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
            {tables.PORTFOLIO}\
        where\
            stamp <= %(stamp)s and\
            status in %(status)s;'.format(tables = db_schema),
            con = engine,
            params = {
                'stamp': order_stamp,
                'status': [OrderStatus.PENDING, OrderStatus.PARTIAL]
            }
        )
        transactions = pd.read_sql('select\
            id,\
            price,\
            symbol,\
            stamp,\
            volume\
        from\
            {tables.TRANSACTIONS}\
        where\
            stamp > %(stamp)s;'.format(tables = db_schema),
            con = engine,
            params = {
                'stamp': order_stamp
            }
        )
        used = pd.read_sql('select\
            transaction,\
            sum(volume) as volume\
        from\
            {tables.USED}\
        where\
            stamp > %(stamp)s\
        group by\
            transaction;'.format(tables = db_schema),
            con = engine,
            params = {
                'stamp': order_stamp
            }
        )
        return (
            orders,
            transactions,
            used
        )
    
    def _match_orders_to_transactions(self, orders, transactions, used):
        currently_used = pd.DataFrame(columns = [
            'transaction',
            'stamp',
            'volume'
        ])
        portfolio = pd.DataFrame(columns = [
            'transaction',
            'price',
            'commission',
            'symbol',
            'stamp',
            'volume'
        ])        
        
        transactions.order_by('stamp')
        
        pending_orders = []
        fulfiled_orders = []
        
        for _, order in orders.iterrows():
            symbol = order['symbol']
            initial_volume = order['volume']
            remaining_volume = volume
            
            filtered = transactions[transactions['symbol'] == symbol]
            if filtered.shape[0] < 1:
                continue
            
            for _, transaction in filtered.iterrows():
                unavailable_volume = 0
                if transaction['id'] in used['transaction'].values:
                    unavailable_volume = used[used['transaction'] == transaction['id']]['volume'][0]
                available_volume = transaction['volume'] - unavailable_volume
                if available_volume <= 0:
                    continue
                used_volume = min(available_volume, volume)
                remaining_volume -= used_volume
                
                fulfil_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
                
                currently_used = currently_used.append({
                    'transaction': transaction['id'],
                    'stamp': transaction['stamp'],
                    'volume': used_volume
                }, ignore_index = True)
                
                portfolio = portfolio.append({
                    'transaction': transaction['id'],
                    'price': transaction['price'],
                    'commission': commission,
                    'symbol': symbol,
                    'stamp': fulfil_stamp,
                    'volume': used_volume
                }, ignore_index = True)
                
                if remaining_volume <= 0:
                    break
            if remaining_volume <= 0:
                fulfiled_orders.append(order['id'])
            elif remaining_volume < initial_volume:
                pending_orders.append(order['id'])
                
        return (
            portfolio,
            currently_used,
            pending_orders,
            fulfiled_orders
        )

    def _save_changes(self, portfolio, currently_used, pending_orders, fulfiled_orders):
        portfolio.to_sql(
            name = db_schema.PORTFOLIO,
            con = engine,
            if_exists = 'append',
            index = False,
            method = 'multi'
        )
        currently_used.to_sql(
            name = db_schema.USED,
            con = engine,
            if_exists = 'append',
            index = False,
            method = 'multi'
        )
        db_schema.orders.update()\
            .where(db_schema.orders.c.id in pending_orders)\
            .values(status = OrderStatus.PENDING)
        db_schema.orders.update()\
            .where(db_schema.orders.c.id in fulfiled_orders)\
            .values(status = OrderStatus.FULFILED)
        
    
    def on_message_callback(self, basic_delivery, properties, body):
        body_object = json.loads(body)
        if 'stamp' not in body_object:
            return
        check_stamp = body_object['stamp']
        
        if self._is_locked():
            return
        
        self._lock()
        
        lookahead = 15 * 60;
        orders = self._get_active_orders(lookahead)
        
        if orders[0].shape[0] < 1:
            self._unlock()
            return
        
        matched = self._match_orders_to_transactions(*orders)
        
        if matched[1].shape[0] < 1:
            return
            
        self._save_changes(*matched)
        
        
params = pika.ConnectionParameters(host='localhost')
subscriber = CheckProfitSubscriber(params)
subscriber['queue'] = 'orders'
subscriber['routing_key'] = 'orders.make'
publisher = Publisher(params)
publisher['queue'] = 'database'
subscriber['publisher'] = publisher

if __name__ == '__main__':
    subscriber.run()