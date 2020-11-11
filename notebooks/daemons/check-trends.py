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
import numpy as np

meta = MetaData()
db_schema = DatabaseSchema(meta)
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
            
    def _trend(self):
        trend_type = 'fixed'
        trend_value = 0.0        
        if isinstance(app_config.buy.trend, str):
            if app_config.buy.trend[-1] == '%':
                try:
                    trend_value = 0.01 * float(app_config.buy.trend[:-1])
                    trend_type = 'percent'
                except:
                    pass
            else:
                try:
                    trend_value = float(app_config.buy.trend)
                except:
                    pass
        elif isinstance(app_config.buy.trend, (int, float)):
            trend_value = float(app_config.buy.trend)
        
        return (
            trend_value,
            trend_type
        )
    
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
        # extract the features
        features = transactions[['stamp', 'volume']].values
        # make the stamps more manageble
        features[:,0] -= min(features[:,0])
        features[:,0] /= 3600 * 1000
        # extract the outputs
        prices = transactions['price'].values
        X = np.concatenate([np.ones((features.shape[0],1)), features], axis = 1)
        y = prices
        # compute the linear regression parameters from the normal form
        theta = np.dot(
            np.linalg.pinv(np.dot(X.transpose(), X)),
            np.dot(X.transpose(), y)
        )
        # compute the edge prices based on model
        predicted = np.dot(
            np.array([[ 1, features[ 0,0], 1],
                      [ 1, features[-1,0], 1]]),
            theta)
        # compute the trend as the margin on item = (sales - cogs) / sales
        absolute_trend = predicted[1] - predicted[0]
        relative_trend = (predicted[1] - predicted[0]) / predicted[1]

        return (
            absolute_trend,
            relative_trend
        )
        
    def _distribute_budget(self, orders, budget):
        amount = budget['amount']
        if orders.shape[0]:
            return
        max_trend = orders['trend'].max()
        orders = orders[orders['trend'] == max_trend]
        orders = orders.iloc[0:1]
        volume = int(amount / orders['price'][0])

        orders.drop(columns = ['price', 'trend'], inplace = True)
        orders['price'][0] = -volume
        
        return orders
    
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
        if 'transactions' not in body_object:
            return
        transactions = pd.DataFrame.from_dict(body_object['transactions'])
        if transactions.shape[0] == 0:
            return
        
        trend_value, trend_type = self._trend()
        
        orders = pd.DataFrame(columns = ['symbol', 'volume', 'trend'])
        for symbol in transactions['symbol'].unique():
            symbol_transactions = transactions[transactions['symbol'] == symbol]
            if symbol_transactions.shape[0] < 3:
                continue
            
            price = np.dot(
                symbol_transactions['price'].values,
                symbol_transactions['volume'].values
            ) / np.sum(symbol_transactions['volume'].values)
            
            absolute_trend, relative_trend = self._compute_trend(symbol_transactions)
            
            if trend_type == 'fixed' and absolute_trend > trend_value:
                orders = orders.append({
                    'symbol': symbol,
                    'volume': 0,
                    'price': price,
                    'trend': absolute_trend
                }, ignore_index = True)
            if trend_type == 'percent' and relative_trend > trend_value:
                orders = orders.append({
                    'symbol': symbol,
                    'volume': 0,
                    'price': price,
                    'trend': relative_trend
                }, ignore_index = True)
        
        orders = self._distribute_budget(orders, budget)
        orders = orders[orders['volume'] < 0]
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