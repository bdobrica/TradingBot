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

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.layers.experimental import preprocessing
import numpy as np

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
        # extract the features
        features = transactions[['stamp', 'volume']].values
        # make the stamps more manageble
        features[:,0] -= min(features[:,0])
        features[:,0] /= 3600 * 1000
        # extract the outputs
        prices = transactions['price'].values
        # use a tensorflow normalizer for the inputs
        normalizer = preprocessing.Normalization(input_shape = [2,])
        normalizer.adapt(features)
        # as models work best with small number, outputs will also be normalized
        mu_price = np.mean(prices)
        sigma_price = np.mean(prices)
        # build a linear regression model
        model = tf.keras.Sequential([
            normalizer,
            layers.Dense(1, activation = 'linear'),
            layers.Lambda(lambda output : output * sigma_price + mu_price)
        ])
        # choose an optimizer and a loss
        model.compile(
            optimizer='adam',
            loss='mean_absolute_error'
        )
        # fit the model
        model.fit(
            features,
            prices,
            validation_split = 0.2,
            epochs = 5,
            verbose = 0
        )
        # evaluate the trend
        predicted = model.predict([
            [features[0,0], 0.0],
            [features[-1,0], 0.0]
        ]).squeeze()
        # compute the slope of the trend
        trend = (predicted[1] - predicted[0]) / (features[-1,0] - features[0,0])
        
        return trend
        
    def _distribute_budget(self, orders, budget):
        amount = budget['amount']
        
    
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
        
        orders = pd.DataFrame(columns = ['symbol', 'volume', 'trend'])
        for symbol in transactions['symbol'].unique():
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