#!/usr/bin/env python3
import datetime
import json
import numpy as np
import pandas as pd
import pika # pylint: disable=import-error
from config import app_config # pylint: disable=import-error
from db import DatabaseSchema, OrderStatus # pylint: disable=import-error
from logger import Logger # pylint: disable=import-error
from pathlib import Path
from rabbitmq import Subscriber # pylint: disable=import-error
from rabbitmq import Publisher # pylint: disable=import-error
from sqlalchemy import create_engine, MetaData

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.level))

# connect to the database
meta = MetaData()
db_schema = DatabaseSchema(meta)
engine = create_engine('{db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))
meta.create_all(engine)
logger.debug('Connected to the database with URL {db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))

class CheckTrendsPublisher(Publisher):
    def log(self, *args, **kwargs):
        super().log(Path(__file__).stem + ':', *args, **kwargs)

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
            return super().__getitem__(key)
            
    def log(self, *args, **kwargs):
        super().log(Path(__file__).stem + ':', *args, **kwargs)

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
    
    def _make_buy_orders(self, orders):
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp()) * 1000
        orders['stamp'] = orders.shape[0] * [ current_stamp ]
        orders['status'] = orders.shape[0] * [ OrderStatus.PENDING ] 
        orders.drop(columns = ['trend'], inplace = True)
        message = {
            'table_name': DatabaseSchema.ORDERS,
            'table_desc': orders.to_dict()
        }
        self.publisher['routing_key'] = 'database.save'
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
        if orders is None or orders.shape[0] < 1:
            logger.debug('There are no potential orders to distribute budget.')
            return orders
        max_trend = orders['trend'].max()
        orders = orders[orders['trend'] == max_trend].iloc[0:1]
        volume = int(amount / orders['price'].iloc[0])

        logger.debug('Decided to buy symbol {volume} x {symbol} @ {price}.'.format(symbol = orders['symbol'].iloc[0], volume = volume, price = orders['price'].iloc[0]))

        orders['volume'].iloc[0] = -volume
        return orders
    
    def on_message_callback(self, basic_delivery, properties, body):
        # received the check profit message. preprocessing it
        logger.debug('Received check trends message.')
        body_object = json.loads(body)
        if 'stamp' not in body_object:
            logger.warning('The check trends message does not contain a stamp.')
            return
        check_stamp = body_object['stamp']
        if 'active_orders' not in body_object:
            logger.warning('The check trends message does not contain the active orders.')
            return
        active_orders = body_object['active_orders']
        if active_orders > 0:
            logger.debug('There are active orders. Cannot compute accurate trends.')
            return
        if 'budget' not in body_object:
            logger.warning('The check trends message does not contain the budget.')
            return
        budget = body_object['budget']
        if budget['amount'] <= 0:
            logger.warning('The budget is negative: {budget}.'.format(budget = budget['amount']))
            return
        if 'transactions' not in body_object:
            logger.warning('The check trends message does not contain transactions.')
            return
        transactions = pd.DataFrame.from_dict(body_object['transactions'])
        if transactions.shape[0] == 0:
            logger.warning('There are no active transactions that can be used for computing the trends.')
            return
        
        # retrieve the trends threshold
        trend_value, trend_type = self._trend()
        # create a template dataframe for orders
        orders = pd.DataFrame(columns = ['symbol', 'volume', 'price', 'trend'])
        # iterate through the transactions' symbols
        for symbol in transactions['symbol'].unique():
            # extract the transactions only for the current symbol
            symbol_transactions = transactions[transactions['symbol'] == symbol]
            if symbol_transactions.shape[0] < 3:
                logger.debug('For symbol {symbol} there are fewer than 3 transactions. Cannot compute trends. Skipping.'.format(symbol = symbol))
                continue
            # getting the average transaction price (weighted average)
            price = np.dot(
                symbol_transactions['price'].values,
                symbol_transactions['volume'].values
            ) / np.sum(symbol_transactions['volume'].values)
            
            # compute the trends for the symbol
            absolute_trend, relative_trend = self._compute_trend(symbol_transactions)
            logger.debug('The symbol {symbol} has {absolute_trend} / {relative_trend}%.'.format(
                symbol = symbol,
                absolute_trend = absolute_trend,
                relative_trend = relative_trend
                ))
            
            # compare the compute trend with the threshold
            if trend_type == 'fixed' and absolute_trend > trend_value:
                logger.debug('The symbol {symbol} is on a positive absolute trend.'.format(symbol = symbol))
                orders = orders.append({
                    'symbol': symbol,
                    'volume': 0,
                    'price': price,
                    'trend': absolute_trend
                }, ignore_index = True)
            if trend_type == 'percent' and relative_trend > trend_value:
                logger.debug('The symbol {symbol} is on a positive relative trend.'.format(symbol = symbol))
                orders = orders.append({
                    'symbol': symbol,
                    'volume': 0,
                    'price': price,
                    'trend': relative_trend
                }, ignore_index = True)
        
        # distribute the budget among the orders
        logger.debug('Distributing the budget among the orders.')
        orders = self._distribute_budget(orders, budget)
        
        # check if there are still orders
        orders = orders[orders['volume'] < 0]
        if orders.shape[0] > 0:
            logger.debug('There are potential orders. Passing them for fulfilment.')
            self._make_buy_orders(orders)

# initialize the Rabbit MQ connection
params = pika.ConnectionParameters(host='localhost')
subscriber = CheckTrendsSubscriber(params)
subscriber['queue'] = 'requested_trends'
subscriber['routing_key'] = 'requested.trends'
logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
    queue = subscriber['queue'],
    routing_key = subscriber['routing_key']
))
# initializing the Rabbit MQ publisher to reply to requests
publisher = CheckTrendsPublisher(params)
publisher['queue'] = 'database_save'
logger.debug('Setting the publisher queue to {queue}.'.format(queue = publisher['queue']))
# bind the publisher to the subscriber
subscriber['publisher'] = publisher

# as this is a script that's intended to be run stand alone, not to be imported
# check whether the script is called directly
if __name__ == '__main__':
    # and make it run continuously, but be aware if CTRL+C is pressed
    # that's why we daemonize it =)
    logger.debug('Subscribing to Rabbit MQ with a daemon.')
    subscriber.daemonize()
