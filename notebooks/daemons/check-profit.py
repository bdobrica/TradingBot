#!/usr/bin/env python3
import datetime
import pika # pylint: disable=import-error
import json
import pandas as pd
from config import app_config # pylint: disable=import-error
from db import DatabaseSchema, OrderStatus # pylint: disable=import-error
from logger import Logger # pylint: disable=import-error
from pathlib import Path
from rabbitmq import Subscriber # pylint: disable=import-error
from rabbitmq import Publisher # pylint: disable=import-error
from sqlalchemy import create_engine, MetaData

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.levels))

# connect to the database
meta = MetaData()
db_schema = DatabaseSchema(meta)
engine = create_engine('{db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))
meta.create_all(engine)
logger.debug('Connected to the database with URL {db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))

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
    
    def _margin(self):
        margin_type = 'fixed'
        margin_value = 0.0        
        if isinstance(app_config.sell.margin, str):
            if app_config.sell.margin[-1] == '%':
                try:
                    margin_value = 0.01 * float(app_config.sell.margin[:-1])
                    margin_type = 'percent'
                except:
                    pass
            else:
                try:
                    margin_value = float(app_config.sell.margin)
                except:
                    pass
        elif isinstance(app_config.sell.margin, (int, float)):
            margin_value = float(app_config.sell.margin)
        
        return (
            margin_value,
            margin_type
        )
    
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
        # received the check profit message. preprocessing it
        logger.debug('Received check profit message.')
        body_object = json.loads(body)
        if 'stamp' not in body_object:
            logger.warning('The check profit message does not contain a stamp.')
            return
        check_stamp = body_object['stamp']
        if 'active_orders' not in body_object:
            logger.warning('The check profit message does not contain the active orders.')
            return
        active_orders = body_object['active_orders']
        if active_orders > 0:
            logger.debug('There are active orders. Cannot compute accurate profit.')
            return
        if 'budget' not in body_object:
            logger.warning('The check profit message does not contain the budget.')
            return
        budget = body_object['budget']
        if 'portfolio' not in body_object:
            logger.warning('The check profit message does not contain the portfolio.')
            return
        portfolio = pd.DataFrame.from_dict(body_object['portfolio'])
        if 'prices' not in body_object:
            logger.warning('The check profit message does not contain the prices.')
            return
        prices = pd.DataFrame.from_dict(body_object['prices'])
        if portfolio.shape[0] == 0:
            logger.warning('The portfolio is empty. Nothing to sell to make a profit.')
            return
        
        # retrieve the margin threshold
        margin_value, margin_type = self._margin()
        # create a template dataframe for orders
        orders = pd.DataFrame(columns = ['symbol', 'volume'])
        # iterate through the portfolio
        for _, row in portfolio.iterrows():
            # extract the parameters for each symbol
            symbol, commission, buy_value, volume, buy_stamp = row
            # get the prices for the current symbol
            symbol_data = prices[prices['symbol'] == symbol]
            if symbol_data.shape[0] == 0:
                logger.debug('Could not find recent prices for the symbol {symbol}. Skipping.'.format(symbol = symbol))
                continue
            _, sell_price, sell_stamp = symbol_data
            
            # check if the cooldown passed
            if buy_stamp + int(app_config.sell.cooldown) * 1000 >=  sell_stamp:
                logger.debug('The symbol {symbol} was purchased less than {cooldown} seconds ago. Skipping.'.format(symbol = symbol, cooldown = app_config.sell.cooldown))
                continue
            
            cogs = buy_value + commission
            sales = sell_price * volume
            margin = (sales - cogs) / sales
            
            if (margin_type == 'fixed' and sales - cogs >= margin_value) or \
            (margin_type == 'percent' and margin >= margin_value):
                logger.debug('The symbol {symbol} makes a profit above the set threshold.')
                orders = orders.append({
                    'symbol': symbol,
                    'volume': volume
                }, ignore_index = True)

        if orders.shape[0] > 0:
            logger.debug('Preparing {number} order(s) for processing.'.format(number = orders.shape[0]))
            self._make_sell_order(orders)

# initialize the Rabbit MQ connection
params = pika.ConnectionParameters(host='localhost')
subscriber = CheckProfitSubscriber(params)
subscriber['queue'] = 'requested'
subscriber['routing_key'] = 'requested.profit'
logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
    queue = subscriber['queue'],
    routing_key = subscriber['routing_key']
))
# initializing the Rabbit MQ publisher to reply to requests
publisher = Publisher(params)
publisher['queue'] = 'orders'
logger.debug('Setting the publisher queue to {queue}.'.format(queue = publisher['queue']))
# bind the publisher to the subscriber
subscriber['publisher'] = publisher

# as this is a script that's intended to be run stand alone, not to be imported
# check whether the script is called directly
if __name__ == '__main__':
    # if so, subscribe to the queue and run continuously,
    # but be aware if CTRL+C is pressed
    try:
        logger.debug('Subscribing to the Rabbit MQ.')
        subscriber.run()
    # if it was pressed
    except KeyboardInterrupt:
        logger.debug('Caught SIGINT. Cleaning up.')