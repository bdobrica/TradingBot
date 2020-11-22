#!/usr/bin/env python3
import datetime
import pika # pylint: disable=import-error
import json
import pandas as pd
import sys
from config import app_config # pylint: disable=import-error
from daemon import Daemon # pylint: disable=import-error
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

class CheckProfitPublisher(Publisher):
    def log(self, *args, **kwargs):
        #super().log(Path(__file__).stem + ':', *args, **kwargs)
        pass

class CheckProfitSubscriber(Subscriber):
    def log(self, *args, **kwargs):
        #super().log(Path(__file__).stem + ':', *args, **kwargs)
        pass

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
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
        
        orders['stamp'] = orders.shape[0] * [ current_stamp ]
        orders['status'] = orders.shape[0] * [ OrderStatus.PENDING ] 
        message = {
            'table_name': DatabaseSchema.ORDERS,
            'table_desc': orders.to_dict()
        }

        publisher = CheckProfitPublisher(self.parameters)
        publisher['queue'] = 'database_save'
        publisher['routing_key'] = 'database.save'
        publisher.publish(message)
        publisher.disconnect()
    
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
        if int(active_orders) > 0:
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
        orders = pd.DataFrame(columns = ['symbol', 'volume', 'price'])
        # iterate through the portfolio
        for _, row in portfolio.iterrows():
            # extract the parameters for each symbol
            symbol, commission, buy_value, volume, buy_stamp = row
            # get the prices for the current symbol
            symbol_data = prices[prices['symbol'] == symbol]
            if symbol_data.shape[0] == 0:
                logger.debug('Could not find recent prices for the symbol {symbol}. Skipping.'.format(symbol = symbol))
                continue
            _, sell_price, sell_stamp = symbol_data.iloc[0]
           
            # check if the cooldown passed
            if int(buy_stamp) + int(app_config.sell.cooldown) * 1000 >=  int(sell_stamp):
                logger.debug('The symbol {symbol} was purchased less than {cooldown} seconds ago. Skipping.'.format(symbol = symbol, cooldown = app_config.sell.cooldown))
                continue
            
            cogs = float(buy_value) + float(commission)
            sales = float(sell_price) * float(volume)
            margin = (sales - cogs) / sales

            logger.debug('The symbol {symbol} makes a profit of {margin}%.'.format(
                symbol = symbol,
                margin = int(margin * 10000) * 0.01
            ))
            
            if (margin_type == 'fixed' and sales - cogs >= margin_value) or \
            (margin_type == 'percent' and margin >= margin_value):
                logger.debug('The symbol {symbol} makes a profit above the set threshold.')
                orders = orders.append({
                    'symbol': symbol,
                    'volume': volume,
                    'price': sell_price
                }, ignore_index = True)

        if orders.shape[0] > 0:
            logger.debug('Preparing {number} order(s) for processing.'.format(number = orders.shape[0]))
            self._make_sell_order(orders)

# initialize the Rabbit MQ connection
params = pika.ConnectionParameters(host='localhost')
subscriber = CheckProfitSubscriber(params)
subscriber['queue'] = 'requested_profit'
subscriber['routing_key'] = 'requested.profit'
logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
    queue = subscriber['queue'],
    routing_key = subscriber['routing_key']
))

class CheckProfitDaemon(Daemon):
    def atexit(self):
        subscriber.stop()
        super().atexit()

    def run(self):
        logger.debug('Subscribing to Rabbit MQ with a daemon.')
        while True:
            subscriber.run()
            if subscriber.should_reconnect:
                logger.debug('Trying to reconnect. First, clean up.')
                subscriber.stop()
                reconnect_delay = subscriber.get_reconnect_delay()
                logger.debug('Awaiting for {seconds} seconds before restarting.'.format(seconds = self._reconnect_delay))
                time.sleep(reconnect_delay)

# as this is a script that's intended to be run stand alone, not to be imported
# check whether the script is called directly
if __name__ == '__main__':
    chroot = Path(__file__).absolute().parent
    pidname = Path(__file__).stem + '.pid'
    daemon = CheckProfitDaemon(
            pidfile = str((chroot / 'run') / pidname),
            chroot = chroot
    )
    if len(sys.argv) == 2:
        if sys.argv[1] == 'start':
            daemon.start()
        elif sys.argv[1] == 'stop':
            daemon.stop()
        elif sys.argv[1] == 'restart':
            daemon.restart()
        else:
            print('Unknow command {command}.'.format(command = sys.argv[1]))
            sys.exit(2)
        sys.exit(0)
    else:
        print('Usage: {command} start|stop|restart'.format(command = sys.argv[0]))
        sys.exit(0)
