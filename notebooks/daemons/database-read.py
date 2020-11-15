#!/usr/bin/env python3
import datetime
import pika
import json
import pandas as pd
from rabbitmq import Subscriber
from rabbitmq import Publisher
from config import app_config
from db import DatabaseSchema, OrderStatus
from os import mkdir
from pathlib import Path

from sqlalchemy import create_engine, MetaData

# connect to the database and create the database schema
meta = MetaData()
db_schema = DatabaseSchema(meta)
engine = create_engine('{db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))
meta.create_all(engine)

# initialize the logger so we see what happens
logging_path = Path(app_config.log.path)
if not app_config.log.path.is_dir():
    try:
        mkdir(app_config.log.path)
    except:
        print('FATAL ERROR: Could not find or create the logging path: {}'.format(str(logging_path)))
        sys.exit()
logging.basicConfig(
    filename = str(logging_path / Path(__file__).stem),
    encoding = 'utf-8',
    level = int(app_config.log.level)
)

class DbSubscriber(Subscriber):
    """
        DbSubscriber extends the Subscriber class to allow message processing,
        reading data from the database and publishing it back to the Rabbit MQ.
    """
    def __setitem__(self, key, value):
        """
            Overloading the [] operator so "publisher" can be set for this object using the brackets.
            
            :param key: The name of the parameter to be set.
            :type key: string
            :param value:
            :type value:
        """
        # set the publisher if the key is "publisher"
        if key == 'publisher':
            self.publisher = value
        # else, just pass the parameters to the original []
        else:
            super().__setitem__(key, value)
    
    def __getitem__(self, key):
        """
            Overloading the [] operator so "publisher" can be get from this object using the brackets.
            
            :param key: The name of the parameter to be get.
            :type key: string
        """
        # get the publisher if the key is "publisher"
        if key == 'publisher':
            return self.publisher
        # else, just pass the parameters to the original []
        else:
            return super().__getitem__(key)
    
    def _send_profits(self):
        """
            Method that retrieves the profits and publishes them to Rabbit MQ.
            To retrieve the profits:
                - the active orders are retrieved. If there are active orders,
                    the profits cannot be computed reliable;
                - the budget is retrieved from the last like of the BUDGET table;
                - the portfolio of symbols is retrieved, grouping by symbols and
                    computing the sum over price times volume
                - the prices are retrieved as the last transacted prices.
            All the data is put to dataframes and sent to requested queue with
            requested.profit routing key.
        """
        # get the current timestamp
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
        # retrieve the PENDING and PARTIAL orders
        orders = pd.read_sql('select\
            count(*) as active_orders\
        from\
            {tables.ORDERS}\
        where\
            stamp <= %(stamp)s and\
            status in %(status)s;'.format(tables = db_schema),
            con = engine,
            params = {
                'stamp': current_stamp,
                'status': [OrderStatus.PENDING, OrderStatus.PARTIAL]
            }
        )
        # retrieve the last budget record
        budget = pd.read_sql('select\
            amount,\
            stamp\
        from\
            {tables.BUDGET}\
        order by\
            stamp desc\
        limit 1;'.format(tables = db_schema),
            con = engine)
        # retrieve the current portfolio
        portfolio = pd.read_sql('select\
            symbol,\
            sum(commission) as commission,\
            sum(price * volume) as value,\
            sum(volume) as volume\
            max(stamp) as stamp\
        from\
            {tables.PORTFOLIO}\
        group by\
            symbol;'.format(tables = db_schema),
            con = engine)
        # retrieve the prices
        prices = pd.read_sql('select\
            A.symbol,\
            A.price,\
            A.stamp\
        from\
            {tables.TRANSACTIONS} A inner join\
            (select\
                symbol,\
                max(stamp) as stamp\
            from\
                {tables.TRANSACTIONS}\
            group by\
                symbol\
            ) B on (A.symbol = B.symbol and A.stamp = B.stamp);'.format(tables = db_schema),
            con = engine)
        # create the message that will be passed back on the Rabbit MQ
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
        # set the routing key for this message
        self.publisher['routing_key'] = 'requested.profit'
        self.publisher.publish(message)
        
    def _send_trends(self, lookahead, lookbehind):
        """
            Method that retrieves the trends and publishes them to Rabbit MQ.
            To retrieve the trends:
                - the active orders are retrieved. If there are active orders,
                    the trends cannot be computed reliable;
                - the budget is retrieved from the last like of the BUDGET table;
                - the transactions are retrieved looking back lookbehind seconds;
            All the data is put to dataframes and sent to requested queue with
            requested.trends routing key.
            :param lookahead: The number of seconds it takes to process an order.
            :type lookahead: int
            :param lookbehind: The number of seconds to look in the transaction history.
            :type lookbehind: int
        """
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
        begin_stamp = current_stamp - (lookbehind + lookahead) * 1000
        end_stamp = current_stamp - lookahead * 1000
        # get the list of PENDING and PARTIAL orders
        orders = pd.read_sql('select\
            count(1) as active_orders\
        from\
            {tables.ORDERS}\
        where\
            stamp <= %(stamp)s and\
            status in %(status)s;'.format(tables = db_schema),
            con = engine,
            params = {
                'stamp': current_stamp,
                'status': [OrderStatus.PENDING, OrderStatus.PARTIAL]
            }
        )
        # get the budget
        budget = pd.read_sql('select\
            amount,\
            stamp\
        from\
            {tables.BUDGET}\
        order by\
            stamp desc\
        limit 1;'.format(tables = db_schema),
            con = engine)
        # get the transaction history
        transactions = pd.read_sql('select\
            id,\
            price,\
            symbol,\
            stamp,\
            volume\
        from\
            {tables.TRANSACTIONS}\
        where\
            stamp >= %(begin)s and\
            stamp < %(end)s;'.format(tables = db_schema),
            con = engine,
            params = {
                'begin': begin_stamp,
                'end': end_stamp
            }
        )
        # create the message that will be pushed back to Rabbit MQ
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
        # set the routing key to requested.trends
        self.publisher['routing_key'] = 'requested.trends'
        self.publisher.publish(message)
    
    def _send_orders(self, lookahead):
        """
            Method that retrieves the active orders and publishes them to Rabbit MQ.
            To retrieve the orders:
                - the active orders are retrieved;
                - the transactions are retrieved looking back lookbehind seconds;
            This way the active orders are matched to transactions.
            
            :param lookahead: The number of seconds it takes to process an order.
            :type lookahead: int
        """
        # first, substract the lookahead time from the current time,
        # cause this is the delay between a order is made and when it is processed
        current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
        order_stamp = current_stamp - lookahead * 1000
        # get the list of PENDING and PARTIAL orders
        orders = pd.read_sql('select\
            id,\
            price,\
            symbol,\
            stamp,\
            volume,\
            status\
        from\
            {tables.ORDERS}\
        where\
            stamp <= %(stamp)s and\
            status in %(status)s;'.format(tables = db_schema),
            con = engine,
            params = {
                'stamp': order_stamp,
                'status': [OrderStatus.PENDING, OrderStatus.PARTIAL]
            }
        )
        begin_stamp = orders['stamp'].min()
        # get the list of transactions that can fulfil the active orders
        transactions = pd.read_sql('select\
            id,\
            price,\
            symbol,\
            stamp,\
            volume\
        from\
            {tables.TRANSACTIONS}\
        where\
            stamp >= %(begin)s;'.format(tables = db_schema),
            con = engine,
            params = {
                'begin': begin_stamp
            }
        )
        message = {
            'stamp': current_stamp,
            'orders': orders.to_dict(),
            'transactions': transactions.to_dict()
        }
        self.publisher['routing_key'] = 'requested.orders'
        self.publisher.publish(message)
    
    def on_message_callback(self, basic_delivery, properties, body):
        """
            Overloading the on_message_callback from the Subscriber class.
            It listens for messages that contain the "type" as key in the messages
            arrived on database queue with database.read routing key.
        """
        body_object = json.loads(body)
        if 'type' not in body_object:
            logging.debug('The type key is not present in the message body.')
            return
        request_type = body_object['type']
        if 'params' in body_object:
            params = body_object['params']
        else:
            params = {} 
        
        # if the type is profit, send the profits
        if request_type == 'profit':
            self._send_profits()
        # if the type is trends, send the trends
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
        # if the type is orders, send the orders
        elif request_type == 'orders':
            if 'lookahead' in params:
                lookahead = params['lookahead']
            else:
                lookahead = 15 * 60
            self._send_orders(lookahead)

# configure the subscriber
params = pika.ConnectionParameters(host='localhost')
subscriber = DbSubscriber(params)
subscriber['queue'] = 'database'
subscriber['routing_key'] = 'database.read'
# bind a publisher to the subscriber
publisher = Publisher(params)
publisher['queue'] = 'requested'
subscriber['publisher'] = publisher

# as this is a script that's intended to be run stand alone, not to be imported
# check whether the script is called directly
if __name__ == '__main__':
    # and make it run continuously, but be aware if CTRL+C is pressed
    try:
        subscriber.run()
    # if it was pressed
    except KeyboardInterrupt:
        logging.debug('Caught SIGINT. Cleaning up.')
        subscriber.stop()