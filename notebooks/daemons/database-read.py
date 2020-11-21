#!/usr/bin/env python3
import datetime
import pika # pylint: disable=import-error
import json
import pandas as pd
from rabbitmq import Subscriber # pylint: disable=import-error
from rabbitmq import Publisher # pylint: disable=import-error
from config import app_config # pylint: disable=import-error
from db import DatabaseSchema, OrderStatus # pylint: disable=import-error
from logger import Logger # pylint: disable=import-error
from pathlib import Path

from sqlalchemy import create_engine, MetaData

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.level))

# connect to the database and create the database schema
meta = MetaData()
db_schema = DatabaseSchema(meta)
engine = create_engine('{db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))
meta.create_all(engine)
logger.debug('Connected to the database with URL {db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))

class DbPublisher(Publisher):
    def log(self, *args, **kwargs):
        #super().log(Path(__file__).stem + ':', *args, **kwargs)
        pass

class DbSubscriber(Subscriber):
    """
        DbSubscriber extends the Subscriber class to allow message processing,
        reading data from the database and publishing it back to the Rabbit MQ.
    """
    def log(self, *args, **kwargs):
        #super().log(Path(__file__).stem + ':', *args, **kwargs)
        pass

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
                'stamp': self.current_stamp,
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
        # if missing, add the default budget from config
        if budget.shape[0] < 1:
            budget = pd.DataFrame(columns = ['amount', 'stamp', 'time'])
            budget['amount'] = [ float(app_config.broker.budget) ]
            budget['stamp'] = [ self.current_stamp ]
            budget['time'] = [ datetime.datetime.utcfromtimestamp(self.current_stamp // 1000) ]
            budget.to_sql(
                name = db_schema.BUDGET,
                con = engine,
                if_exists = 'append',
                index = False,
                method = 'multi'
            )
        # retrieve the current portfolio
        portfolio = pd.read_sql('select\
            symbol,\
            sum(commission) as commission,\
            -sum(price * volume) as value,\
            -sum(volume) as volume,\
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
            'stamp': self.current_stamp,
            'active_orders': int(orders['active_orders'].iloc[0]),
            'budget': {
                'amount': float(budget['amount'].iloc[0]) if budget.shape[0] > 0 else 0.0,
                'stamp': int(budget['stamp'].iloc[0]) if budget.shape[0] > 0 else self.current_stamp
            },
            'portfolio': portfolio.to_dict(),
            'prices': prices.to_dict()
        }
        # set the routing key for this message
        publisher = DbPublisher(self.parameters)
        publisher['queue'] = 'requested_profit'
        publisher['routing_key'] = 'requested.profit'
        publisher.publish(message)
        publisher.disconnect()
        
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
        begin_stamp = self.current_stamp - (lookbehind + lookahead) * 1000
        end_stamp = self.current_stamp - lookahead * 1000
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
                'stamp': self.current_stamp,
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
        # if missing, add the default budget from config
        if budget.shape[0] < 1:
            budget = pd.DataFrame(columns = ['amount', 'stamp', 'time'])
            budget['amount'] = [ float(app_config.broker.budget) ]
            budget['stamp'] = [ self.current_stamp ]
            budget['time'] = [ datetime.datetime.utcfromtimestamp(self.current_stamp // 1000) ]
            budget.to_sql(
                name = db_schema.BUDGET,
                con = engine,
                if_exists = 'append',
                index = False,
                method = 'multi'
            )
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
            'stamp': self.current_stamp,
            'active_orders': int(orders['active_orders'].iloc[0]),
            'budget': {
                'amount': float(budget['amount'].iloc[0]) if budget.shape[0] > 0 else 0.0,
                'stamp': int(budget['stamp'].iloc[0]) if budget.shape[0] > 0 else self.current_stamp
            },
            'transactions': transactions.to_dict()
        }

        # set the routing key to requested.trends
        publisher = DbPublisher(self.parameters)
        publisher['queue'] = 'requested_trends'
        publisher['routing_key'] = 'requested.trends'
        publisher.publish(message)
        publisher.disconnect()
    
    def on_message_callback(self, basic_delivery, properties, body):
        """
            Overloading the on_message_callback from the Subscriber class.
            It listens for messages that contain the "type" as key in the messages
            arrived on database queue with database.read routing key.
        """
        body_object = json.loads(body)
        if 'type' not in body_object:
            logger.debug('The type key is not present in the message body: {message}.'.format(message = body))
            return
        request_type = body_object['type']
        if 'stamp' not in body_object:
            self.current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
        else:
            self.current_stamp = int(body_object['stamp'])

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

# configure the subscriber
params = pika.ConnectionParameters(host='localhost')
subscriber = DbSubscriber(params)
subscriber['queue'] = 'database_read'
subscriber['routing_key'] = 'database.read'

# as this is a script that's intended to be run stand alone, not to be imported
# check whether the script is called directly
if __name__ == '__main__':
    # and make it run continuously, but be aware if CTRL+C is pressed
    # that's why we daemonize it =)
    logger.debug('Subscribing to Rabbit MQ with a daemon.')
    subscriber.daemonize()
