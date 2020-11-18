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
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

# adds the word IGNORE after INSERT in sqlalchemy
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kwords):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kwords)

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.levels))

# connect to the database
meta = MetaData()
db_schema = DatabaseSchema(meta)
engine = create_engine('{db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))
meta.create_all(engine)
logger.debug('Connected to the database with URL {db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))

class BrokerSubscriber(Subscriber):
    def _lock(self):
        """
            Prevents other broker subscribers to work on the database.
        """
        self.locked = True
    
    def _unlock(self):
        """
            Removes the restrictions on the database.
        """
        self.locked = False
    
    def _is_locked(self):
        """
            Check if the database is restricted for this broker.
            
            :return: True if the database is restricted, False otherwise.
            :rtype: bool
        """
        return hasattr(self, 'locked') and self.locked
    
    def _commission(self):
        """
            Retrieves and processes the commission found in config file.
            If the commission is a number followed by %, then the commission
            is considered percentage of each transaction. Else, if the
            commission is just a number, then the commission is fixed to
            that value for every transaction.
            
            :return: A tuple with the commission value as a float and the
                commission type as a string (either 'fixed' or 'percent').
            :rtype: tuple
        """
        
        commission_type = 'fixed'
        commission_value = 0.0        
        if isinstance(app_config.broker.commission, str):
            if app_config.broker.commission[-1] == '%':
                try:
                    commission_value = float(app_config.broker.commission[:-1])
                    commission_type = 'percent'
                except:
                    pass
            else:
                try:
                    commission_value = float(app_config.broker.commission)
                except:
                    pass
        elif isinstance(app_config.broker.commission, (int, float)):
            commission_value = float(app_config.broker.commission)
        
        return (
            commission_value,
            commission_type
        )
    
    def _get_active_orders(self, lookahead):
        """
            Gets the necesary elements from the database to allow
            processing of orders. These elements are:
            - the active orders: meaning the ones with a pending or a
                partial status, before a given time (the lookahead time);
            - the transactions ariving after the lookahead time;
            - a list of previously used transactions;
            - the available budget;
            
            :param lookahead: The number of seconds from the current time
                that represents the delay orders are being processed.
                This means that an order that arrived at moment T will be
                processed only after the T + lookahead moment.
            :type lookahead: int
            :return: A tuple containing the orders, the transactions,
                the previosly used transactions and the budget, all as
                dataframes.
            :rtype: tuple
        """
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
        budget = pd.read_sql('select\
            amount,\
            stamp\
        from\
            {tables.BUDGET}\
        order by\
            stamp desc\
        limit 1;'.format(tables = db_schema),
            con = engine
        )
        if budget.shape[0] == 0:
            budget = pd.DataFrame(columns = [
                'amount',
                'stamp'
            ])
            budget = budget.append({
                'amount': float(app_config.broker.budget),
                'stamp': int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
            }, ignore_index = True)
            
        return (
            orders,
            transactions,
            used,
            budget
        )
    
    def _match_orders_to_transactions(self, orders, transactions, used, budget):
        """
            For a dataframe with orders and one with transactions, will
            match each order to one or more transactions to fulfil said order.
            To make sure that a transaction is not used multiple times, the
            used dataframe will keep a record on each transaction matched to
            fulfil an order.
            
            :param orders: A dataframe containing the proposed orders.
            :type orders: pandas.DataFrame
            :param transactions: A dataframe containing the real transactions.
            :type transactions: pandas.DataFrame
            :param used: A dataframe containing the list of already used transactions.
            :type used: pandas.DataFrame
            :param budget: A dataframe with one row, containing the budget amount and stamp.
            :type budget: pandas.DataFrame
            
            :return: 
        """
        
        # create two containers that will hold new data:
        # one for transactions used to fulfil curent orders
        currently_used = pd.DataFrame(columns = [
            'transaction',
            'stamp',
            'volume'
        ])
        # one for the orders fulfiled, that will be added to portfolio
        # in an order, like in portfolio, the sign of the volume gives
        # the type of the transaction: (minus) = buy, (plus) = sell
        portfolio = pd.DataFrame(columns = [
            'transaction',
            'price',
            'commission',
            'symbol',
            'stamp',
            'volume'
        ])
        
        transactions.order_by('stamp')
        
        # retrieve the budget amount
        budget_amount = budget['amount'][0]
        # the variation of the budget amount
        delta_budget = 0
        # the total commission paid to fulfil current orders
        commissions = 0
        # the list of order IDs for orders that will be partially processed
        partial_orders = []
        # the list of order IDs for orders that are completely fulfiled
        fulfiled_orders = []
        
        # prepare the commission
        commission_value, commission_type = self._commission()
        
        # go through each of the orders
        for _, order in orders.iterrows():
            # retrieve the order symbol
            symbol = order['symbol']
            # get the proposed order volume
            initial_volume = order['volume']
            # set the remaining volume as a positive number
            # from it, we'll substract each transaction that
            # we can make
            remaining_volume = abs(initial_volume)
            # get the sign of the transaction
            if initial_volume < 0.0:
                volume_sign = -1.0 # this means buy
            elif initial_volume > 0.0:
                volume_sign = 1.0 # this means sell
            else:
                # if the volume is 0, go to the next order
                continue
            
            # retrieve only the transactions that match current symbol
            filtered = transactions[transactions['symbol'] == symbol]
            # if we have none, go to the next order
            if filtered.shape[0] < 1:
                continue
            
            # for each transaction mathing current symbol
            for _, transaction in filtered.iterrows():
                # check the used dataframe to see if we still have volume that we didn't use to fulfil orders
                unavailable_volume = 0
                if transaction['id'] in used['transaction'].values:
                    unavailable_volume = used[used['transaction'] == transaction['id']]['volume'][0]
                # the available volume is the difference
                available_volume = transaction['volume'] - unavailable_volume
                # if we don't have any unused volume, go to the next transaction
                if available_volume <= 0:
                    continue
                # the volume we can use is the minimum volume between
                # the one that we want to trade and the one that's available
                used_volume = min(available_volume, remaining_volume)
                # compute the remaining volume
                remaining_volume -= used_volume
                
                # get this moment in time
                fulfil_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
                
                # compute the value of the volume traded
                value = transaction['price'] * used_volume
                # and the commission
                commission = 0.0
                if commission_type == 'fixed':
                    commission = commission_value
                elif commission_type == 'percent':
                    commission = 0.01 * commission_value * value
                commissions += commission
                
                # don't let a transaction consume all the budget
                if budget_amount + delta_budget + volume_sign * value - commission < app_config.broker.reserve:
                    # if this happens, the order won't be fulfiled and go to the next order
                    break
                
                # compute the variation in the budget
                delta_budget += volume_sign * value - commission
                
                # add the used transaction to the records
                currently_used = currently_used.append({
                    'transaction': transaction['id'],
                    'stamp': transaction['stamp'],
                    'volume': used_volume
                }, ignore_index = True)
                
                # add this order to the records
                portfolio = portfolio.append({
                    'transaction': transaction['id'],
                    'price': transaction['price'],
                    'commission': commission,
                    'symbol': symbol,
                    'stamp': fulfil_stamp,
                    'volume': volume_sign * used_volume
                }, ignore_index = True)
                
                # check if there's still some leftovers
                if remaining_volume <= 0:
                    # if not, go to the next order
                    break
            
            # mark the order as fulfiled if there's no remaining volume
            # actually, remaining volume cannot be a negative number!
            if remaining_volume <= 0:
                fulfiled_orders.append(order['id'])
            # mark the order as pending if part of it was processed
            elif remaining_volume < initial_volume:
                partial_orders.append(order['id'])
            
            # clear the budget dataframe, so we won't push bad data to the database
            budget = budget.iloc[0:0]
            # if there are transactions made
            if fulfiled_orders or remaining_volume:
                # add a new row to the budget log
                budget = budget.append({
                    'amount': budget_amount + delta_budget,
                    'stamp': int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
                }, ignore_index = True)
                
        return (
            portfolio,
            currently_used,
            partial_orders,
            fulfiled_orders,
            budget
        )

    def _save_changes(self, portfolio, currently_used, partial_orders, fulfiled_orders, budget):
        """
            Save all the changes to the database.
            
            :param portfolio: A dataframe containing the new orders that will be registered in the portfolio.
            :type portfolio: pandas.DataFrame
            :param currently_used: A dataframe containg the transactions used to fulfil the new orders.
            :type currently_used: pandas.DataFrame
            :param partial_orders: A list of order IDs for the orders that were partially processed.
            :type partial_orders: list
            :param fulfiled_orders: A list of order IDs for the orders that are completed.
            :type fulfiled_orders: list
            :param budget: A dataframe with one row with the current budget, after transactions.
            :type budget: pandas.DataFrame
        """
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
        budget.to_sql(
            name = db_schema.BUDGET,
            con = engine,
            if_exists = 'append',
            index = False,
            method = 'multi'
        )
        db_schema.orders.update()\
            .where(db_schema.orders.c.id in partial_orders)\
            .values(status = OrderStatus.PARTIAL)
        db_schema.orders.update()\
            .where(db_schema.orders.c.id in fulfiled_orders)\
            .values(status = OrderStatus.FULFILED)
        
    
    def on_message_callback(self, basic_delivery, properties, body):
        # received the check orders message. preprocessing it
        logger.debug('Received check orders message.')
        body_object = json.loads(body)
        if 'stamp' not in body_object:
            logger.warning('The check orders message does not contain a stamp.')
            return
        check_stamp = body_object['stamp']
        if 'lookahead' not in body_object:
            logger.warning('The check orders message does not contain the lookahead time.')
            return
        lookahead = body_object['lookahead']
        
        if self._is_locked():
            logger.warning('The orders were previously locked. Skipping.')
            return
        
        self._lock()
        logger.debug('The orders are currently locked.')
        
        logger.debug('Retrieving the active orderds.')
        orders = self._get_active_orders(lookahead)
        
        if orders[0].shape[0] < 1:
            logger.debug('No active orders right now. Unlocking orders and skipping.')
            self._unlock()
            return
        
        logger.debug('Matching the active orders to the transactions.')
        matched = self._match_orders_to_transactions(*orders)
        
        if matched[1].shape[0] < 1:
            logger.debug('Could not match any orders to transactions. Unlocking orders and skipping.')
            self._unlock()
            return
            
        logger.debug('Saving changes.')
        self._save_changes(*matched)
        logger.debug('Unlocking orders.')
        self._unlock()
        
# initialize the Rabbit MQ connection
params = pika.ConnectionParameters(host='localhost')
subscriber = BrokerSubscriber(params)
subscriber['queue'] = 'orders'
subscriber['routing_key'] = 'orders.make'
logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
    queue = subscriber['queue'],
    routing_key = subscriber['routing_key']
))

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