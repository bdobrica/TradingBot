#!/usr/bin/env python3
import json
import pandas as pd
import pika # pylint: disable=import-error
import websocket
import sys
from config import app_config # pylint: disable=import-error
from db import DatabaseSchema # pylint: disable=import-error
from logger import Logger # pylint: disable=import-error
from pathlib import Path
from rabbitmq import Publisher # pylint: disable=import-error
from time import sleep

class ApiPublisher(Publisher):
    def log(self, *args, **kwargs):
        super().log(Path(__file__).stem + ':', *args, **kwargs)

# create the connection to RabbitMQ
params = pika.ConnectionParameters(host='localhost')
publisher = ApiPublisher(params)
# the queue is "database", the routing key is "database.save"
publisher['queue'] = 'database_save'
publisher['routing_key'] = 'database.save'

# create the template container for transactions
df = pd.DataFrame(columns = ['price', 'symbol', 'stamp', 'volume'])

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.level))

def on_message(ws, message):
    """
        Callback function for when the websocket receives a message.
        Collects the transaction data (price, symbol, stamp and volume)
        and sends them to the database via Rabbit MQ messages. The
        function stores api.buffer transactions before sending them.
        
        :param message: JSON-encoded string that contains a dictionary
            which under the "data" key has a list of transaction-dicts
            with keys price, symbol, stamp and volume
        :type message: string
    """
    global df
    
    # get the JSON data
    json_data = json.loads(message)
    if 'data' not in json_data:
        return

    # iterate through stored transactions
    for item in json_data['data']:
        # append each-one to the dataframe
        df = df.append({
            'price': item['p'],
            'symbol': item['s'],
            'stamp': item['t'],
            'volume': item['v']
        }, ignore_index = True)
    
    # check if the buffer limit has been reached
    if df.shape[0] > int(app_config.api.buffer):
        # if so, prepare a message containing a JSON description of the dataframe
        message = {
            'table_name': DatabaseSchema.TRANSACTIONS,
            'table_desc': df.to_dict()
        }
        # and publish the message
        publisher.publish(message)
        # then clean the dataframe
        df.drop(df.index, inplace=True)

def on_error(ws, error):
    """
        Callback function called by the websocket when an error happens.
        It doesn't do much, just sends buffered transactions (if any) to
        the Rabbit MQ queue.
        
        :param error: The error object.
        :type error:
    """
    logger.error('An error occured while reading data from the websocket.')
    
    # check if the dataframe buffer has any transactions
    if df.shape[0] > 0:
        # if so, prepare a message containing the JSON description of the dataframe
        message = {
            'table_name': DatabaseSchema.TRANSACTIONS,
            'table_desc': df.to_dict()
        }
        # send it to the queue
        publisher.publish(message)
        # and then clean the dataframe
        df.drop(df.index, inplace = True)

def on_close(ws):
    """
        Callback function called by the websocket when the connection is closed.
        It doesn't do much, just sends buffered transactions (if any) to
        the Rabbit MQ queue.
    """
    logger.error('The websocket was closed.')
    
    # check if the dataframe buffer has any transactions
    if df.shape[0] > 0:
        # if so, prepare a message containing the JSON description of the dataframe
        message = {
            'table_name': DatabaseSchema.TRANSACTIONS,
            'table_desc': df.to_dict()
        }
        # send it to the queue
        publisher.publish(message)
        # and then clean the dataframe
        df.drop(df.index, inplace = True)

def on_open(ws):
    """
        Callback function called by the websocket just after opening the connection.
        In this case, it just tells the API which traded symbols to look after.
    """
    logger.debug('Initializing the websocket.')
    
    # first, check that the config symbols path exists
    symbols_path = Path(app_config.symbols.path)
    if not symbols_path.is_dir():
        logger.debug('You need to create the {} directory and populate it with symbols you\'re interested in.'.format(str(app_config.symbols.path)))
        return
    
    # read all the files from the symbols path that should contain a JSON with the symbol name
    symbols = []
    for symbol_file in symbols_path.glob(app_config.symbols.mask):
        symbol_data = None
        with symbol_file.open('r') as fp:
            try:
                symbol_data = json.load(fp)
            except:
                pass
        if symbol_data is None:
            continue
        if 'symbol' not in symbol_data:
            continue
        symbols.append(symbol_data['symbol'])
        
    # if there aren't symbols to watch, return
    if not symbols:
        logger.debug('You need to add at least one symbol file with the correct structure in {}.'.format(str(app_config.symbols.path)))
        return
    
    logger.debug('Watching the following symbols: [{}].'.format(','.join(symbols)))
    
    # else, subscribe the websocket to all the symbols' transactions
    for symbol in symbols:
        ws.send(json.dumps({
            'type': 'subscribe',
            'symbol': symbol
        }))
        
# as this is a script that's intended to be run stand alone, not to be imported
# check whether the script is called directly
if __name__ == '__main__':
    websocket.enableTrace(False)
    # make it run continuously, but be aware if CTRL+C is pressed
    try:
        while True:
            # initialize a websocket client and connect the proper callbacks
            logger.debug('Initializing websocket.')
            ws = websocket.WebSocketApp(
                'wss://ws.finnhub.io?token={api.token}'.format(api = app_config.api),
                on_message = on_message,
                on_error = on_error,
                on_close = on_close)
            ws.on_open = on_open
            # run continuously
            ws.run_forever()
            logger.warning('The websocket client exited. Waiting {respawn} seconds and trying to respawn.'.format(respawn = app_config.api.respawn))
            # but sometimes, the socket can close itself. so clean first
            del ws
            # then wait, and respawn
            sleep(int(app_config.api.respawn))
    # if it was pressed
    except KeyboardInterrupt:
        logger.debug('Caught SIGINT. Cleaning up.')
        
        # and the buffer dataframe has some transactions not sent,
        if df.shape[0] > 0:
            # prepare a message containing the JSON description of the dataframe
            message = {
                'table_name': DatabaseSchema.TRANSACTIONS,
                'table_desc': df.to_dict()
            }
            # send it to the queue to not lose them
            publisher.publish(message)
            # and then clean the dataframe
            df.drop(df.index, inplace = True)
            # and exit
            sys.exit()
