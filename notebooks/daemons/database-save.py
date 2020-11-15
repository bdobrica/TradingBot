#!/usr/bin/env python3
import datetime
import logging
import pika
import json
import pandas as pd
from rabbitmq import Subscriber
from config import app_config
from db import DatabaseSchema
from os import mkdir
from pathlib import Path

from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

# adds the word IGNORE after INSERT in sqlalchemy so dataframe insert can be used seamless
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kwords):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kwords)

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
        DbSubscriber extends the Subscriber class to allow message processing and inserting data into the database.
    """
    
    def on_message_callback(self, basic_delivery, properties, body):
        """
            The callback called when a message is received from the Rabbit MQ.
            It searches for the "table_name" (string) and "table_desc" (JSON encoded dataframe) and if they are present,
            will save to the database the information stored in the dataframe.
            
            :param basic_delivery:
            :type basic_delivery:
            :param properties:
            :type properties:
            :param body: JSON-encoded message string
            :type body: string
        """
        # check if the message contains table_name and table_description
        body_object = json.loads(body)
        if 'table_name' not in body_object:
            logging.debug('The message did not contain the table_name key.')
            return
        table_name = body_object['table_name']
        if 'table_desc' not in body_object:
            logging.debug('The message did not contain the table_desc key.')
            return
        table_desc = body_object['table_desc']
        
        # check if the dataframe data is correct and if so convert it to a dataframe
        try:
            df = pd.DataFrame.from_dict(table_desc)
        except:
            logging.debug('The received dataframe data is corrupted and could not be converted to a valid dataframe.')
            return
        
        # check if there's a stamp column, but not a time column and if so, create the time column
        if 'stamp' in df.columns and 'time' not in df.columns:
            time_col = df['stamp'].apply(lambda stamp : datetime.datetime.utcfromtimestamp(stamp // 1000))
            time_pos = df.columns.get_loc('stamp') + 1
            df.insert(time_pos, column = 'time', value = time_col)
        
        # try to insert the data into the database
        try:
            df.to_sql(
                name = table_name,
                con = engine,
                if_exists = 'append',
                index = False,
                method = 'multi'
            )
        except:
            logging.debug('Could not insert the received data in the database.')

# configure the subscriber
params = pika.ConnectionParameters(host='localhost')
subscriber = DbSubscriber(params)
subscriber['queue'] = 'database'
subscriber['routing_key'] = 'database.save'

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