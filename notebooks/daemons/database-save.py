#!/usr/bin/env python3
import datetime
import pika # pylint: disable=import-error
import json
import pandas as pd
import time
import sys
from config import app_config # pylint: disable=import-error
from daemon import Daemon # pylint: disable=import-error
from db import DatabaseSchema # pylint: disable=import-error
from logger import Logger # pylint: disable=import-error
from pathlib import Path
from rabbitmq import Subscriber # pylint: disable=import-error

from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

# adds the word IGNORE after INSERT in sqlalchemy so dataframe insert can be used seamless
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kwords):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kwords)

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.level))

# connect to the database and create the database schema
meta = MetaData()
db_schema = DatabaseSchema(meta)
engine = create_engine('{db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))
meta.create_all(engine)
logger.debug('Connected to the database with URL {db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))

class DbSubscriber(Subscriber):
    """
        DbSubscriber extends the Subscriber class to allow message processing and inserting data into the database.
    """

    def log(self, *args, **kwargs):
        #super().log(Path(__file__).stem + ':', *args, **kwargs)
        pass
    
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
            logger.debug('The message did not contain the table_name key.')
            return
        table_name = body_object['table_name']
        if 'table_desc' not in body_object:
            logger.debug('The message did not contain the table_desc key.')
            return
        table_desc = body_object['table_desc']
        
        # check if the dataframe data is correct and if so convert it to a dataframe
        try:
            df = pd.DataFrame.from_dict(table_desc)
        except:
            logger.debug('The received dataframe data is corrupted and could not be converted to a valid dataframe.')
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
            logger.debug('Could not insert the received data in the database.')

# configure the subscriber
params = pika.ConnectionParameters(host='localhost')
subscriber = DbSubscriber(params)
subscriber['queue'] = 'database_save'
subscriber['routing_key'] = 'database.save'

class DbDaemon(Daemon):
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
    daemon = DbDaemon(
            pidfile = str((chroot / 'run') / pidname),
            chroot = chroot
    )
    if len(sys.argv) >= 2:
        if sys.argv[-1] == 'start':
            daemon.start()
        elif sys.argv[-1] == 'stop':
            daemon.stop()
        elif sys.argv[-1] == 'restart':
            daemon.restart()
        else:
            print('Unknow command {command}.'.format(command = sys.argv[1]))
            sys.exit(2)
        sys.exit(0)
    else:
        print('Usage: {command} start|stop|restart'.format(command = sys.argv[0]))
        sys.exit(0)
