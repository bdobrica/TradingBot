#!/usr/bin/env python3
import datetime
import pika # pylint: disable=import-error
from config import app_config # pylint: disable=import-error
from logger import Logger # pylint: disable=import-error
from pathlib import Path
from rabbitmq import Publisher # pylint: disable=import-error

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.level))

# intialize the Rabbit MQ connection
params = pika.ConnectionParameters(host='localhost')
publisher = Publisher(params)
publisher['queue'] = 'database_read'
publisher['routing_key'] = 'database.read'
logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
    queue = publisher['queue'],
    routing_key = publisher['routing_key']
))

# send the Rabbit MQ message
logger.debug('Sending check profit message.')
current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp() * 1000)
#current_stamp = int(datetime.datetime(2020, 11, 20, 19, 00, 00, tzinfo = datetime.timezone.utc).timestamp() * 1000)

publisher.publish({
    'type': 'profit',
    'stamp': current_stamp,
    'params': {}
})
logger.debug('Sent check profit message.')
