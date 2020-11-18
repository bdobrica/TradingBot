#!/usr/bin/env python3
import pika # pylint: disable=import-error
from config import app_config # pylint: disable=import-error
from logger import Logger # pylint: disable=import-error
from pathlib import Path
from rabbitmq import Publisher # pylint: disable=import-error

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.levels))

# intialize the Rabbit MQ connection
params = pika.ConnectionParameters(host='localhost')
publisher = Publisher(params)
publisher['queue'] = 'database'
publisher['routing_key'] = 'database.read'
logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
    queue = publisher['queue'],
    routing_key = publisher['routing_key']
))

# send the Rabbit MQ message
logger.debug('Sending check trends message.')
publisher.publish({
    'type': 'trends',
    'params': {
        'lookahead': int(app_config.orders.lookahead),
        'lookbehind': int(app_config.orders.lookbehind)
    }
})
logger.debug('Sent check trends message.')