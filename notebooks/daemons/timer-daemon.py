#!/usr/bin/env python3
import datetime
import pika # pylint: disable=import-error
import sys
from config import app_config # pylint: disable=import-error
from logger import Logger # pylint: disable=import-error
from pathlib import Path
from rabbitmq import Publisher # pylint: disable=import-error

TIMER_STATES = [
    'trends',
    'orders',
    'profit',
    'orders'
]

# initialize the logger so we see what happens
logger_path = Path(app_config.log.path)
logger = Logger(path = logger_path / Path(__file__).stem, level = int(app_config.log.level))

# intialize the Rabbit MQ connection
params = pika.ConnectionParameters(host='localhost')
publisher = Publisher(params)

current_stamp = int(datetime.datetime.now(tz = datetime.timezone.utc).timestamp()) * 1000
#current_stamp = int(datetime.datetime(2020, 11, 20, 19, 20, 00, tzinfo = datetime.timezone.utc).timestamp() * 1000)

# send the Rabbit MQ message
logger.debug('Sending check orders message.')
publisher.publish({
    'stamp': current_stamp,
    'lookahead': int(app_config.orders.lookahead)
})
logger.debug('Sent check orders message.')

if __name__ == '__main__':
    chroot = Path(__file__).absolute().parent
    state_file_name = Path(__file__).stem + '.state'
    state_file_path = chroot / state_file_name
    state_no = 0

    # get the current state
    try:
        with open(state_file_path, 'r') as fp:
            state_no = int(fp.read().strip())
    except IOError:
        pass
    
    next_state_no = (state_no + 1) % len(TIMER_STATES)

    # write the next state
    try:
        with open(state_file_path, 'w') as fp:
            fp.write(str(next_state_no))
    except IOError:
        logger.error('Cannot write next state to state file {state_file}.'.format(
            state_file = state_file_path.as_posix()
        ))
        sys.exit(1)
        
    
    # prefered to use a list with states to make things more verbose
    state = TIMER_STATES[state_no]

    # so, if the state is trends, actually do the same thing as check-trends-timer.py
    if state == 'trends':
        publisher['queue'] = 'database_read'
        publisher['routing_key'] = 'database.read'
        logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
            queue = publisher['queue'],
            routing_key = publisher['routing_key']
        ))

        # send the Rabbit MQ message
        logger.debug('Sending check trends message.')

        publisher.publish({
            'type': 'trends',
            'stamp': current_stamp,
            'params': {
                'lookahead': int(app_config.orders.lookahead),
                'lookbehind': int(app_config.orders.lookbehind)
            }
        })
        logger.debug('Sent check trends message.')

    # if the state is profit, do the same thing as check-profit-timer.py
    elif state == 'profit':
        publisher['queue'] = 'database_read'
        publisher['routing_key'] = 'database.read'
        logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
            queue = publisher['queue'],
            routing_key = publisher['routing_key']
        ))

        # send the Rabbit MQ message
        logger.debug('Sending check profit message.')

        publisher.publish({
            'type': 'profit',
            'stamp': current_stamp,
            'params': {}
        })
        logger.debug('Sent check profit message.')

    # if the state is orders, do the same thing as check-orders-timer.py
    elif state == 'orders':
        publisher['queue'] = 'orders'
        publisher['routing_key'] = 'orders.make'
        logger.debug('Initialized the Rabbit MQ connection: queue = {queue} / routing key = {routing_key}.'.format(
            queue = publisher['queue'],
            routing_key = publisher['routing_key']
        ))

        # send the Rabbit MQ message
        logger.debug('Sending check orders message.')

        publisher.publish({
            'stamp': current_stamp,
            'lookahead': int(app_config.orders.lookahead)
        })
        logger.debug('Sent check orders message.')