#!/usr/bin/env python
import pika
from rabbitmq import Publisher

params = pika.ConnectionParameters(host='localhost')
publisher = Publisher(params)
publisher['queue'] = 'database'
publisher['routing_key'] = 'database.read'
publisher.publish({
    'type': 'trends',
    'params': {
        'lookahead': 15 * 60, # 15 minutes estimation
        'lookbehind': 60 * 60 # 1 hour trends
    }
})