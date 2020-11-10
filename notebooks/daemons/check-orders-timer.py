#!/usr/bin/env python3
import pika
from rabbitmq import Publisher

params = pika.ConnectionParameters(host='localhost')
publisher = Publisher(params)
publisher['queue'] = 'broker'
publisher['routing_key'] = 'broker.fulfil'
publisher.publish({
    'type': 'orders',
    'params': {
        'lookahead': 15 * 60 # 15 minutes delay
    }
})