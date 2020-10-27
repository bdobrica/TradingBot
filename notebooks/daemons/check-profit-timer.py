#!/usr/bin/env python3
import pika
from rabbitmq import Publisher

params = pika.ConnectionParameters(host='localhost')
publisher = Publisher(params)
publisher['queue'] = 'database'
publisher['routing_key'] = 'database.read'
publisher.publish({
    'type': 'profit',
    'params': {}
})