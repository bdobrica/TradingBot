import pika
import json

class Publisher:
    def __init__(self, parameters):
        self.parameters = parameters
        
        self.exchange = 'message'
        self.exchange_type = 'topic'
        self.queue = 'queue'
        self.routing_key = 'queue.text'
        
        self.app_id = 'publisher'
        
        self.reconnect_delay = 5
        self.publish_interval = 1
        
        self._connection = None
        self._channel = None

        self._message = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self._stopping = False
    
    def log(self, *args, **kwargs):
        print('PUBLISHER:', *args, **kwargs)
        pass

    def __setitem__(self, key, value):
        key = key.lower()
        if key == 'exchange':
            self.exchange = value
        elif key == 'exchange_type':
            self.exchange_type = value
        elif key == 'queue':
            self.queue = value
            if self._channel is not None:
                self._channel.queue_declare(
                    queue = self.queue
                )
        elif key == 'routing_key':
            self.routing_key = value
        else:
            raise NotImplementedError('Could not set {} property on object {}.'.format(key, type(self)))
    
    def __getitem__(self, key):
        key = key.lower()
        if key == 'exchange':
            return self.exchange
        elif key == 'exchange_type':
            return self.exchange_type
        elif key == 'queue':
            return self.queue
        elif key == 'routing_key':
            return self.routing_key
        else:
            raise NotImplementedError('Could not find {} property on object {}.'.format(key, type(self)))
    
    def connect(self):
        if self._connection is None:
            self.log('Connecting to {}.'.format(self.parameters))
            self._connection = pika.BlockingConnection(
                self.parameters
            )
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange = self.exchange,
                exchange_type = self.exchange_type
            )
            self._channel.queue_declare(
                queue = self.queue
            )
            #self._channel.queue_bind(
            #    self.queue,
            #    self.exchange,
            #    routing_key = self.routing_key
            #)
            self._channel.confirm_delivery()
    
    def disconnect(self):
        if self._connection is None:
            return
        if self._connection.is_closed:
            self._connection = None
            return
        self._connection.close()
    
    def on_bind_ok(self, _unused_frame):
        self.log('Queue bind to exchange. Activating delivery confirmation.')
        self._channel.confirm_delivery(self.on_delivery_confirmation)
        #self._connection.ioloop.call_later(self.publish_interval, self.publish_callback)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        self.log('Received {} for delivery tag: {}'.format(confirmation_type, method_frame.method.delivery_tag))
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        
        #self._connection.ioloop._poller.deactivate_poller()
        self._connection.ioloop.add_callback(self._connection.ioloop.stop)
    
    def publish(self, message):
        if self._connection is None:
            self.log('No connection found. Trying to connect.')
            self.connect()
        
        self._message = message
        
        self.log('Trying to publish the message {}.'.format(self._message))
        
        headers = {}
        properties = pika.BasicProperties(
            app_id = self.app_id,
            content_type = 'application/json',
            headers = headers
        )
        try:
            self._channel.basic_publish(
                self.exchange,
                self.routing_key,
                json.dumps(self._message, ensure_ascii=False),
                properties
            )
            self.log('Message published successfully.')
        except pika.exceptions.StreamLostError:
            self.log('Found the connection lost. Trying to reconnect.')
            self.disconnect()
            self.connect()
            self._channel.basic_publish(
                self.exchange,
                self.routing_key,
                json.dumps(self._message, ensure_ascii=False),
                properties
            )
            self.log('Message published successfully.')
            
        self._message_number += 1
        self._deliveries.append(self._message_number)
