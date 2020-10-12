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
        self._reconnecting = False

        self._stopping = False
    
    def log(self, *args, **kwargs):
        print(*args, **kwargs)

    def connect(self):
        if self._connection is None:
            self.log('Connecting to {}.'.format(self.parameters))
            self._connection = pika.SelectConnection(
                self.parameters,
                on_open_callback = self.on_connection_open,
                on_open_error_callback = self.on_connection_open_error,
                on_close_callback = self.on_connection_closed
            )

    def on_connection_open(self, _unused_connection):
        self.log('Connection opened. Opening a channel.')
        self._connection.channel(on_open_callback = self.on_channel_open)
    
    def on_channel_open(self, channel):
        self.log('Channel opened. Declaring exchange.')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.exchange_declare(
            exchange = self.exchange,
            exchange_type = self.exchange_type,
            callback = self.on_exchange_declare_ok
        )
    
    def on_exchange_declare_ok(self, _unused_frame):
        self.log('Exchange declared. Declaring queue.')
        self._channel.queue_declare(
            queue = self.queue,
            callback = self.on_queue_declare_ok
        )
    
    def on_queue_declare_ok(self, _unused_frame):
        self.log('Queue declared. Binding.')
        self._channel.queue_bind(
            self.queue,
            self.exchange,
            routing_key = self.routing_key,
            callback = self.on_bind_ok
        )
    
    def on_bind_ok(self, _unused_frame):
        self.log('Queue bind to exchange. Activating delivery confirmation.')
        self._channel.confirm_delivery(self.on_delivery_confirmation)
        if self._reconnecting:
            self._connection.ioloop.call_later(self.publish_interval, self.publish_callback)
            self._connection.ioloop.start()
            

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
    
    def on_channel_closed(self, channel, reason):
        self.log('Channel closed. Closing the connection.')
        self._channel = None
        if not self._stopping:
            self._connection.close()
    
    def on_connection_open_error(self, _unused_connection, error):
        self.log('Error opening connection: {}. Reconnecting in {} seconds.'.format(error, self.reconnect_delay))
        self._connection.ioloop.call_later(self.reconnect_delay, self._connection.ioloop.stop)
    
    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            self.log('Connection closed, reopening in {} seconds: {}.'.format(self.reconnect_delay, reason))
            self._connection.ioloop.call_later(self.reconnect_delay, self._connection.ioloop.stop)

    def publish(self, message):
        if self._connection is None or (self._connection.is_closing or self._connection.is_closed):
            self.log('The connection is closed. To publish a message, opening.')
            self._connection = None
            self.connect()
        #else:
        #    self._connection.ioloop._poller.activate_poller()
            
        self._message = message
        self._connection.ioloop.call_later(self.publish_interval, self.publish_callback)
        self.log('Starting the ioloop.')
        try:
            self._connection.ioloop.start()
        except pika.exceptions.ConnectionWrongStateError:
            self.log('The connection is closed. To publish a message, opening.')
            self._connection = None
            self._reconnecting = True
            self.connect()
        
    def publish_callback(self):
        if self._channel is None or not self._channel.is_open:
            return
        
        self.log('Publishing the message {}.'.format(self._message))
        
        headers = {}
        properties = pika.BasicProperties(
            app_id = self.app_id,
            content_type = 'application/json',
            headers = headers
        )
        self._channel.basic_publish(
            self.exchange,
            self.routing_key,
            json.dumps(self._message, ensure_ascii=False),
            properties
        )
        self._message_number += 1
        self._deliveries.append(self._message_number)