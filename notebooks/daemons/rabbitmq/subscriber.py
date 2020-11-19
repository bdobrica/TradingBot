#!/usr/bin/env python
import pika
import time

class Subscriber:
    def __init__(self, parameters):
        self.parameters = parameters
        
        self.exchange = 'message'
        self.exchange_type = 'topic'
        self.queue = 'queue'
        self.routing_key = 'queue.text'
        
        self.should_reconnect = False
        self.was_consuming = False
        
        self.prefetch_count = 1
        self.max_reconnect_delay = 30
        
        self._connection = None
        self._channel = None
        self._closing = False
        self._consuming = False
        self._subscriber_tag = None
    
    def log(self, *args, **kwargs):
        print(*args, **kwargs)
        pass
    
    def __setitem__(self, key, value):
        key = key.lower()
        if key == 'exchange':
            self.exchange = value
        elif key == 'exchange_type':
            self.exchange_type = value
        elif key == 'queue':
            self.queue = value
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
            self.log('Connecting using {}.'.format(self.parameters))
            
            self._connection = pika.SelectConnection(
                parameters = self.parameters,
                on_open_callback = self.on_connection_open,
                on_open_error_callback = self.on_connection_open_error,
                on_close_callback = self.on_connection_closed
            )
        else:
            self.log('There is an active connection {}.'.format(self._connection))
    
    def disconnect(self):
        self._consuming = False
        if self._connection is None:
            self.log('There is no active connection.')
        elif self._connection.is_closing or self._connection.is_closed:
            self.log('The connection is closing or already closed.')
        else:
            self.log('Closing the connection {}.'.format(self._connection))
            self._connection.close()
    
    def reconnect(self):
        self.should_reconnect = True
        self.stop()
    
    def on_connection_open(self, _unused_connection):
        self.log('Connection opened. Opening a channel.')
        self._connection.channel(on_open_callback = self.on_channel_open)
    
    def on_channel_open(self, channel):
        self.log('Channel opened. Declaring an exchange.')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.exchange_declare(
            exchange = self.exchange,
            exchange_type = self.exchange_type,
            callback = self.on_exchange_declare_ok
        )
    
    def on_channel_closed(self, channel, reason):
        self.log('Channel closed. Disconnecting.')
        self.disconnect()
    
    def on_connection_open_error(self, _unused_connection, error):
        self.log('Connection failed with error {}.'.format(error))
        self.reconnect()
    
    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.log('Connection closed, reconnect necessary: {}.'.format(reason))
            self.reconnect()
    
    def on_exchange_declare_ok(self, _unused_frame):
        self.log('Exchange declared succeded. Declaring a queue.')
        self._channel.queue_declare(queue = self.queue, callback = self.on_queue_declare_ok)
    
    def on_cancel_ok(self, _unused_frame):
        self._consuming = False
        self.log('RabbitMQ acknowledged the cancellation of the subscriber: {}.'.format(self._subscriber_tag))
        self._channel.close()
    
    def on_queue_declare_ok(self, _unused_frame):
        self.log('Queue declared succeded. Binding the queue to the exchange.')
        self._channel.queue_bind(self.queue, self.exchange, routing_key = self.routing_key, callback = self.on_bind_ok)
    
    def on_bind_ok(self, _unused_frame):
        self.log('Binding succeded. Adding QOS.')
        self._channel.basic_qos(prefetch_count = self.prefetch_count, callback = self.on_basic_qos_ok)
    
    def on_basic_qos_ok(self, _unused_frame):
        self.log('Adding QOS succeded. Set to {}.'.format(self.prefetch_count))
        self._channel.add_on_cancel_callback(self.on_subscriber_cancelled)
        self._subscriber_tag = self._channel.basic_consume(self.queue, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def on_subscriber_cancelled(self, method_frame):
        self.log('Subscriber was canceled remotely. Shutting down: {}.'.format(method_frame))
        if self._channel:
            self._channel.close()
            
    def on_message_callback(self, basic_delivery, properties, body):
        self.log('You should overload the on_message_callback callback.')
    
    def on_message(self, _unused_channel, basic_delivery, properties, body):
        self.log('Received message # {} from {}: {}'.format(
            basic_delivery.delivery_tag,
            properties.app_id,
            body
        ))
        self.on_message_callback(basic_delivery, properties, body)
        self._channel.basic_ack(basic_delivery.delivery_tag)

    def run(self):
        self.log('Starting the subscriber.')
        self.connect()
        self._connection.ioloop.start()
    
    def stop(self):
        if not self._closing:
            self._closing = True
            self.log('Stopping the subscriber.')
            if self._consuming:
                if self._channel:
                    self.log('Sending a Basic.Cancel RPC command to RabbitMQ server.')
                    self._channel.basic_cancel(self._subscriber_tag, self.on_cancel_ok)
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            self.log('Subscriber stopped.')
                
    def daemonize(self):
        while True:
            try:
                self.run()
            except KeyboardInterrupt:
                self.stop()
                break
            if self.should_reconnect:
                self.stop()
                self.get_reconnect_delay()
                time.sleep(self._reconnect_delay)
                
    def get_reconnect_delay(self):
        if self.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        self._reconnect_delay = min(self._reconnect_delay, self.max_reconnect_delay)
