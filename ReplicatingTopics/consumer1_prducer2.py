from confluent_kafka import Consumer
import sys

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)
topic = 'b'


running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=5.0)
            if msg is None: continue

            if msg.error():
                print("Error: %s" % msg.error())
            else:
               producer.produce(topic, value=msg.value().decode('utf-8'))
               producer.poll(1)
    except KeyboardInterrupt:
        shutdown()
    finally:
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, ['a'])