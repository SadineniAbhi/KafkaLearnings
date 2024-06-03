from confluent_kafka import Consumer
from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)
# topic name
topic = 'b'
# variable to stop reading messages
running = True


# function to handle the var running
def shutdown():
    running = False


def basic_consume_loop(consumer_obj, topics):
    try:
        consumer_obj.subscribe(topics)

        while running:
            msg = consumer_obj.poll(timeout=1)
            if msg is None:
                continue

            if msg.error():
                print("Error: %s" % msg.error())
            else:
                producer.produce(topic, value=msg.value().decode('utf-8'))
                producer.poll(1)
    except KeyboardInterrupt:
        shutdown()
    finally:
        consumer.close()


basic_consume_loop(consumer, ['a'])
