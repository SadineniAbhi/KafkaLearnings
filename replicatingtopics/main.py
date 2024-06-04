from confluent_kafka import Consumer
from confluent_kafka import Producer
import socket
from producer import produce
from replicator import replicate
from consumer import consume

consumer_conf = {'bootstrap.servers': 'localhost:9092',
                 'group.id': 'foo',
                 'auto.offset.reset': 'smallest'}

producer_conf = {'bootstrap.servers': 'localhost:9092',
                 'client.id': socket.gethostname()}


producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)

if __name__ == '__main__':
    try:
        produce(producer, 'a')
        replicate(consumer, producer, 'a', 'b')
        consume(consumer, 'b')
    except KeyboardInterrupt:
        print('Interrupted')
    finally:
        consumer.close()
