from confluent_kafka import Consumer
from confluent_kafka import Producer

consumer_conf = {'bootstrap.servers': 'localhost:9092',
                 'group.id': 'foo',
                 'auto.offset.reset': 'smallest'}

producer_conf = {'bootstrap.servers': 'localhost:9092',
                 'client.id': 'replicator_producer'}

producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)


def replicate(consumer_obj, producer_obj, from_topic, to_topic):
    consumer_obj.subscribe(from_topic)
    while True:
        msg = consumer_obj.poll(timeout=1)
        if msg is None:
            continue
        elif msg.error():
            print("Error: %s" % msg.error())
        else:
            producer_obj.produce(to_topic, value=msg.value().decode('utf-8'))
            producer_obj.poll(1)


try:
    replicate(consumer, producer, 'a', 'b')
except KeyboardInterrupt:
    print("stopped consuming")
finally:
    consumer.close()
