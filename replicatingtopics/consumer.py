from confluent_kafka import Consumer
import sys

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# var to stop reading from topic
running = True


# function to handle var running
def shutdown():
    running = False


def basic_consume_loop(consumer_object, topics):
    try:
        consumer_object.subscribe(topics)

        while running:
            msg = consumer_object.poll(timeout=1)
            if msg is None:
                continue

            if msg.error():
                print("Error: %s" % msg.error())
            else:
                print(msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        shutdown()
    finally:

        consumer.close()


basic_consume_loop(consumer, ['b'])
