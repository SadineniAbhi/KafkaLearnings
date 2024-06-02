from confluent_kafka import Consumer
import sys

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)


running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=5)
            if msg is None: continue

            if msg.error():
                print("Error: %s" % msg.error())
            else:
               print(msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        shutdown()
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, ['b'])