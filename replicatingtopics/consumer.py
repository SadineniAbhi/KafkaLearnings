from confluent_kafka import Consumer
consumer_conf = {'bootstrap.servers': 'localhost:9092',
                 'group.id': 'testing_consumer',
                 'auto.offset.reset': 'smallest'}
consumer = Consumer(consumer_conf)


def consume(consumer_object, topics):
    consumer_object.subscribe(topics)
    while True:
        msg = consumer_object.poll(timeout=1)
        if msg is None:
            continue
        if msg.error():
            print("Error: %s" % msg.error())
        else:
            print(msg.value().decode('utf-8'))


try:
    consume(consumer, ['b'])
except KeyboardInterrupt:
    print("stopped consuming")
finally:
    consumer.close()