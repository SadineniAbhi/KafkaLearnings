from confluent_kafka import Producer

producer_conf = {'bootstrap.servers': 'localhost:9092',
                 'client.id': 'main_producer'}
producer = Producer(producer_conf)
# list of words mock messages
text_data_to_produce = "hello this program is used to replicate topics".split(" ")


def produce(producer_object, topic):
    for word in text_data_to_produce:
        producer_object.produce(topic, value=word)
        producer_object.poll()


# !hard coded topic name any ideas on this?
produce(producer, 'a')
