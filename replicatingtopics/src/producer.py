from confluent_kafka import Producer
def produce(producer_object:Producer, topic,text_data_to_produce:list):
    for word in text_data_to_produce:
        producer_object.produce(topic, value=word)
        producer_object.poll()


if __name__ == '__main__':
    producer_obj = Producer(
        {'bootstrap.servers': 'localhost:9092',
        'client.id': 'replicator_producer'})
    produce(producer_obj, 'a', ['hello', 'world', 'from', 'producer'])

    