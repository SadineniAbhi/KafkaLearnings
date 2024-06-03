from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# topic must be created through cli
# topic name
topic = 'a'

# list of words mock messages
text_data_to_produce = "hello this program is used to replicate topics".split(" ")

for word in text_data_to_produce:
    producer.produce(topic, value=word)
    producer.poll(1)
