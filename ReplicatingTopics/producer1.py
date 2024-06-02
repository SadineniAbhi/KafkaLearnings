from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)
topic = 'a'
while True:
    s = input()
    producer.produce(topic, value=s)

    producer.poll(1)