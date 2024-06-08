from confluent_kafka import Consumer

def consume(consumer_object:Consumer, topics:list)->list:
    messages = []
    consumer_object.subscribe(topics)
    while True:
        msg = consumer_object.poll(timeout=1)
        if msg is None:
            break #stops consuming after 1 second of no messages
        if msg.error():
            print("Error: %s" % msg.error())
        else:
            messages.append(msg.value().decode('utf-8'))
    return messages


if __name__ == "__main__":
    consumer_obj=Consumer({'bootstrap.servers': 'localhost:9092',
                 'group.id': 'replicator_group',
                 'auto.offset.reset': 'smallest'})
    
    print(consume(consumer_obj, ['b']))