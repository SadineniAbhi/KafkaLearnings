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
