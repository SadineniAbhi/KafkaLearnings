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