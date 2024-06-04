# list of words mock messages
text_data_to_produce = "hello this program is used to replicate topics".split(" ")


def produce(producer_object, topic):
    for word in text_data_to_produce:
        producer_object.produce(topic, value=word)
        producer_object.poll()
